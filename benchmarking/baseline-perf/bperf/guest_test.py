# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for guest.py: --guest's value and files, and the template copies."""

import json
import os
import struct
import tempfile
import unittest
from unittest import mock

# Plain imports, not google3 ones: these run with plain python3.
from bperf import common
from bperf import guest


def _elf(path, machine):
  with open(path, "wb") as f:
    f.write(b"\x7fELF\x02\x01" + b"\0" * 12 + struct.pack("<H", machine))
    f.write(b"\0" * 600)


class ParseTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.dir = tempfile.mkdtemp()
    _elf(os.path.join(self.dir, "vmlinux"), 62)
    with open(os.path.join(self.dir, "rootfs.img"), "wb") as f:
      f.write(b"x" * 100)

  def test_directory(self):
    files, config = guest.parse(self.dir)
    self.assertEqual(config, "")
    self.assertEqual(
        files,
        {
            "kernel": os.path.join(self.dir, "vmlinux"),
            "rootfs": os.path.join(self.dir, "rootfs.img"),
        },
    )

  def test_key_value(self):
    files, config = guest.parse(f"kernel={self.dir}/vmlinux, rootfs=/x/r.img")
    self.assertEqual(config, "")
    self.assertEqual(
        files, {"kernel": f"{self.dir}/vmlinux", "rootfs": "/x/r.img"}
    )

  def test_sandbox_config_name(self):
    self.assertEqual(guest.parse("microvm"), ({}, "microvm"))

  def test_bad_values(self):
    for bad in (
        "bogus=/x",  # unknown key
        os.path.join(self.dir, "vmlinux"),  # a file without its key
        tempfile.mkdtemp(),  # a directory without guest files
    ):
      with self.assertRaises(common.Error, msg=bad):
        guest.parse(bad)


class CheckAssetTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.dir = tempfile.mkdtemp()

  def _path(self, name):
    return os.path.join(self.dir, name)

  def test_good_files(self):
    _elf(self._path("vmlinux"), 62)
    with open(self._path("rootfs.img"), "wb") as f:
      f.write(b"x" * 100)
    guest.check_asset("kernel", self._path("vmlinux"), "amd64")
    guest.check_asset("rootfs", self._path("rootfs.img"), "amd64")

  def test_bad_files(self):
    bz = bytearray(0x300)
    bz[0x202:0x206] = b"HdrS"
    with open(self._path("bzImage"), "wb") as f:
      f.write(bz)
    _elf(self._path("arm"), 183)
    open(self._path("empty"), "wb").close()
    for key, name in (
        ("kernel", "bzImage"),  # not the uncompressed vmlinux
        ("kernel", "arm"),  # wrong architecture
        ("rootfs", "empty"),
        ("kernel", "missing"),
    ):
      with self.assertRaises(common.Error, msg=name):
        guest.check_asset(key, self._path(name), "amd64")


class GuestTest(unittest.TestCase):

  def test_started(self):
    self.assertFalse(guest.Guest().started())
    self.assertTrue(guest.Guest(files=True).started())
    self.assertTrue(guest.Guest(config="microvm").started())


class _Proc:
  returncode, stdout, stderr = 0, "", ""


class _Kube:
  """Records kubectl-ate calls; the template is there until it's deleted.

  Its golden snapshot is ready from the golden_at-th look at the template on
  (never if 0), and its one worker logged worker_log.
  """

  def __init__(self, exists=True, golden_at=0, worker_log=()):
    self.calls, self.exists, self.looks = [], exists, 0
    self.golden_at, self.worker_log = golden_at, worker_log

  def ate(self, *args, **_):
    self.calls.append(args)
    if args[:2] == ("delete", "actor-template"):
      self.exists = False
    return _Proc()

  def get_template(self, name):
    if not self.exists:
      return {}, "not found"
    self.looks += 1
    tmpl = {"metadata": {"name": name, "uid": "u-1"}}
    if self.golden_at and self.looks >= self.golden_at:
      tmpl["status"] = {"goldenSnapshotStatus": {"goldenSnapshot": "gs://g"}}
    return tmpl, ""

  def running_pods(self, namespace, selector):
    del namespace  # unused
    worker = {"metadata": {"name": "worker-1", "namespace": "w"}}
    return ([] if selector == "app=atelet" else [worker]), None

  def kubectl(self, *_, **__):
    proc = _Proc()
    proc.stdout = "\n".join(self.worker_log)
    return proc


class DeleteTemplateTest(unittest.TestCase):

  GOLDEN = ("delete", "actor", "u-1", "-a", "ate-golden", "--any-state")

  def test_delete_template_deletes_its_golden_actor_after_it(self):
    kube = _Kube()
    self.assertTrue(
        guest.delete_template(
            kube, {"metadata": {"name": "t-copy", "uid": "u-1"}}
        )
    )
    self.assertEqual(
        kube.calls,
        [
            ("delete", "actor-template", "t-copy", "-a", common.NAMESPACE),
            self.GOLDEN,
        ],
    )

  def test_drop_template_deletes_a_leftover_and_its_golden_actor(self):
    kube = _Kube()
    guest._drop_template(kube, {"metadata": {"name": "t-copy"}})
    self.assertEqual(kube.calls[-1], self.GOLDEN)
    kube = _Kube(exists=False)
    guest._drop_template(kube, {"metadata": {"name": "t-copy"}})
    self.assertEqual(kube.calls, [])


def _failed_run(template):
  """What a worker logs when RunWorkload fails for template's golden actor."""
  return json.dumps({
      "level": "INFO",
      "msg": "Handle RPC",
      "method": "/ateom.Ateom/RunWorkload",
      "req": {"actor_template_name": template},
      "err": "dial unix credential-broker.sock: no such file or directory",
  })


class CreateTemplateTest(unittest.TestCase):

  MANIFEST = {"metadata": {"name": "t-copy"}}

  def setUp(self):
    super().setUp()
    for patcher in (
        mock.patch.object(guest, "FAILURE_CHECK_S", 0),
        mock.patch.object(guest.time, "sleep"),
        mock.patch.object(common, "log"),
    ):
      patcher.start()
      self.addCleanup(patcher.stop)

  def test_a_call_that_keeps_failing_ends_the_wait(self):
    kube = _Kube(worker_log=[_failed_run("t-copy")] * 3)
    with self.assertRaises(common.Error) as raised:
      guest._create_template(kube, self.MANIFEST, 0, "arm A")
    self.assertEqual(kube.looks, 1)
    self.assertEqual(
        str(raised.exception),
        "the golden snapshot on arm A can't start: the same call failed 3"
        " times\n      ateom: RunWorkload: dial unix credential-broker.sock:"
        " no such file or directory",
    )

  def test_fewer_failures_wait_for_the_golden_snapshot(self):
    kube = _Kube(golden_at=2, worker_log=[_failed_run("t-copy")] * 2)
    tmpl = guest._create_template(kube, self.MANIFEST, 0, "arm A")
    self.assertEqual(kube.looks, 2)
    self.assertIn("goldenSnapshot", tmpl["status"]["goldenSnapshotStatus"])


if __name__ == "__main__":
  unittest.main()
