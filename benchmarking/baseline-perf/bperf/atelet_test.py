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

"""Tests for atelet.py: the DaemonSet patches, that Swap puts everything back in a safe order, the node flush."""

import contextlib
import copy
import io
import itertools
import json
import unittest

# Plain imports, not google3 ones: these run with plain python3.
from bperf import atelet
from bperf import common
from bperf import logs

STRATEGY = {
    "type": "RollingUpdate",
    "rollingUpdate": {"maxSurge": 0, "maxUnavailable": 1},
}


def _ds(args=("--x=1",), annotations=None):
  containers = [
      {"name": "sidecar", "image": "s"},
      {"name": "atelet", "image": "stock", "args": list(args)},
  ]
  return {
      "metadata": {"name": "atelet-ds", "annotations": dict(annotations or {})},
      "spec": {
          "updateStrategy": copy.deepcopy(STRATEGY),
          "template": {"spec": {"containers": containers}},
      },
  }


class PatchTest(unittest.TestCase):

  def test_split_source(self):
    self.assertIsNone(atelet.split_source("gcr.io/p/atelet:v1@sha256:662323fd"))
    self.assertIsNone(atelet.split_source("gcr.io/p/atelet:dev"))
    self.assertEqual(atelet.split_source("./substrate"), ("./substrate", ""))
    self.assertEqual(
        atelet.split_source("/src/substrate@main"), ("/src/substrate", "main")
    )
    self.assertEqual(
        atelet.split_source("substarte@main"), ("substarte", "main")
    )
    with self.assertRaises(common.Error):
      atelet.split_source("./substrate@")
    with self.assertRaisesRegex(common.Error, "is not a directory"):
      atelet.resolve_image("substarte@main", "A")

  def test_with_cache_dir(self):
    want = ["--a=1", "--image-cache-dir=/c"]
    self.assertEqual(atelet.with_cache_dir(["--a=1"], "/c"), want)
    self.assertEqual(
        atelet.with_cache_dir(["--image-cache-dir=/old", "--a=1"], "/c"), want
    )
    self.assertEqual(
        atelet.with_cache_dir(["--image-cache-dir", "/old", "--a=1"], "/c"),
        want,
    )

  def test_cache_dir(self):
    self.assertEqual(
        atelet.cache_dir("A"), "/var/lib/ate/image-cache-bperf-a"
    )

  def test_stock_comes_from_the_annotation_once_there_is_one(self):
    ds = _ds()
    want = {"image": "stock", "args": ["--x=1"], "updateStrategy": STRATEGY}
    self.assertEqual(atelet.stock(ds), want)
    changed = _ds(
        args=["--y"], annotations={atelet.ANNOTATION: json.dumps(want)}
    )
    changed["spec"]["template"]["spec"]["containers"][1]["image"] = "test"
    self.assertEqual(atelet.stock(changed), want)

  def test_template_patch(self):
    path = "/spec/template/spec/containers/1"
    self.assertEqual(
        atelet.template_patch(_ds(), "img", ["--a"]),
        [
            {"op": "test", "path": f"{path}/name", "value": "atelet"},
            {"op": "replace", "path": f"{path}/image", "value": "img"},
            {"op": "add", "path": f"{path}/args", "value": ["--a"]},
        ],
    )
    ds = _ds()
    ds["spec"]["template"]["spec"]["containers"].pop()
    with self.assertRaises(common.Error):
      atelet.template_patch(ds, "img", [])

  def test_runs_and_ready(self):
    pod = {
        "spec": _ds()["spec"]["template"]["spec"],
        "status": {"conditions": [{"type": "Ready", "status": "True"}]},
    }
    self.assertTrue(atelet.runs(pod, "stock", ["--x=1"]))
    self.assertFalse(
        atelet.runs(pod, "stock", ["--x=1", "--image-cache-dir=/c"])
    )
    self.assertFalse(atelet.runs(pod, "other", ["--x=1"]))
    self.assertTrue(atelet.ready(pod))
    self.assertFalse(
        atelet.ready(
            {"status": {"conditions": [{"type": "Ready", "status": "False"}]}}
        )
    )


class _Proc:
  returncode, stdout, stderr = 0, "", ""


class _FakeKube:
  """Just enough of kube.Kube for Swap: a DaemonSet on two nodes whose deleted pods come back from its template."""

  def __init__(self):
    self.ds = _ds()
    self.uids = itertools.count()
    self.pods = {node: self._pod(node) for node in ("n1", "n2")}
    self.log = []  # (what, detail), in order

  def _pod(self, node):
    return {
        "metadata": {
            "name": f"atelet-{node}-{next(self.uids)}",
            "uid": str(next(self.uids)),
            "ownerReferences": [{"kind": "DaemonSet", "name": "atelet-ds"}],
        },
        "spec": dict(
            copy.deepcopy(self.ds["spec"]["template"]["spec"]), nodeName=node
        ),
        "status": {"conditions": [{"type": "Ready", "status": "True"}]},
    }

  def get_json(self, *args):
    if "daemonset" in args:
      return copy.deepcopy(self.ds)
    if "daemonsets" in args:
      return {"items": [copy.deepcopy(self.ds)]}
    node = next(
        (a.split("=", 1)[1] for a in args if a.startswith("spec.nodeName=")),
        None,
    )
    return {
        "items": [
            copy.deepcopy(p) for n, p in self.pods.items() if node in (None, n)
        ]
    }

  def kubectl(self, *args, **_):
    verb = args[2]
    if verb == "patch":
      for op in json.loads(args[-1]):
        if op["op"] == "test":
          continue
        keys = op["path"].strip("/").split("/")
        target = self.ds
        for key in keys[:-1]:
          target = target[int(key)] if isinstance(target, list) else target[key]
        target[keys[-1]] = op["value"]
        self.log.append(("patch", op["path"]))
    elif verb == "annotate":
      key, _, value = args[-1].partition("=")
      if key.endswith("-"):
        self.ds["metadata"]["annotations"].pop(key[:-1], None)
      else:
        self.ds["metadata"]["annotations"][key] = value
      self.log.append(("annotate", key))
    elif verb == "delete":
      node = next(
          n for n, p in self.pods.items() if p["metadata"]["name"] == args[4]
      )
      self.pods[node] = self._pod(
          node
      )  # the DaemonSet recreates it from its template
      self.log.append(("delete", node))
    return _Proc()


class SwapTest(unittest.TestCase):

  def test_use_changes_one_node_and_restore_puts_everything_back(self):
    kube = _FakeKube()
    before = copy.deepcopy(kube.ds)
    swap = atelet.Swap(kube, "atelet-ds")
    swap.use("n1", "test-image", "/cache-a")
    self.assertEqual(kube.ds["spec"]["updateStrategy"], {"type": "OnDelete"})
    self.assertTrue(
        atelet.runs(
            kube.pods["n1"],
            "test-image",
            ["--x=1", "--image-cache-dir=/cache-a"],
        )
    )
    self.assertTrue(
        atelet.runs(kube.pods["n2"], "stock", ["--x=1"])
    )  # the other node is untouched
    self.assertEqual(
        json.loads(kube.ds["metadata"]["annotations"][atelet.ANNOTATION])[
            "image"
        ],
        "stock",
    )

    kube.log.clear()
    swap.restore()
    self.assertEqual(kube.ds, before)
    self.assertTrue(atelet.runs(kube.pods["n1"], "stock", ["--x=1"]))
    # The template goes back before the strategy: RollingUpdate on the test
    # template would roll every node.
    whats = [what for what, _ in kube.log]
    self.assertLess(
        kube.log.index(("patch", "/spec/template/spec/containers/1/image")),
        kube.log.index(("patch", "/spec/updateStrategy")),
    )
    self.assertEqual(whats[-1], "annotate")
    self.assertEqual([d for w, d in kube.log if w == "delete"], ["n1"])

  def test_restore_after_a_killed_run(self):
    kube = _FakeKube()
    before = copy.deepcopy(kube.ds)
    atelet.Swap(kube, "atelet-ds").use(
        "n2", "test-image", "/cache-b"
    )  # and then the run is killed
    swap = atelet.Swap(kube, "atelet-ds")
    self.assertTrue(swap.leftover)
    self.assertEqual(swap.stock["image"], "stock")
    swap.restore()
    self.assertEqual(kube.ds, before)
    self.assertTrue(atelet.runs(kube.pods["n2"], "stock", ["--x=1"]))
    self.assertEqual(atelet.changed_daemonsets(kube), [])

  def test_restore_without_changes_does_nothing(self):
    kube = _FakeKube()
    atelet.Swap(kube, "atelet-ds").restore()
    self.assertEqual(kube.log, [])

  def test_restore_keeps_annotation_when_replace_fails(self):
    kube = _FakeKube()
    swap = atelet.Swap(kube, "atelet-ds")
    swap.use("n1", "test-image", "/cache-a")
    orig_kubectl = kube.kubectl

    def fail_on_delete(*args, **kwargs):
      if args[2] == "delete":
        raise common.Error("connection reset")
      return orig_kubectl(*args, **kwargs)

    kube.kubectl = fail_on_delete
    with self.assertRaisesRegex(common.Error, "connection reset"):
      swap.restore()
    self.assertTrue(swap.changed)
    self.assertEqual(atelet.changed_daemonsets(kube), ["atelet-ds"])


class FlushTest(unittest.TestCase):

  def test_dirty_mb(self):
    meminfo = (
        "MemTotal: 1584569956 kB\nDirty: 2900000 kB\nWriteback: 100000"
        " kB\nWritebackTmp: 7 kB\n"
    )
    self.assertEqual(atelet.dirty_mb(meminfo), 3000.0)
    self.assertIsNone(atelet.dirty_mb("MemTotal: 1 kB\n"))

  def test_flush_node_syncs_in_the_worker_pod(self):
    calls = []

    class Kube:

      def kubectl(self, *args, **_):
        calls.append(args)
        proc = _Proc()
        proc.stdout = "Dirty: 2048000 kB\nWriteback: 0 kB\n"
        return proc

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
      atelet.flush_node(Kube(), {"workerNamespace": "ns", "workerPod": "w-1"})
    self.assertEqual(
        calls[0][:6], ("-n", "ns", "exec", "w-1", "-c", logs.WORKER_CONTAINER)
    )
    self.assertTrue(calls[0][-1].endswith("sync"))
    self.assertIn("2,048 MB", out.getvalue())


if __name__ == "__main__":
  unittest.main()
