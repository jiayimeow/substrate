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

"""Tests for the runner pod's spec and the bootstrap that ships runner.py."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

# Plain imports, not google3 ones: these run with plain python3.
from bperf import common
from bperf import pod
from bperf import sampler


class PodTest(unittest.TestCase):

  def test_runner_pod_command(self):
    spec = pod.runner_pod("img", False, ("Pause",), "tmpl-x")
    container = spec["spec"]["containers"][0]
    self.assertEqual(
        container["command"][:4], ["python3", "-u", "-c", pod.BOOTSTRAP]
    )
    self.assertEqual(
        container["command"][4:], ["--parks=Pause", "--template=tmpl-x"]
    )
    keep = pod.runner_pod("img", True, ("Pause", "Suspend"), "t")
    self.assertEqual(
        keep["spec"]["containers"][0]["command"][4:],
        ["--keep", "--parks=Pause,Suspend", "--template=t"],
    )
    env = {e["name"]: e.get("value") for e in container["env"]}
    self.assertEqual(
        set(json.loads(env[pod.SOURCES_ENV])),
        {"bperf/common.py", "bperf/runner.py"},
    )

  def test_capacity_step_runner_pod(self):
    spec = pod.runner_pod(
        "img", False, ("Suspend",), "t", actors=8, duration_s=60
    )
    container = spec["spec"]["containers"][0]
    self.assertEqual(
        container["command"][4:],
        ["--parks=Suspend", "--template=t", "--actors=8", "--duration=60"],
    )
    self.assertEqual(container["resources"]["requests"]["cpu"], "1")
    one = pod.runner_pod("img", False, ("Suspend",), "t")["spec"]
    self.assertEqual(
        one["containers"][0]["resources"]["requests"]["cpu"], "250m"
    )

  def test_bootstrap_runs_shipped_runner(self):
    sources = {
        "bperf/runner.py": (
            "import sys\ndef main(argv):\n  print(argv)\n  return 3\n"
        )
    }
    env = dict(os.environ, **{pod.SOURCES_ENV: json.dumps(sources)})
    proc = subprocess.run(
        [sys.executable, "-c", pod.BOOTSTRAP, "--keep", "--parks=Pause"],
        env=env,
        capture_output=True,
        text=True,
        cwd=tempfile.gettempdir(),
        check=False,
    )
    self.assertEqual(proc.returncode, 3, proc.stderr)
    self.assertEqual(proc.stdout.strip(), "['--keep', '--parks=Pause']")

  def test_shipped_modules_import_alone(self):
    # The pod gets only these files, and has no grpc until runner.main().
    root = tempfile.mkdtemp()
    os.makedirs(os.path.join(root, "bperf"))
    sources = json.loads(
        pod.runner_pod("i", False, ("Pause",), "t")["spec"]["containers"][0][
            "env"
        ][-1]["value"]
    )
    for name, source in sources.items():
      with open(os.path.join(root, name), "w", encoding="utf-8") as f:
        f.write(source)
    code = "from bperf import runner; print(runner.TARGET_ACTOR)"
    proc = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            f"import sys; sys.path.insert(0, {root!r}); {code}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    self.assertEqual(proc.returncode, 0, proc.stderr)
    self.assertEqual(proc.stdout.strip(), "benchmark-workloads/perf-eval-actor")

  def test_sampler_pod_runs_shipped_sampler_on_the_node(self):
    spec = pod.sampler_pod("img", "node-1")
    self.assertEqual(spec["spec"]["nodeName"], "node-1")
    container = spec["spec"]["containers"][0]
    self.assertEqual(
        container["command"][:4], ["python3", "-u", "-c", pod.SAMPLER_BOOTSTRAP]
    )
    env = {e["name"]: e["value"] for e in container["env"]}
    # -I: nothing but the standard library and the shipped source.
    proc = subprocess.run(
        [sys.executable, "-I", *container["command"][1:], "--samples=1"],
        env=dict(os.environ, **{pod.SAMPLER_ENV: env[pod.SAMPLER_ENV]}),
        capture_output=True,
        text=True,
        cwd=tempfile.gettempdir(),
        check=False,
    )
    self.assertEqual(proc.returncode, 0, proc.stderr)
    self.assertEqual(len(sampler.parse(proc.stdout.splitlines())), 1)

  def test_create_raises_when_pod_fails_without_top_level_reason(self):
    class FakeKube:

      def kubectl(self, *args, **kwargs):
        del args, kwargs

      def get_json(self, *args):
        del args
        return {
            "status": {
                "phase": "Failed",
                "containerStatuses": [{
                    "state": {
                        "terminated": {
                            "exitCode": 1,
                            "reason": "Error",
                            "message": "SyntaxError",
                        }
                    }
                }],
            }
        }

    with self.assertRaisesRegex(
        common.Error, "the runner pod can't start: Error: SyntaxError"
    ):
      pod._create(
          FakeKube(), {"metadata": {"name": "p"}}, "the runner pod", 5.0
      )

  def test_relay_stops_at_result_line_and_kills_proc(self):
    class FakeProc:

      def __init__(self):
        self.stdout = iter([
            "progress 1\n",
            f'{common.RESULT_PREFIX}{{"paths": []}}\n',
            "never reached\n",
        ])
        self.killed = False
        self.waited = False

      def kill(self):
        self.killed = True

      def wait(self):
        self.waited = True

    fake_proc = FakeProc()
    with mock.patch.object(subprocess, "Popen", return_value=fake_proc):
      with mock.patch.object(common, "log") as log_mock:
        res = pod.relay(None)
    self.assertEqual(res, {"paths": []})
    log_mock.assert_called_once_with("progress 1")
    self.assertTrue(fake_proc.killed)
    self.assertTrue(fake_proc.waited)


if __name__ == "__main__":
  unittest.main()
