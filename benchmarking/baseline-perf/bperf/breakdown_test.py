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

"""Tests for logs.py and breakdown.py, on synthetic log lines."""

import json
import unittest

# Plain imports, not google3 ones: these run with plain python3.
from bperf import breakdown
from bperf import logs

TRACE = "0123456789abcdef0123456789abcdef"


def _at(seconds: float) -> str:
  return f"2026-10-01T18:00:{seconds:06.3f}Z"


def _record(component, at, msg, **fields):
  line = json.dumps({"time": _at(at), "msg": msg, "trace_id": TRACE, **fields})
  return logs.parse_record(line, component=component, pod="p", node="n")


def _resume_records():
  """One ResumeActor: ateapi 500 ms > atelet Restore 400 ms > ateom 250 ms."""
  return [
      _record(
          "ateapi",
          10.5,
          "Handle RPC",
          method="/ateapi.Control/ResumeActor",
          **{"elapsed-time": "500ms"},
      ),
      _record(
          "atelet",
          10.45,
          "Handle RPC",
          method="/atelet.Atelet/Restore",
          **{"elapsed-time": "400ms"},
      ),
      _record(
          "atelet",
          10.44,
          "Restore timing breakdown",
          **{
              "ate.actor.restore.duration.download": 0.1,
              "ate.actor.restore.duration.ateom": 0.25,
              "ate.actor.restore.duration.total": 0.35,
          },
      ),
      _record(
          "ateom",
          10.43,
          "Handle RPC",
          method="/ateom.Ateom/RestoreWorkload",
          **{"elapsed-time": "250ms"},
      ),
      _record(
          "ateom",
          10.42,
          "Actor restore phases",
          vm_restore=200_000_000,
          total=250_000_000,
      ),
      _record("ateom", 10.41, "unrelated line"),
  ]


class ParseTest(unittest.TestCase):

  def test_parse_time(self):
    self.assertAlmostEqual(
        logs.parse_time("1970-01-01T00:00:01.5Z"), 1.5, places=6
    )
    self.assertAlmostEqual(
        logs.parse_time("1970-01-01T01:00:00.000000001+01:00"), 1e-9, places=9
    )
    self.assertIsNone(logs.parse_time("yesterday"))
    self.assertIsNone(logs.parse_time(""))

  def test_parse_go_duration(self):
    cases = {
        "1.5ms": 0.0015,
        "2m3.5s": 123.5,
        "850µs": 0.00085,
        "0": 0.0,
        "-1s": -1.0,
        "1h": 3600.0,
        "100ns": 1e-7,
    }
    for text, want in cases.items():
      self.assertAlmostEqual(logs.parse_go_duration(text), want, msg=text)
    for bad in ("", "5", "1.5 ms", "ms", None, 3):
      self.assertIsNone(logs.parse_go_duration(bad), bad)

  def test_parse_record_keeps_key_order(self):
    rec = logs.parse_record('{"msg": "m", "b": 1, "a": 2, "b": 3}')
    self.assertEqual([k for k, _ in rec.fields], ["msg", "b", "a", "b"])
    self.assertEqual(rec.get("b"), 1)
    self.assertIsNone(logs.parse_record("not json"))


def _call(method, err=None, template="t-copy", at=0.0):
  """A line ateom logs for a call about template; err is None if it worked."""
  return json.dumps({
      "time": _at(at),
      "level": "INFO",
      "msg": "Handle RPC",
      "method": f"/ateom.Ateom/{method}",
      "req": {"actor_template_name": template},
      "err": err,
  })


class _Proc:
  returncode, stdout, stderr = 0, "", ""


class _LogKube:
  """One atelet pod and one worker pod, with the lines each logged."""

  def __init__(self, atelet=(), worker=()):
    self.lines = {"atelet-1": atelet, "worker-1": worker}

  def running_pods(self, namespace, selector):
    name = "atelet-1" if selector == "app=atelet" else "worker-1"
    return [{"metadata": {"name": name, "namespace": namespace or "w"}}], None

  def kubectl(self, *args, **_):
    proc = _Proc()
    proc.stdout = "\n".join(self.lines[args[3]])  # -n NS logs POD ...
    return proc


class ProblemsTest(unittest.TestCase):

  SINCE = logs.parse_time(_at(0))

  def test_recent_problems_shows_failed_calls_once_with_a_count(self):
    boom = [_call("RunWorkload", "boom", at=t) for t in (2, 3, 4)]
    kube = _LogKube(
        atelet=[json.dumps({"time": _at(1), "level": "WARN", "msg": "slow"})],
        worker=boom + [_call("GetActiveWorkloadStats", at=5)],
    )
    self.assertEqual(
        logs.recent_problems(kube, self.SINCE),
        "\n      atelet: slow\n      ateom: RunWorkload: boom (×3)",
    )

  def test_repeated_failure_is_the_same_failed_call_three_times(self):
    def repeated(*worker):
      kube = _LogKube(worker=worker)
      return logs.repeated_failure(kube, self.SINCE, "t-copy")

    boom = _call("RunWorkload", "boom")
    self.assertIsNone(repeated(boom, boom))
    self.assertEqual(
        repeated(boom, boom, boom), ("ateom: RunWorkload: boom", 3)
    )
    other = _call("RunWorkload", "boom", template="other")
    self.assertIsNone(repeated(other, other, other))
    warning = json.dumps({"level": "WARN", "msg": "slow t-copy"})
    self.assertIsNone(repeated(warning, warning, warning))


class BuildTreeTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.call = {
        "method": "/ateapi.Control/ResumeActor",
        "trace_id": TRACE,
        "client_ms": 510.0,
        "server_us": 500000,
    }

  def test_nests_atelet_and_ateom(self):
    tree, missing = breakdown.build_tree(self.call, _resume_records())
    self.assertEqual(missing, [])
    self.assertEqual(tree["name"], "ResumeActor")
    self.assertAlmostEqual(tree["ms"], 500, places=3)
    outside, atelet = tree["children"]
    self.assertEqual(outside["name"], "ateapi (outside atelet)")
    self.assertAlmostEqual(outside["ms"], 100, places=3)
    self.assertEqual(atelet["name"], "atelet Restore")
    self.assertEqual(
        [c["name"] for c in atelet["children"]], ["download", "ateom", "other"]
    )
    ateom = atelet["children"][1]["children"][0]
    self.assertEqual(ateom["name"], "ateom RestoreWorkload")
    self.assertEqual(
        [(c["name"], c["ms"]) for c in ateom["children"]],
        [("vm_restore", 200.0), ("other", 50.0)],
    )

  def test_top_phases(self):
    tree, _ = breakdown.build_tree(self.call, _resume_records())
    top = dict(breakdown.top_phases([tree, None]))
    self.assertEqual(top["vm_restore"], 200.0)
    self.assertEqual(top["ateapi (outside atelet)"], 100.0)
    self.assertEqual(top["other in ateom RestoreWorkload"], 50.0)

  def test_no_ateapi_line(self):
    tree, missing = breakdown.build_tree(self.call, _resume_records()[1:])
    self.assertEqual(tree, {"name": "ResumeActor", "ms": 500.0})
    self.assertEqual(missing, ["ateapi log line for " + self.call["method"]])

  def test_other_trace_ignored(self):
    call = dict(self.call, trace_id="f" * 32)
    tree, _ = breakdown.build_tree(call, _resume_records())
    self.assertNotIn("children", tree)

  def test_critical_path_counts_parallel_phases_once(self):
    nodes = [
        {"s": 1.0},
        {"s": 3.0},
        {"s": 2.0, "parallel": True},
        {"s": 4.0, "parallel": True},
        {"s": 0.5},
    ]
    # 1 + max(3, 2, 4) + 0.5
    self.assertEqual(breakdown.critical_path(nodes), 5.5)

  def test_top_phases_excludes_shadowed_parallel_phases(self):
    tree = {
        "name": "SuspendActor",
        "ms": 350.0,
        "children": [
            {"name": "pause", "ms": 30.0, "s": 0.03},
            {"name": "snapshot", "ms": 220.0, "s": 0.22},
            {
                "name": "rootfs_upper (parallel)",
                "ms": 180.0,
                "s": 0.18,
                "parallel": True,
            },
            {"name": "upload", "ms": 100.0, "s": 0.10},
        ],
    }
    top = breakdown.top_phases([tree], n=2)
    self.assertEqual(top, [("snapshot", 220.0), ("upload", 100.0)])


if __name__ == "__main__":
  unittest.main()
