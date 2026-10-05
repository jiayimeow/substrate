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

"""Tests for --capacity's parts that don't need a cluster."""

import contextlib
import io
import json
import os
import subprocess
import unittest
from unittest import mock

# Plain imports, not google3 ones: these run with plain python3.
from bperf import capacity
from bperf import common

SETUP = [
    ("machine", ["c3-highmem-192-metal", "bare metal"]),
    (
        "disk",
        [
            "hyperdisk-balanced 100 GB",
            "3,600 IOPS",
            "290 MiB/s",
            "no local SSD",
        ],
    ),
]


def _cycle(start, resume=1000.0, park=2000.0, exec_ms=3000.0, busy=0.0):
  return {
      "actor": 0,
      "chunk": 1,
      "start": start,
      "end": start + 6,
      "resume_ms": resume,
      "busy_ms": busy,
      "exec_ms": exec_ms,
      "park_ms": park,
  }


def _result(cycles, actors=1, errors=(), error=None, load_start=1000.0):
  return {
      "actors": actors,
      "park": "Suspend",
      "duration": 60,
      "load_start": load_start,
      "cycles": cycles,
      "errors": list(errors),
      "deleted": actors,
      "error": error,
  }


def _steady(actors=1, **kw):
  """A step's results: 5 cycles, all after the warmup."""
  return _result([_cycle(1020 + 5 * i, **kw) for i in range(5)], actors=actors)


def _load(**kw):
  load = {
      "seconds": 45,
      "busy_cores": 1.2,
      "ncpu": 192,
      "disk": "nvme0n1",
      "disk_pct": 20.0,
      "disk_mibps": 40.0,
      "disk_iops": 300.0,
      "psi": {"io": 1.0, "cpu": 0.0, "memory": 0.0},
  }
  load.update(kw)
  return load


class ParseLevelsTest(unittest.TestCase):

  def test_levels(self):
    self.assertEqual(capacity.parse_levels("1,2,4,8"), (1, 2, 4, 8))
    self.assertEqual(capacity.parse_levels("1"), (1,))
    for bad in ("2,4", "1,4,2", "1,1", "1,two", "", "1,128"):
      with self.subTest(bad=bad):
        with self.assertRaises(ValueError):
          capacity.parse_levels(bad)


class StepStatsTest(unittest.TestCase):

  def test_percentile_interpolates_like_numpy(self):
    self.assertEqual(capacity.percentile([3, 1, 2, 4], 50), 2.5)
    self.assertAlmostEqual(capacity.percentile([1, 2, 3, 4], 90), 3.7)
    self.assertEqual(capacity.percentile([5], 90), 5)
    self.assertIsNone(capacity.percentile([], 50))

  def test_counts_the_cycles_after_the_warmup(self):
    cycles = [
        _cycle(1005, resume=9999),  # in the warmup: left out
        _cycle(1016, resume=1000, park=2000, busy=120),
        _cycle(1030, resume=1100, park=2200),
        _cycle(1050, resume=1300, park=2600),
    ]
    step = capacity.step_stats(_result(cycles))
    self.assertEqual(step["cycles"], 3)
    self.assertAlmostEqual(step["per_s"], 3 / 45)
    self.assertEqual(step["window"], (1015.0, 1060.0))
    self.assertEqual(step["resume"]["p50"], 1100)
    self.assertAlmostEqual(step["resume"]["p90"], 1260)
    self.assertEqual(step["park"]["p50"], 2200)
    self.assertEqual(step["busy"], [120])
    self.assertEqual(step["verdict"], "")

  def test_a_failure_ends_the_window(self):
    errors = [{"actor": 1, "time": 1040.0, "error": "ResumeActor x failed"}]
    step = capacity.step_stats(
        _result([_cycle(1020), _cycle(1045)], errors=errors)
    )
    self.assertEqual(step["window"], (1015.0, 1040.0))
    self.assertEqual(step["cycles"], 1)

  def test_no_load(self):
    step = capacity.step_stats(
        _result([], load_start=None, error="2 of 2 actors didn't start: x")
    )
    self.assertEqual(step["cycles"], 0)
    self.assertIsNone(step["window"])
    self.assertIsNone(step["resume"])


class VerdictTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.base = capacity.step_stats(_steady())

  def test_the_first_step_is_held_to_nothing(self):
    self.assertEqual(capacity.verdict(self.base, self.base), "")

  def test_over_twice_the_first_steps_p90(self):
    slow = capacity.step_stats(_steady(actors=4, park=4100))
    self.assertEqual(
        capacity.verdict(slow, self.base),
        "Suspend P90 4,100 ms > 2 × 2,000 ms (its P90 with 1 actor)",
    )
    slow = capacity.step_stats(_steady(actors=4, resume=2500))
    self.assertTrue(capacity.verdict(slow, self.base).startswith("Resume P90"))
    ok = capacity.step_stats(_steady(actors=2, park=3900))
    self.assertEqual(capacity.verdict(ok, self.base), "")

  def test_failures(self):
    errors = [
        {"actor": 1, "time": 1030.0, "error": "SuspendActor y failed: x"}
    ] * 2
    failed = capacity.step_stats(
        _result([_cycle(1020)], errors=errors, actors=2)
    )
    self.assertEqual(
        capacity.verdict(failed, self.base),
        "2 calls failed: SuspendActor y failed: x",
    )
    broken = capacity.step_stats(
        _result([], load_start=None, error="1 of 2 actors didn't start: x")
    )
    self.assertEqual(
        capacity.verdict(broken, self.base), "1 of 2 actors didn't start: x"
    )
    early = capacity.step_stats(_result([_cycle(1005)]))
    self.assertEqual(
        capacity.verdict(early, early), "no cycle started after the first 15 s"
    )


class SaturatedTest(unittest.TestCase):

  def test_disk_limits(self):
    self.assertEqual(capacity.disk_limits(SETUP), (3600, 290))
    pd = [("disk", ["pd-balanced 200 GB", "no local SSD"])]
    self.assertEqual(capacity.disk_limits(pd), (None, None))
    self.assertEqual(capacity.disk_limits([]), (None, None))

  def test_what_ran_out(self):
    self.assertEqual(capacity.saturated(_load(), SETUP), [])
    near_limit = _load(
        disk_pct=60.0, disk_mibps=270.0, psi={"io": 4.0, "memory": 0.0}
    )
    self.assertEqual(
        capacity.saturated(near_limit, SETUP),
        ["disk (60% busy · 270 of 290 MiB/s · 300 of 3,600 IOPS · PSI io 4%)"],
    )
    self.assertEqual(
        capacity.saturated(near_limit, []),
        [],  # without the disk's limits, 270 MiB/s alone isn't full
    )
    stalled = _load(psi={"io": 1.0, "cpu": 12.0, "memory": 15.0})
    self.assertEqual(
        capacity.saturated(stalled, SETUP),
        [
            "memory (PSI memory 15%)",
            "CPU (1 of 192 cores busy · PSI cpu 12%)",
        ],
    )
    self.assertEqual(
        capacity.saturated(_load(busy_cores=180.0), SETUP),
        ["CPU (180 of 192 cores busy · PSI cpu 0%)"],
    )

  def test_node_text(self):
    self.assertEqual(
        capacity.node_text(_load(), SETUP),
        "1.2 of 192 cores busy · disk 20% busy, 40 of 290 MiB/s, 300 of 3,600"
        " IOPS · PSI io 1%, cpu 0%, memory 0%",
    )


class ReportTest(unittest.TestCase):

  def _steps(self):
    base = capacity.step_stats(_steady())
    base["node"] = _load()
    over = capacity.step_stats(_steady(actors=2, park=5000))
    over["node"] = _load(
        disk_pct=97.0, disk_mibps=281.0, psi={"io": 41.0, "cpu": 0.0}
    )
    over["verdict"] = capacity.verdict(over, base)
    return [base, over]

  def test_step_lines(self):
    base, over = self._steps()
    self.assertEqual(
        capacity.step_lines(base, True, SETUP),
        [
            (
                "  ✔ baseline: 0.11 cycles/s · Resume P90 1,000 ms · Suspend"
                " P90 2,000 ms"
            ),
            (
                "    node 1.2 of 192 cores busy · disk 20% busy, 40 of 290"
                " MiB/s, 300 of 3,600 IOPS · PSI io 1%, cpu 0%, memory 0%"
            ),
        ],
    )
    self.assertEqual(
        capacity.step_lines(over, False, SETUP)[:2],
        [
            (
                "  ✗ over capacity: Suspend P90 5,000 ms > 2 × 2,000 ms (its"
                " P90 with 1 actor)"
            ),
            "    0.11 cycles/s · Resume P90 1,000 ms · Suspend P90 5,000 ms",
        ],
    )

  def test_table(self):
    lines = capacity.table_lines(self._steps())
    self.assertEqual(
        lines,
        [
            (
                "   N  cycles/s  Resume P50/P90  Suspend P50/P90  Exec P50 "
                " cores  disk  MiB/s  PSI io"
            ),
            (
                "   1      0.11   1,000 / 1,000    2,000 / 2,000     3,000 "
                "   1.2   20%     40      1%"
            ),
            (
                "   2      0.11   1,000 / 1,000    5,000 / 5,000     3,000 "
                "   1.2   97%    281     41%  ✗"
            ),
        ],
    )
    unwatched = capacity.step_stats(_steady())
    self.assertTrue(capacity.table_lines([unwatched])[1].endswith("–"))

  def test_conclusion(self):
    steps = self._steps()
    self.assertEqual(
        capacity.conclusion(steps, SETUP, None),
        [
            (
                "Capacity: 1 actor at once; with 2, Suspend P90 5,000 ms > 2 ×"
                " 2,000 ms (its P90 with 1 actor)"
            ),
            (
                "Ran out first: disk (97% busy · 281 of 290 MiB/s · 300 of"
                " 3,600 IOPS · PSI io 41%)"
            ),
        ],
    )
    steps[1]["node"] = _load()  # slow, with room on the node
    self.assertEqual(
        capacity.conclusion(steps, SETUP, None)[1],
        "Ran out first: nothing on the node: CPU, disk and memory had room, so"
        " look at atelet, ate-api, GCS or the network",
    )
    held = steps[:1]
    self.assertEqual(
        capacity.conclusion(held, SETUP, None),
        [
            "Capacity: at least 1 actor at once: every step held",
            (
                "At 1 actor, the node still had room: 1.2 of 192 cores busy ·"
                " disk 20% busy, 40 of 290 MiB/s, 300 of 3,600 IOPS · PSI io"
                " 1%, cpu 0%, memory 0%"
            ),
        ],
    )
    self.assertEqual(
        capacity.conclusion(held, SETUP, "interrupted"),
        [
            "Capacity: at least 1 actor at once; the run stopped before the"
            " next step"
        ],
    )
    first = [dict(steps[0], verdict="1 call failed: x")]
    self.assertEqual(
        capacity.conclusion(first, SETUP, None),
        ["No capacity measured: with 1 actor, 1 call failed: x"],
    )
    self.assertEqual(capacity.conclusion([], SETUP, "preflight"), [])

  def test_print_report(self):
    idle = _load(
        busy_cores=0.1,
        disk_pct=2.0,
        disk_mibps=0.2,
        disk_iops=4.0,
        psi={"io": 0.0},
    )
    out = io.StringIO()
    with mock.patch.dict(os.environ, {"NO_COLOR": "1"}):
      with contextlib.redirect_stdout(out):
        capacity.print_report(self._steps(), SETUP, {"before": idle}, 400, 60)
    text = out.getvalue()
    self.assertIn(
        " baseline-perf --capacity · swebench-astropy-7336 · suspend path · 7"
        " min\n",
        text,
    )
    self.assertIn(
        " disk  hyperdisk-balanced 100 GB · 3,600 IOPS · 290 MiB/s", text
    )
    self.assertIn(" node idle: 0.1 busy cores · disk 2% · PSI io 0%\n", text)
    self.assertIn(" Capacity: 1 actor at once; with 2, Suspend P90", text)
    self.assertIn(
        " Note: latencies in ms, over the last 45 s of each 60 s step", text
    )


class _FakeKube:
  """Kube, answering from canned objects and recording every command."""

  def __init__(
      self, pool=None, pods=(), workers=(), actors=None, delete_errors=None
  ):
    self.pool = pool or {"metadata": {}, "spec": {"replicas": 1}}
    self.pods = list(pods)
    self.workers = list(workers)
    self.actors = actors or {}
    self.delete_errors = delete_errors or {}
    self.commands = []

  def get_json(self, *args):
    self.commands.append(("get_json",) + args)
    if "workerpool" in args:
      return self.pool
    if "workerpools" in args:
      meta = dict(self.pool["metadata"], namespace="ns", name="p")
      return {"items": [dict(self.pool, metadata=meta)]}
    if "pods" in args:
      return {"items": self.pods}
    return None

  def kubectl(self, *args, **kwargs):
    del kwargs  # unused
    self.commands.append(("kubectl",) + args)
    return subprocess.CompletedProcess(args, 0, "", "")

  def ate(self, *args, check=True):
    del check  # unused
    self.commands.append(("ate",) + args)
    out, err = "", self.delete_errors.get(args[2], "")
    if args[:2] == ("get", "workers"):
      out = json.dumps({"workers": self.workers})
    elif args[:2] == ("get", "actors"):
      out = json.dumps(self.actors)
    return subprocess.CompletedProcess(args, 1 if err else 0, out, err)


def _pod(name, node="node-1", ready=True, phase="Running", deleting=False):
  ready_condition = {"type": "Ready", "status": "True" if ready else "False"}
  pod = {
      "metadata": {"name": name},
      "spec": {"nodeName": node},
      "status": {"phase": phase, "conditions": [ready_condition]},
  }
  if deleting:
    pod["metadata"]["deletionTimestamp"] = "2026-10-03T00:00:00Z"
  return pod


def _worker(pod, state=capacity.ACTIVE):
  return {"workerNamespace": "ns", "workerPod": pod, "status": {"state": state}}


class PoolTest(unittest.TestCase):

  def test_scale_saves_the_original_once_and_restore_puts_it_back(self):
    kube = _FakeKube()
    pool = capacity.Pool(kube, "ns", "p")
    self.assertEqual(
        (pool.original, pool.leftover, pool.changed), (1, False, False)
    )
    pool.scale(2)
    pool.scale(3)
    pool.restore()
    pool.restore()  # nothing left to put back
    wp = ("-n", "ns")
    self.assertEqual(
        [c[1:] for c in kube.commands if c[0] == "kubectl"],
        [
            wp
            + (
                "annotate",
                "workerpool",
                "p",
                f"{capacity.ANNOTATION}=1",
                "--overwrite",
            ),
            wp + ("scale", "workerpool", "p", "--replicas=2"),
            wp + ("scale", "workerpool", "p", "--replicas=3"),
            wp + ("scale", "workerpool", "p", "--replicas=1"),
            wp + ("annotate", "workerpool", "p", f"{capacity.ANNOTATION}-"),
        ],
    )

  def test_a_stopped_runs_annotation_says_the_original(self):
    annotated = {
        "metadata": {"annotations": {capacity.ANNOTATION: "2"}},
        "spec": {"replicas": 9},
    }
    kube = _FakeKube(pool=annotated)
    pool = capacity.Pool(kube, "ns", "p")
    self.assertEqual(
        (pool.original, pool.leftover, pool.changed), (2, True, True)
    )
    self.assertEqual(capacity.scaled_pools(kube), [("ns", "p")])
    self.assertEqual(capacity.scaled_pools(_FakeKube()), [])

  def test_of_worker(self):
    worker = {"workerNamespace": "ns", "workerPool": "p", "workerPod": "p-1"}
    pool = capacity.Pool.of_worker(_FakeKube(), worker)
    self.assertEqual((pool.namespace, pool.name), ("ns", "p"))


class WaitForWorkersTest(unittest.TestCase):

  def test_ready_and_registered(self):
    pods = [_pod("p-1"), _pod("p-2"), _pod("p-0", deleting=True)]
    workers = [_worker("p-1"), _worker("p-2"), _worker("p-0"), _worker("x")]
    kube = _FakeKube(pods=pods, workers=workers)
    pool = capacity.Pool(kube, "ns", "p")
    waited, ready = capacity.wait_for_workers(kube, pool, "node-1", 2)
    self.assertLess(waited, 1)
    self.assertEqual([w["workerPod"] for w in ready], ["p-1", "p-2"])

  def test_a_worker_on_another_node_fails(self):
    kube = _FakeKube(pods=[_pod("p-1"), _pod("p-2", node="node-2")])
    with self.assertRaisesRegex(common.Error, "on node-2 too"):
      capacity.wait_for_workers(
          kube, capacity.Pool(kube, "ns", "p"), "node-1", 2
      )

  def test_times_out_saying_why(self):
    pending = _pod("p-2", node="", ready=False, phase="Pending")
    pending["status"]["conditions"] = [{
        "type": "PodScheduled",
        "status": "False",
        "message": "0/5 nodes are available: Too many pods.",
    }]
    kube = _FakeKube(pods=[_pod("p-1"), pending], workers=[_worker("p-1")])
    with mock.patch.object(capacity, "WORKERS_TIMEOUT_S", -1):
      with self.assertRaisesRegex(
          common.Error,
          r"has 1 of 2 workers ready after -1 s; p-2 is Pending: 0/5 nodes",
      ):
        capacity.wait_for_workers(
            kube, capacity.Pool(kube, "ns", "p"), "node-1", 2
        )


class ActorsTest(unittest.TestCase):

  def test_leftover_actors_are_the_numbered_ones(self):
    names = (
        "perf-eval-actor-10",
        "perf-eval-actor",
        "perf-eval-actor-2",
        "x-3",
    )
    actors = {"actors": [{"metadata": {"name": n}} for n in names]}
    self.assertEqual(
        capacity.leftover_actors(_FakeKube(actors=actors)),
        ["perf-eval-actor-2", "perf-eval-actor-10"],
    )
    self.assertEqual(capacity.leftover_actors(_FakeKube()), [])  # prints {}

  def test_delete_actors_lets_gone_ones_be(self):
    kube = _FakeKube(
        delete_errors={
            "a-1": "Error: rpc error: code = NotFound desc = a-1 not found",
            "a-2": "Error: rpc error: code = Unavailable desc = no",
        }
    )
    capacity.delete_actors(kube, ["a-0", "a-1"])
    with self.assertRaisesRegex(
        common.Error, "couldn't delete 1 of 3 actors: a-2: Error: rpc error"
    ):
      capacity.delete_actors(kube, ["a-0", "a-1", "a-2"])
    deleted = [c[3] for c in kube.commands if c[1:3] == ("delete", "actor")]
    self.assertCountEqual(deleted, ["a-0", "a-1", "a-0", "a-1", "a-2"])


if __name__ == "__main__":
  unittest.main()
