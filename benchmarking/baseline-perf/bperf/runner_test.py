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

"""Tests for runner.py's parts that don't need a cluster."""

import contextlib
import io
import time
import unittest
from unittest import mock

# Plain imports, not google3 ones: these run with plain python3.
from bperf import common
from bperf import runner


class ActorRequestsTest(unittest.TestCase):

  def test_resume_is_an_actor_ref(self):
    ref = (
        b"\x0a"
        + bytes([len(common.NAMESPACE)])
        + common.NAMESPACE.encode()
        + b"\x12"
        + bytes([len(common.ACTOR_NAME)])
        + common.ACTOR_NAME.encode()
    )
    want = b"\x0a" + bytes([len(ref)]) + ref
    requests = runner.actor_requests("tmpl")
    for verb in ("Resume", "Suspend", "Pause"):
      self.assertEqual(requests[verb], want, verb)
    self.assertEqual(requests["Delete"], want + b"\x10\x01")

  def test_create_names_the_template(self):
    create = runner.actor_requests("my-template")["Create"]
    self.assertIn(b"\x22", create)  # Actor.actor_template is field 4
    self.assertTrue(create.endswith(b"\x12\x0bmy-template"))

  def test_long_fields_use_multibyte_lengths(self):
    create = runner.actor_requests("t" * 200)["Create"]
    self.assertIn(b"\x12\xc8\x01" + b"t" * 200, create)


class SplitCredentialBundleTest(unittest.TestCase):

  def test_splits_key_and_chain(self):
    key = b"-----BEGIN PRIVATE KEY-----\nk\n-----END PRIVATE KEY-----\n"
    cert1 = b"-----BEGIN CERTIFICATE-----\na\n-----END CERTIFICATE-----\n"
    cert2 = b"-----BEGIN CERTIFICATE-----\nb\n-----END CERTIFICATE-----\n"
    got_key, chain = runner.split_credential_bundle(cert1 + key + cert2)
    self.assertEqual(got_key, key)
    self.assertEqual(chain, cert1 + cert2)

  def test_missing_key_fails(self):
    cert = b"-----BEGIN CERTIFICATE-----\na\n-----END CERTIFICATE-----\n"
    with self.assertRaises(common.Error):
      runner.split_credential_bundle(cert)


class _FakeApi:
  """AteApi, with every call answered at once; fail: a verb whose calls fail."""

  def __init__(self, fail=None):
    self.verbs = []
    self.fail = fail

  def _call(self, verb):
    self.verbs.append(verb)
    if verb == self.fail:
      raise common.Error(f"{verb}Actor x failed: UNAVAILABLE")

  def delete(self):
    self._call("Delete")

  def create(self):
    self._call("Create")

  def resume(self):
    self._call("Resume")
    return {"client_ms": 500.0}

  def call(self, verb):
    self._call(verb)
    return {"client_ms": 300.0}


class _FakeRouter:
  """Router, with the replay server up and every job done after delay_s."""

  def __init__(self, delay_s=0.0):
    self.delay_s = delay_s

  def wait_until_up(self):
    return {"status": "UP", "total_steps": 21}

  def run_chunk(self, start, end, poll_s=None):
    del start, end, poll_s  # unused
    time.sleep(self.delay_s)
    return 2000.0

  def close(self):
    pass


class RunPathTest(unittest.TestCase):

  def test_progress_is_status_lines_then_one_line(self):
    api, out = _FakeApi(), io.StringIO()
    with contextlib.redirect_stdout(out):
      path = runner._run_path(api, _FakeRouter(), "Pause", 4, 6)
    lines = out.getvalue().splitlines()
    self.assertTrue(lines[0].startswith("[4/6] Pause path"), lines[0])
    for line in lines[1:-1]:
      self.assertTrue(line.startswith(common.STATUS_PREFIX), line)
    self.assertIn(
        f"{common.STATUS_PREFIX}  Cycle 4 (Steps 17-21): resume 500 ms · exec"
        " 2000 ms · pause...",
        lines,
    )
    self.assertRegex(
        lines[-1], r"^  ✔ cold start and 4 cycles \(21 steps\) in \d+ s$"
    )
    self.assertEqual(
        api.verbs,
        ["Delete", "Create", "Resume", "Pause"] + ["Resume", "Pause"] * 4,
    )
    self.assertEqual(
        [c["cycle"] for c in path["cycles"]],
        [name for name, _, _ in common.CHUNKS],
    )

  def test_run_chunk_retries_transient_503_on_execute(self):
    router = runner.Router()
    responses = iter([
        (503, {"error": "backend not ready"}, "503"),
        (202, {"job_id": "j1"}, "202"),
        (200, {"status": "COMPLETED"}, "200"),
    ])
    with mock.patch.object(
        router, "request", side_effect=lambda *a, **kw: next(responses)
    ):
      wall_ms = router.run_chunk(1, 5, poll_s=0.0)
    self.assertGreaterEqual(wall_ms, 0.0)


class CapacityTest(unittest.TestCase):

  def test_each_actor_has_its_own_requests_and_headers(self):
    resume = runner.actor_requests("tmpl", "perf-eval-actor-3")["Resume"]
    self.assertTrue(resume.endswith(b"\x12\x11perf-eval-actor-3"))
    headers = runner.Router(runner.capacity_actor(3)).headers
    self.assertEqual(
        headers["Host"],
        "perf-eval-actor-3.benchmark-workloads.actors.resources.substrate.ate.dev",
    )
    self.assertEqual(
        headers["ate-target-actor"], "benchmark-workloads/perf-eval-actor-3"
    )

  def test_in_parallel_returns_the_failures_in_order(self):
    def fn(x):
      if x == 4:
        raise ValueError("four")
      if x % 2:
        raise common.Error(f"odd {x}")

    self.assertEqual(
        runner._in_parallel(fn, range(6), 3),
        ["odd 1", "odd 3", "runner: ValueError: four", "odd 5"],
    )
    self.assertEqual(runner._in_parallel(fn, [], 3), [])

  def test_actors_go_round_the_chunks_until_the_end(self):
    clients = [(_FakeApi(), _FakeRouter(delay_s=0.01)) for _ in range(2)]
    result = {"cycles": [], "errors": []}
    with mock.patch.object(runner, "STAGGER_S", 0):
      with contextlib.redirect_stdout(io.StringIO()) as out:
        runner._load(clients, "Suspend", 0.1, result)
    for actor in (0, 1):
      chunks = [c["chunk"] for c in result["cycles"] if c["actor"] == actor]
      self.assertGreaterEqual(len(chunks), 2)
      self.assertEqual(chunks, [i % 4 + 1 for i in range(len(chunks))])
    self.assertEqual(clients[0][0].verbs[:4], ["Resume", "Suspend"] * 2)
    cycle = result["cycles"][0]
    self.assertEqual(
        (cycle["resume_ms"], cycle["exec_ms"], cycle["park_ms"]),
        (500.0, 2000.0, 300.0),
    )
    self.assertEqual(result["errors"], [])
    self.assertRegex(
        out.getvalue().splitlines()[-1], r"^  ✔ \d+ cycles in \d+ s$"
    )

  def test_a_failed_call_stops_every_actor(self):
    clients = [
        (_FakeApi(fail="Suspend"), _FakeRouter(delay_s=0.01)),
        (_FakeApi(), _FakeRouter(delay_s=0.01)),
    ]
    result = {"cycles": [], "errors": []}
    started = time.monotonic()
    with mock.patch.object(runner, "STAGGER_S", 0):
      with contextlib.redirect_stdout(io.StringIO()) as out:
        runner._load(clients, "Suspend", 30, result)
    self.assertLess(time.monotonic() - started, 5)  # not the 30 s
    self.assertEqual(
        result["errors"],
        [{
            "actor": 0,
            "time": mock.ANY,
            "error": "SuspendActor x failed: UNAVAILABLE",
        }],
    )
    self.assertIn("then a call failed and every actor stopped", out.getvalue())

  def _run_capacity(self, fail=None):
    """run_capacity with 2 fake actors; fail: {actor: verb that fails}."""
    apis = {}

    class FakeAteApi(_FakeApi):

      def __init__(self, grpc, template, actor):
        del grpc, template  # unused
        super().__init__((fail or {}).get(actor))
        apis[actor] = self

      def for_actor(self, actor):
        return FakeAteApi(None, None, actor)

    with contextlib.ExitStack() as stack:
      stack.enter_context(mock.patch.object(runner, "AteApi", FakeAteApi))
      stack.enter_context(
          mock.patch.object(
              runner, "Router", lambda actor: _FakeRouter(delay_s=0.01)
          )
      )
      stack.enter_context(mock.patch.object(runner, "STAGGER_S", 0))
      stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
      result = runner.run_capacity(None, "tmpl", "Suspend", 2, 0.05)
    return result, apis

  def test_run_capacity_cold_starts_loads_and_deletes_every_actor(self):
    result, apis = self._run_capacity()
    self.assertIsNone(result["error"])
    self.assertIsNotNone(result["load_start"])
    self.assertGreater(len(result["cycles"]), 0)
    self.assertEqual(result["deleted"], 2)
    self.assertEqual(sorted(apis), ["perf-eval-actor-0", "perf-eval-actor-1"])
    for api in apis.values():
      self.assertEqual(api.verbs[:4], ["Delete", "Create", "Resume", "Suspend"])
      self.assertEqual(api.verbs[-1], "Delete")

  def test_run_capacity_fails_if_an_actor_does_not_start(self):
    result, apis = self._run_capacity(fail={"perf-eval-actor-1": "Create"})
    self.assertEqual(
        result["error"],
        "1 of 2 actors didn't start: CreateActor x failed: UNAVAILABLE",
    )
    self.assertIsNone(result["load_start"])
    self.assertEqual(result["cycles"], [])
    self.assertEqual(result["deleted"], 2)
    self.assertEqual(apis["perf-eval-actor-0"].verbs[-1], "Delete")


if __name__ == "__main__":
  unittest.main()
