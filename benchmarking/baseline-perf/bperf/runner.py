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

# fmt: off
# pylint: disable=line-too-long,g-doc-args,g-doc-return-or-yield,g-doc-exception
# 100 columns and short docstrings, kept compact on purpose: see README.md#the-code.
"""The runner: the pod's side of a run, and everything in it that is timed.

Runs in the runner pod (see pod.py), which calls ate-api over gRPC and the actor
through atenet-router directly, so no number includes a kubectl-ate process or a
port-forward. It prints its progress (what it's doing right now on STATUS_PREFIX
lines), then one RESULT_PREFIX line with its results as JSON, which the launcher
reads from the pod's log:

  {"paths": [{"park": "Pause" | "Suspend",
              "cold": {"resume": call, "suspend": call},
              "cycles": [{"cycle", "resume": call, "exec_wall", "suspend": call}]}],
   "calls": [call, ...],  # every ate-api call, in order
   "deleted": bool,       # whether it deleted the actor at the end
   "error": str | None}

  call = {"label", "method", "trace_id", "start", "end", "client_ms",
          "server_us", "error"?, "busy_ms"?, "busy_retries"?}

A path's "suspend" entries are its park calls: suspends or pauses. With
--actors (one step of --capacity, see run_capacity) the results are instead:

  {"actors", "park", "duration", "load_start": Unix seconds, or None if the load never started,
   "cycles": [{"actor", "chunk", "start", "end", "resume_ms", "busy_ms", "exec_ms", "park_ms"}],
   "errors": [{"actor", "time", "error"}],  # the calls that failed during the load
   "deleted": int,                          # how many of the actors it deleted at the end
   "error": str | None}                     # why the step couldn't run, e.g. an actor didn't start

Importing this needs only the standard library; grpc, which only the runner
image has, is imported by main().
"""

import argparse
import concurrent.futures
import datetime
import http.client
import json
import os
import re
import threading
import time
from typing import Any
import urllib.parse

# Plain imports, not google3 ones: this runs with python3 in the runner pod.
from bperf import common

ATEAPI_TARGET = "dns:///api.ate-system.svc.cluster.local:443"  # ate-api, from inside the cluster
ATEAPI_SERVER_NAME = "api.ate-system.svc"  # the DNS name on ate-api's serving certificate
CA_FILE = "/run/servicedns-ca/ca.crt"  # verifies ate-api's serving certificate
CRED_BUNDLE = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"  # the pod's client cert and key
SERVER_ELAPSED_TRAILER = "x-server-elapsed-us"  # ate-api's own time for a call
# atenet-router, plain HTTP on port 80. It picks the actor from the Host header, newer ones from the
# ate-target-actor header; the runner sends both.
ROUTER = "atenet-router.ate-system.svc.cluster.local"


def target(actor: str) -> str:
  """'benchmark-workloads/<actor>': the actor as atenet-router's ate-target-actor header and the call labels name it."""
  return f"{common.NAMESPACE}/{actor}"


def router_host(actor: str) -> str:
  """The Host header that has atenet-router route a request to actor."""
  return f"{actor}.{common.NAMESPACE}.actors.resources.substrate.ate.dev"


ROUTER_HOST = router_host(common.ACTOR_NAME)
TARGET_ACTOR = target(common.ACTOR_NAME)

WORKER_WAIT_S = 30  # a resume retries this long while no worker is free
CREATE_WAIT_S = 60  # a create waits this long for a previous actor to go
CONNECT_TIMEOUT_S = 30  # the gRPC channel's TLS handshake
HTTP_TIMEOUT_S = 10  # one HTTP request through atenet-router
READY_TIMEOUT_S = 60  # the replay server answers after the cold start
JOB_TIMEOUT_S = 120  # one cycle's /execute job (boomer allows 600 x 200 ms)
POLL_INTERVAL_S = 0.1
_HTTP_ERRORS = (OSError, http.client.HTTPException)

# A --capacity step (run_capacity): many actors at once.
CAPACITY_POLL_S = 0.2  # how often each actor's job is polled, as boomer does: N actors poll N times as much
COLD_START_LIMIT = 8  # actors created and cold-started at once: that isn't timed, so it needn't be a burst
DELETE_LIMIT = 16  # actors deleted at once at the end
STAGGER_S = 10.0  # about a cycle: the actors' first cycles start spread over this long, so their calls don't line up
STATUS_INTERVAL_S = 1.0  # how often the status line says how the load is going


# ── ate-api requests, encoded by hand ───────────────────────────────────────


def _pb_varint(n: int) -> bytes:
  out = bytearray()
  while n > 0x7F:
    out.append(n & 0x7F | 0x80)
    n >>= 7
  out.append(n)
  return bytes(out)


def _pb_field(number: int, value: bool | str | bytes) -> bytes:
  """One protobuf field: a bool (varint), or a string or message (bytes)."""
  if isinstance(value, bool):
    return _pb_varint(number << 3) + _pb_varint(int(value))
  if isinstance(value, str):
    value = value.encode()
  return _pb_varint(number << 3 | 2) + _pb_varint(len(value)) + value


def actor_requests(template: str, actor: str = common.ACTOR_NAME) -> dict[str, bytes]:
  """Verb ("Create", "Resume", ...) -> the serialized request for actor, encoded by hand so the runner needs no protos.

  Field numbers are from pkg/proto/ateapipb/ateapi.proto, the same in v0.1.0 and
  on main: ObjectRef and ResourceMetadata {atespace = 1, name = 2}; Actor
  {metadata = 1, actor_template = 4}; every *ActorRequest, PauseActorRequest
  included, {actor = 1}; DeleteActorRequest {any_state = 2}.
  """
  ref = _pb_field(1, common.NAMESPACE) + _pb_field(2, actor)
  message = _pb_field(1, ref) + _pb_field(4, _pb_field(1, common.NAMESPACE) + _pb_field(2, template))
  by_ref = _pb_field(1, ref)
  return {"Create": _pb_field(1, message), "Resume": by_ref, "Suspend": by_ref, "Pause": by_ref,
          "Delete": by_ref + _pb_field(2, True)}


# ── Clients ─────────────────────────────────────────────────────────────────


def split_credential_bundle(bundle: bytes) -> tuple[bytes, bytes]:
  """Splits a pod-certificate credential bundle -> (the private key, the certificate chain), both PEM."""
  key, chain = None, []
  for m in re.finditer(rb"-----BEGIN ([A-Z ]+)-----.*?-----END \1-----\n?", bundle, re.DOTALL):
    if m.group(1) == b"PRIVATE KEY":
      key = m.group(0)
    else:
      chain.append(m.group(0))
  if key is None or not chain:
    raise common.Error(f"{CRED_BUNDLE} has no private key or no certificate")
  return key, b"".join(chain)


def _rfc3339(t: float) -> str:
  utc = datetime.datetime.fromtimestamp(t, datetime.timezone.utc)
  return utc.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _connect(grpc):
  """A channel to ate-api over mTLS as the pod's own identity, already connected."""
  with open(CA_FILE, "rb") as f:
    ca = f.read()
  with open(CRED_BUNDLE, "rb") as f:
    key, chain = split_credential_bundle(f.read())
  creds = grpc.ssl_channel_credentials(root_certificates=ca, private_key=key, certificate_chain=chain)
  # ate-api is a headless Service, one address per replica; spread the calls over them the way
  # kubectl-ate does.
  round_robin = {"loadBalancingConfig": [{"round_robin": {}}]}
  channel = grpc.secure_channel(ATEAPI_TARGET, creds, options=[
      ("grpc.ssl_target_name_override", ATEAPI_SERVER_NAME), ("grpc.service_config", json.dumps(round_robin))])
  try:
    # Connect now, so that no timed call pays for the TLS handshake.
    grpc.channel_ready_future(channel).result(timeout=CONNECT_TIMEOUT_S)
  except grpc.FutureTimeoutError:
    raise common.Error(f"can't connect to ate-api at {ATEAPI_TARGET} from the runner pod") from None
  return channel


class AteApi:
  """ate-api's Control service, for one actor, over mTLS as the pod's own identity."""

  def __init__(self, grpc, template: str, actor: str = common.ACTOR_NAME, channel=None):
    """Connects to ate-api, or uses channel: another client's connection."""
    self._grpc, self._template = grpc, template
    self.target = target(actor)  # the actor, as the call labels and errors name it
    self._channel = channel or _connect(grpc)
    self._requests = actor_requests(template, actor)
    self.calls = []  # the record of every call made, in order (see the module docstring)

  def for_actor(self, actor: str) -> "AteApi":
    """A client for another actor, on the same connection, with its own record of calls."""
    return AteApi(self._grpc, self._template, actor, self._channel)

  def call(self, verb: str, ok=()) -> dict[str, Any]:
    """Calls <verb>Actor with a new trace ID, which the servers log too; returns the call's record."""
    method = f"/ateapi.Control/{verb}Actor"
    trace_id = os.urandom(16).hex()
    metadata = [("traceparent", f"00-{trace_id}-{os.urandom(8).hex()}-01")]
    rpc = self._channel.unary_unary(method)  # bytes in, bytes out
    start = time.time()
    t0 = time.perf_counter()
    try:
      _, call = rpc.with_call(self._requests[verb], timeout=common.ATE_TIMEOUT_S, metadata=metadata)
      code = self._grpc.StatusCode.OK
    except self._grpc.RpcError as e:
      call, code = e, e.code()
    client_ms = (time.perf_counter() - t0) * 1000
    server_us = dict(call.trailing_metadata() or ()).get(SERVER_ELAPSED_TRAILER, "")
    record = {
        "label": f"{verb}Actor {self.target}", "method": method, "trace_id": trace_id,
        "start": _rfc3339(start), "end": _rfc3339(start + client_ms / 1000),
        "client_ms": client_ms, "server_us": int(server_us) if server_us.isdigit() else 0,
    }
    if code != self._grpc.StatusCode.OK:
      record["error"] = f"{code.name}: {call.details()}"
    self.calls.append(record)
    if code != self._grpc.StatusCode.OK and code not in ok:
      raise common.Error(f"{verb}Actor {self.target} failed: {record['error']}")
    return record

  def create(self) -> None:
    """Creates the actor, waiting out the deletion of a previous run's."""
    deadline = time.monotonic() + CREATE_WAIT_S
    while "error" in (record := self.call("Create", ok=(self._grpc.StatusCode.ALREADY_EXISTS,))):
      if time.monotonic() > deadline:
        raise common.Error(f"create actor {self.target} failed: {record['error']}")
      time.sleep(1)

  def resume(self) -> dict[str, Any]:
    """Resumes the actor, retrying while ate-api finds no free worker; returns the call that succeeded.

    Right after a suspend, the ate-api replica that takes the next call may not
    see the worker as free yet: its worker cache follows the database a moment
    later. It answers RESOURCE_EXHAUSTED ("no free workers available"), and this
    retries, as atenet-router does. When it took retries, busy_ms is how long
    they held the resume up, from the first attempt to the start of the one that
    succeeded; that isn't part of client_ms.
    """
    t0 = time.perf_counter()
    deadline = time.monotonic() + WORKER_WAIT_S
    retries = 0
    while "error" in (record := self.call("Resume", ok=(self._grpc.StatusCode.RESOURCE_EXHAUSTED,))):
      if time.monotonic() > deadline:
        raise common.Error(f"ResumeActor {self.target} still failed after {WORKER_WAIT_S}s: {record['error']}")
      retries += 1
      time.sleep(POLL_INTERVAL_S)
    if retries:
      record["busy_ms"] = (time.perf_counter() - t0) * 1000 - record["client_ms"]
      record["busy_retries"] = retries
    return record

  def delete(self) -> None:
    """Deletes the actor in any state; one that isn't there is fine."""
    self.call("Delete", ok=(self._grpc.StatusCode.NOT_FOUND,))


class Router:
  """HTTP to one actor through atenet-router's Service. Connects on the first request."""

  def __init__(self, actor: str = common.ACTOR_NAME):
    self._conn = None
    self.headers = {"Host": router_host(actor), "ate-target-actor": target(actor), "Content-Type": "application/json"}

  def request(self, method: str, path: str, body=None):
    """Sends one request to the actor -> (HTTP status, the JSON body or None, the body's text).

    A POST opens a new connection and is never retried, so no job can start
    twice. A GET reuses the connection, and retries once if it was dropped.
    """
    post = method == "POST"
    for attempt in range(1 if post else 2):
      if post or self._conn is None:
        self.close()
        self._conn = http.client.HTTPConnection(ROUTER, 80, timeout=HTTP_TIMEOUT_S)
      try:
        self._conn.request(method, path, body=None if body is None else json.dumps(body).encode(),
                           headers=self.headers)
        response = self._conn.getresponse()
        text = response.read().decode(errors="replace")
        break
      except _HTTP_ERRORS:
        self.close()
        if post or attempt == 1:
          raise
    try:
      parsed = json.loads(text)
    except ValueError:
      parsed = None
    return response.status, parsed, text

  def close(self) -> None:
    if self._conn:
      self._conn.close()
      self._conn = None

  def wait_until_up(self) -> dict[str, Any]:
    """Waits for the replay server (GET /status -> {"status": "UP", ...}); returns its answer."""
    deadline = time.monotonic() + READY_TIMEOUT_S
    last = "no answer"
    while time.monotonic() < deadline:
      try:
        status, body, text = self.request("GET", "/status")
        if status == 200 and str((body or {}).get("status", "")).upper() == "UP":
          return body
        last = f"HTTP {status}: {text.strip()[:200]}"
      except _HTTP_ERRORS as e:
        last = str(e)
      time.sleep(0.5)
    raise common.Error(f"the SWE-Perf server in the actor didn't come up within {READY_TIMEOUT_S}s; "
                       f"last answer: {last}")

  def run_chunk(self, start: int, end: int, poll_s: float = POLL_INTERVAL_S) -> float:
    """Replays trace steps start..end (1-based) and waits; returns the wall ms until the job reads COMPLETED.

    poll_s: how often it asks; the wall time is a multiple of it, give or take.
    """
    steps = f"steps {start}-{end}"
    t0 = time.perf_counter()
    for attempt in range(5):
      try:
        status, body, text = self.request("POST", "/execute", {"start_step": start, "end_step": end})
      except _HTTP_ERRORS as e:
        raise common.Error(f"POST /execute for {steps}: {e}") from None
      if status not in (502, 503, 504) or attempt == 4:  # 502/503/504 from atenet-router never reached replay.py
        break
      time.sleep(poll_s)
    if status not in (200, 202):  # replay.py answers 202 Accepted
      raise common.Error(f"POST /execute for {steps}: HTTP {status}: {text.strip()[:300]}")
    job_id = (body or {}).get("job_id")
    if not job_id:
      raise common.Error(f"POST /execute returned no job_id: {text.strip()[:300]} "
                         "(the image needs the async replay.py)")
    query = f"/status?job_id={urllib.parse.quote(job_id)}"
    deadline = time.monotonic() + JOB_TIMEOUT_S
    while time.monotonic() < deadline:
      time.sleep(poll_s)
      try:
        status, job, _ = self.request("GET", query)
      except _HTTP_ERRORS:
        continue  # a dropped poll; the deadline still bounds the wait
      job = job if status == 200 and isinstance(job, dict) else {}
      state = str(job.get("status", "")).upper()  # RUNNING, COMPLETED, FAILED
      if state == "COMPLETED":
        return (time.perf_counter() - t0) * 1000
      if state == "FAILED":
        raise common.Error(f"{steps}: job {job_id} FAILED: {job.get('error') or job}")
    raise common.Error(f"{steps}: job {job_id} not done after {JOB_TIMEOUT_S}s")


# ── The run ─────────────────────────────────────────────────────────────────


def _status(text: str) -> None:
  """Says what the runner is doing now, on a line that the launcher draws over the previous one (see common.log)."""
  print(common.STATUS_PREFIX + text, flush=True)


def _run_path(api: AteApi, router: Router, park: str, step: int, total: int):
  """Runs one path: a fresh actor, its cold start from the golden snapshot, then the cycles, each ending with park.

  Its progress goes on status lines, which a terminal shows as one line that
  changes; when the path is done, one line says so.
  """
  common.log(f"[{step}/{total}] {common.PATHS[park]}: new actor {common.ACTOR_NAME}, "
             f"cold start, 4 x (Resume -> Execute -> {park})...")
  started = time.monotonic()
  verb = park.lower()
  _status(f"  Creating {common.ACTOR_NAME}...")
  api.delete()  # a previous run or path may have left it, in any state
  api.create()
  _status("  Cold start (golden snapshot): resume...")
  resume = api.resume()
  cold = f"  Cold start (golden snapshot): resume {common.ms(resume['client_ms'])}"
  _status(f"{cold} · waiting for the server...")
  info = router.wait_until_up()
  _status(f"{cold} · server UP · {verb}...")
  parked = api.call(park)  # park it again, so that every cycle resumes from this actor's own snapshot
  path = {"park": park, "cold": {"resume": resume, "suspend": parked}, "cycles": []}
  for name, start, end in common.CHUNKS:
    _status(f"  {name}: resume...")
    resume = api.resume()
    done = f"  {name}: resume {common.ms(resume['client_ms'])}"
    _status(f"{done} · exec...")
    exec_wall = router.run_chunk(start, end)
    _status(f"{done} · exec {common.ms(exec_wall)} · {verb}...")
    parked = api.call(park)
    path["cycles"].append({"cycle": name, "resume": resume, "exec_wall": exec_wall, "suspend": parked})
  common.log(f"  ✔ cold start and {len(common.CHUNKS)} cycles ({info.get('total_steps', '?')} steps) "
             f"in {time.monotonic() - started:.0f} s")
  return path


def run(grpc, template: str, parks, keep: bool) -> dict[str, Any]:
  """Runs one path per entry of parks, back to back on the same worker, each from a fresh actor.

  "Pause" parks each cycle with a checkpoint kept on the node, "Suspend" with a
  snapshot to object storage.
  """
  result = {"paths": [], "calls": [], "deleted": False, "error": None}
  api = None
  try:
    api = AteApi(grpc, template)
    router = Router()
    for i, park in enumerate(parks):
      result["paths"].append(_run_path(api, router, park, common.FIRST_PATH_STEP + i, common.steps_total(parks)))
  except common.Error as e:
    result["error"] = str(e)
  except Exception as e:  # pylint: disable=broad-exception-caught
    result["error"] = f"runner: {type(e).__name__}: {e}"  # reported with the results so far, not as a traceback
  finally:
    if api and api.calls and not keep:
      try:
        api.delete()
        result["deleted"] = True
      except common.Error as e:
        common.log(f"  ⚠ {e}")
    result["calls"] = api.calls if api else []
  return result


# ── A --capacity step: many actors at once ──────────────────────────────────


def capacity_actor(i: int) -> str:
  """The name of a --capacity step's actor i, from 0."""
  return f"{common.ACTOR_NAME}-{i}"


def _error_text(e: BaseException) -> str:
  """An exception as the results give it: common.Error's message, or another's type and message."""
  return str(e) if isinstance(e, common.Error) else f"runner: {type(e).__name__}: {e}"


def _in_parallel(fn, items, limit: int) -> list[str]:
  """Calls fn(item) for each item, at most limit at once. Returns what failed, as text, in the items' order."""
  items = list(items)
  if not items:
    return []
  with concurrent.futures.ThreadPoolExecutor(max_workers=min(limit, len(items))) as pool:
    futures = [pool.submit(fn, item) for item in items]
  return [_error_text(f.exception()) for f in futures if f.exception() is not None]


def _cold_start(client, park: str) -> None:
  """Readies one actor for the load: a fresh one, cold-started from the golden snapshot, its server up, parked."""
  api, router = client
  api.delete()  # a stopped run may have left it, in any state
  api.create()
  api.resume()
  router.wait_until_up()
  api.call(park)


def _actor_loop(i: int, n: int, client, park: str, end: float, stop: threading.Event, result) -> None:
  """Actor i of n: Resume -> Exec -> park, chunk after chunk, until end (time.monotonic()); a record per cycle.

  Like the boomer client's users, each actor starts at the trace's first chunk
  and goes back to it after the last. Their first cycles start spread over
  STAGGER_S, so that their calls don't line up. A failed call stops every actor
  (stop) once its cycle is done: the step has failed, and an actor stopped
  mid-call may be left stuck.
  """
  api, router = client
  if stop.wait(STAGGER_S * i / n):
    return
  k = 0
  while not stop.is_set() and time.monotonic() < end:
    _, first, last = common.CHUNKS[k % len(common.CHUNKS)]
    start = time.time()
    try:
      resume = api.resume()
      exec_ms = router.run_chunk(first, last, poll_s=CAPACITY_POLL_S)
      parked = api.call(park)
    except Exception as e:  # pylint: disable=broad-exception-caught  # a thread: recorded, not raised
      result["errors"].append({"actor": i, "time": round(time.time(), 3), "error": _error_text(e)})
      stop.set()
      return
    result["cycles"].append({  # list.append is atomic: the threads share result without a lock
        "actor": i, "chunk": k % len(common.CHUNKS) + 1, "start": round(start, 3), "end": round(time.time(), 3),
        "resume_ms": round(resume["client_ms"], 1), "busy_ms": round(resume.get("busy_ms", 0.0), 1),
        "exec_ms": round(exec_ms, 1), "park_ms": round(parked["client_ms"], 1)})
    k += 1


def _load(clients, park: str, duration_s: float, result) -> None:
  """Runs every actor's loop for duration_s, saying how it goes on a status line. Returns once all have stopped."""
  n, stop = len(clients), threading.Event()
  started = time.monotonic()
  threads = [threading.Thread(target=_actor_loop, args=(i, n, client, park, started + duration_s, stop, result),
                              daemon=True) for i, client in enumerate(clients)]
  for t in threads:
    t.start()
  due = started
  while any(t.is_alive() for t in threads):
    elapsed = time.monotonic() - started
    phase = (f"{elapsed:.0f} of {duration_s:.0f} s" if elapsed < duration_s and not stop.is_set()
             else "finishing the last cycles")
    failed = f" · {len(result['errors'])} failed" if result["errors"] else ""
    _status(f"  {n} actor{'s' if n > 1 else ''} · {phase} · {len(result['cycles'])} cycles done{failed}")
    due += STATUS_INTERVAL_S
    for t in threads:
      t.join(max(0.0, due - time.monotonic()))
  stopped = ", then a call failed and every actor stopped" if result["errors"] else ""
  common.log(f"  {'✗' if stopped else '✔'} {len(result['cycles'])} cycles in {time.monotonic() - started:.0f} s{stopped}")


def run_capacity(grpc, template: str, park: str, actors: int, duration_s: float) -> dict[str, Any]:
  """One --capacity step: actors actors at once, each going round Resume -> Exec -> park for duration_s.

  The actors are created and cold-started first, COLD_START_LIMIT at a time and
  untimed; the load starts once all of them are parked. All are deleted at the end.
  """
  result = {"actors": actors, "park": park, "duration": duration_s, "load_start": None, "cycles": [], "errors": [],
            "deleted": 0, "error": None}
  clients = []
  try:
    names = [capacity_actor(i) for i in range(actors)]
    api = AteApi(grpc, template, names[0])
    clients = [(api if i == 0 else api.for_actor(name), Router(name)) for i, name in enumerate(names)]
    _status(f"  Creating {actors} actors, each cold-started from the golden snapshot...")
    started = time.monotonic()
    failed = _in_parallel(lambda client: _cold_start(client, park), clients, COLD_START_LIMIT)
    if failed:
      raise common.Error(f"{len(failed)} of {actors} actors didn't start: {failed[0]}")
    plural = "s" if actors > 1 else ""
    common.log(f"  ✔ {actors} actor{plural} created, started and parked in {time.monotonic() - started:.0f} s")
    result["load_start"] = round(time.time(), 3)
    _load(clients, park, duration_s, result)
  except Exception as e:  # pylint: disable=broad-exception-caught  # reported with the results so far
    result["error"] = _error_text(e)
  finally:
    for _, router in clients:
      router.close()
    if clients:
      failed = _in_parallel(lambda client: client[0].delete(), clients, DELETE_LIMIT)
      result["deleted"] = len(clients) - len(failed)
      if failed:
        common.log(f"  ⚠ {len(failed)} of {len(clients)} actors weren't deleted: {failed[0]}")
  return result


def main(argv) -> int:
  """The pod's entry point. Returns the process exit code."""
  import grpc  # pylint: disable=g-import-not-at-top  # only the runner image has grpc

  parser = argparse.ArgumentParser(prog="baseline-perf runner")  # not absl: the runner image has none
  parser.add_argument("--keep", action="store_true")
  parser.add_argument("--parks", default=",".join(common.PATHS))
  parser.add_argument("--template", default=common.TEMPLATE)
  parser.add_argument("--actors", type=int, default=0, help="one --capacity step: this many actors at once")
  parser.add_argument("--duration", type=float, default=60.0, help="with --actors: the seconds of load")
  args = parser.parse_args(argv)
  if args.actors:
    result = run_capacity(grpc, args.template, args.parks.split(",")[0], args.actors, args.duration)
  else:
    result = run(grpc, args.template, tuple(args.parks.split(",")), args.keep)
  print(common.RESULT_PREFIX + json.dumps(result, separators=(",", ":")), flush=True)
  return 1 if result["error"] or result.get("errors") else 0
