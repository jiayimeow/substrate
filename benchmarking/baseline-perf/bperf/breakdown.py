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
"""Where each Resume and park call's time went, from the servers' logs.

build_tree() assembles one call's log lines (see logs.py) into a tree: the root
is ateapi's handling of the call; under it, atelet's, with its phases
("Restore timing breakdown", ...); and under atelet's ateom call, ateom's
phases ("Actor restore phases", ...). What a level doesn't explain is its
"other" (or "ateapi (outside atelet)", "atelet (outside ateom ...)").

A tree node is {"name", "ms", "children"?, "detail"?, "derived"?, "parallel"?}.
"""

import re
import statistics
import time
from typing import Any

# Plain imports, not google3 ones: baseline-perf runs with plain python3.
from bperf import common
from bperf import kube as kube_lib
from bperf import logs

LOG_WAIT_S = 5  # keep re-reading this long for log lines not yet flushed
LOG_RETRY_S = 0.5
MSG_HANDLE_RPC = "Handle RPC"
SKEW_S = 0.050  # node clocks disagree a little; span containment allows this
MIN_REMAINDER_S = 0.001  # remainders below both of these are noise, not shown
MIN_REMAINDER_FRACTION = 0.02

# atelet method -> the ateom method it calls.
ATEOM_METHOD_FOR = {"Restore": "RestoreWorkload", "Checkpoint": "CheckpointWorkload", "Run": "RunWorkload"}
# ateom phase log -> the ateom method it belongs to.
ATEOM_PHASE_METHOD = {
    "Actor restore phases": "RestoreWorkload",
    "Actor boot phases": "RunWorkload",
    "Agent setup phases": "RunWorkload",
    "Actor checkpointed": "CheckpointWorkload",
}
# Per ateom phase log, the phases that run concurrently with the phase logged before them: a
# checkpoint tars the durable dir and the rootfs upper while cloud-hypervisor writes the snapshot.
PARALLEL_PHASES = {"Actor checkpointed": {"durable_dir", "rootfs_upper"}}
TOTAL_KEYS = {"total", "since_boot"}  # keys that sum the other phases
META_KEYS = {"time", "level", "msg", "id", "trace_id", "span_id", "trace_flags", "scope", "snapshot_files", "source"}
# atelet's per-phase keys, e.g. ate.actor.restore.duration.download, in seconds.
ATELET_PHASE_KEY = re.compile(r"^ate\.actor\.([a-z_]+)\.duration\.([a-z_]+)$")


class _Span:
  """One server's handling of one RPC, from its "Handle RPC" line."""

  def __init__(self, rec, elapsed):
    self.rec, self.component, self.method = rec, rec.component, rec.str("method")
    self.start, self.end = rec.time - elapsed, rec.time
    self.short = self.method.rsplit("/", 1)[-1]
    self.duration = self.end - self.start  # not elapsed: keeps the floats as they were
    self.phases, self.children = [], []

  def contains(self, t):
    return self.start - SKEW_S <= t <= self.end + SKEW_S


# ── Tree nodes ──────────────────────────────────────────────────────────────


def _node(name, seconds, detail="", derived=False, parallel=False, children=None):
  n = {"name": name, "s": seconds, "children": children or []}
  n.update({k: v for k, v in (("detail", detail), ("derived", derived), ("parallel", parallel)) if v})
  return n


def _remainder(name, seconds, parent):
  """The unexplained part of a parent; marked to drop if too small (a zero parent keeps it if positive)."""
  n = _node(name, seconds, derived=True)
  if seconds < MIN_REMAINDER_S or (parent > 0 and seconds < MIN_REMAINDER_FRACTION * parent):
    n["drop"] = True
  return n


def critical_path(nodes) -> float:
  """The time a row of sibling phases takes: a parallel run counts as its longest phase."""
  total = segment = 0.0
  for n in nodes:
    if n.get("parallel"):
      segment = max(segment, n["s"])
      continue
    total += segment
    segment = n["s"]
  return total + segment


def _finish(n):
  """Drops the marked remainders and turns seconds into milliseconds (to the microsecond)."""
  n["ms"] = round(n.pop("s") * 1e6) / 1000
  n["children"] = [_finish(c) for c in n["children"] if not c.get("drop")]
  if not n["children"]:
    del n["children"]
  return n


# ── Matching log lines to spans ─────────────────────────────────────────────


def _pick_parent(s, atelets):
  """The atelet span that forwarded an ateom span, or None.

  The one with the paired method whose window contains it, else any whose window
  contains it, else the paired one.
  """
  paired = containing = None
  for a in atelets:
    is_pair = ATEOM_METHOD_FOR.get(a.short) == s.short
    inside = a.contains(s.start) and a.contains(s.end)
    if is_pair and inside:
      return a
    if inside and containing is None:
      containing = a
    elif is_pair and paired is None:
      paired = a
  return containing or paired or (atelets[0] if len(atelets) == 1 else None)


def _pick_phase_owner(rec, spans, method):
  """The span a phase log belongs to, or None if there are none.

  The one with the expected method whose window contains the log, else the one
  containing it, else the one with the expected method, else the closest.
  """
  by_method = by_time = closest = None
  best = float("inf")
  for s in spans:
    is_method = bool(method) and s.short == method
    inside = s.contains(rec.time)
    if is_method and inside:
      return s
    if inside and by_time is None:
      by_time = s
    elif is_method and by_method is None:
      by_method = s
    if abs(rec.time - s.end) < best:
      best, closest = abs(rec.time - s.end), s
  return by_time or by_method or closest


def _atelet_owner_method(rec):
  """An atelet phase log's method (restore -> Restore), or '' if it isn't one."""
  if rec.component != "atelet" or rec.msg == MSG_HANDLE_RPC:
    return ""
  return next((m.group(1).capitalize() for k, _ in rec.fields if (m := ATELET_PHASE_KEY.match(k))), "")


# ── Phase values ────────────────────────────────────────────────────────────


def _seconds_value(v):
  """atelet's phase values: float seconds."""
  return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _nanos_value(v):
  """ateom's phase values: integer nanoseconds, or a Go duration string."""
  if isinstance(v, bool):
    return None
  if isinstance(v, int):
    return v / 1e9
  if isinstance(v, str):
    return int(v) / 1e9 if re.fullmatch(r"-?\d+", v) else logs.parse_go_duration(v)
  return None


def _phase_nodes(rec):
  """One ateom phase log -> nodes, in log order."""
  parallel = PARALLEL_PHASES.get(rec.msg, set())
  out = []
  for k, v in rec.fields:
    if k in TOTAL_KEYS or k.endswith("count") or k in META_KEYS or (d := _nanos_value(v)) is None:
      continue
    out.append(_node(k, d, detail="parallel" if k in parallel else "", parallel=k in parallel))
  return out


# ── Building the tree ───────────────────────────────────────────────────────


def _ateom_node(s):
  """An ateom span's node, with its phases as children."""
  n = _node("ateom " + s.short, s.duration)
  groups, containers, agent_setup = [], None, []
  for rec in s.phases:
    # Agent setup is logged first but happens inside the boot's "containers" phase, so it is placed
    # once the boot phases are known.
    if rec.msg == "Agent setup phases":
      agent_setup.append(rec)
      continue
    phases = _phase_nodes(rec)
    if rec.msg == "Actor boot phases":
      containers = next((p for p in phases if p["name"] == "containers"), containers)
    groups += phases
  for rec in agent_setup:
    phases = _phase_nodes(rec)
    if containers is None:
      groups += phases
      continue
    other = _remainder("other", containers["s"] - sum(p["s"] for p in phases), containers["s"])
    containers["children"] += phases + [other]
  n["children"] = groups
  if groups:
    n["children"].append(_remainder("other", n["s"] - critical_path(groups), n["s"]))
  return n


def _atelet_node(a):
  """An atelet span's node, with its phases and ateom calls as children."""
  n = _node("atelet " + a.short, a.duration)
  if not a.phases:
    # Only the ateom call is logged. What atelet did around it is the remainder; for a checkpoint
    # that is mostly the snapshot upload.
    n["children"] = [_ateom_node(c) for c in a.children]
    label = "atelet (outside ateom: snapshot upload, …)" if a.short == "Checkpoint" else "atelet (outside ateom)"
    if a.children:
      n["children"].append(_remainder(label, n["s"] - sum(c.duration for c in a.children), n["s"]))
    return n
  phase_log = a.phases[-1]
  if kind := phase_log.str("ate.snapshot.kind"):
    n["detail"] = "snapshot=" + kind
  total, nested = 0.0, False
  for k, v in phase_log.fields:
    m = ATELET_PHASE_KEY.match(k)
    if not m or m.group(2) in TOTAL_KEYS or (d := _seconds_value(v)) is None:
      continue
    p = _node(m.group(2), d)
    # atelet times its ateom call as one phase; the ateom span explains it.
    if m.group(2).startswith("ateom") and not nested:
      p["children"], nested = [_ateom_node(c) for c in a.children], True
    n["children"].append(p)
    total += d
  if not nested:
    n["children"] += [_ateom_node(c) for c in a.children]
  n["children"].append(_remainder("other", n["s"] - total, n["s"]))
  return n


def _spans(call, mine):
  """The call's spans: (ateapi's, atelet's, ateom's)."""
  root, atelets, ateoms = None, [], []
  for r in mine:
    if r.msg != MSG_HANDLE_RPC or (elapsed := logs.parse_go_duration(r.str("elapsed-time"))) is None:
      continue
    s = _Span(r, elapsed)
    if r.component == "ateapi":
      if root is None and s.method == call["method"]:
        root = s
    elif r.component == "atelet":
      atelets.append(s)
    elif r.component == "ateom":
      ateoms.append(s)
  atelets.sort(key=lambda s: s.start)
  ateoms.sort(key=lambda s: s.start)
  return root, atelets, ateoms


def _attach(root, atelets, ateoms, mine):
  """Hangs each ateom span under its atelet span, and each phase on its span."""
  for s in ateoms:
    (_pick_parent(s, atelets) or root).children.append(s)  # if no parent fits, keep the time visible
  for r in mine:
    if method := _atelet_owner_method(r):
      owner = _pick_phase_owner(r, atelets, method)
    elif r.component == "ateom" and (r.msg in ATEOM_PHASE_METHOD or r.msg.endswith(" phases")):
      owner = _pick_phase_owner(r, ateoms, ATEOM_PHASE_METHOD.get(r.msg, ""))
    else:
      continue
    if owner:
      owner.phases.append(r)


def build_tree(call: dict[str, Any], records):
  """The breakdown of one runner call -> (the tree, the log lines it expected but didn't find)."""
  mine = [r for r in records if r.trace_id == call["trace_id"]]
  root, atelets, ateoms = _spans(call, mine)
  name = call["method"].rsplit("/", 1)[-1]
  if root is None:
    seconds = call["server_us"] / 1e6 if call.get("server_us") else call["client_ms"] / 1000
    return _finish(_node(name, seconds)), ["ateapi log line for " + call["method"]]
  _attach(root, atelets, ateoms, mine)

  missing = []
  for a in atelets:
    want = ATEOM_METHOD_FOR.get(a.short)  # a failed atelet call may legitimately never have reached ateom
    if want and not any(c.short == want for c in a.children) and a.rec.get("err") is None:
      missing.append("ateom log line for " + want)
  top = _node(name, root.duration)
  if atelets:
    top["children"].append(_remainder("ateapi (outside atelet)", top["s"] - sum(a.duration for a in atelets), 0))
    top["children"] += [_atelet_node(a) for a in atelets]
  top["children"] += [_ateom_node(s) for s in root.children]
  return _finish(top), missing


def breakdowns(kube: kube_lib.Kube, calls):
  """Where the runner's Resume and park calls' time went.

  Re-reads the logs for up to LOG_WAIT_S until every breakdown is complete, since
  the last lines may not be flushed yet. Returns ({trace ID: tree, or None if the
  logs explain nothing}, problems reading the logs, the Records read).
  """
  wanted = [c for c in calls if c["method"].endswith(common.PARKED_CALLS) and "error" not in c]
  if not wanted:
    return {}, [], []
  since = min(logs.parse_time(c["start"]) for c in wanted) - logs.LOG_SLACK_S
  ids = [c["trace_id"] for c in wanted]
  deadline = time.monotonic() + LOG_WAIT_S
  while True:
    records, errors = logs.collect_records(kube, since, ids)
    trees, complete = {}, True
    for c in wanted:
      tree, missing = build_tree(c, records)
      trees[c["trace_id"]] = tree if tree.get("children") else None
      complete = complete and not missing
    if complete or (not records and errors) or time.monotonic() > deadline:
      return trees, errors, records
    time.sleep(LOG_RETRY_S)


def _critical_children(children):
  """The children on the critical path: from each parallel group, only its slowest phase."""
  groups = []
  for c in children:
    if c.get("parallel") and groups:
      groups[-1].append(c)
    else:
      groups.append([c])
  return [max(g, key=lambda c: c.get("ms", 0.0)) for g in groups]


def _leaves(node, parent=""):
  """Yields (label, ms) for the leaf phases on the critical path of a tree."""
  children = node.get("children") or []
  if not children:
    label = f"other in {parent}" if node["name"] == "other" and parent else node["name"]
    yield label + (" (parallel)" if node.get("parallel") else ""), node.get("ms", 0.0)
  for child in _critical_children(children):
    yield from _leaves(child, node["name"])


def top_phases(trees, n=5):
  """The slowest leaf phases over some trees, by median -> [(label, median ms)], slowest first."""
  samples = {}
  for tree in trees:
    if tree and tree.get("children"):
      for label, value in _leaves(tree):
        samples.setdefault(label, []).append(value)
  return sorted(((label, statistics.median(v)) for label, v in samples.items()), key=lambda kv: -kv[1])[:n]
