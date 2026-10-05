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
"""--capacity: how many actors one node takes at once before Resume or Suspend slows down.

Each step runs N actors at once on the worker's node, N = 1, 2, 4, 8 by default.
The runner pod (runner.run_capacity) creates and cold-starts them, then each goes
round Resume -> Exec -> Suspend for --duration seconds on its own, as the boomer
client's users do. Before each step the template's WorkerPool is scaled to N + 1
workers, one per actor and a spare, so that no resume waits for a worker: Pool
saves the original count in an annotation first and puts it back at the end, and
--capacity-restore does that after a run that was killed. A step's numbers leave
out its first WARMUP_S. It holds if no call failed and its Resume and Suspend
P90 are within SLOWDOWN times the first step's; the first step that doesn't is
over capacity, and the node's load during it, from the sampler pod, says what
ran out.
"""

import concurrent.futures
import itertools
import json
import re
import time
from typing import Any

# Plain imports, not google3 ones: baseline-perf runs with plain python3.
from bperf import atelet
from bperf import common
from bperf import kube as kube_lib
from bperf import report
from bperf import sampler

LEVELS = (1, 2, 4, 8)  # actors at once, step by step, by default
MAX_ACTORS = 64
DURATION_S = 60  # each step's load, by default
MIN_DURATION_S = 30
WARMUP_S = 15  # left out at the start of each step's load, while the actors' first cycles start
PARK = "Suspend"  # how each cycle parks the actor: a snapshot to object storage, the path that loads the disk
SLOWDOWN = 2.0  # a step is over capacity once its Resume or Suspend P90 is more than this times the first step's
POOL_LABEL = "ate.dev/worker-pool"  # on each worker pod: its WorkerPool's name
ANNOTATION = "bperf.ate.dev/original-replicas"  # on the WorkerPool while a run has it scaled: its replicas before
ACTIVE = "WORKER_STATE_ACTIVE"  # a worker that ate-api can put an actor on
WORKERS_TIMEOUT_S = 180  # scaling up: scheduling, starting, registering with ate-api
DELETE_LIMIT = 8  # kubectl-ate processes deleting actors at once
FULL_PCT = 90  # a disk this busy or this close to its provisioned IOPS or MiB/s, or CPUs this busy, ran out
STALL_PCT = 10  # PSI: tasks stalled on a resource this much of the time means it ran out
ROW = " {:>3}{:>10}{:>16}{:>17}{:>10}{:>7}{:>6}{:>7}{:>8}"


def parse_levels(text: str) -> tuple[int, ...]:
  """'1,2,4,8' -> (1, 2, 4, 8). Raises ValueError unless they're whole numbers going up from 1 to at most MAX_ACTORS."""
  try:
    levels = tuple(int(x) for x in text.split(","))
  except ValueError:
    raise ValueError(f"{text!r} isn't a list of whole numbers like 1,2,4,8") from None
  if levels[0] != 1:
    raise ValueError("the first step must be 1 actor: the other steps are compared with it")
  if any(b <= a for a, b in zip(levels, levels[1:])):
    raise ValueError(f"{text!r} must go up")
  if levels[-1] > MAX_ACTORS:
    raise ValueError(f"at most {MAX_ACTORS} actors at once")
  return levels


def _actors(n: int) -> str:
  return f"{n} actor{'' if n == 1 else 's'}"


# ── The WorkerPool and the actors ────────────────────────────────────────────


class Pool:
  """The template's WorkerPool: scale() sets how many workers it runs, restore() puts back how many it had."""

  def __init__(self, kube: kube_lib.Kube, namespace: str, name: str):
    self.kube, self.namespace, self.name = kube, namespace, name
    pool = self.get()
    saved = (pool["metadata"].get("annotations") or {}).get(ANNOTATION)
    self.leftover = saved is not None  # a stopped run left it scaled
    self.original = int(saved if saved is not None else pool["spec"].get("replicas", 1))
    self.changed = self.leftover  # whether restore() has anything to put back

  @classmethod
  def of_worker(cls, kube: kube_lib.Kube, worker) -> "Pool":
    """The WorkerPool of a worker, one of `kubectl-ate get workers`'."""
    namespace, name = worker.get("workerNamespace", ""), worker.get("workerPool", "")
    if not name:  # an older kubectl-ate: the worker pod's label says
      pod = kube.get_json("-n", namespace, "get", "pod", worker.get("workerPod", "")) or {}
      name = (pod.get("metadata", {}).get("labels") or {}).get(POOL_LABEL, "")
    if not name:
      raise common.Error(f"can't tell which WorkerPool worker {namespace}/{worker.get('workerPod')} belongs to")
    return cls(kube, namespace, name)

  def get(self):
    pool = self.kube.get_json("-n", self.namespace, "get", "workerpool", self.name)
    if not pool:
      raise common.Error(f"can't read WorkerPool {self.namespace}/{self.name}")
    return pool

  def scale(self, replicas: int) -> None:
    """Has the pool run this many workers, saving how many it had first."""
    if not self.changed:
      self.kube.kubectl("-n", self.namespace, "annotate", "workerpool", self.name, f"{ANNOTATION}={self.original}",
                        "--overwrite")
      self.changed = True
    self.kube.kubectl("-n", self.namespace, "scale", "workerpool", self.name, f"--replicas={replicas}")

  def restore(self) -> None:
    """Puts back how many workers the pool had, then removes the annotation."""
    if not self.changed:
      return
    self.kube.kubectl("-n", self.namespace, "scale", "workerpool", self.name, f"--replicas={self.original}")
    self.kube.kubectl("-n", self.namespace, "annotate", "workerpool", self.name, f"{ANNOTATION}-")
    self.changed = False


def scaled_pools(kube: kube_lib.Kube) -> list[tuple[str, str]]:
  """(namespace, name) of each WorkerPool that a stopped --capacity run left scaled."""
  items = (kube.get_json("get", "workerpools", "-A") or {}).get("items", [])
  return [(p["metadata"]["namespace"], p["metadata"]["name"]) for p in items
          if ANNOTATION in (p["metadata"].get("annotations") or {})]


def _pending(pods) -> str:
  """'; <pod> is Pending: <why>' for the first pod that is, or ''."""
  for p in pods:
    if p.get("status", {}).get("phase") == "Pending":
      why = next((c.get("message", "") for c in p["status"].get("conditions", [])
                  if c.get("type") == "PodScheduled" and c.get("status") == "False"), "")
      return f"; {p['metadata']['name']} is Pending: {why or 'not started yet'}"
  return ""


def wait_for_workers(kube: kube_lib.Kube, pool: Pool, node: str, want: int):
  """Waits until the pool runs exactly want workers, all on node, ready and registered with ate-api.

  Returns (the seconds it waited, those workers as kubectl-ate lists them).
  """
  started = time.monotonic()
  while True:
    pods = (kube.get_json("-n", pool.namespace, "get", "pods", "-l", f"{POOL_LABEL}={pool.name}") or {}).get("items", [])
    pods = [p for p in pods if not p["metadata"].get("deletionTimestamp")]
    elsewhere = sorted({p.get("spec", {}).get("nodeName") for p in pods} - {node, None, ""})
    if elsewhere:
      raise common.Error(f"WorkerPool {pool.namespace}/{pool.name} put workers on {', '.join(elsewhere)} too: "
                         f"--capacity measures one node, {node}")
    ready = {p["metadata"]["name"] for p in pods if atelet.ready(p)}
    listed = json.loads(kube.ate("get", "workers", "-o", "json").stdout or "{}").get("workers", [])
    workers = [w for w in listed if w.get("workerNamespace") == pool.namespace and w.get("workerPod") in ready
               and w.get("status", {}).get("state") == ACTIVE]
    if len(pods) == want and len(workers) == want:
      return time.monotonic() - started, workers
    if time.monotonic() - started > WORKERS_TIMEOUT_S:
      raise common.Error(f"WorkerPool {pool.namespace}/{pool.name} has {len(workers)} of {want} workers ready "
                         f"after {WORKERS_TIMEOUT_S} s{_pending(pods)}")
    time.sleep(2)


def leftover_actors(kube: kube_lib.Kube) -> list[str]:
  """The --capacity actors that are there, which only a stopped run leaves: the runner deletes its own."""
  listed = json.loads(kube.ate("get", "actors", "-a", common.NAMESPACE, "-o", "json").stdout or "{}")
  names = (a.get("metadata", {}).get("name", "") for a in listed.get("actors", []))
  mine = [n for n in names if re.fullmatch(rf"{re.escape(common.ACTOR_NAME)}-\d+", n)]
  return sorted(mine, key=lambda n: int(n.rsplit("-", 1)[1]))


def delete_actors(kube: kube_lib.Kube, names) -> None:
  """Deletes actors in any state, DELETE_LIMIT at a time; one that's already gone is fine. Raises if any other fails."""

  def delete(name: str) -> str:
    proc = kube.ate("delete", "actor", name, "-a", common.NAMESPACE, "--any-state", check=False)
    out = (proc.stderr or proc.stdout).strip()
    return "" if proc.returncode == 0 or "NotFound" in out else f"{name}: {out}"

  with concurrent.futures.ThreadPoolExecutor(max_workers=DELETE_LIMIT) as pool:
    errors = [e for e in pool.map(delete, names) if e]
  if errors:
    raise common.Error(f"couldn't delete {len(errors)} of {len(names)} actors: {errors[0]}")


# ── A step's numbers ─────────────────────────────────────────────────────────


def percentile(values, pct: float) -> float | None:
  """The pct-th percentile, interpolating between the two nearest values as numpy does by default; None if empty."""
  if not values:
    return None
  s = sorted(values)
  k = (len(s) - 1) * pct / 100
  lo = int(k)
  hi = min(lo + 1, len(s) - 1)
  return s[lo] + (s[hi] - s[lo]) * (k - lo)


def step_stats(result) -> dict[str, Any]:
  """A step's numbers, from the runner's results: the cycles that started after the warmup, their rate and percentiles.

  {"actors", "error", "errors", "deleted", "cycles": how many counted, "per_s", "resume" / "park" / "exec":
  {"p50", "p90"} in ms or None, "busy": the counted resumes' waits for a free worker, "window": (start, end) of the
  counted load in Unix seconds or None, "node": the node's load in it (the caller adds it), "verdict": ""}.
  """
  step = {"actors": result.get("actors"), "error": result.get("error"), "errors": result.get("errors") or [],
          "deleted": result.get("deleted", 0), "cycles": 0, "per_s": None, "resume": None, "park": None,
          "exec": None, "busy": [], "window": None, "node": None, "verdict": ""}
  load_start = result.get("load_start")
  if load_start is None:
    return step
  begin = load_start + WARMUP_S
  end = min([load_start + (result.get("duration") or 0)] + [e["time"] for e in step["errors"]])  # a failure ends it
  if end <= begin:
    return step
  counted = [c for c in result.get("cycles", []) if begin <= c["start"] < end]
  step.update(cycles=len(counted), per_s=len(counted) / (end - begin), window=(begin, end),
              busy=[c["busy_ms"] for c in counted if c.get("busy_ms")])
  for key in ("resume", "park", "exec"):
    values = [c[f"{key}_ms"] for c in counted]
    if values:
      step[key] = {"p50": percentile(values, 50), "p90": percentile(values, 90)}
  return step


def verdict(step, base) -> str:
  """Why a step is over capacity, or '' if it holds. base: the first step, whose P90s the others are held to."""
  if step["errors"]:
    n = len(step["errors"])
    return f"{n} call{'' if n == 1 else 's'} failed: {step['errors'][0]['error']}"
  if step["error"]:
    return step["error"]
  if not step["cycles"]:
    return f"no cycle started after the first {WARMUP_S} s"
  if base is not step:
    for key, name in (("resume", "Resume"), ("park", PARK)):
      if base[key] and step[key]["p90"] > SLOWDOWN * base[key]["p90"]:
        return (f"{name} P90 {step[key]['p90']:,.0f} ms > {SLOWDOWN:g} × {base[key]['p90']:,.0f} ms "
                f"(its P90 with {_actors(base['actors'])})")
  return ""


def disk_limits(setup) -> tuple[int | None, int | None]:
  """(IOPS, MiB/s) provisioned for the node's boot disk, from the header's disk row; None where it doesn't say."""
  limits = {}
  for item in next((items for label, items in setup if label == "disk"), []):
    m = re.fullmatch(r"([\d,]+) (IOPS|MiB/s)", item)
    if m:
      limits[m.group(2)] = int(m.group(1).replace(",", ""))
  return limits.get("IOPS"), limits.get("MiB/s")


def _disk(load, setup):
  """(['96% busy', '280 of 290 MiB/s', '3,400 of 3,600 IOPS'], whether the disk ran out)."""
  iops, mibps = disk_limits(setup)
  parts, full = [f"{load['disk_pct']:.0f}% busy"], load["disk_pct"] >= FULL_PCT
  for value, limit, unit in ((load.get("disk_mibps", 0.0), mibps, "MiB/s"), (load.get("disk_iops", 0.0), iops, "IOPS")):
    parts.append(f"{value:,.0f} of {limit:,} {unit}" if limit else f"{value:,.0f} {unit}")
    full = full or bool(limit and value >= FULL_PCT / 100 * limit)
  return parts, full


def saturated(load, setup) -> list[str]:
  """What ran out on the node under load, e.g. ['disk (96% busy · 280 of 290 MiB/s · ...)']; [] if nothing did."""
  psi = load.get("psi", {})
  out = []
  parts, full = _disk(load, setup)
  if full or psi.get("io", 0) >= STALL_PCT:
    out.append("disk (" + " · ".join(parts + [f"PSI io {psi.get('io', 0):.0f}%"]) + ")")
  if psi.get("memory", 0) >= STALL_PCT:
    out.append(f"memory (PSI memory {psi['memory']:.0f}%)")
  ncpu = load.get("ncpu")
  if psi.get("cpu", 0) >= STALL_PCT or (ncpu and load["busy_cores"] >= FULL_PCT / 100 * ncpu):
    cores = f"{load['busy_cores']:.0f} of {ncpu} cores busy" if ncpu else f"{load['busy_cores']:.0f} busy cores"
    out.append(f"CPU ({cores} · PSI cpu {psi.get('cpu', 0):.0f}%)")
  return out


def node_text(load, setup) -> str:
  """'6.0 of 192 cores busy · disk 34% busy, 98 of 290 MiB/s, 1,200 of 3,600 IOPS · PSI io 2%, cpu 0%, memory 0%'."""
  ncpu = load.get("ncpu")
  cores = f"{load['busy_cores']:.1f} of {ncpu} cores busy" if ncpu else f"{load['busy_cores']:.1f} busy cores"
  parts, _ = _disk(load, setup)
  psi = ", ".join(f"{r} {load['psi'][r]:.0f}%" for r in sampler.PSI_RESOURCES if r in load.get("psi", {}))
  return " · ".join([cores, "disk " + ", ".join(parts)] + ([f"PSI {psi}"] if psi else []))


# ── Output ───────────────────────────────────────────────────────────────────


def step_lines(step, first: bool, setup) -> list[str]:
  """The progress lines after a step: whether it held, its numbers, and the node's load."""
  parts = [f"{step['per_s']:.2f} cycles/s"] if step["per_s"] is not None else []
  parts += [f"{name} P90 {step[key]['p90']:,.0f} ms" for key, name in (("resume", "Resume"), ("park", PARK)) if step[key]]
  numbers = " · ".join(parts)
  if step["verdict"]:
    lines = [f"  ✗ over capacity: {step['verdict']}"] + ([f"    {numbers}"] if numbers else [])
  else:
    lines = [f"  ✔ {'baseline' if first else 'holds'}: {numbers}"]
  if step["node"]:
    lines.append(f"    node {node_text(step['node'], setup)}")
  return lines


def _pair(stats) -> str:
  """'1,030 / 1,190': P50 / P90 in ms; '–' if there are none."""
  return "–" if not stats else f"{stats['p50']:,.0f} / {stats['p90']:,.0f}"


def table_lines(steps, color: bool = False) -> list[str]:
  """The table: per step, its cycles/s, Resume and Suspend P50 / P90, Exec P50 (ms), and the node's load."""
  head = ROW.format("N", "cycles/s", "Resume P50/P90", f"{PARK} P50/P90", "Exec P50", "cores", "disk", "MiB/s", "PSI io")
  lines = [report.paint(head, report.BOLD, color)]
  for step in steps:
    cells = [step["actors"], "–" if step["per_s"] is None else f"{step['per_s']:.2f}", _pair(step["resume"]),
             _pair(step["park"]), f"{step['exec']['p50']:,.0f}" if step["exec"] else "–"]
    node = step["node"]
    if node:
      io = node.get("psi", {}).get("io")
      cells += [f"{node['busy_cores']:.1f}", f"{node['disk_pct']:.0f}%", f"{node.get('disk_mibps', 0):,.0f}",
                "–" if io is None else f"{io:.0f}%"]
    else:
      cells += ["–"] * 4
    line = ROW.format(*cells)
    lines.append(report.paint(line + "  ✗", report.RED, color) if step["verdict"] else line)
  return lines


def conclusion(steps, setup, error: str | None) -> list[str]:
  """The report's verdict: how many actors the node takes at once, then what ran out first or how much room was left."""
  failed = next((i for i, s in enumerate(steps) if s["verdict"]), None)
  if failed == 0:
    return [f"No capacity measured: with 1 actor, {steps[0]['verdict']}"]
  if failed is None:
    if not steps:
      return []
    last = steps[-1]
    if error:
      return [f"Capacity: at least {_actors(last['actors'])} at once; the run stopped before the next step"]
    lines = [f"Capacity: at least {_actors(last['actors'])} at once: every step held"]
    if last["node"]:
      out = saturated(last["node"], setup)
      room = (f"near a limit already: {' and '.join(out)}" if out
              else f"the node still had room: {node_text(last['node'], setup)}")
      lines.append(f"At {_actors(last['actors'])}, {room}")
    return lines
  step = steps[failed]
  lines = [f"Capacity: {_actors(steps[failed - 1]['actors'])} at once; with {step['actors']}, {step['verdict']}"]
  if not step["node"]:
    lines.append("Ran out first: unknown: the node's load during the step wasn't measured")
  else:
    out = saturated(step["node"], setup)
    lines.append("Ran out first: " + (" and ".join(out) if out else "nothing on the node: CPU, disk and memory had "
                                      "room, so look at atelet, ate-api, GCS or the network"))
  return lines


def print_report(steps, setup, node, elapsed_s: float, duration_s: float, error: str | None = None) -> None:
  """Prints the report: what it ran on, a row per step, how many actors the node takes, and what ran out first.

  node: the node's load before the run (cli._Run.node).
  """
  if not steps:
    return
  color = report.use_color()
  rule = report.paint("═" * common.WIDTH, report.CYAN, color)
  common.log()
  common.log(rule)
  title = f" baseline-perf --capacity · {common.TEMPLATE} · {PARK.lower()} path · {elapsed_s / 60:.0f} min"
  common.log(report.paint(title, report.BOLD, color))
  for line in report.headline(setup):
    common.log(report.paint(f" {line}", report.DIM, color))
  for line in report.node_lines(node, color=color):
    common.log(f" {line}")
  common.log(rule)
  for line in table_lines(steps, color=color):
    common.log(line)
  common.log()
  for i, line in enumerate(conclusion(steps, setup, error)):
    common.log(report.paint(f" {line}", report.BOLD, color) if i == 0 else f" {line}")
  notes = [(f"latencies in ms, over the last {duration_s - WARMUP_S:g} s of each {duration_s:g} s step; each actor "
            f"goes round Resume -> Exec -> {PARK} back to back, with a spare worker", False)]
  busy = list(itertools.chain.from_iterable(s["busy"] for s in steps))
  if busy:
    notes.append((f"{len(busy)} resume{'' if len(busy) == 1 else 's'} waited up to {max(busy):,.0f} ms "
                  "for a free worker (not counted)", False))
  over = sampler.busy_reasons(node["before"]) if (node or {}).get("before") else []
  if over:
    notes.append((f"the node was busy before the run ({', '.join(over)}): the steps may include other load", True))
  elif (node or {}).get("error"):
    notes.append((f"couldn't watch the node, so what ran out is unknown: {node['error']}", True))
  if error:
    notes.append((f"stopped early: {error}", True))
  common.log()
  for note, warn in notes:
    common.log(report.paint(f" Note: {note}", report.YELLOW if warn else report.DIM, color))
