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
"""baseline-perf: SWE-Perf single-actor benchmark for Substrate.

Replays the 21-step astropy trace on one actor as 4 cycles of Resume ->
Execute -> park (split the same way as the boomer client), once per path:

  pause path     each cycle ends with PauseActor: a checkpoint kept on the node
  suspend path   each cycle ends with SuspendActor: a snapshot to object storage

Each path starts from a new actor, whose first resume (the cold start) restores
the template's golden snapshot. The report gives one row of medians per path
and where each path's time goes.

Everything that is timed runs in a pod in the cluster, which calls ate-api over
gRPC and the actor through atenet-router directly: no number includes a
kubectl-ate process or a port-forward. This script checks the cluster, starts
that pod, relays its progress and prints the report. Another small pod, on the
worker's node, reads the node's CPU, disk and pressure counters
(bperf/sampler.py): the report says how busy the node was before and during
the run, and warns if it wasn't idle to begin with.

  ./baseline-perf               # both paths, then the report (about 2 minutes)
  ./baseline-perf --keep        # leave the actor behind afterwards, to debug
  ./baseline-perf --guest kernel=./vmlinux,rootfs=./rootfs.img  # another guest
  ./baseline-perf --guest ./my-guest/  # the files assemble.sh writes, any of
  ./baseline-perf --guest NAME  # a SandboxConfig that's already on the cluster
  ./baseline-perf --compare kernel=./vmlinux  # compare stock vs a change
  ./baseline-perf --compare atelet=~/substrate@v0.1.0 atelet=~/substrate
  ./baseline-perf --capacity    # how many actors at once before Resume or Suspend slows down
  ./baseline-perf --capacity 1,2,4,8,16 --duration 90

--guest uploads the files to the template's bucket under
kata-assets/custom/<sha256>/, makes a SandboxConfig that differs from the
template's only in those assets, and runs on a temporary copy of the template,
with its own golden snapshot; the copy is deleted afterwards. The SandboxConfig
and uploads stay for the next run on that guest; --cleanup-guest removes them.

--compare runs the whole benchmark --rounds times on each of two setups,
alternating A, B, A, B, ..., then prints how they compare. With one SPEC, arm A
is 'stock' (the cluster as it is) and arm B is SPEC; with two, A and B. Each
spec is KEY=VALUE pairs, comma-separated, or 'stock'. Keys:
atelet=IMAGE|DIR[@REF] (builds ./cmd/atelet with ko when given a local checkout
or DIR@git-ref), kernel=PATH, rootfs=PATH, kata-config=PATH,
cloud-hypervisor=PATH, virtiofsd=PATH, or guest=DIR|CONFIG. Each arm runs on its
own temporary copy of the template (and its own atelet layer cache when atelet
is compared). At the end the cluster is back as it was; --compare-restore does
that after a run that was killed.

--capacity runs steps of N actors at once on the worker's node (1, 2, 4, 8 by
default), each actor going round Resume -> Exec -> Suspend on its own for
--duration seconds (default 60; the first 15 aren't counted). For each step the
template's WorkerPool is scaled to N + 1 workers, and at the end back to what it
had; --capacity-restore does that after a run that was killed. The first step
in which a call fails, or Resume or Suspend P90 is more than twice the 1-actor
step's, is over capacity, and the report says what ran out on the node then.

The pod runs bperf/runner.py on an image with python3 and grpcio. The first run
builds that image with docker and pushes it to $KO_DOCKER_REPO; --runner-image
names an existing one instead.

Needs, on the current kube context: atespace benchmark-workloads holding
ActorTemplate swebench-astropy-7336 with a ready golden snapshot, and a
registered worker for it. All of that is checked before anything is created.
Where the Resume and park time went comes from the ateapi, atelet and ateom
logs, read with `kubectl logs` after the run (see bperf/breakdown.py).
kubectl-ate, chosen with --kubectl-ate or $KUBECTL_ATE, only checks the cluster
and cleans up.
"""

# argparse rather than absl, and plain imports rather than google3 ones: this runs with plain python3.
import argparse
import dataclasses
import json
import os
import shutil
import textwrap
import time
from typing import Any

from bperf import ab
from bperf import atelet
from bperf import breakdown
from bperf import capacity
from bperf import cluster_info
from bperf import common
from bperf import guest as guest_lib
from bperf import kube as kube_lib
from bperf import logs
from bperf import pod
from bperf import report
from bperf import sampler

IDLE_WAIT_SLACK_S = 30  # how much longer than the idle window to wait for the sampler's samples


def _parser() -> argparse.ArgumentParser:
  """The command line's flags; the module docstring is --help's text."""
  files = ", ".join(name for _, name in guest_lib.ASSETS.values())
  guest_keys = ", ".join(guest_lib.ASSETS)
  guest_help = (f"run on other guest assets: kernel=PATH[,rootfs=PATH,...] (keys: {guest_keys}), "
                f"a directory holding {files}, or the name of a SandboxConfig on the cluster. "
                "Runs on a temporary copy of the template, deleted afterwards")
  compare_help = (
      "compare two setups: run the benchmark on each in turn, A, B, A, B, ..., then compare them. "
      f"With one SPEC, arm A is '{ab.STOCK}' (the cluster as it is) and arm B is SPEC; with two, A and B. "
      f"A spec is KEY=VALUE[,KEY=VALUE...] or '{ab.STOCK}'. Keys: atelet=IMAGE|DIR[@REF], {guest_keys}, "
      "or guest=DIR|CONFIG. Everything is put back at the end"
  )
  parser = argparse.ArgumentParser(prog="baseline-perf", description=__doc__,
                                   formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument("--only", choices=("pause", "suspend"),
                      help="run only one path (default: both, pause then suspend)")
  parser.add_argument("--keep", action="store_true", help="don't delete the actor at the end")
  parser.add_argument("--kubectl-ate", default=os.environ.get("KUBECTL_ATE", "kubectl-ate"),
                      help="kubectl-ate binary to use (default: $KUBECTL_ATE or kubectl-ate)")
  parser.add_argument("--runner-image", default=os.environ.get("BASELINE_PERF_RUNNER_IMAGE"),
                      help="image for the runner pod, with python3 and grpcio "
                           "(default: $BASELINE_PERF_RUNNER_IMAGE, else built once in $KO_DOCKER_REPO)")
  parser.add_argument("--guest", metavar="ASSETS", help=guest_help)
  parser.add_argument("--cleanup-guest", action="store_true",
                      help="with --guest or --compare guest files: also remove the SandboxConfig and uploads it made "
                           "(kept by default, so the next run reuses them)")
  parser.add_argument("--compare", nargs="+", metavar="SPEC", help=compare_help)
  parser.add_argument("--ab", dest="compare", nargs="+", help=argparse.SUPPRESS)
  parser.add_argument("--rounds", type=int, metavar="N",
                      help=f"with --compare: how many runs on each arm (default {ab.ROUNDS})")
  parser.add_argument("--compare-restore", action="store_true",
                      help="undo what a stopped --compare run left: put back the atelet DaemonSet, delete the "
                           "template copies and the layer caches; then exit")
  parser.add_argument("--ab-restore", dest="compare_restore", action="store_true", help=argparse.SUPPRESS)
  levels = ",".join(map(str, capacity.LEVELS))
  capacity_help = (f"measure how many actors the node takes at once: a step of N actors at once for each N in "
                   f"LEVELS (default {levels}), until a call fails or Resume or Suspend P90 is more than "
                   f"{capacity.SLOWDOWN:g} times the 1-actor step's. Scales the template's WorkerPool to N + 1 "
                   "workers for each step, and back at the end")
  duration_help = (f"with --capacity: the seconds of load in each step (default {capacity.DURATION_S}, at least "
                   f"{capacity.MIN_DURATION_S}); the first {capacity.WARMUP_S} aren't counted")
  parser.add_argument("--capacity", nargs="?", const=levels, metavar="LEVELS", help=capacity_help)
  parser.add_argument("--duration", type=int, metavar="SECONDS", help=duration_help)
  parser.add_argument("--capacity-restore", action="store_true",
                      help="undo what a stopped --capacity run left: delete its pods and actors, and put the "
                           "WorkerPool back to its size; then exit")
  return parser


# ── Preflight ────────────────────────────────────────────────────────────────


def _check_template(kube, tmpl, error, context) -> list[str]:
  """The problems with the template; prints what is fine."""
  name = f"{common.NAMESPACE}/{common.TEMPLATE}"
  if error:
    return [f"ActorTemplate {name} is missing: the SWE-Perf environment (atespace, WorkerPool, template) "
            f"isn't deployed on {context}. ({error})"]
  golden = tmpl.get("status", {}).get("goldenSnapshotStatus", {})
  if golden.get("errorMessage"):
    return [f"The golden snapshot of {common.TEMPLATE} failed: {golden['errorMessage']}"]
  if not (golden.get("goldenSnapshot") or golden.get("goldenTag")):  # v0.1.0, newer
    return [f"The golden snapshot of {common.TEMPLATE} isn't ready yet (it needs a worker and about a minute). Check: "
            f"{kube.kubectl_ate} get actor-template {common.TEMPLATE} -a {common.NAMESPACE} -o yaml"]
  common.log(f"  template  {name}, golden snapshot ready")
  return []


def _matching_workers(tmpl, workers) -> list[dict[str, Any]]:
  """The workers that the template's workerSelector picks; prints how many."""
  want = tmpl.get("workerSelector", {}).get("matchLabels", {})
  mine = [w for w in workers if want.items() <= w.get("labels", {}).items()]
  selector = ",".join(f"{k}={v}" for k, v in want.items())
  common.log(f"  workers   {len(mine)} of {len(workers)} registered workers match {selector}")
  if not mine:
    common.log("  ⚠ no registered worker matches the template's workerSelector; resume will likely find no worker")
  return mine


def _preflight(kube: kube_lib.Kube, ab_run: bool = False, capacity_run: bool = False):
  """Checks everything the run needs and raises with all problems at once. Returns (template, workers).

  ab_run: a --compare run that changes atelet itself, so it isn't warned about.
  capacity_run: a --capacity run, which scales the WorkerPool itself, so it isn't warned about either.
  """
  if not shutil.which(kube.kubectl_ate):
    raise common.Error(f"{kube.kubectl_ate} is not on PATH (try: export PATH=$PATH:$HOME/go/bin)")
  if not shutil.which("kubectl"):
    raise common.Error("kubectl is not on PATH")
  context = kube.current_context()
  common.log(f"  context   {context}")
  problems, mine = [], []
  workers = json.loads(kube.ate("get", "workers", "-o", "json").stdout or "{}").get("workers", [])
  if not workers:
    problems.append("No workers are registered with ate-api, so no actor can start. On this cluster that has meant "
                    "ate-controller can't reach ate-api: look for x509 errors in "
                    "`kubectl -n ate-system logs deploy/ate-controller`.")
  tmpl, error = kube.get_template(common.TEMPLATE)
  problems += _check_template(kube, tmpl, error, context)
  if not error and workers:
    mine = _matching_workers(tmpl, workers)
  changed = [] if ab_run else atelet.changed_daemonsets(kube)
  if changed:
    common.log(f"  ⚠ a --compare run that was stopped left DaemonSet {', '.join(changed)} changed, so atelet may "
               "not be the stock one. Put it back with: baseline-perf --compare-restore")
  scaled = [] if capacity_run else capacity.scaled_pools(kube)
  if scaled:
    common.log(f"  ⚠ a --capacity run that was stopped left WorkerPool {', '.join(f'{ns}/{n}' for ns, n in scaled)} "
               "scaled up. Put it back with: baseline-perf --capacity-restore")
  if problems:
    fill = dict(initial_indent="    - ", subsequent_indent="      ", break_on_hyphens=False, break_long_words=False)
    raise common.Error("preflight found problems:\n"
                       + "\n".join(textwrap.fill(p, common.WIDTH, **fill) for p in problems))
  return tmpl, mine


# ── The run ──────────────────────────────────────────────────────────────────


@dataclasses.dataclass
class _Run:
  """What a run has done so far: what the report and the cleanup need."""
  parks: tuple[str, ...]  # the paths to run, in order
  total: int  # the progress output's step count
  setup: list[tuple[str, list[str]]] = dataclasses.field(default_factory=list)  # the header's facts
  result: dict[str, Any] = dataclasses.field(default_factory=dict)  # the runner's results
  pod_created: bool = False  # the runner or sampler pod may exist
  error: str | None = None
  trees: dict[str, Any] | None = None  # breakdown trees, None until built
  log_errors: list[str] = dataclasses.field(default_factory=list)  # why the server logs couldn't be read
  sizes: dict[str, Any] | None = None  # the suspend path's snapshot sizes
  records: list[Any] = dataclasses.field(default_factory=list)  # the server log lines of the run's calls (logs.Record)
  guest: guest_lib.Guest = dataclasses.field(default_factory=guest_lib.Guest)
  # The worker node's load (see sampler.load): "before" and "during" the run, or "error" saying why it's unknown.
  node: dict[str, Any] = dataclasses.field(default_factory=dict)

  def has_cycles(self) -> bool:
    return any(p.get("cycles") for p in self.result.get("paths", []))


def _watch_node(kube: kube_lib.Kube, image: str, nodes, run: _Run) -> bool:
  """Starts the sampler pod on the worker's node. Returns whether it runs; says why if it doesn't."""
  if not nodes:
    run.node["error"] = "the workers don't say which node they run on"
  else:
    if len(nodes) > 1:
      common.log(f"  ⚠ the template's workers are on {len(nodes)} nodes; this watches only {nodes[0]}")
    try:
      pod.start_sampler(kube, image, nodes[0])
      return True
    except common.Error as e:
      run.node["error"] = str(e)
  common.log(f"  ⚠ can't watch the node, so the report won't say if it was idle: {run.node['error']}")
  return False


def _idle_load(kube: kube_lib.Kube, node: dict[str, Any]):
  """Waits until the sampler has watched the node for IDLE_WINDOW_S, and says whether it's idle. Returns its load."""
  wait_s = sampler.IDLE_WINDOW_S + IDLE_WAIT_SLACK_S
  deadline = time.monotonic() + wait_s
  while True:
    lines = pod.sampler_log(kube)
    samples = sampler.parse(lines)
    if len(samples) > 1 and samples[-1]["m"] - samples[0]["m"] >= sampler.IDLE_WINDOW_S:
      break
    other = [line for line in lines if not line.startswith(sampler.SAMPLE_PREFIX)]  # a traceback, say
    if other or time.monotonic() > deadline:
      node["error"] = (f"the sampler pod failed: {other[-1]}" if other else
                       f"the sampler pod printed {len(samples)} samples in {wait_s} s")
      common.log(f"  ⚠ {node['error']}")
      return None
    time.sleep(1)
  idle = sampler.load(samples[0], samples[-1])
  over = sampler.busy_reasons(idle)
  if over:
    common.log(f"  ⚠ busy: {', '.join(over)}. The numbers may include contention, not just one actor's cost")
  else:
    common.log(f"  ✔ idle over {idle['seconds']:.0f} s: {sampler.describe(idle)}")
  return idle


def _call_span(result):
  """(the first ate-api call's start, the last one's end) in Unix seconds, or None if there were no calls."""
  times = [logs.parse_time(c.get(key)) for c in result.get("calls", []) for key in ("start", "end")]
  times = [t for t in times if t is not None]
  return (min(times), max(times)) if times else None


def _measure(kube: kube_lib.Kube, args, run: _Run, arm_template=None, cached_setup=None) -> None:
  """Checks the cluster and that its node is idle, runs the runner pod, and reads where the time went.

  arm_template: with --ab, the arm's copy of the template, to run on instead of the template.
  cached_setup: with --ab, the arm's cluster_info.describe_setup() rows, read before the idle window.
  """
  common.log(f"[1/{run.total}] Preflight checks...")
  template, workers = _preflight(kube, ab_run=arm_template is not None)
  image = pod.runner_image(args.runner_image)
  run_tmpl = arm_template or template
  if args.guest:
    guest_lib.prepare(kube, args.guest, template, workers, run.guest)
    run_tmpl = run.guest.template or template
    worker = next((w for w in workers if w.get("workerPod")), None)
    if run.guest.template and worker:
      atelet.flush_node(kube, worker)

  nodes = cluster_info.worker_nodes(workers)
  common.log(f"[2/{run.total}] Checking that node {nodes[0] if nodes else '?'} is idle ({sampler.IDLE_WINDOW_S} s)...")
  run.pod_created = True
  watching = _watch_node(kube, image, nodes, run)
  # In the idle window on a single run (hides its ~8 s); in --compare, cached once per arm before the idle window.
  run.setup = cached_setup if cached_setup is not None else cluster_info.describe_setup(kube, workers)
  if watching:
    run.node["before"] = _idle_load(kube, run.node)

  common.log(f"[3/{run.total}] Starting pod {common.RUNNER_POD}, which makes every timed call...")
  # The last suspend's snapshot is deleted with the actor, so after a suspend path the runner leaves
  # the actor to us: its GCS size is read below, then it's deleted.
  leave_actor = args.keep or "Suspend" in run.parks
  started, waited = pod.start(kube, image, leave_actor, run.parks, run_tmpl["metadata"]["name"])
  node = started.get("spec", {}).get("nodeName", "")
  common.log(f"  ✔ running on {node} after {waited:.0f} s")
  run.result = pod.relay(kube)
  run.error = run.result.get("error")
  span = _call_span(run.result)
  if watching and span:
    run.node["during"] = sampler.window(sampler.parse(pod.sampler_log(kube)), *span)
  if run.has_cycles():
    run.trees, run.log_errors, run.records = breakdown.breakdowns(kube, run.result.get("calls", []))
    storage = (run_tmpl.get("snapshotConfig") or run_tmpl.get("snapshotsConfig") or {}).get("storageLocation", "")
    suspend = [p for p in run.result["paths"] if p["park"] == "Suspend"]
    run.sizes = report.snapshot_sizes(suspend[0], run.records, storage) if suspend else None


def _clean_up(kube: kube_lib.Kube, args, run: _Run) -> None:
  """Removes what the run made: the pod, the actor, then what --guest made."""
  guest = run.guest
  if run.pod_created or guest.started():
    common.log(f"[{run.total}/{run.total}] Cleaning up...")
  if run.pod_created:
    pod.delete(kube)
    if args.keep:
      common.log(f"  kept {common.NAMESPACE}/{common.ACTOR_NAME}; remove it with: "
                 f"{kube.kubectl_ate} delete actor {common.ACTOR_NAME} -a {common.NAMESPACE} --any-state")
    elif not run.result.get("deleted"):  # the runner left the actor to us, was stopped, or failed to delete it
      try:
        kube.ate("delete", "actor", common.ACTOR_NAME, "-a", common.NAMESPACE, "--any-state", check=False)
      except common.Error as e:
        common.log(f"  ⚠ {e}")
  try:
    name = (guest.template or {}).get("metadata", {}).get("name")
    gone = not name
    if name and args.keep:
      common.log(f"  kept ActorTemplate {common.NAMESPACE}/{name} (the actor uses it); remove it with: "
                 f"{kube.kubectl_ate} delete actor-template {name} -a {common.NAMESPACE}")
    elif name and guest_lib.delete_template(kube, guest.template):
      gone = True
      common.log(f"  deleted the temporary ActorTemplate {name} and its snapshots")
    if args.cleanup_guest and guest.started() and gone:
      guest_lib.cleanup(kube, guest)
    elif guest.files and guest.config:
      common.log(f"  kept SandboxConfig {guest.config} for the next run on this guest (--cleanup-guest removes it)")
  except common.Error as e:
    common.log(f"  ⚠ {e}")


# ── --compare ────────────────────────────────────────────────────────────────


def _ab_clean_up(kube: kube_lib.Kube, args, swap, templates, worker, arm_guests) -> None:
  """Deletes the arms' templates, puts the stock atelet back, removes layer caches, and cleans up guest files."""
  has_guests = any(g.started() for g in arm_guests.values())
  if not templates and not (swap and swap.changed) and not has_guests:
    return
  common.log()
  common.log("━━ Cleaning up " + "━" * 40)
  for tmpl in templates.values():
    try:
      if guest_lib.delete_template(kube, tmpl):
        common.log(f"  deleted the temporary ActorTemplate {tmpl['metadata']['name']} and its snapshots")
    except common.Error as e:
      common.log(f"  ⚠ {e}")
  if swap and swap.changed:
    try:
      swap.restore()
      common.log(f"  ✔ the stock atelet is back: DaemonSet {swap.name} runs {ab.short_image(swap.stock['image'])}")
    except common.Error as e:
      common.log(f"  ⚠ couldn't put the stock atelet back: {e}. Try again with: baseline-perf --compare-restore")
  if worker:
    atelet.remove_caches(kube, worker)
  seen_configs = set()
  for g in arm_guests.values():
    if not g.started() or g.config in seen_configs:
      continue
    seen_configs.add(g.config)
    try:
      if getattr(args, "cleanup_guest", False):
        guest_lib.cleanup(kube, g)
      elif g.files and g.config:
        common.log(f"  kept SandboxConfig {g.config} for the next run on this guest (--cleanup-guest removes it)")
    except common.Error as e:
      common.log(f"  ⚠ {e}")


def _ab(kube: kube_lib.Kube, args, parks, setups) -> int:
  """--compare: the whole benchmark on each arm's setup in turn, then how they compare (see bperf/ab.py).

  setups: arm -> {key: value}, from ab.arms().
  """
  started = time.monotonic()
  sequence = ab.order(args.rounds or ab.ROUNDS)
  has_atelet = "atelet" in setups["A"]
  has_guest = "guest" in setups["A"]
  common.log("=" * common.WIDTH)
  common.log(f" 🚀 SWE-Perf A/B: {len(sequence)} runs ({' '.join(sequence)}), "
             f"each the {' then '.join(p.lower() for p in parks)} path")
  common.log("=" * common.WIDTH)
  runs, templates, arm_setups, labels, swap, worker, error = [], {}, {}, {}, None, None, None
  arm_guests = {arm: guest_lib.Guest() for arm in ab.ARMS}
  try:
    common.log("Preflight checks...")
    template, workers = _preflight(kube, ab_run=has_atelet)
    nodes = cluster_info.worker_nodes(workers)
    worker = next((w for w in workers if w.get("workerPod")), None)
    stock = {}
    if has_atelet:
      if len(nodes) != 1:
        raise common.Error("--compare with atelet= changes atelet on the worker's node, so the template's workers "
                           f"must all be on one node; they are on {len(nodes)}: {', '.join(nodes) or 'none known'}")
      swap = atelet.Swap(kube, atelet.daemonset_on(kube, nodes[0]))
      if swap.leftover:
        common.log(f"  ⚠ a stopped --compare run left DaemonSet {swap.name} changed; putting it back first")
        swap.restore()
      stock["atelet"] = swap.stock["image"]
    if has_guest:
      stock["guest"] = template.get("sandboxConfig", {}).get("configName", "")
    resolved = {arm: ab.resolve(setups[arm], stock) for arm in ab.ARMS}
    if has_atelet:
      arch = guest_lib.node_arch(kube, workers)
      for arm in ab.ARMS:
        resolved[arm]["atelet"] = atelet.resolve_image(resolved[arm]["atelet"], arm, arch)
    if has_guest:
      for arm in ab.ARMS:
        if setups[arm]["guest"] == ab.STOCK:
          arm_guests[arm].config = stock["guest"]
        else:
          guest_lib.prepare_config(kube, resolved[arm]["guest"], template, workers, arm_guests[arm])
          common.log(f"  guest {arm}   {arm_guests[arm].note}")
    labels = {arm: ab.describe(setups[arm], stock, resolved[arm]) for arm in ab.ARMS}
    for arm in ab.ARMS:
      common.log(f"  {arm}  {labels[arm]}")
    effective = {arm: (resolved[arm].get("atelet"), arm_guests[arm].config if has_guest else "") for arm in ab.ARMS}
    if effective["A"] == effective["B"]:
      raise common.Error(f"both arms run {labels['B']}: there's nothing to compare")
    if has_atelet and worker:
      atelet.remove_caches(kube, worker)  # what a stopped run left
    for i, arm in enumerate(sequence, 1):
      run_started = time.monotonic()
      common.log()
      common.log(f"━━ Run {i}/{len(sequence)}: arm {arm}, {labels[arm]} " + "━" * 20)
      if has_atelet:
        name = swap.use(nodes[0], resolved[arm]["atelet"], atelet.cache_dir(arm))
        common.log(f"  ✔ its atelet runs on {nodes[0]} ({name}) after {time.monotonic() - run_started:.0f} s")
      if arm not in templates:  # its golden snapshot, and the layers under it, come from this setup
        cfg = (arm_guests[arm].config,) if has_guest else ()
        templates[arm] = guest_lib.temporary_template(kube, template, ab.template_name(arm), f"arm {arm}", *cfg)
      if arm not in arm_setups:  # before the idle window, so kubectl exec / gcloud don't run inside it
        arm_setups[arm] = cluster_info.describe_setup(kube, workers)
      if worker:
        atelet.flush_node(kube, worker, atelet.cache_dir(arm) if has_atelet else "")
      run = _Run(parks=parks, total=common.steps_total(parks))
      runs.append((arm, run))
      try:
        _measure(kube, args, run, arm_template=templates[arm], cached_setup=arm_setups[arm])
      except common.Error as e:
        run.error = str(e)
      finally:
        _clean_up(kube, args, run)
      if run.trees is None and run.has_cycles():  # now: the next atelet's pod doesn't have this one's logs
        run.trees, run.log_errors, run.records = breakdown.breakdowns(kube, run.result.get("calls", []))
      report.print_report(run.result, run.trees, run.setup, time.monotonic() - run_started, log_errors=run.log_errors,
                          sizes=run.sizes, node=run.node, arm_note=f"{arm} · {labels[arm]}")
      if run.error:
        raise common.Error(f"run {i} (arm {arm}) failed: {run.error}")
  except common.Error as e:
    error = str(e)
  except KeyboardInterrupt:
    error = "interrupted"
  finally:
    _ab_clean_up(kube, args, swap, templates, worker if has_atelet else None, arm_guests)
  ab.print_report(runs, labels, parks, time.monotonic() - started, error)
  if error:
    common.log(f"\n[-] {error}")
    return 1
  return 0


def _ab_restore(kube: kube_lib.Kube) -> int:
  """--compare-restore: undoes what stopped --compare runs left: the DaemonSet, the template copies, the caches."""
  try:
    names = atelet.changed_daemonsets(kube)
    if not names:
      common.log("Nothing to put back: no atelet DaemonSet has --compare's changes.")
    for name in names:
      common.log(f"Putting back DaemonSet {common.SYSTEM_NAMESPACE}/{name}...")
      swap = atelet.Swap(kube, name)
      swap.restore()
      common.log(f"  ✔ it runs {ab.short_image(swap.stock['image'])} on every node again")
    for arm in ab.ARMS:
      arm_tmpl, error = kube.get_template(ab.template_name(arm))
      if not error and guest_lib.delete_template(kube, arm_tmpl):
        common.log(f"  deleted the temporary ActorTemplate {ab.template_name(arm)} and its snapshots")
    tmpl, _ = kube.get_template(common.TEMPLATE)
    workers = json.loads(kube.ate("get", "workers", "-o", "json").stdout or "{}").get("workers", [])
    for node in cluster_info.worker_nodes(_matching_workers(tmpl, workers) if tmpl else []):
      worker = next((w for w in workers if w.get("nodeName") == node and w.get("workerPod")), None)
      if worker:
        atelet.remove_caches(kube, worker)
  except common.Error as e:
    common.log(f"[-] {e}")
    return 1
  return 0


# ── --capacity ───────────────────────────────────────────────────────────────


def _capacity_clean_up(kube: kube_lib.Kube, pool, pods: bool) -> None:
  """Stops the runner, deletes the actors it left, and puts the WorkerPool back, in that order."""
  if not pods and not (pool and pool.changed):
    return
  common.log()
  common.log("━━ Cleaning up " + "━" * 40)
  try:
    if pods:
      pod.delete(kube, wait=True)  # first, so that the runner touches no actor after this
    leftover = capacity.leftover_actors(kube)
    if leftover:
      capacity.delete_actors(kube, leftover)
      common.log(f"  deleted the {len(leftover)} actors the runner left")
  except common.Error as e:
    common.log(f"  ⚠ {e}")
  if pool and pool.changed:
    try:
      pool.restore()
      common.log(f"  ✔ WorkerPool {pool.namespace}/{pool.name} is back to {pool.original} "
                 f"worker{'' if pool.original == 1 else 's'}")
    except common.Error as e:
      common.log(f"  ⚠ couldn't put WorkerPool {pool.namespace}/{pool.name} back to {pool.original}: {e}. "
                 "Try again with: baseline-perf --capacity-restore")


def _capacity(kube: kube_lib.Kube, args, levels) -> int:
  """--capacity: steps of more and more actors at once on the worker's node, until one is over capacity.

  See bperf/capacity.py.
  """
  started = time.monotonic()
  duration = args.duration or capacity.DURATION_S
  common.log("=" * common.WIDTH)
  common.log(f" 🚀 SWE-Perf capacity: {', '.join(map(str, levels))} actors at once, each going round "
             f"Resume -> Exec -> {capacity.PARK} for {duration} s")
  common.log("=" * common.WIDTH)
  run = _Run(parks=(capacity.PARK,), total=0)
  steps, pool, error = [], None, None
  try:
    common.log("Preflight checks...")
    template, workers = _preflight(kube, capacity_run=True)
    nodes = cluster_info.worker_nodes(workers)
    if len(nodes) != 1:
      raise common.Error("--capacity measures one node, so the template's workers must all be on one; they are on "
                         f"{len(nodes)}: {', '.join(nodes) or 'none known'}")
    worker = next((w for w in workers if w.get("workerPod")), None)
    if not worker:
      raise common.Error("the template's workers don't say which pods they are")
    pool = capacity.Pool.of_worker(kube, worker)
    common.log(f"  pool      WorkerPool {pool.namespace}/{pool.name}, {pool.original} "
               f"worker{'' if pool.original == 1 else 's'}")
    if pool.leftover:
      common.log(f"  ⚠ a stopped --capacity run left it scaled; it goes back to {pool.original} at the end")
    leftover = capacity.leftover_actors(kube)
    if leftover:
      capacity.delete_actors(kube, leftover)
      common.log(f"  deleted the {len(leftover)} actors a stopped run left")
    image = pod.runner_image(args.runner_image)

    common.log(f"Checking that node {nodes[0]} is idle ({sampler.IDLE_WINDOW_S} s)...")
    run.pod_created = True
    watching = _watch_node(kube, image, nodes, run)
    run.setup = cluster_info.describe_setup(kube, workers)  # in the idle window, as in _measure
    if watching:
      run.node["before"] = _idle_load(kube, run.node)

    for i, n in enumerate(levels, 1):
      common.log()
      common.log(f"━━ Step {i}/{len(levels)}: {n} actor{'' if n == 1 else 's'} at once " + "━" * 40)
      pool.scale(n + 1)
      waited, ready = capacity.wait_for_workers(kube, pool, nodes[0], n + 1)
      common.log(f"  ✔ {n + 1} workers ready after {waited:.0f} s: one per actor, and a spare")
      atelet.flush_node(kube, ready[0])  # what came before mustn't be written back during the step
      # This replaces the last step's runner pod; the sampler pod keeps running.
      runner_pod, waited = pod.start(kube, image, False, run.parks, template["metadata"]["name"], actors=n,
                                     duration_s=duration)
      common.log(f"  ✔ runner running on {runner_pod.get('spec', {}).get('nodeName', '?')} after {waited:.0f} s")
      step = capacity.step_stats(pod.relay(kube))
      if watching and step["window"]:
        step["node"] = sampler.window(sampler.parse(pod.sampler_log(kube)), *step["window"])
      step["verdict"] = capacity.verdict(step, steps[0] if steps else step)
      steps.append(step)
      for line in capacity.step_lines(step, i == 1, run.setup):
        common.log(line)
      if step["verdict"]:
        break
  except common.Error as e:
    error = str(e)
  except KeyboardInterrupt:
    error = "interrupted"
  finally:
    _capacity_clean_up(kube, pool, run.pod_created)
  capacity.print_report(steps, run.setup, run.node, time.monotonic() - started, duration, error)
  if error:
    common.log(f"\n[-] {error}")
  return 1 if error or (steps and steps[0]["verdict"]) else 0


def _capacity_restore(kube: kube_lib.Kube) -> int:
  """--capacity-restore: undoes what a stopped --capacity run left: its pods, its actors, the WorkerPool's size."""
  try:
    pod.delete(kube, wait=True)
    leftover = capacity.leftover_actors(kube)
    if leftover:
      capacity.delete_actors(kube, leftover)
      common.log(f"Deleted the {len(leftover)} actors a stopped --capacity run left.")
    scaled = capacity.scaled_pools(kube)
    if not scaled and not leftover:
      common.log("Nothing to put back: no WorkerPool has --capacity's annotation, and no actor of its is left.")
    for namespace, name in scaled:
      pool = capacity.Pool(kube, namespace, name)
      pool.restore()
      common.log(f"WorkerPool {namespace}/{name} is back to {pool.original} worker{'' if pool.original == 1 else 's'}.")
  except common.Error as e:
    common.log(f"[-] {e}")
    return 1
  return 0


def main(argv=None) -> int:
  """Runs baseline-perf. Returns the process exit code."""
  parser = _parser()
  args = parser.parse_args(argv)
  if args.cleanup_guest and not (args.guest or args.compare):
    parser.error("--cleanup-guest needs --guest or --compare")
  if args.compare and (args.guest or args.keep):
    parser.error("--compare takes guest specs directly (e.g. --compare kernel=PATH) and cleans up after itself: "
                 "drop --guest and --keep")
  if args.rounds is not None and (not args.compare or args.rounds < 1):
    parser.error("--rounds needs --compare, and at least 1")
  setups = None
  if args.compare:
    try:
      setups = ab.arms(args.compare)
    except ValueError as e:
      parser.error(f"--compare: {e}")
  levels = None
  if args.capacity is not None:
    if args.compare or args.guest or args.keep or args.cleanup_guest or args.only:
      parser.error("--capacity runs on the cluster as it is and cleans up after itself: drop --compare, --guest, "
                   "--keep, --cleanup-guest and --only")
    try:
      levels = capacity.parse_levels(args.capacity)
    except ValueError as e:
      parser.error(f"--capacity: {e}")
  if args.duration is not None and (levels is None or args.duration < capacity.MIN_DURATION_S):
    parser.error(f"--duration needs --capacity, and at least {capacity.MIN_DURATION_S} seconds")
  kube = kube_lib.Kube(args.kubectl_ate)
  if args.compare_restore:
    return _ab_restore(kube)
  if args.capacity_restore:
    return _capacity_restore(kube)
  if levels:
    return _capacity(kube, args, levels)

  parks = (args.only.capitalize(),) if args.only else tuple(common.PATHS)  # both paths, pause first
  if setups:
    return _ab(kube, args, parks, setups)
  started = time.monotonic()
  run = _Run(parks=parks, total=common.steps_total(parks))
  common.log("=" * common.WIDTH)
  common.log(f" 🚀 SWE-Perf single-actor run, {' then '.join(p.lower() for p in parks)} path: "
             "cold start, then 4 x (Resume -> Execute -> park)")
  common.log("=" * common.WIDTH)
  try:
    _measure(kube, args, run)
  except common.Error as e:
    run.error = str(e)
  except KeyboardInterrupt:
    run.error = "interrupted"
  finally:
    _clean_up(kube, args, run)

  if run.trees is None and run.has_cycles():
    run.trees, run.log_errors, _ = breakdown.breakdowns(kube, run.result.get("calls", []))
  report.print_report(run.result, run.trees, run.setup, time.monotonic() - started,
                      log_errors=run.log_errors, sizes=run.sizes, guest_note=run.guest.note, node=run.node)
  if run.error:
    common.log(f"\n[-] {run.error}")
    return 1
  return 0
