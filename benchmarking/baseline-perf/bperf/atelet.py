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
"""--compare's atelet=IMAGE|DIR[@REF]: another atelet on the worker's node, and back.

atelet runs as a DaemonSet. Swap.use() first sets its updateStrategy to
OnDelete, so that changing its pod template restarts no pod by itself, then
changes the template's image and args and deletes only the atelet pod on the
worker's node, which the DaemonSet recreates from the changed template. Before
the first change, the DaemonSet's image, args and updateStrategy are saved in an
annotation on it. Swap.restore() puts them back from there, and the stock atelet
on every node that runs another, so a run that was killed can still be undone
(--compare-restore).

Each arm's atelet gets its own layer cache (--image-cache-dir): atelet reuses
cached layers by diffID, so with one cache the second arm would run on the
layers the first one unpacked, and a change to unpacking wouldn't show.
"""

import json
import os
import shutil
import subprocess
import tempfile
import time
from typing import Any

# Plain imports, not google3 ones: baseline-perf runs with plain python3.
from bperf import common
from bperf import kube as kube_lib
from bperf import logs

SELECTOR = "app=atelet"  # atelet's pods in common.SYSTEM_NAMESPACE; ate-api finds them by this label too
CONTAINER = "atelet"
ANNOTATION = "bperf.ate.dev/original"  # on the DaemonSet: its image, args and updateStrategy before use(), as JSON
CACHE_FLAG = "--image-cache-dir"
CACHE_ROOT = "/var/lib/ate"  # the host directory atelet shares with the workers; caches must be in it
CACHE_PREFIX = "image-cache-bperf-"  # + the arm's letter
SWAP_TIMEOUT_S = 420  # the old pod may drain for up to its grace period (330 s); then the new one pulls and starts
CLEAN_TIMEOUT_S = 300  # removing the caches: GBs of small files
FLUSH_TIMEOUT_S = 300  # writing back a new cache's GBs of small files, at the disk's IOPS limit
BUILD_TIMEOUT_S = 300  # ko build ./cmd/atelet
_VERSION_PKG = "github.com/agent-substrate/substrate/internal/version"
_FATAL_WAITING = ("ImagePullBackOff", "InvalidImageName", "CrashLoopBackOff", "CreateContainerConfigError",
                  "CreateContainerError")
_NS = ("-n", common.SYSTEM_NAMESPACE)


def split_source(value: str) -> tuple[str, str] | None:
  """(repo_dir, git_ref) if value is a local checkout path or PATH@REF; None if it's an image reference."""
  left, at, right = value.partition("@")
  if at and ":" in right:  # image@sha256:…
    return None
  path = os.path.expanduser(left)
  if not (at or left.startswith((".", "/", "~")) or os.path.exists(path)):
    return None
  if at and not right:
    raise common.Error(f"atelet={value}: missing git ref after '@'")
  return path, right


def _ko_build(ko_bin: str, build_dir: str, value: str, arm: str, arch: str) -> str:
  """Builds ./cmd/atelet in build_dir with ko and returns the published image reference."""
  if not os.path.isdir(os.path.join(build_dir, "cmd", "atelet")):
    raise common.Error(f"atelet={value}: no ./cmd/atelet directory in {build_dir}")
  common.log(f"  building atelet for arm {arm} from {value} with ko...")
  ver = subprocess.run(["git", "-C", build_dir, "describe", "--tags", "--always", "--dirty"],
                       capture_output=True, text=True, check=False).stdout.strip()
  ldflags = (f"--ldflags=-X={_VERSION_PKG}.Version={ver}",) if ver else ()
  cmd = (ko_bin, "build", "./cmd/atelet", "-B", f"--platform=linux/{arch}", f"--tags=bperf-{arm.lower()}", *ldflags)
  try:
    proc = subprocess.run(cmd, cwd=build_dir, capture_output=True, text=True, timeout=BUILD_TIMEOUT_S, check=False)
  except subprocess.TimeoutExpired as e:
    raise common.Error(f"atelet={value}: ko build timed out after {BUILD_TIMEOUT_S} s") from e
  lines = [line.strip() for line in (proc.stdout or "").splitlines() if line.strip()]
  if proc.returncode != 0 or not lines:
    raise common.Error(f"atelet={value}: ko build failed: {(proc.stderr or proc.stdout).strip()}")
  return lines[-1]


def resolve_image(value: str, arm: str, arch: str = "amd64") -> str:
  """Returns value if it's an image ref, or builds ./cmd/atelet with ko if value is DIR or DIR@REF."""
  source = split_source(value)
  if source is None:
    return value
  repo_dir, git_ref = source
  if not os.path.isdir(repo_dir):
    raise common.Error(f"atelet={value}: {repo_dir} is not a directory")
  if not os.environ.get("KO_DOCKER_REPO"):
    raise common.Error(f"atelet={value}: set $KO_DOCKER_REPO so ko can push the built atelet image")
  home_ko = os.path.expanduser("~/go/bin/ko")
  ko_bin = shutil.which("ko") or (home_ko if os.path.isfile(home_ko) else "")
  if not ko_bin:
    run_tool = os.path.join(repo_dir, "hack", "run-tool.sh")
    if os.path.isfile(run_tool):
      ko_bin = subprocess.run([run_tool, "--print-bin-path", "ko"],
                              capture_output=True, text=True, check=False).stdout.strip()
  if not ko_bin:
    raise common.Error(f"atelet={value}: ko is not on PATH (try: go install github.com/google/ko@latest)")
  if not git_ref:
    return _ko_build(ko_bin, repo_dir, value, arm, arch)
  tmpdir = tempfile.mkdtemp(prefix=f"bperf-atelet-{arm.lower()}-")
  add = subprocess.run(["git", "-C", repo_dir, "worktree", "add", "--detach", tmpdir, git_ref],
                       capture_output=True, text=True, check=False)
  if add.returncode != 0:
    shutil.rmtree(tmpdir, ignore_errors=True)
    raise common.Error(f"atelet={value}: git worktree add failed: {(add.stderr or add.stdout).strip()}")
  try:
    return _ko_build(ko_bin, tmpdir, value, arm, arch)
  finally:
    subprocess.run(["git", "-C", repo_dir, "worktree", "remove", "--force", tmpdir],
                   capture_output=True, text=True, check=False)


def cache_dir(arm: str) -> str:
  """The layer cache of an arm's atelet, on the node."""
  return f"{CACHE_ROOT}/{CACHE_PREFIX}{arm.lower()}"


def with_cache_dir(args, path: str) -> list[str]:
  """atelet's args with --image-cache-dir set to path, replacing any value they had."""
  out, value_next = [], False
  for arg in args:
    if value_next:
      value_next = False
    elif arg == CACHE_FLAG:
      value_next = True  # "--image-cache-dir PATH": drop PATH too
    elif not arg.startswith(CACHE_FLAG + "="):
      out.append(arg)
  return out + [f"{CACHE_FLAG}={path}"]


def _container(spec):
  """(index, container) of atelet's container in a pod spec."""
  for i, c in enumerate(spec.get("containers", [])):
    if c.get("name") == CONTAINER:
      return i, c
  raise common.Error(f"atelet's pods have no container named {CONTAINER}")


def stock(ds) -> dict[str, Any]:
  """What the DaemonSet ran before baseline-perf changed it: {"image", "args", "updateStrategy"}."""
  saved = (ds["metadata"].get("annotations") or {}).get(ANNOTATION)
  if saved:
    return json.loads(saved)
  _, c = _container(ds["spec"]["template"]["spec"])
  return {"image": c["image"], "args": c.get("args", []), "updateStrategy": ds["spec"].get("updateStrategy", {})}


def template_patch(ds, image: str, args) -> list[dict[str, Any]]:
  """The JSON patch that sets the image and args of atelet's container in the DaemonSet's pod template."""
  i, _ = _container(ds["spec"]["template"]["spec"])
  path = f"/spec/template/spec/containers/{i}"
  return [{"op": "test", "path": f"{path}/name", "value": CONTAINER},
          {"op": "replace", "path": f"{path}/image", "value": image},
          {"op": "add", "path": f"{path}/args", "value": list(args)}]  # add replaces args if they're there


def runs(pod, image: str, args) -> bool:
  """Whether a pod's atelet container has this image and these args."""
  _, c = _container(pod.get("spec", {}))
  return c.get("image") == image and c.get("args", []) == list(args)


def ready(pod) -> bool:
  conditions = pod.get("status", {}).get("conditions", [])
  return any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions)


def _fatal(pod) -> str:
  """Why a pod's atelet can't start ('' if it may yet)."""
  for cs in pod.get("status", {}).get("containerStatuses", []):
    waiting = cs.get("state", {}).get("waiting") or {}
    if waiting.get("reason") in _FATAL_WAITING:
      return f"{waiting['reason']}: {waiting.get('message', '').strip()}"
  return ""


def daemonset_on(kube: kube_lib.Kube, node: str) -> str:
  """The name of the DaemonSet whose atelet pod runs on node."""
  pods = (kube.get_json(*_NS, "get", "pods", "-l", SELECTOR, "--field-selector", f"spec.nodeName={node}") or {})
  for pod in pods.get("items", []):
    for owner in pod["metadata"].get("ownerReferences", []):
      if owner.get("kind") == "DaemonSet":
        return owner["name"]
  raise common.Error(f"no atelet pod of a DaemonSet runs on node {node} ({SELECTOR} in {common.SYSTEM_NAMESPACE})")


def changed_daemonsets(kube: kube_lib.Kube) -> list[str]:
  """The DaemonSets that an --ab run changed and didn't put back."""
  items = (kube.get_json(*_NS, "get", "daemonsets") or {}).get("items", [])
  return [d["metadata"]["name"] for d in items if ANNOTATION in (d["metadata"].get("annotations") or {})]


class Swap:
  """One atelet DaemonSet: use() runs another image on a node, restore() puts the stock one back."""

  def __init__(self, kube: kube_lib.Kube, name: str):
    self.kube, self.name = kube, name
    ds = self.get()
    self.leftover = ANNOTATION in (ds["metadata"].get("annotations") or {})  # a stopped run didn't put it back
    self.stock = stock(ds)
    self.changed = self.leftover  # whether restore() has anything to put back

  def get(self):
    ds = self.kube.get_json(*_NS, "get", "daemonset", self.name)
    if not ds:
      raise common.Error(f"can't read DaemonSet {common.SYSTEM_NAMESPACE}/{self.name}")
    return ds

  def _patch(self, ops) -> None:
    self.kube.kubectl(*_NS, "patch", "daemonset", self.name, "--type=json", "-p", json.dumps(ops))

  def _pods(self, node=None):
    """The DaemonSet's pods, on node if given, including any being deleted."""
    where = ["--field-selector", f"spec.nodeName={node}"] if node else []
    pods = (self.kube.get_json(*_NS, "get", "pods", "-l", SELECTOR, *where) or {}).get("items", [])
    return [p for p in pods if any(o.get("kind") == "DaemonSet" and o.get("name") == self.name
                                   for o in p["metadata"].get("ownerReferences", []))]

  def use(self, node: str, image: str, cache: str) -> str:
    """Runs image on node, with its layer cache in cache, and waits until it's ready. Returns the pod's name."""
    ds = self.get()
    if ANNOTATION not in (ds["metadata"].get("annotations") or {}):
      self.kube.kubectl(*_NS, "annotate", "daemonset", self.name, f"{ANNOTATION}={json.dumps(self.stock)}")
    self.changed = True
    if ds["spec"].get("updateStrategy", {}).get("type") != "OnDelete":
      self._patch([{"op": "add", "path": "/spec/updateStrategy", "value": {"type": "OnDelete"}}])
    args = with_cache_dir(self.stock["args"], cache)
    self._patch(template_patch(ds, image, args))
    return self._replace(node, image, args)

  def _replace(self, node: str, image: str, args) -> str:
    """Makes the DaemonSet's only pod on node one that runs image and args, and waits until it's ready.

    ate-api dials the one atelet pod it sees on a node, by pod IP, and fails
    while there are two, so this waits until the old pod is gone.
    """
    pods = self._pods(node)
    old = {p["metadata"]["uid"] for p in pods if not runs(p, image, args)}
    for p in pods:
      if p["metadata"]["uid"] in old and not p["metadata"].get("deletionTimestamp"):
        self.kube.kubectl(*_NS, "delete", "pod", p["metadata"]["name"], "--wait=false")
    deadline = time.monotonic() + SWAP_TIMEOUT_S
    while True:
      pods = self._pods(node)
      for p in pods:
        why = "" if p["metadata"]["uid"] in old else _fatal(p)
        if why:
          raise common.Error(f"atelet {image} can't start on {node}: {why} "
                             f"(see kubectl -n {common.SYSTEM_NAMESPACE} logs {p['metadata']['name']} --previous)")
      if (len(pods) == 1 and not pods[0]["metadata"].get("deletionTimestamp") and runs(pods[0], image, args)
          and ready(pods[0])):
        return pods[0]["metadata"]["name"]
      if time.monotonic() > deadline:
        raise common.Error(f"atelet on {node} isn't running {image} after {SWAP_TIMEOUT_S} s")
      time.sleep(2)

  def restore(self) -> None:
    """Puts back the DaemonSet's image, args and updateStrategy, and the stock atelet on any node that runs another."""
    if not self.changed:
      return
    image, args = self.stock["image"], self.stock["args"]
    self._patch(template_patch(self.get(), image, args))
    try:
      # Every node, not just the worker's: under OnDelete, a pod recreated meanwhile got the changed template.
      for node in sorted({p["spec"].get("nodeName") for p in self._pods() if not runs(p, image, args)} - {None, ""}):
        self._replace(node, image, args)
    finally:
      # The template is the stock one again, so the stock strategy restarts no pod that already runs it.
      self._patch([{"op": "add", "path": "/spec/updateStrategy", "value": self.stock["updateStrategy"]}])
    self.kube.kubectl(*_NS, "annotate", "daemonset", self.name, f"{ANNOTATION}-")
    self.changed = False


def _worker_exec(worker) -> tuple[str, ...]:
  """kubectl's args to run a command in the worker pod, which mounts CACHE_ROOT and sees the node's /proc."""
  return ("-n", worker.get("workerNamespace", ""), "exec", worker.get("workerPod", ""), "-c", logs.WORKER_CONTAINER)


def remove_caches(kube: kube_lib.Kube, worker) -> None:
  """Removes the arms' layer caches from the worker's node, through the worker pod."""
  pattern = f"{CACHE_ROOT}/{CACHE_PREFIX}*"
  try:
    proc = kube.kubectl(*_worker_exec(worker), "--", "sh", "-c", f"rm -rf {pattern}", timeout=CLEAN_TIMEOUT_S,
                        check=False)
    error = (proc.stderr or proc.stdout).strip() if proc.returncode else ""
  except common.Error as e:
    error = str(e)
  if error:
    common.log(f"  ⚠ couldn't remove the layer caches {pattern} on the worker's node: {error}")


def dirty_mb(meminfo: str) -> float | None:
  """Dirty plus Writeback in a /proc/meminfo, in MB: what sync has to write. None if they aren't in it."""
  kb = [int(line.split()[1]) for line in meminfo.splitlines() if line.split(":")[0] in ("Dirty", "Writeback")]
  return sum(kb) / 1000 if kb else None


def flush_node(kube: kube_lib.Kube, worker, cache: str = "") -> None:
  """Writes the node's dirty pages to disk, so that what came before isn't written back during the run.

  A new layer cache unpacks GBs of files, and Linux writes dirty pages back only
  about 30 s later: after the idle check, during the run. sync(2) in the worker
  pod covers the whole node. When cache is set, drops old page caches and warms
  the arm's layer cache so every round starts with the same cache state.
  """
  started = time.monotonic()
  cmd = "cat /proc/meminfo && sync"
  if cache:
    cmd += f"; tar -cf /dev/null {cache} {CACHE_ROOT}/static-files 2>/dev/null || true"
  try:
    proc = kube.kubectl(*_worker_exec(worker), "--", "sh", "-c", cmd, timeout=FLUSH_TIMEOUT_S,
                        check=False)
    error = (proc.stderr or proc.stdout).strip() if proc.returncode else ""
  except common.Error as e:
    proc, error = None, str(e)
  if error:
    common.log(f"  ⚠ couldn't write the node's dirty pages to disk first: {error}")
    return
  mb = dirty_mb(proc.stdout)
  amount = "?" if mb is None else f"{mb:,.0f}"
  common.log(f"  ✔ wrote the node's dirty pages to disk first: {amount} MB in {time.monotonic() - started:.0f} s")
