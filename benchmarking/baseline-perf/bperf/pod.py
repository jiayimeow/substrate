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
"""The runner pod and the sampler pod: their image, their specs, and reading their output.

The runner pod runs runner.py on an image with python3 and grpcio. It has no copy of this package:
the launcher ships common.py and runner.py to it in an environment variable, and a short bootstrap
command writes them out and runs the runner. The sampler pod runs sampler.py, also shipped in an
environment variable, on the worker's node.
"""

import hashlib
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
from bperf import runner
from bperf import sampler

# The runner image. Its tag is a hash of this text, so changing it builds a new image.
RUNNER_DOCKERFILE = "FROM python:3.12-slim\nRUN pip install --no-cache-dir grpcio==1.84.0\n"
POD_START_TIMEOUT_S = 120  # scheduling, image pull, client certificate
SAMPLER_START_TIMEOUT_S = 60  # the image pull, the first time on that node
SAMPLER_MAX_S = 3600  # the sampler pod stops by itself after this, if nothing deletes it
RUN_TIMEOUT_S = 900  # the pod's whole run, if its log stream breaks off
BUILD_TIMEOUT_S = 900  # one docker build or push
DELETE_TIMEOUT_S = 60  # deleting the pods and waiting until they're gone: their grace periods are 5 s and 1 s
RUNNER_MODULES = ("common", "runner")  # what the pod runs, from bperf/
SOURCES_ENV = "BPERF_SOURCES"  # the variable that carries them
SAMPLER_ENV = "BPERF_SAMPLER"  # the variable that carries sampler.py

# The pod's command: python3 -c BOOTSTRAP <runner.main's arguments>.
BOOTSTRAP = f"""\
import json, os, sys, tempfile
root = tempfile.mkdtemp(prefix="bperf-")
for name, source in json.loads(os.environ[{SOURCES_ENV!r}]).items():
  path = os.path.join(root, name)
  os.makedirs(os.path.dirname(path), exist_ok=True)
  with open(path, "w", encoding="utf-8") as f:
    f.write(source)
sys.path.insert(0, root)
from bperf import runner
sys.exit(runner.main(sys.argv[1:]))
"""
# The sampler pod's command: python3 -c SAMPLER_BOOTSTRAP <sampler.main's arguments>.
SAMPLER_BOOTSTRAP = f"import os; exec(os.environ[{SAMPLER_ENV!r}])"

_FATAL_WAITING = ("ImagePullBackOff", "InvalidImageName", "CreateContainerConfigError", "CreateContainerError")
_POD = ("-n", common.NAMESPACE)


def runner_image(explicit: str | None) -> str:
  """--runner-image, or an image in $KO_DOCKER_REPO that this builds and pushes the first time."""
  if explicit:
    common.log(f"  runner    {explicit}")
    return explicit
  repo = os.environ.get("KO_DOCKER_REPO", "").rstrip("/")
  if not repo:
    raise common.Error("the runner pod needs an image with python3 and grpcio. Set KO_DOCKER_REPO to a registry "
                       "the cluster pulls from (the one deploy.sh uses) to have one built there, "
                       "or pass --runner-image")
  image = f"{repo}/baseline-perf-runner:{hashlib.sha256(RUNNER_DOCKERFILE.encode()).hexdigest()[:12]}"
  if not shutil.which("docker"):
    common.log(f"  runner    {image} (no docker here to check or build it)")
    return image
  inspect = subprocess.run(["docker", "manifest", "inspect", image], capture_output=True, timeout=60, check=False)
  if inspect.returncode == 0:
    common.log(f"  runner    {image}")
    return image
  common.log(f"  runner    building {image} (python3 + grpcio, once)...")
  with tempfile.TemporaryDirectory() as context_dir:
    with open(os.path.join(context_dir, "Dockerfile"), "w") as f:
      f.write(RUNNER_DOCKERFILE)
    for cmd in (["docker", "build", "-q", "-t", image, context_dir], ["docker", "push", "-q", image]):
      proc = subprocess.run(cmd, capture_output=True, text=True, timeout=BUILD_TIMEOUT_S, check=False)
      if proc.returncode != 0:
        raise common.Error(f"`{' '.join(cmd)}` failed: {(proc.stderr or proc.stdout).strip()[-800:]}")
  common.log(f"  runner    built and pushed {image}")
  return image


def _source(name: str) -> str:
  """The text of bperf/<name>.py."""
  with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{name}.py"), encoding="utf-8") as f:
    return f.read()


def _sources() -> str:
  """The runner's modules, as SOURCES_ENV's JSON value."""
  return json.dumps({f"bperf/{name}.py": _source(name) for name in RUNNER_MODULES})


def runner_pod(image: str, keep: bool, parks, template: str, actors: int = 0, duration_s: float = 0) -> dict[str, Any]:
  """The runner pod's spec, with the credentials to call ate-api. actors: a --capacity step's, for duration_s."""
  avoid_workers = {  # keep the client off the nodes that run actors
      "weight": 100,
      "podAffinityTerm": {
          "topologyKey": "kubernetes.io/hostname", "namespaceSelector": {},
          "labelSelector": {"matchExpressions": [{"key": "ate.dev/worker-pool", "operator": "Exists"}]}},
  }
  args = (["--keep"] if keep else []) + [f"--parks={','.join(parks)}", f"--template={template}"]
  resources = {"requests": {"cpu": "250m", "memory": "128Mi"}, "limits": {"memory": "256Mi"}}
  if actors:  # a thread per actor, each polling its job 5 times a second: give the client room, so it's not the limit
    args += [f"--actors={actors}", f"--duration={duration_s:g}"]
    resources = {"requests": {"cpu": "1", "memory": "256Mi"}, "limits": {"memory": "1Gi"}}
  return {
      "apiVersion": "v1",
      "kind": "Pod",
      "metadata": {"name": common.RUNNER_POD, "namespace": common.NAMESPACE,
                   "labels": {"app.kubernetes.io/name": common.RUNNER_POD}},
      "spec": {
          "restartPolicy": "Never",
          "terminationGracePeriodSeconds": 5,
          "automountServiceAccountToken": False,
          "affinity": {"podAntiAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [avoid_workers]}},
          "containers": [{
              "name": "runner",
              "image": image,
              "command": ["python3", "-u", "-c", BOOTSTRAP] + args,
              "env": [
                  {"name": "PYTHONUTF8", "value": "1"},
                  {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
                  {"name": SOURCES_ENV, "value": _sources()},
              ],
              "resources": resources,
              "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
              "volumeMounts": [
                  {"name": "servicedns-ca", "mountPath": os.path.dirname(runner.CA_FILE), "readOnly": True},
                  {"name": "podidentity", "mountPath": os.path.dirname(runner.CRED_BUNDLE), "readOnly": True},
              ],
          }],
          # The same credentials as ate-controller's: the CA that signed ate-api's serving
          # certificate, and a client certificate for this pod's identity.
          "volumes": [
              {"name": "servicedns-ca", "projected": {"sources": [{"clusterTrustBundle": {
                  "signerName": "servicedns.podcert.ate.dev/identity",
                  "labelSelector": {"matchLabels": {"podcert.ate.dev/canarying": "live"}},
                  "path": os.path.basename(runner.CA_FILE)}}]}},
              {"name": "podidentity", "projected": {"sources": [{"podCertificate": {
                  "signerName": "podidentity.podcert.ate.dev/identity",
                  "keyType": "ECDSAP256",
                  "credentialBundlePath": os.path.basename(runner.CRED_BUNDLE)}}]}},
          ],
      },
  }


def sampler_pod(image: str, node: str) -> dict[str, Any]:
  """The sampler pod's spec: on the given node, with no credentials and no privileges."""
  return {
      "apiVersion": "v1",
      "kind": "Pod",
      "metadata": {"name": common.SAMPLER_POD, "namespace": common.NAMESPACE,
                   "labels": {"app.kubernetes.io/name": common.SAMPLER_POD}},
      "spec": {
          "nodeName": node,  # not left to the scheduler: it must be the worker's node
          "tolerations": [{"operator": "Exists"}],  # whatever taints the node has
          "restartPolicy": "Never",
          "terminationGracePeriodSeconds": 1,
          "activeDeadlineSeconds": SAMPLER_MAX_S,
          "automountServiceAccountToken": False,
          "containers": [{
              "name": "sampler",
              "image": image,
              "command": ["python3", "-u", "-c", SAMPLER_BOOTSTRAP, f"--interval={sampler.INTERVAL_S:g}"],
              "env": [
                  {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
                  {"name": SAMPLER_ENV, "value": _source("sampler")},
              ],
              "resources": {"requests": {"cpu": "10m", "memory": "32Mi"}, "limits": {"memory": "64Mi"}},
              "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
          }],
      },
  }


def _get_pod(kube: kube_lib.Kube, name: str = common.RUNNER_POD) -> dict[str, Any]:
  return kube.get_json(*_POD, "get", "pod", name) or {}


def _pod_failure(status: dict[str, Any]) -> str:
  """Why a Failed pod stopped: its top-level reason/message, or its container's termination status."""
  if status.get("reason"):
    return f"{status['reason']}: {status.get('message', '').strip()}".rstrip(": ")
  for cs in status.get("containerStatuses", []):
    term = cs.get("state", {}).get("terminated") or {}
    if term:
      reason = term.get("reason") or f"exit {term.get('exitCode', '?')}"
      msg = (term.get("message") or "").strip()
      return f"{reason}: {msg}".rstrip(": ")
  return "pod failed"


def _create(kube: kube_lib.Kube, spec: dict[str, Any], what: str, timeout_s: float):
  """Creates a pod, replacing one an earlier run left, and waits until it runs. Returns (the pod, seconds waited)."""
  name = spec["metadata"]["name"]
  kube.kubectl(*_POD, "delete", "pod", name, "--ignore-not-found", "--wait=true", timeout=60)
  kube.kubectl("create", "-f", "-", stdin=json.dumps(spec))
  started = time.monotonic()
  while True:
    pod = _get_pod(kube, name)
    status = pod.get("status", {})
    if status.get("phase") == "Failed":  # kubelet refused it (OutOfcpu), or the container exited non-zero right away
      raise common.Error(f"{what} can't start: {_pod_failure(status)}")
    if status.get("phase") in ("Running", "Succeeded"):
      return pod, time.monotonic() - started
    for cs in status.get("containerStatuses", []):
      waiting = cs.get("state", {}).get("waiting") or {}
      if waiting.get("reason") in _FATAL_WAITING:
        raise common.Error(f"{what} can't start: {waiting['reason']}: {waiting.get('message', '').strip()}")
    if time.monotonic() - started > timeout_s:
      selector = f"involvedObject.name={name}"
      events = (kube.get_json(*_POD, "get", "events", "--field-selector", selector) or {}).get("items", [])
      events.sort(key=lambda e: e.get("lastTimestamp") or e.get("eventTime") or "")
      latest = "; ".join(f"{e.get('reason')}: {e.get('message', '').strip()}" for e in events[-3:]) or "no events"
      raise common.Error(f"{what} didn't start within {timeout_s}s: {latest}")
    time.sleep(1)


def start(kube: kube_lib.Kube, image: str, keep: bool, parks, template: str, actors: int = 0, duration_s: float = 0):
  """Starts the runner pod, replacing the last one, and waits until it runs. Returns (the pod, seconds waited)."""
  spec = runner_pod(image, keep, parks, template, actors, duration_s)
  return _create(kube, spec, "the runner pod", POD_START_TIMEOUT_S)


def start_sampler(kube: kube_lib.Kube, image: str, node: str):
  """Starts the sampler pod on node and waits until it runs. Returns (the pod, seconds waited)."""
  return _create(kube, sampler_pod(image, node), "the sampler pod", SAMPLER_START_TIMEOUT_S)


def sampler_log(kube: kube_lib.Kube) -> list[str]:
  """The sampler pod's output so far, as lines (see sampler.parse); [] if it can't be read."""
  try:
    proc = kube.kubectl(*_POD, "logs", common.SAMPLER_POD, check=False)
  except common.Error:  # timed out
    return []
  return proc.stdout.splitlines() if proc.returncode == 0 else []


def find_result(lines):
  """The runner's results in its output lines, or None if they aren't there."""
  for line in lines:
    if line.startswith(common.RESULT_PREFIX):
      return json.loads(line[len(common.RESULT_PREFIX):])
  return None


def relay(kube: kube_lib.Kube) -> dict[str, Any]:
  """Prints the runner's progress as it comes and returns its results (see runner.py)."""
  proc = subprocess.Popen(["kubectl", *_POD, "logs", "-f", common.RUNNER_POD], stdout=subprocess.PIPE,
                          stderr=subprocess.DEVNULL, text=True, encoding="utf-8", errors="replace")
  result = None
  try:
    for line in proc.stdout:
      line = line.rstrip("\n")
      if line.startswith(common.RESULT_PREFIX):
        result = find_result([line])
        break
      common.log(line)
  finally:
    proc.kill()
    proc.wait()
  if result is not None:
    return result
  # The log stream can break off before the end: wait for the pod, then read it all.
  deadline = time.monotonic() + RUN_TIMEOUT_S
  while time.monotonic() < deadline and _get_pod(kube).get("status", {}).get("phase") in ("Pending", "Running"):
    time.sleep(2)
  lines = kube.kubectl(*_POD, "logs", common.RUNNER_POD, check=False).stdout.splitlines()
  result = find_result(lines)
  if result is None:
    raise common.Error("the runner pod ended without results. Its last lines:\n"
                       + "\n".join(f"    {line}" for line in lines[-10:]))
  return result


def delete(kube: kube_lib.Kube, wait: bool = False) -> None:
  """Deletes the runner and sampler pods; with wait, until they're gone (raises common.Error if that takes too long)."""
  kube.kubectl(*_POD, "delete", "pod", common.RUNNER_POD, common.SAMPLER_POD, "--ignore-not-found",
               f"--wait={'true' if wait else 'false'}", timeout=DELETE_TIMEOUT_S if wait else kube_lib.KUBECTL_TIMEOUT_S,
               check=False)
