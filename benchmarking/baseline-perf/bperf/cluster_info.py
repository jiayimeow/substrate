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
"""What the numbers were measured on: the facts in the report's header.

The worker node's machine type and whether it's bare metal, its boot disk and local SSDs, the
Substrate version the installer labels each node with (`git describe` of the build, or the release
tag), the commit the running binaries were built from, and the Kubernetes version. Anything that
can't be read shows as unknown; none of this ever fails the run. Rows are (label, [items]);
report.headline lays them out.
"""

import json
import re
import shutil

# Plain imports, not google3 ones: baseline-perf runs with plain python3.
from bperf import common
from bperf import kube as kube_lib

INSTANCE_TYPE_LABEL = "node.kubernetes.io/instance-type"
_KO_SUFFIX = re.compile(r"-[0-9a-f]{32}(?=:|$)")  # ko names images <name>-<md5 of the import path>
_GCE_NODE = re.compile(r"gce://([^/]+)/([^/]+)/([^/]+)")  # a GKE node's providerID: project, zone, VM


def image_name(ref: str) -> str:
  """'…/ateom-microvm-<md5>:tag@sha256:…' -> 'ateom-microvm'."""
  return _KO_SUFFIX.sub("", ref.split("@")[0].rsplit("/", 1)[-1].split(":")[0])


def worker_nodes(workers) -> list[str]:
  """The nodes the workers run on, sorted."""
  return sorted({w["nodeName"] for w in workers if w.get("nodeName")})


def _container(pods, prefix, node=None):
  """(pod, container) for the first container whose image name starts with prefix, or (None, None)."""
  for pod in pods:
    if not node or pod.get("spec", {}).get("nodeName") == node:
      for c in pod.get("spec", {}).get("containers", []):
        if image_name(c["image"]).startswith(prefix):
          return pod, c
  return None, None


def _binary_commit(kube, pod, prefix) -> str:
  """The git commit the pod's Substrate binary was built from, or "".

  Every Substrate binary has a --version flag that prints '<version> commit=<sha>[-dirty]
  built=<date> <os>/<arch>'; -dirty means the checkout had uncommitted changes. This runs it in the
  pod's container; ko puts the binary at /ko-app/<name>.
  """
  pod, c = _container([pod] if pod else [], prefix)
  if not c:
    return ""
  meta = pod.get("metadata", {})
  binary = (c.get("command") or [f"/ko-app/{image_name(c['image'])}"])[0]
  try:
    proc = kube.kubectl("-n", meta.get("namespace", ""), "exec", meta.get("name", ""), "-c", c["name"],
                        "--", binary, "--version", check=False)
  except common.Error:
    return ""
  m = re.search(r"commit=([0-9a-f]{7,40}(?:-dirty)?)", proc.stdout)
  return m.group(1) if m else ""


def _commit_row(kube, ate_pods, worker_pod, node):
  """The "commit" row: one commit, or one per binary when they differ (a worker rebuilt by deploy.sh can)."""
  commits = {
      "ate-api": _binary_commit(kube, _container(ate_pods, "ateapi")[0], "ateapi"),
      "atelet": _binary_commit(kube, _container(ate_pods, "atelet", node=node)[0], "atelet"),
      "worker": _binary_commit(kube, worker_pod, "ateom"),
  }
  found = {name: c for name, c in commits.items() if c}
  if len(set(found.values())) == 1:
    return ("commit", [next(iter(found.values()))])
  if found:
    return ("commit", [f"{name} {c}" for name, c in found.items()])
  return ("commit", ["unknown (couldn't run --version in the pods)"])


def _gcloud_json(*args):
  """`gcloud <args> --format=json`, parsed -> (the value, "") or (None, why not)."""
  proc = kube_lib.gcloud(*args, "--format=json")
  if proc is None:
    return None, f"`gcloud {' '.join(args[:3])}` timed out"
  if proc.returncode != 0:
    return None, (proc.stderr.strip().splitlines() or ["gcloud failed"])[-1]
  try:
    return json.loads(proc.stdout), ""
  except ValueError:
    return None, f"`gcloud {' '.join(args[:3])}` printed no JSON"


def disk_items(vm, boot_disk) -> list[str]:
  """The "disk" row's items, from a Compute Engine instance and its boot disk (None if unknown).

  Hyperdisks have provisioned IOPS and throughput (MiB/s); pd-* disks get theirs from their size and
  the machine, so those show the size only. Local SSDs are the instance's SCRATCH disks.
  """
  items = []
  if boot_disk:
    items.append(f"{boot_disk.get('type', '').rsplit('/', 1)[-1]} {boot_disk.get('sizeGb', '?')} GB")
    if boot_disk.get("provisionedIops"):
      items.append(f"{int(boot_disk['provisionedIops']):,} IOPS")
    if boot_disk.get("provisionedThroughput"):
      items.append(f"{int(boot_disk['provisionedThroughput']):,} MiB/s")
  ssds = [d for d in vm.get("disks", []) if d.get("type") == "SCRATCH"]
  if ssds:
    size = sum(int(d.get("diskSizeGb") or 0) for d in ssds)
    items.append(f"{len(ssds)} local SSD{'s' if len(ssds) > 1 else ''} ({size:,} GB)")
  else:
    items.append("no local SSD")
  return items


def _disk_row(node):
  """The "disk" row: the node's boot disk and local SSDs, from Compute Engine (needs gcloud)."""
  m = _GCE_NODE.fullmatch(node.get("spec", {}).get("providerID", ""))
  if not m:
    return ("disk", ["unknown (not a Compute Engine node)"])
  if not shutil.which("gcloud"):
    return ("disk", ["unknown (gcloud is not on PATH)"])
  project, zone, name = m.groups()
  vm, error = _gcloud_json("compute", "instances", "describe", name, f"--zone={zone}", f"--project={project}")
  if vm is None:
    return ("disk", [f"unknown ({error})"])
  boot = next((d for d in vm.get("disks", []) if d.get("boot")), {})
  boot_disk, error = _gcloud_json("compute", "disks", "describe", boot["source"]) if boot.get("source") else (None, "")
  items = disk_items(vm, boot_disk)
  return ("disk", items if boot_disk else [f"unknown (boot disk: {error or 'none listed'})"] + items)


def describe_setup(kube: kube_lib.Kube, workers):
  """The header's facts about the cluster and the template's worker node, as rows."""
  server = (kube.get_json("version") or {}).get("serverVersion", {}).get("gitVersion")
  rows = [("cluster", [f"k8s {server}"] if server else [])]
  nodes = worker_nodes(workers)
  if nodes:
    node = kube.get_json("get", "node", nodes[0]) or {}
    labels = node.get("metadata", {}).get("labels", {})
    machine = labels.get(INSTANCE_TYPE_LABEL)
    sandbox_class = next((w["sandboxClass"] for w in workers if w.get("sandboxClass")), "")
    if machine and re.search(r"[-.]metal$", machine):
      virt = "bare metal"
    else:
      virt = "nested virtualization" if sandbox_class == "microvm" else "VM"
    rows.append(("machine", [machine, virt] if machine else ["machine type unknown"]))
    rows.append(_disk_row(node))
    rows.append(("version", [f"substrate {labels.get('ate.dev/substrate-version', 'unknown')}"]))
  ate_pods = (kube.get_json("-n", common.SYSTEM_NAMESPACE, "get", "pods") or {}).get("items", [])
  worker = next((w for w in workers if w.get("workerPod")), None)
  pod = worker and kube.get_json("-n", worker.get("workerNamespace", ""), "get", "pod", worker["workerPod"])
  rows.append(_commit_row(kube, ate_pods, pod, nodes[0] if nodes else None))
  return rows
