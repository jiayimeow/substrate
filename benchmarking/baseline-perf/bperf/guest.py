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
"""--guest: run on other guest assets than the template's SandboxConfig.

Files are uploaded under CUSTOM_ASSETS in the template's bucket, named by their
sha256, and go into a copy of the template's SandboxConfig named
<its name>-g<hash of the assets>, so the same guest always gets the same name
and nothing is ever overwritten. The run uses a temporary copy of the template
that names that SandboxConfig, with its own golden snapshot (a snapshot taken on
one guest can't be restored on another). The template and the stock
SandboxConfig are never changed.
"""

import copy
import dataclasses
import hashlib
import json
import os
import shutil
import time
from typing import Any
import urllib.parse

# Plain imports, not google3 ones: baseline-perf runs with plain python3.
from bperf import common
from bperf import kube as kube_lib
from bperf import logs

# --guest key -> (SandboxConfig asset name, the file name assemble.sh writes).
ASSETS = {
    "kernel": ("kata-kernel", "vmlinux"),
    "rootfs": ("kata-image", "rootfs.img"),
    "kata-config": ("kata-config", "configuration-clh.toml"),
    "cloud-hypervisor": ("cloud-hypervisor", "cloud-hypervisor"),
    "virtiofsd": ("virtiofsd", "virtiofsd"),
}
ELF_FILES = ("kernel", "cloud-hypervisor", "virtiofsd")
ELF_MACHINE = {"amd64": 62, "arm64": 183}  # e_machine: EM_X86_64, EM_AARCH64
CUSTOM_ASSETS = "kata-assets/custom"  # in the bucket: <sha256>/<file name>
SANDBOX_CONFIGS = "sandboxconfigs.ate.dev"
UPLOAD_TIMEOUT_S = 600
GOLDEN_TIMEOUT_S = 300  # the copy's golden snapshot: boot, start, snapshot
FAILURE_CHECK_S = 10  # how often that wait reads the logs for a call that keeps failing
SYNC_WAIT_S = 30  # how long ate-api may take to see a new SandboxConfig
NAME_MAX = 63
GOLDEN_ATESPACE = "ate-golden"  # where ate-api keeps a template's golden actor, named after the template's uid


@dataclasses.dataclass
class Guest:
  """What --guest set up. prepare() fills it in as it goes, so cleanup works even if it fails."""

  files: bool = False  # --guest named files (so --cleanup-guest may remove what it made from them)
  config: str = ""  # the SandboxConfig the run uses
  uploads: list[str] = dataclasses.field(default_factory=list)  # gs:// URLs this run uploaded
  template: dict[str, Any] | None = None  # the temporary ActorTemplate; None: the stock one
  note: str = ""  # the report's guest line

  def started(self) -> bool:
    """Whether prepare() got as far as reading --guest's value."""
    return self.files or bool(self.config)


def parse(value: str):
  """Reads --guest's value -> ({key: path}, SandboxConfig name); one of the two is empty.

  key=path[,key=path...] names files; a directory supplies whichever of ASSETS's
  file names it holds; anything else names a SandboxConfig.
  """
  if "=" in value:
    files = {}
    for part in value.split(","):
      key, _, path = (s.strip() for s in part.partition("="))
      if key not in ASSETS:
        raise common.Error(f"--guest: unknown asset {key!r}; use one of {', '.join(ASSETS)}")
      files[key] = os.path.expanduser(path)
    return files, ""
  path = os.path.expanduser(value)
  if os.path.isdir(path):
    files = {key: os.path.join(path, name) for key, (_, name) in ASSETS.items()
             if os.path.isfile(os.path.join(path, name))}
    if not files:
      raise common.Error(f"--guest: {value} holds none of {', '.join(name for _, name in ASSETS.values())}")
    return files, ""
  if os.path.exists(path):
    raise common.Error(f"--guest: {value} is a file; say which asset it is, e.g. --guest kernel={value}")
  return {}, value


def check_asset(key: str, path: str, arch: str) -> None:
  """Checks that a file can be that guest asset on an arch node."""
  if not os.path.isfile(path):
    raise common.Error(f"--guest: {key}={path}: no such file")
  with open(path, "rb") as f:
    head = f.read(0x210)
  if not head:
    raise common.Error(f"--guest: {key}={path} is empty")
  if key not in ELF_FILES:
    return
  if head[:4] != b"\x7fELF":
    hint = ""
    if key == "kernel" and head[0x202:0x206] == b"HdrS":
      hint = " It looks like a bzImage; cloud-hypervisor boots the uncompressed vmlinux."
    raise common.Error(f"--guest: {key}={path} is not an ELF file.{hint}")
  machine = int.from_bytes(head[18:20], "little" if head[5] == 1 else "big")
  if ELF_MACHINE.get(arch) not in (None, machine):
    raise common.Error(f"--guest: {key}={path} is not built for {arch} (ELF machine {machine}), "
                       "the worker node's architecture")


def _file_sha256(path):
  h = hashlib.sha256()
  with open(path, "rb") as f:
    for block in iter(lambda: f.read(1 << 20), b""):
      h.update(block)
  return h.hexdigest()


def _upload(bucket, path, sha, filename):
  """Puts a file at gs://bucket/CUSTOM_ASSETS/<sha>/<filename> unless it's there -> (URL, uploaded)."""
  url = f"gs://{bucket}/{CUSTOM_ASSETS}/{sha}/{filename}"
  found = kube_lib.gcloud("storage", "ls", url)
  if found and found.returncode == 0:
    return url, False
  common.log(f"  uploading {path} -> {url}")
  proc = kube_lib.gcloud("storage", "cp", path, url, timeout=UPLOAD_TIMEOUT_S)
  if proc is None or proc.returncode != 0:
    raise common.Error(f"--guest: uploading {path} to {url} failed: {proc.stderr.strip() if proc else 'timed out'}")
  return url, True


def node_arch(kube, workers):
  """The architecture of the template's worker node; amd64 if unknown."""
  for name in sorted({w["nodeName"] for w in workers if w.get("nodeName")}):
    arch = (kube.get_json("get", "node", name) or {}).get("metadata", {}).get("labels", {}).get("kubernetes.io/arch")
    if arch:
      return arch
  return "amd64"


def _template_bucket(template):
  storage = (template.get("snapshotConfig") or template.get("snapshotsConfig") or {}).get("storageLocation", "")
  if storage.startswith("gs://") and urllib.parse.urlparse(storage).netloc:
    return urllib.parse.urlparse(storage).netloc
  raise common.Error(f"--guest: {common.TEMPLATE} stores snapshots at {storage or 'no location'}, not in GCS, "
                     "so there is no bucket to upload the guest files to")


def _config_from_files(kube, files, base, base_assets, arch, template, guest):
  """Uploads the files that differ from the stock ones and makes a SandboxConfig of them.

  Returns (its name, its assets for arch, the temporary template's name suffix);
  the base config's name and '' if every file is stock.
  """
  for key, path in files.items():
    check_asset(key, path, arch)
  bucket = _template_bucket(template)
  if not shutil.which("gcloud"):
    raise common.Error("--guest with files needs gcloud, to upload them to the snapshot bucket")
  spec = copy.deepcopy(base["spec"])
  assets = spec.setdefault("assets", {}).setdefault(arch, {})
  shas = {key: _file_sha256(path) for key, path in files.items()}
  # A file equal to the stock one changes nothing.
  new = {k: sha for k, sha in shas.items() if sha != base_assets.get(ASSETS[k][0], {}).get("sha256")}
  for key, sha in new.items():
    asset, filename = ASSETS[key]
    url, uploaded = _upload(bucket, files[key], sha, filename)
    assets[asset] = {"url": url, "sha256": sha}
    guest.uploads += [url] if uploaded else []
  base_name = base["metadata"]["name"]
  if not new:
    guest.files = False  # nothing was made, so nothing to clean up
    return base_name, assets, ""
  asset_shas = json.dumps({a: x.get("sha256") for a, x in assets.items()}, sort_keys=True)
  digest = hashlib.sha256(asset_shas.encode()).hexdigest()[:8]
  name = guest.config = f"{base_name}-g{digest}"
  manifest = {"apiVersion": base.get("apiVersion", "ate.dev/v1alpha1"), "kind": "SandboxConfig",
              "metadata": {"name": name}, "spec": spec}
  kube.kubectl("apply", "-f", "-", stdin=json.dumps(manifest))
  return name, assets, f"g{digest}"


def _existing_config(kube, name, base, arch):
  """The assets of a SandboxConfig on the cluster, checked against the base."""
  config = kube.get_json("get", SANDBOX_CONFIGS, name)
  if not config:
    raise common.Error(f"--guest: there is no SandboxConfig {name} on this cluster (see `kubectl get sandboxconfigs`), "
                       "and no such file or directory")
  want, got = base.get("spec", {}).get("sandboxClass"), config.get("spec", {}).get("sandboxClass")
  if want != got:
    raise common.Error(f"--guest: SandboxConfig {name} is for {got}, but {common.TEMPLATE}'s worker runs {want}")
  return config.get("spec", {}).get("assets", {}).get(arch, {})


def _note(value, files, config, base_name, assets, base_assets):
  """The report's guest line: the assets that differ from the stock ones."""
  changed = [f"{key} {assets[asset].get('sha256', '?')[:8]}" for key, (asset, _) in ASSETS.items()
             if asset in assets and assets[asset].get("sha256") != base_assets.get(asset, {}).get("sha256")]
  if not changed:
    if files:
      return f"stock: {value} matches SandboxConfig {base_name}"
    if config == base_name:
      return f"stock (SandboxConfig {base_name})"
    return f"SandboxConfig {config}: the same assets as {base_name}"
  if files:
    where = f" ({value})" if os.path.isdir(os.path.expanduser(value)) else ""
    return " · ".join(changed) + where + " · rest stock"
  return f"SandboxConfig {config}: " + " · ".join(changed) + " · rest stock"


def _create_template(kube, manifest, since, what, hint=""):
  """Creates the ActorTemplate and waits for its golden snapshot on what ('the new guest'); returns the template.

  ate-api may not see a SandboxConfig that was just applied yet, so the create
  is retried for SYNC_WAIT_S. The golden snapshot boots the guest, starts the
  workload and snapshots it, so a guest or atelet that doesn't work fails here,
  with atelet's and the worker's own errors. The wait stops as soon as a call
  for it has failed the same way logs.REPEATS_TO_FAIL times: retries won't help.
  """
  name = manifest["metadata"]["name"]
  deadline = time.monotonic() + SYNC_WAIT_S
  create = ("create", "actor-template", "-f", "-")
  while (proc := kube.ate(*create, stdin=json.dumps(manifest), check=False)).returncode != 0:
    if time.monotonic() > deadline:
      raise common.Error(f"creating ActorTemplate {name} failed: {(proc.stderr or proc.stdout).strip()}")
    time.sleep(2)
  common.log(f"  waiting for the golden snapshot of {name} on {what}...")
  created = time.time()
  deadline = time.monotonic() + GOLDEN_TIMEOUT_S
  next_check = time.monotonic() + FAILURE_CHECK_S
  while True:
    tmpl, _ = kube.get_template(name)
    golden = tmpl.get("status", {}).get("goldenSnapshotStatus", {})
    if golden.get("errorMessage"):
      raise common.Error(f"the golden snapshot on {what} failed: {golden['errorMessage']}"
                         + logs.recent_problems(kube, since))
    if golden.get("goldenSnapshot") or golden.get("goldenTag"):  # v0.1.0, newer
      return tmpl
    if time.monotonic() > deadline:
      raise common.Error(f"no golden snapshot on {what} after {GOLDEN_TIMEOUT_S}s{hint}"
                         + logs.recent_problems(kube, since))
    if time.monotonic() >= next_check:
      next_check = time.monotonic() + FAILURE_CHECK_S
      if failure := logs.repeated_failure(kube, created, name):
        error, times = failure
        raise common.Error(f"the golden snapshot on {what} can't start: the same call failed {times} times{hint}"
                           f"\n      {error}")
    time.sleep(2)


def _template_copy(template, name, config):
  """The manifest of a copy of the template that uses another SandboxConfig and its own snapshot location."""
  manifest = {k: v for k, v in template.items() if k not in ("metadata", "status")}
  manifest["metadata"] = {"atespace": common.NAMESPACE, "name": name}
  manifest["sandboxConfig"] = dict(template.get("sandboxConfig", {}), configName=config)
  snap_key = "snapshotConfig" if "snapshotConfig" in template else "snapshotsConfig"
  snapshots = dict(template.get(snap_key) or {})
  storage = snapshots.get("storageLocation", "").rstrip("/")
  if storage.endswith(f"/{common.TEMPLATE}"):
    snapshots["storageLocation"] = storage[: -len(common.TEMPLATE)] + name + "/"
    manifest[snap_key] = snapshots
  return manifest


def _delete_golden_actor(kube, tmpl) -> None:
  """Deletes a template's golden actor: ate-api v0.1.0 keeps it when the template is deleted."""
  uid = tmpl.get("metadata", {}).get("uid")
  if not uid:
    return
  proc = kube.ate("delete", "actor", uid, "-a", GOLDEN_ATESPACE, "--any-state", check=False)
  if proc.returncode and "not found" not in (proc.stderr or "").lower():
    common.log(f"  ⚠ couldn't delete {tmpl['metadata'].get('name')}'s golden actor {GOLDEN_ATESPACE}/{uid}: "
               f"{(proc.stderr or proc.stdout).strip()}")


def _drop_template(kube, manifest):
  """Deletes a temporary template, its golden actor and its snapshots, if there, without waiting for its actors."""
  existing, error = kube.get_template(manifest["metadata"]["name"])
  if not error:
    kube.ate("delete", "actor-template", manifest["metadata"]["name"], "-a", common.NAMESPACE, check=False)
    _delete_golden_actor(kube, existing)
  remove_snapshots(manifest)


def temporary_template(kube: kube_lib.Kube, template, name: str, what: str, config: str = "", since=None, hint=""):
  """Makes a copy of the template named name, with its own snapshot location, and waits for its golden snapshot.

  config is the SandboxConfig the copy uses (default: the template's); what says
  where the golden snapshot is taken, for the progress output and errors. A copy
  a stopped run left behind (maybe on another guest or atelet) is replaced, and
  a copy whose golden snapshot fails is deleted. Returns the copy.
  """
  since = time.time() if since is None else since
  config = config or template.get("sandboxConfig", {}).get("configName", "")
  manifest = _template_copy(template, name[:NAME_MAX].rstrip("-"), config)
  _drop_template(kube, manifest)
  try:
    return _create_template(kube, manifest, since, what, hint)
  except (common.Error, KeyboardInterrupt):
    _drop_template(kube, manifest)
    raise


def prepare_config(kube: kube_lib.Kube, value: str, template, workers, guest: Guest) -> tuple[str, str]:
  """Uploads changed guest files and creates or checks the SandboxConfig -> (config_name, suffix)."""
  base_name = template.get("sandboxConfig", {}).get("configName", "")
  base = (kube.get_json("get", SANDBOX_CONFIGS, base_name) or {}) if base_name else {}
  if not base:
    raise common.Error(f"--guest: can't read SandboxConfig {base_name or '(none)'}, which {common.TEMPLATE} uses")
  base.setdefault("metadata", {})["name"] = base_name
  arch = node_arch(kube, workers)
  base_assets = base.get("spec", {}).get("assets", {}).get(arch, {})
  files, config = parse(value)
  guest.files, guest.config = bool(files), config
  if files:
    config, assets, suffix = _config_from_files(kube, files, base, base_assets, arch, template, guest)
    guest.config = config
  else:
    assets, suffix = _existing_config(kube, config, base, arch), config
  guest.note = _note(value, files, config, base_name, assets, base_assets)
  return config, suffix


def prepare(kube: kube_lib.Kube, value: str, template, workers, guest: Guest):
  """Sets up what --guest asks for, filling in guest as it goes."""
  since = time.time()
  base_name = template.get("sandboxConfig", {}).get("configName", "")
  config, suffix = prepare_config(kube, value, template, workers, guest)
  common.log(f"  guest     {guest.note}")
  if config == base_name:
    return  # the template's own SandboxConfig: run the template itself
  guest.template = temporary_template(kube, template, f"{common.TEMPLATE}-{suffix}", "the new guest", config=config,
                                      since=since, hint="; the guest may not boot")


def remove_snapshots(tmpl) -> None:
  """Removes a temporary template's snapshots from GCS (deleting a template leaves its golden snapshot).

  Only a location named after the template itself, which prepare() gave it, is removed.
  """
  name = tmpl.get("metadata", {}).get("name", "")
  storage = (tmpl.get("snapshotConfig") or tmpl.get("snapshotsConfig") or {}).get("storageLocation", "").rstrip("/")
  mine = name and name != common.TEMPLATE and storage.startswith("gs://") and storage.endswith(f"/{name}")
  if mine and shutil.which("gcloud"):
    kube_lib.gcloud("storage", "rm", "-r", storage + "/", timeout=UPLOAD_TIMEOUT_S)


def delete_template(kube: kube_lib.Kube, tmpl) -> bool:
  """Deletes a temporary template, its golden actor and its snapshots; returns whether it's gone.

  Waits out the deletion of the run's actor, which still refers to it for a moment.
  """
  name = tmpl["metadata"]["name"]
  deadline = time.monotonic() + SYNC_WAIT_S
  while True:
    proc = kube.ate("delete", "actor-template", name, "-a", common.NAMESPACE, check=False)
    if proc.returncode == 0 or "not found" in (proc.stderr or "").lower():
      _delete_golden_actor(kube, tmpl)
      remove_snapshots(tmpl)
      return True
    if time.monotonic() > deadline:
      common.log(f"  ⚠ couldn't delete ActorTemplate {name}: {(proc.stderr or proc.stdout).strip()}")
      return False
    time.sleep(2)


def _asset_urls(config):
  """The URLs of a SandboxConfig's assets, for every arch."""
  urls = set()
  for assets in config.get("spec", {}).get("assets", {}).values():
    urls |= {a.get("url") for a in assets.values()}
  return urls


def cleanup(kube: kube_lib.Kube, guest: Guest) -> None:
  """--cleanup-guest: removes the SandboxConfig --guest made from files, and its uploads nothing else uses.

  A SandboxConfig named with --guest NAME is never removed.
  """
  if not guest.files:
    common.log(f"  kept SandboxConfig {guest.config}: --cleanup-guest only removes what --guest made from files")
    return
  config = (kube.get_json("get", SANDBOX_CONFIGS, guest.config) or {}) if guest.config else {}
  mine = {u for u in _asset_urls(config) if f"/{CUSTOM_ASSETS}/" in (u or "")} | set(guest.uploads)
  if config:
    kube.kubectl("delete", SANDBOX_CONFIGS, guest.config, "--ignore-not-found", check=False)
  others = kube.get_json("get", SANDBOX_CONFIGS)
  if others is None:
    common.log(f"  removed SandboxConfig {guest.config}; kept its uploads: couldn't list the other SandboxConfigs "
               "to see which still use them")
    return
  unused = sorted(mine - set().union(*(_asset_urls(c) for c in others.get("items", []))))
  for url in unused:
    kube_lib.gcloud("storage", "rm", url)
  common.log(f"  removed SandboxConfig {guest.config or '(none)'} and {len(unused)} uploaded file(s)")
