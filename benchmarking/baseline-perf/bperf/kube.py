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
"""Every kubectl, kubectl-ate and gcloud call from your machine goes through here."""

import json
import subprocess
from typing import Any

# Plain imports, not google3 ones: baseline-perf runs with plain python3.
from bperf import common

KUBECTL_TIMEOUT_S = 20  # one kubectl call
GCLOUD_TIMEOUT_S = 60  # one gcloud call that isn't an upload


def _run(cmd, shown, stdin, timeout, check) -> subprocess.CompletedProcess[str]:
  """Runs cmd; raises common.Error, naming it as shown, if it hangs, or fails and check is set."""
  try:
    proc = subprocess.run(cmd, input=stdin, capture_output=True, text=True, timeout=timeout, check=False)
  except subprocess.TimeoutExpired:
    raise common.Error(f"`{shown}` did not finish within {timeout}s") from None
  if check and proc.returncode != 0:
    raise common.Error(f"`{shown}` failed: {(proc.stderr or proc.stdout).strip()}")
  return proc


class Kube:
  """kubectl and kubectl-ate on the current kube context."""

  def __init__(self, kubectl_ate: str):
    self.kubectl_ate = kubectl_ate

  def ate(self, *args, stdin=None, check=True) -> subprocess.CompletedProcess[str]:
    cmd = [self.kubectl_ate, *args]
    return _run(cmd, " ".join(cmd), stdin, common.ATE_TIMEOUT_S, check)

  def kubectl(self, *args, stdin=None, timeout=KUBECTL_TIMEOUT_S, check=True) -> subprocess.CompletedProcess[str]:
    cmd = ["kubectl", *args]
    return _run(cmd, " ".join(cmd[:4]) + " ...", stdin, timeout, check)

  def get_json(self, *args):
    """`kubectl <args> -o json`, parsed; None if it fails."""
    try:
      proc = subprocess.run(["kubectl", *args, "-o", "json"], capture_output=True, text=True,
                            timeout=KUBECTL_TIMEOUT_S, check=False)
      return json.loads(proc.stdout) if proc.returncode == 0 else None
    except (subprocess.TimeoutExpired, ValueError):
      return None

  def current_context(self) -> str:
    return self.kubectl("config", "current-context", check=False).stdout.strip()

  def running_pods(self, namespace, selector, field_selector=None):
    """The running pods matching a selector, in namespace or (None) all -> (pods, None) or (None, error)."""
    args = ["get", "pods", "-l", selector, "-o", "json"] + (["-A"] if namespace is None else ["-n", namespace])
    if field_selector:
      args += ["--field-selector", field_selector]
    proc = self.kubectl(*args, check=False)
    if proc.returncode != 0:
      return None, (proc.stderr or proc.stdout).strip()
    return [p for p in json.loads(proc.stdout).get("items", []) if p.get("status", {}).get("phase") == "Running"], None

  def get_template(self, name: str) -> tuple[dict[str, Any], str]:
    """An ActorTemplate in common.NAMESPACE. Returns (the template, "") or ({}, kubectl-ate's error)."""
    proc = self.ate("get", "actor-template", name, "-a", common.NAMESPACE, "-o", "json", check=False)
    if proc.returncode != 0:
      return {}, proc.stderr.strip()
    tmpl = json.loads(proc.stdout or "{}")
    if "actorTemplates" in tmpl:  # kubectl-ate v0.1.0 wraps it; newer ones print the template itself
      tmpl = (tmpl.get("actorTemplates") or [{}])[0]
    return tmpl, ""


def gcloud(*args, timeout=GCLOUD_TIMEOUT_S):
  """Runs gcloud. Returns the finished process, or None if it didn't finish in time."""
  try:
    return subprocess.run(["gcloud", *args], capture_output=True, text=True, timeout=timeout, check=False)
  except subprocess.TimeoutExpired:
    return None
