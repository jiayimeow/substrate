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
"""Reads the ateapi, atelet and ateom logs of the runner's calls.

ateapi, atelet and ateom each log the work they do for a call, and every log
line carries the call's W3C trace ID, which the runner chose. After the run,
collect_records() reads those logs with `kubectl logs` and keeps the lines of
the runner's calls. No server change is needed: the servers already write all
of this.
"""

import concurrent.futures
import datetime
import json
import re

# Plain imports, not google3 ones: baseline-perf runs with plain python3.
from bperf import common
from bperf import kube as kube_lib

# (component, label selector, the container that holds its logs).
LOG_SOURCES = (("ateapi", "app=ate-api-server", "ate-api-server"), ("atelet", "app=atelet", "atelet"))
WORKER_SELECTOR = "ate.dev/worker-pool"  # every WorkerPool pod, any namespace
WORKER_CONTAINER = "ateom"
LOG_SLACK_S = 2  # `kubectl logs --since-time` has whole-second granularity
LOG_READERS = 8  # pods read in parallel
RECENT_PROBLEMS = 6  # distinct problems recent_problems() shows
REPEATS_TO_FAIL = 3  # a call that failed the same way this many times won't work on a retry either

_RFC3339 = re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)$")
_GO_DURATION_PART = re.compile(r"(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|μs|ms|s|m|h)")
_GO_DURATION_UNIT_S = {"ns": 1e-9, "us": 1e-6, "µs": 1e-6, "μs": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0}


def parse_time(s: str) -> float | None:
  """RFC 3339 (nanoseconds allowed) -> Unix seconds, or None."""
  m = _RFC3339.match(s or "")
  if not m:
    return None
  zone = "+00:00" if m.group(3) == "Z" else m.group(3)
  base = datetime.datetime.fromisoformat(m.group(1) + zone)
  return base.timestamp() + (int(m.group(2)) / 10 ** len(m.group(2)) if m.group(2) else 0)


def parse_go_duration(s) -> float | None:
  """A Go duration string ("1.5ms", "2m3.1s", "850µs") -> seconds, or None."""
  if not isinstance(s, str) or not s:
    return None
  sign, body = (-1, s[1:]) if s[0] == "-" else (1, s.lstrip("+"))
  if body == "0":
    return 0.0
  pos, total = 0, 0.0
  for m in _GO_DURATION_PART.finditer(body):
    if m.start() != pos:
      return None
    total += float(m.group(1)) * _GO_DURATION_UNIT_S[m.group(2)]
    pos = m.end()
  return sign * total if pos == len(body) and pos else None


def _since_arg(since: float) -> str:
  return datetime.datetime.fromtimestamp(int(since), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Record:
  """One JSON log line of ateapi, atelet or ateom, with its keys in log order."""

  def __init__(self, fields, component="", pod="", node=""):
    self.fields = fields  # [(key, value)] as logged, duplicates kept
    self.component, self.pod, self.node = component, pod, node  # who logged it, and where
    self.time = parse_time(self.str("time")) or 0.0  # Unix seconds (0 if it has none)
    self.msg = self.str("msg")
    self.trace_id = self.str("trace_id")

  def get(self, key, default=None):
    """The first value logged for key."""
    return next((v for k, v in self.fields if k == key), default)

  def str(self, key) -> str:
    """The first value logged for key if it's a string, else ''."""
    v = self.get(key)
    return v if isinstance(v, str) else ""


def parse_record(line: str, **where) -> Record | None:
  """One log line -> a Record (where: its component, pod, node), or None."""
  try:
    fields = json.loads(line, object_pairs_hook=lambda pairs: pairs)
  except ValueError:
    return None
  return Record(fields, **where) if isinstance(fields, list) else None


def _log_targets(component, pods, container):
  """(component, pod, container) to read: the component's own container if the pod has it, else all."""
  out = []
  for pod in pods:
    names = [c["name"] for c in pod["spec"].get("containers", [])]
    out += [(component, pod, name) for name in ([container] if container in names else names)]
  return out


def _read_one(kube, target, since_arg, want):
  """The Records of the wanted trace IDs in one container's log, and an error."""
  component, pod, container = target
  meta = pod["metadata"]
  where = f"{meta['namespace']}/{meta['name']}"
  try:
    proc = kube.kubectl("-n", meta["namespace"], "logs", meta["name"], "-c", container,
                        f"--since-time={since_arg}", timeout=common.ATE_TIMEOUT_S, check=False)
  except common.Error:
    return [], f"reading logs of {where}: timed out"
  if proc.returncode != 0:
    return [], f"reading logs of {where}: {proc.stderr.strip()}"
  out = []
  for line in proc.stdout.splitlines():
    if not any(t in line for t in want):  # cheap pre-filter
      continue
    rec = parse_record(line, component=component, pod=meta["name"], node=pod["spec"].get("nodeName", ""))
    if rec and rec.trace_id in want:
      out.append(rec)
  return out, None


def _read_logs(kube, targets, since, want):
  """The Records of the wanted trace IDs in the targets' logs, and errors."""
  since_arg = _since_arg(since)
  records, errors = [], []
  with concurrent.futures.ThreadPoolExecutor(LOG_READERS) as pool:
    for recs, err in pool.map(lambda t: _read_one(kube, t, since_arg, want), targets):
      records += recs
      errors += [err] if err else []
  return records, errors


def collect_records(kube: kube_lib.Kube, since: float, trace_ids):
  """Reads the log lines of these trace IDs -> (Records sorted by time, problems met reading them).

  ateom runs in the worker pods, and atelet only talks to the workers on its own
  node, so only the workers on nodes where atelet logged one of the calls are
  read.
  """
  want = set(trace_ids)
  targets, errors = [], []
  for component, selector, container in LOG_SOURCES:
    pods, err = kube.running_pods(common.SYSTEM_NAMESPACE, selector)
    if err:
      errors.append(f"listing {component} pods: {err}")
      continue
    targets += _log_targets(component, pods, container)
  records, errs = _read_logs(kube, targets, since, want)
  errors += errs

  targets = []
  for node in sorted({r.node for r in records if r.component == "atelet" and r.node}):
    pods, err = kube.running_pods(None, WORKER_SELECTOR, f"spec.nodeName={node}")
    if err:
      errors.append(f"listing worker pods on {node}: {err}")
      continue
    targets += _log_targets("ateom", pods, WORKER_CONTAINER)
  worker_records, errs = _read_logs(kube, targets, since, want)
  return sorted(records + worker_records, key=lambda r: r.time), errors + errs


def _err(rec):
  return rec.get("err") or rec.get("error") or ""


def _problem(rec) -> str:
  """What went wrong in a log line, or '': an error or a warning, or a call that failed (ateom logs those at INFO)."""
  err = _err(rec)
  if not err and rec.str("level") not in ("ERROR", "WARN"):
    return ""
  what = rec.str("method").rsplit("/", 1)[-1] or rec.msg
  return f"{rec.component}: {what}" + (f": {err}" if err else "")


def _problems(kube, since, about="", calls_only=False):
  """The problems atelet and the workers logged since since, each once -> [(text, times, last time)], oldest first.

  about keeps only the lines that mention it (a template's name, say), and
  calls_only only the calls that failed.
  """
  since_arg = _since_arg(since - LOG_SLACK_S)
  targets = []
  sources = (("atelet", common.SYSTEM_NAMESPACE, "app=atelet"), ("ateom", None, WORKER_SELECTOR))
  for component, namespace, selector in sources:
    pods, _ = kube.running_pods(namespace, selector)
    targets += [(component, pod["metadata"]) for pod in pods or []]

  def read(target):
    component, meta = target
    try:
      proc = kube.kubectl("-n", meta["namespace"], "logs", meta["name"], "--all-containers",
                          f"--since-time={since_arg}", check=False)
    except common.Error:  # timed out: the other pods may still say what's wrong
      return []
    found = []
    for line in proc.stdout.splitlines():
      rec = parse_record(line, component=component) if about in line else None
      if not rec or (calls_only and not (rec.str("method") and _err(rec))):
        continue
      if text := _problem(rec):
        found.append((rec.time, text[:300]))
    return found

  seen = {}  # text -> (times, last time)
  with concurrent.futures.ThreadPoolExecutor(LOG_READERS) as pool:
    for found in pool.map(read, targets):
      for t, text in found:
        times, last = seen.get(text, (0, 0.0))
        seen[text] = (times + 1, max(last, t))
  return sorted(((text, times, last) for text, (times, last) in seen.items()), key=lambda p: p[2])


def recent_problems(kube: kube_lib.Kube, since: float) -> str:
  """The last few problems of atelet and the workers, each once, as indented lines; '' if none."""
  shown = _problems(kube, since)[-RECENT_PROBLEMS:]
  return "".join(f"\n      {text}" + (f" (×{times})" if times > 1 else "") for text, times, _ in shown)


def repeated_failure(kube: kube_lib.Kube, since: float, about: str) -> tuple[str, int] | None:
  """A call that failed the same way REPEATS_TO_FAIL times since since, about about (a template) -> (error, times)."""
  for text, times, _ in reversed(_problems(kube, since, about, calls_only=True)):
    if times >= REPEATS_TO_FAIL:
      return text, times
  return None
