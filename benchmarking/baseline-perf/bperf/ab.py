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
"""--compare: the benchmark on two setups in turn, and how they compare.

Each arm is a setup, given as a spec: KEY=VALUE pairs, comma-separated, or
'stock' for the cluster as it is. With one spec (--compare SPEC), arm A is
'stock' and arm B is SPEC. A key that only one arm sets is 'stock' in the other,
so both arms get the same treatment for it and only the values differ. cli.py
runs the arms alternately, A, B, A, B, ..., each run a whole baseline-perf run
on its arm's own copy of the template, made right after the arm's setup is first
in place. This module has the specs, the run order, the numbers each run gives,
and the comparison printed at the end: per arm, the mean of its runs and their
range, and B - A.
"""

import posixpath
import statistics
from typing import Any

# Plain imports, not google3 ones: baseline-perf runs with plain python3.
from bperf import common
from bperf import guest
from bperf import report
from bperf import sampler

ARMS = ("A", "B")
ROUNDS = 3  # runs per arm by default
STOCK = "stock"  # as a spec: the cluster as it is; as a value: what the cluster runs now
GUEST_ASSETS = tuple(guest.ASSETS)  # kernel, rootfs, kata-config, cloud-hypervisor, virtiofsd
# The keys a spec can set: atelet=IMAGE|DIR[@REF], guest=DIR|CONFIG, or individual guest asset keys.
KNOBS = ("atelet", "guest") + GUEST_ASSETS
UPPER = "rootfs-upper.tar"  # the snapshot file that holds what the actor changed in its rootfs
MEMORY = "memory-ranges"  # the snapshot file that holds the VM's RAM
PATH_ROWS = (  # (label, metric) for each path; the metric's key is "<park> <metric>"
    ("Cold start (golden)", "cold"),
    ("Resume, median", "resume"),
    ("Exec, median", "exec"),
    ("Park, median", "park"),
    ("Exec, 4 cycles", "exec_total"),
    ("Exec, cycle 1", "exec_1"),
)
SIZE_ROWS = (  # (label, metric): the suspend path's snapshots, in bytes
    ("rootfs upper, total", "upper_total"),
    ("Last memory, populated", "memory_populated"),
    ("Last rootfs upper", "upper_last"),
    ("Last snapshot, populated", "populated"),
    ("Last snapshot, in GCS", "stored"),
)
ROW = " {:<26}{:>10} {:<13}{:>10} {:<13}{:>10} {:>5}{}"
RUN_ROW = " {:>3}  {:<6}{:>14}{:>14}{:>14}{:>14}  {}"


def order(rounds: int) -> list[str]:
  """The arms in run order: A, B, A, B, ..."""
  return list(ARMS) * rounds


def parse_spec(text: str) -> dict[str, str]:
  """One arm's spec -> {'atelet': ..., 'guest': ...}, or {} for 'stock'. Raises ValueError saying what's wrong."""
  if text.strip() == STOCK:
    return {}
  spec = {}
  for item in text.split(","):
    key, eq, value = (part.strip() for part in item.partition("="))
    if not (key and eq and value):
      raise ValueError(f"{text!r} isn't {STOCK!r} or KEY=VALUE[,KEY=VALUE...], such as atelet=IMAGE or kernel=PATH")
    if key not in KNOBS:
      raise ValueError(f"unknown key {key!r} in {text!r}; the keys are: {', '.join(KNOBS)}")
    if key in spec:
      raise ValueError(f"{key!r} is set twice in {text!r}")
    spec[key] = value
  assets = [f"{k}={spec.pop(k)}" for k in GUEST_ASSETS if k in spec]
  if assets:
    if "guest" in spec:
      raise ValueError(f"in {text!r}: use either guest=... or asset keys ({', '.join(GUEST_ASSETS)}), not both")
    spec["guest"] = ",".join(assets)
  return spec


def arms(specs) -> dict[str, dict[str, str]]:
  """Each arm's setup, from --compare's 1 or 2 specs (with 1, arm A is 'stock'). Raises ValueError."""
  if len(specs) == 1:
    specs = [STOCK, specs[0]]
  if len(specs) != 2:
    raise ValueError(f"pass 1 spec (compared against {STOCK}) or 2 specs (A and B); got {len(specs)}")
  parsed = [parse_spec(s) for s in specs]
  keys = [k for k in ("atelet", "guest") if any(k in p for p in parsed)]
  setups = {arm: {k: p.get(k, STOCK) for k in keys} for arm, p in zip(ARMS, parsed)}
  if setups["A"] == setups["B"]:
    raise ValueError(f"both arms are the same setup ({' '.join(specs)}); set what B changes, as in "
                     "--compare atelet=IMAGE or --compare kernel=./vmlinux")
  return setups


def resolve(setup: dict[str, str], stock: dict[str, str]) -> dict[str, str]:
  """An arm's setup with each 'stock' replaced by what the cluster runs (stock: key -> that)."""
  return {key: stock[key] if value == STOCK else value for key, value in setup.items()}


def _describe_item(key: str, raw: str, value: str) -> str:
  shown = short_image(value) if key == "atelet" else value
  suffix = " (stock)" if raw == STOCK else f" ({raw})" if key == "atelet" and raw != value else ""
  return f"{key}={shown}{suffix}"


def describe(setup: dict[str, str], stock: dict[str, str], resolved: dict[str, str] | None = None) -> str:
  """An arm's setup, short: 'atelet=atelet:v2@662323fd', or 'atelet=atelet:v1@f79efcec (stock)'."""
  items = resolved or resolve(setup, stock)
  return " · ".join(_describe_item(k, setup[k], v) for k, v in items.items())


def template_name(arm: str) -> str:
  """The name of an arm's copy of the template."""
  return f"{common.TEMPLATE}-ab-{arm.lower()}"


def short_image(ref: str) -> str:
  """'gcr.io/p/r/atelet:tag@sha256:f79efcec06d2…' -> 'atelet:tag@f79efcec'."""
  name, _, digest = ref.partition("@")
  name = name.rsplit("/", 1)[-1]
  return f"{name}@{digest.removeprefix('sha256:')[:8]}" if digest else name


def atelet_commit(setup) -> str:
  """The commit of the atelet that ran, from the header's "commit" row (see cluster_info), or '?'."""
  items = next((i for label, i in setup if label == "commit"), [])
  for item in items:
    name, _, commit = item.partition(" ")
    if name == "atelet" and commit:
      return report.short_commit(commit)
  if len(items) == 1 and " " not in items[0]:  # every binary has the same one
    return report.short_commit(items[0])
  return "?"


def upload_sizes(path: dict[str, Any], records) -> list[dict[str, tuple[int, int]]]:
  """For each park call of a path, the cold start's first: {file: (logical, populated bytes)} atelet uploaded."""
  calls = ([path["cold"]["suspend"]] if path.get("cold") else []) + [c["suspend"] for c in path.get("cycles", [])]
  out = []
  for call in calls:
    uploads = [r for r in records if r.msg == report.MSG_UPLOAD and r.component == "atelet"
               and r.trace_id == call.get("trace_id")]
    out.append({posixpath.basename(r.str("object")).removesuffix(".zstd"):
                (int(r.get("logical_bytes") or 0), int(r.get("populated_bytes") or 0)) for r in uploads})
  return out


def metrics(result: dict[str, Any], records, sizes) -> dict[str, Any]:
  """One run's numbers by metric (see PATH_ROWS, SIZE_ROWS): ms, or bytes. A path missing a cycle gives none.

  "uppers" is the rootfs-upper.tar size of each suspend, the cold start's first.
  """
  out = {}
  for p in result.get("paths", []):
    park, cycles = p["park"], p.get("cycles", [])
    cold = ((p.get("cold") or {}).get("resume") or {}).get("client_ms")
    if cold is not None:
      out[f"{park} cold"] = cold
    if len(cycles) != len(common.CHUNKS):
      continue
    execs = [c.get("exec_wall") for c in cycles]
    for name, values in (("resume", [c["resume"].get("client_ms") for c in cycles]), ("exec", execs),
                         ("park", [c["suspend"].get("client_ms") for c in cycles])):
      if None not in values:
        out[f"{park} {name}"] = statistics.median(values)
    if None not in execs:
      out[f"{park} exec_total"], out[f"{park} exec_1"] = sum(execs), execs[0]
    if park != "Suspend":
      continue
    uploads = upload_sizes(p, records)
    if uploads and all(UPPER in u for u in uploads):
      out["uppers"] = [u[UPPER][0] for u in uploads]
      out["upper_total"] = sum(out["uppers"])
      out["upper_last"] = out["uppers"][-1]
    if uploads and MEMORY in uploads[-1]:
      out["memory_populated"] = uploads[-1][MEMORY][1]
    if sizes and sizes.get("cycles"):
      out["populated"] = sizes["cycles"][-1]["populated"]
    if sizes and sizes.get("stored") is not None:
      out["stored"] = sizes["stored"]
  return out


def compare(a, b):
  """How two arms' values compare: (mean A, mean B, B - A, B - A in % of A or None, whether their ranges are apart)."""
  ma, mb = statistics.mean(a), statistics.mean(b)
  return ma, mb, mb - ma, (100 * (mb - ma) / ma if ma else None), max(a) < min(b) or max(b) < min(a)


def _value(v: float, unit: str) -> str:
  return f"{v:,.0f} ms" if unit == "ms" else f"{v / 1e6:,.1f} MB"


def _span(values, unit: str) -> str:
  """'470-500': the range of an arm's values, or '' for one value."""
  if len(values) < 2:
    return ""
  lo, hi = (min(values), max(values)) if unit == "ms" else (min(values) / 1e6, max(values) / 1e6)
  return f"{lo:,.0f}-{hi:,.0f}" if unit == "ms" else f"{lo:,.1f}-{hi:,.1f}"


def _signed(v: float, unit: str) -> str:
  return ("+" if v > 0 else "-" if v < 0 else "±") + _value(abs(v), unit)


def row(label: str, a, b, unit: str, color: bool = False) -> str:
  """One line of the comparison; '' if an arm has no value for it."""
  if not a or not b:
    return ""
  ma, mb, delta, pct, apart = compare(a, b)
  pct_text = "" if pct is None else ("+" if pct > 0 else "-" if pct < 0 else "±") + f"{abs(pct):.0f}%"
  mark = " *" if apart and len(a) > 1 and len(b) > 1 else ""
  if not color:
    return ROW.format(label, _value(ma, unit), _span(a, unit), _value(mb, unit), _span(b, unit), _signed(delta, unit),
                      pct_text, mark).rstrip()
  col_sa = report.paint(f"{_span(a, unit):<13}", report.DIM)
  col_sb = report.paint(f"{_span(b, unit):<13}", report.DIM)
  diff_cell = f"{_signed(delta, unit):>10} {pct_text:>5}{mark}".rstrip()
  if delta < 0:
    diff_style = report.BOLD + report.GREEN if mark else report.GREEN
  elif delta > 0:
    diff_style = report.BOLD + report.RED if mark else report.YELLOW
  else:
    diff_style = report.DIM
  return f" {label:<26}{_value(ma, unit):>10} {col_sa}{_value(mb, unit):>10} {col_sb}{report.paint(diff_cell, diff_style)}"


def table_lines(by_arm: dict[str, list[dict[str, Any]]], parks, color: bool = False) -> list[str]:
  """The comparison table: per metric, each arm's mean and range over its runs, and B - A."""
  n = {arm: len(by_arm.get(arm, [])) for arm in ARMS}
  runs = f"{n['A']}" if n["A"] == n["B"] else f"{n['A']} and {n['B']}"
  runs += " run" if runs == "1" else " runs"
  head = ROW.format(f"Mean (range) of {runs}", "A", "", "B", "", "B - A", "", "").rstrip()
  lines = [report.paint(head, report.BOLD, color)]

  def values(arm, key):
    return [m[key] for m in by_arm.get(arm, []) if m.get(key) is not None]

  for park in parks:
    rows = [row(f"  {label}", values("A", f"{park} {key}"), values("B", f"{park} {key}"), "ms", color=color)
            for label, key in PATH_ROWS]
    if any(rows):
      lines += [report.paint(f" {common.PATHS[park]}", report.BOLD + report.CYAN, color)] + [r for r in rows if r]
  rows = [row(f"  {label}", values("A", key), values("B", key), "MB", color=color) for label, key in SIZE_ROWS]
  if any(rows):
    lines += [report.paint(" Suspend path snapshots", report.BOLD + report.CYAN, color)] + [r for r in rows if r]
  for arm in ARMS:
    uppers = [m["uppers"] for m in by_arm.get(arm, []) if m.get("uppers")]
    if uppers and len({len(u) for u in uppers}) == 1:
      means = [statistics.mean(u[i] for u in uppers) / 1e6 for i in range(len(uppers[0]))]
      upper_line = f"   rootfs upper per suspend, {arm}: " + " · ".join(f"{v:,.1f}" for v in means) + " MB"
      lines.append(report.paint(upper_line, report.DIM, color))
  return lines


def run_lines(runs, parks, color: bool = False) -> list[str]:
  """One line per run, in run order: its arm, each path's exec over 4 cycles, the cold start, the upper total."""
  heads = [f"{p.lower()} exec" for p in parks] + [""] * (2 - len(parks)) + ["cold start", "upper total"]
  lines = [report.paint(RUN_ROW.format("Run", "Arm", *heads, "node").rstrip(), report.BOLD, color)]
  for i, (arm, run, m) in enumerate(runs, 1):
    cells = [_value(m[f"{p} exec_total"], "ms") if f"{p} exec_total" in m else "–" for p in parks]
    cells += [""] * (2 - len(parks))
    cold = m.get(f"{parks[0]} cold")
    cells += [_value(cold, "ms") if cold is not None else "–",
              _value(m["upper_total"], "MB") if "upper_total" in m else "–"]
    before = (run.node or {}).get("before")
    reasons = sampler.busy_reasons(before) if before else []
    node = ("busy: " + ", ".join(reasons) if reasons else "idle") if before else "?"
    status = node + (f" · failed: {run.error}" if run.error else "")
    status_style = report.RED if run.error else report.YELLOW if reasons else report.GREEN if node == "idle" else report.DIM
    lines.append(RUN_ROW.format(i, arm, *cells, report.paint(status, status_style, color)).rstrip())
  return lines


def print_report(runs, labels: dict[str, str], parks, elapsed_s: float, error: str | None = None) -> None:
  """Prints how the arms compare. runs: [(arm, cli._Run)] in run order; labels: arm -> its setup, from describe()."""
  done = [(arm, run, metrics(run.result, run.records, run.sizes)) for arm, run in runs if run.has_cycles()]
  if not done:
    return
  color = report.use_color()
  rule = report.paint("═" * common.WIDTH, report.CYAN, color)
  by_arm = {arm: [m for a, _, m in done if a == arm] for arm in ARMS}
  rounds = max(len(v) for v in by_arm.values())
  has_atelet = "atelet=" in labels.get("A", "")
  common.log()
  common.log(rule)
  plural = "s" if rounds > 1 else ""
  title = f" baseline-perf A/B · {common.TEMPLATE} · {rounds} round{plural} · {elapsed_s / 60:.0f} min"
  common.log(report.paint(title, report.BOLD, color))
  setup = done[0][1].setup
  head = [(label, items) for label, items in setup if not (has_atelet and label == "commit")]
  for line in report.headline(head):
    common.log(report.paint(f" {line}", report.DIM, color))
  commits = {}
  for arm, run, _ in done:
    commits.setdefault(arm, atelet_commit(run.setup))
  for arm in ARMS:
    commit_suffix = f" · atelet commit {commits.get(arm, '?')}" if has_atelet else ""
    common.log(f" {report.paint(arm, report.BOLD, color)}  {labels[arm]}{report.paint(commit_suffix, report.DIM, color)}")
  common.log(rule)
  for line in table_lines(by_arm, parks, color=color):
    common.log(line)
  common.log()
  for line in run_lines(done, parks, color=color):
    common.log(line)
  notes = []
  if any(r.endswith(" *") for r in table_lines(by_arm, parks)):
    notes.append(("* the arms' ranges don't overlap: the difference is bigger than the spread between runs", False))
  if has_atelet and commits.get("A") == commits.get("B") and commits.get("A") != "?":
    notes.append((f"both arms report atelet commit {commits['A']}: check that the atelet images differ", True))
  if any(sampler.busy_reasons((run.node or {}).get("before")) for _, run, _ in done if (run.node or {}).get("before")):
    notes.append(("the node was busy before some runs (see the node column): those may include contention", True))
  if error:
    notes.append((f"stopped early: {error}", True))
  if notes:
    common.log()
  for note, warn in notes:
    common.log(report.paint(f" Note: {note}", report.YELLOW if warn else report.DIM, color))
