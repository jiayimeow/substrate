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
"""The report printed at the end of a run, and the snapshot sizes in it.

What it ran on and how busy the node was, one row of medians per path, where each path's time goes,
and at most a few notes.
"""

import os
import posixpath
import re
import shutil
import statistics
import sys
from typing import Any
import urllib.parse

# Plain imports, not google3 ones: baseline-perf runs with plain python3.
from bperf import breakdown
from bperf import common
from bperf import kube as kube_lib
from bperf import sampler

BAR_WIDTH = 20  # characters for 100% in the breakdown bars
TOP_PHASES = 2  # phases named in each breakdown; the rest is "other"
SHORT_WIDTH = 62
MEDIAN_ROW = " {:<22}{:>9} {:>9} {:>9}"
PATH_ROWS = {"Pause": "Pause (on the node)", "Suspend": "Suspend (to GCS)"}
MSG_UPLOAD = "Compressed zstd upload"  # atelet, once per snapshot file uploaded
BOLD, DIM, RED, GREEN, YELLOW, CYAN, RESET = "\033[1m", "\033[2m", "\033[31m", "\033[32m", "\033[33m", "\033[36m", "\033[0m"


def use_color() -> bool:
  """Whether to colorize report output (on a non-dumb terminal unless $NO_COLOR is set, or $FORCE_COLOR is set)."""
  if os.environ.get("NO_COLOR"):
    return False
  return bool(os.environ.get("FORCE_COLOR")) or (sys.stdout.isatty() and os.environ.get("TERM") != "dumb")


def paint(text: str, code: str, color: bool = True) -> str:
  """Wraps text in an ANSI style code when color is True and text is non-empty."""
  return f"{code}{text}{RESET}" if color and text else text


# ── Snapshot sizes ───────────────────────────────────────────────────────────
#
# atelet logs one MSG_UPLOAD line per snapshot file, with the file's logical size (the memory file
# is the VM's whole RAM, mostly holes) and its populated bytes (what is actually read, compressed
# and sent). It doesn't log the compressed size, so that is read from GCS, for the last snapshot
# only: each suspend deletes the snapshot it replaces, so the actor must still exist. A pause's
# checkpoint stays on the node and its size isn't logged.


def _stored_bytes(storage, last_dir):
  """(the last snapshot's bytes in GCS or None, why it's None or '')."""
  bucket = urllib.parse.urlparse(storage).netloc if storage.startswith("gs://") else ""
  if not bucket or not last_dir:
    return None, f"no GCS location in the template ({storage or 'none'})"
  if not shutil.which("gcloud"):
    return None, "gcloud is not on PATH"
  proc = kube_lib.gcloud("storage", "ls", "-l", f"gs://{bucket}/{last_dir}/")
  if proc is None:
    return None, "gcloud storage ls timed out"
  m = re.search(r"^TOTAL: \d+ objects?, (\d+) bytes", proc.stdout, re.M)
  if m:
    return int(m.group(1)), ""
  return None, (proc.stderr.strip().splitlines() or ["no listing"])[-1]


def snapshot_sizes(path: dict[str, Any], records, storage: str) -> dict[str, Any] | None:
  """How big a suspend path's snapshots are, from atelet's logs and GCS.

  Returns {"cycles": [{"logical", "populated", "memory_populated", "upper"}], "stored": bytes or
  None, "stored_error": why stored is None, or ""}; None if no upload was logged.
  """
  cycles, last_dir = [], ""
  for c in path.get("cycles", []):
    trace_id = c["suspend"]["trace_id"]
    uploads = [r for r in records if r.msg == MSG_UPLOAD and r.component == "atelet" and r.trace_id == trace_id]
    if not uploads:
      continue
    by_name = {posixpath.basename(r.str("object")).removesuffix(".zstd"): r for r in uploads}
    mem = by_name.get("memory-ranges")
    upper = by_name.get("rootfs-upper.tar")
    cycles.append({
        "logical": sum(int(r.get("logical_bytes") or 0) for r in uploads),
        "populated": sum(int(r.get("populated_bytes") or 0) for r in uploads),
        "memory_populated": int(mem.get("populated_bytes") or 0) if mem else None,
        "upper": int(upper.get("logical_bytes") or 0) if upper else None,
    })
    last_dir = posixpath.dirname(uploads[0].str("object"))
  if not cycles:
    return None
  stored, error = _stored_bytes(storage, last_dir)
  return {"cycles": cycles, "stored": stored, "stored_error": error}


def _mb(n) -> str:
  return "–" if n is None else f"{n / 1e6:,.0f} MB"


def _snapshot_line(sizes, color: bool = False) -> str:
  image = statistics.median(c["logical"] for c in sizes["cycles"])
  populated = statistics.median(c["populated"] for c in sizes["cycles"])
  mems = [c["memory_populated"] for c in sizes["cycles"] if c.get("memory_populated") is not None]
  uppers = [c["upper"] for c in sizes["cycles"] if c.get("upper") is not None]
  detail = f" ({_mb(statistics.median(mems))} memory, {_mb(statistics.median(uppers))} rootfs upper)" if mems and uppers else ""
  head = paint("Snapshot", BOLD, color)
  return f" {head}  {_mb(image)} image · {_mb(populated)} populated{detail} · {_mb(sizes['stored'])} in GCS"


# ── Formatting helpers ───────────────────────────────────────────────────────


def short_commit(commit: str) -> str:
  """'fa6d949685a6…-dirty' -> 'fa6d9496 (dirty)'."""
  return commit.removesuffix("-dirty")[:8] + (" (dirty)" if commit.endswith("-dirty") else "")


def headline(setup) -> list[str]:
  """Up to three header lines: the Substrate version and commit, the machine and k8s version, the disk."""

  def items(label):
    return next((i for row_label, i in setup if row_label == label), [])

  lines, parts = [], []
  version = next((i for i in items("version") if i.startswith("substrate ")), "")
  if version and "unknown" not in version:
    parts.append(version)
  commits = items("commit")
  if len(commits) == 1 and "unknown" not in commits[0]:
    parts.append(f"commit {short_commit(commits[0])}")
  elif len(commits) > 1:  # "ate-api <sha>", "atelet <sha>", "worker <sha>"
    named = (item.split(" ", 1) for item in commits)
    parts.append("commits differ: " + ", ".join(f"{name} {short_commit(c)}" for name, c in named))
  if parts:
    lines.append(" · ".join(parts))
  parts = []
  machine = items("machine")
  if machine and "unknown" not in machine[0]:
    virt = next((i for i in machine if i in ("bare metal", "nested virtualization", "VM")), "")
    parts.append(machine[0] + (f", {virt.replace('virtualization', 'virt')}" if virt else ""))
  m = re.match(r"k8s v?(\d+\.\d+)", next((i for i in items("cluster") if i.startswith("k8s ")), ""))
  if m:
    parts.append(f"k8s {m.group(1)}")
  if parts:
    lines.append(" · ".join(parts))
  disk = items("disk")
  if disk:  # even if unknown: the reader must not take a run on a slow disk for one on local SSDs
    lines.append("disk  " + " · ".join(disk))
  return lines


def node_lines(node, color: bool = False) -> list[str]:
  """The header's lines on the worker node's load: before the run (flagged if it wasn't idle), and during it."""
  lines = []
  if (node or {}).get("before"):
    busy = sampler.busy_reasons(node["before"])
    desc = sampler.describe(node["before"])
    if busy:
      lines.append(paint(f"⚠ node busy before the run: {desc}", BOLD + YELLOW, color))
    else:
      lines.append(f"{paint('node idle:', GREEN, color)} {paint(desc, DIM, color)}")
  if (node or {}).get("during"):
    lines.append(paint("node during the run: " + sampler.describe(node["during"]), DIM, color))
  return lines


def short_phase(label: str, park: str = "Suspend") -> str:
  """The breakdown name of a phase: 'atelet (outside ateom: snapshot upload, …)' -> 'snapshot upload'.

  A pause keeps its checkpoint on the node, so on the pause path atelet's time outside ateom is
  just atelet's own, and its "download" phase copies the checkpoint between two local directories
  (copyLocalCheckpoint) instead of fetching it from object storage.
  """
  if park == "Pause":
    if label.startswith("atelet (outside ateom"):
      return "atelet"
    if label == "download":
      return "local copy"
  m = re.match(r"\S+ \(outside [^:)]+: ([^,)…]+)", label)
  return m.group(1).strip() if m else label


def breakdown_lines(verb: str, rows, name: str, park: str, color: bool = False) -> list[str]:
  """The '<name>  N ms' block: the slowest phases by median, then 'other'; [] if there's none."""
  top = breakdown.top_phases([r[f"{verb}_tree"] for r in rows], n=TOP_PHASES)
  totals = [r[verb] for r in rows if r[verb] is not None and r[f"{verb}_tree"]]
  if not top or not totals:
    return []
  total = statistics.median(totals)
  items = [(short_phase(label, park), value) for label, value in top]
  rest = total - sum(value for _, value in items)
  if rest >= 0.5:
    items.append(("other", rest))
  width = max(15, *(len(label) for label, _ in items))
  lines = [f" {paint(name, BOLD, color)}  {paint(common.ms(total), BOLD, color)}"]
  for i, (label, value) in enumerate(items):
    pct = 100 * value / total if total else 0
    bar = "█" * round(pct * BAR_WIDTH / 100)
    if label == "other":
      row = paint(f"   {label:<{width}}{common.ms(value):>8}  {pct:>3.0f}%  {bar}".rstrip(), DIM, color)
    else:
      bar_color = YELLOW if i == 0 else CYAN
      colored_bar = paint(bar, bar_color, color)
      row = f"   {label:<{width}}{common.ms(value):>8}  {pct:>3.0f}%  {colored_bar}".rstrip()
    lines.append(row)
  return lines


def path_rows(path: dict[str, Any], trees: dict[str, Any]) -> list[dict[str, Any]]:
  """One row per cycle of a path: its numbers and its calls' trees ("suspend" is the park call)."""
  return [{"cycle": c["cycle"], "resume": c["resume"]["client_ms"], "exec_wall": c["exec_wall"],
           "suspend": c["suspend"]["client_ms"],
           "resume_tree": trees.get(c["resume"]["trace_id"]), "suspend_tree": trees.get(c["suspend"]["trace_id"])}
          for c in path.get("cycles", [])]


def _median_of(rows, key):
  values = [r[key] for r in rows if r[key] is not None]
  return statistics.median(values) if values else None


def _median_line(label: str, cells, color: bool) -> str:
  styled = [paint(f"{c:>9}", DIM if c == "–" else BOLD, color) for c in cells]
  return f" {label:<22}{styled[0]} {styled[1]} {styled[2]}"


# ── Sections ─────────────────────────────────────────────────────────────────


def _header(paths, rows, setup, elapsed_s, guest_note, node, arm_note="", color: bool = False):
  """The title, the headline, the node's load, and the medians table."""
  rule = paint("═" * SHORT_WIDTH, CYAN, color)
  common.log()
  common.log(rule)
  common.log(paint(f" baseline-perf · {common.TEMPLATE} · {elapsed_s:.0f} s", BOLD, color))
  for line in headline(setup):
    common.log(paint(f" {line}", DIM, color))
  if guest_note:
    common.log(paint(f" guest  {guest_note}", DIM, color))
  if arm_note:
    common.log(paint(f" arm  {arm_note}", DIM, color))
  for line in node_lines(node, color=color):
    common.log(f" {line}")
  common.log(rule)
  head = MEDIAN_ROW.format(f"Median of {max(len(r) for r in rows.values())} cycles", "Resume", "Exec", "Park")
  common.log(paint(head, BOLD, color))
  for p in paths:
    r = rows[p["park"]]
    medians = [common.ms(_median_of(r, k)) for k in ("resume", "exec_wall", "suspend")]
    common.log(_median_line(PATH_ROWS[p["park"]], medians, color))
  if paths[0].get("cold"):
    cold_cells = [common.ms(paths[0]["cold"]["resume"]["client_ms"]), "–", "–"]
    common.log(_median_line("Cold start (golden)", cold_cells, color))


def _breakdowns(paths, rows, sizes, color: bool = False):
  """Each path's breakdown blocks: where its Resume and park time went."""
  for p in paths:
    park = p["park"]
    blocks = [breakdown_lines(verb, rows[park], name, park, color=color)
              for verb, name in (("resume", "Resume"), ("suspend", park))]
    if park == "Suspend" and sizes:
      blocks.append([_snapshot_line(sizes, color=color)])
    blocks = [b for b in blocks if b]
    if not blocks:
      continue
    common.log()
    common.log(paint(f" ── {common.PATHS[park]} ──", BOLD + CYAN, color))
    for i, lines in enumerate(blocks):
      if i:
        common.log()
      for line in lines:
        common.log(line)


def _notes(paths, rows, log_errors, sizes, node, color: bool = False):
  """The notes under the breakdowns: what the numbers leave out."""
  resumes = []
  for p in paths:
    resumes += ([p["cold"]["resume"]] if p.get("cold") else []) + [c["resume"] for c in p.get("cycles", [])]
  busy = [r["busy_ms"] for r in resumes if r.get("busy_ms")]
  notes = []
  over = sampler.busy_reasons(node["before"]) if (node or {}).get("before") else []
  if over:
    notes.append((f"the node was busy before the run ({', '.join(over)}): these numbers may include contention", True))
  elif (node or {}).get("error"):
    notes.append((f"couldn't check that the node was idle: {node['error']}", True))
  if busy:
    many = len(busy) > 1
    notes.append((f"{len(busy)} resume{'s' if many else ''} waited {'up to ' if many else ''}{max(busy):.0f} ms "
                  "for a free worker (not counted)", False))
  all_rows = sum(rows.values(), [])
  if all_rows and not any(r["resume_tree"] or r["suspend_tree"] for r in all_rows):
    notes.append((f"no phase breakdown: couldn't read the server logs ({log_errors[0]})" if log_errors
                  else "no phase breakdown: the server logs had none", True))
  if sizes and sizes["stored"] is None:
    notes.append((f"snapshot size in GCS unknown: {sizes['stored_error']}", False))
  if notes:
    common.log()
  for note, warn in notes:
    common.log(paint(f" Note: {note}", YELLOW if warn else DIM, color))


def print_report(result: dict[str, Any], trees, setup, elapsed_s: float, log_errors=(), sizes=None,
                 guest_note: str = "", node=None, arm_note: str = "") -> None:
  """Prints the report. trees and sizes are None when there are none; node is the node's load (cli._Run.node).

  arm_note: with --ab, the run's arm and its setup.
  """
  paths = [p for p in result.get("paths", []) if p.get("cold") or p.get("cycles")]
  if not paths:
    return
  color = use_color()
  rows = {p["park"]: path_rows(p, trees or {}) for p in paths}
  _header(paths, rows, setup, elapsed_s, guest_note, node, arm_note, color=color)
  _breakdowns(paths, rows, sizes, color=color)
  _notes(paths, rows, log_errors, sizes, node, color=color)
