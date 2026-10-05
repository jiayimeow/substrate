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
"""The sampler: how busy the worker node is, read by a small pod on it.

The sampler pod (see pod.py) runs this file with plain python3, so it imports only the standard
library. Every INTERVAL_S it prints one SAMPLE_PREFIX line with the node's cumulative counters as
JSON: /proc/stat, /proc/pressure and /proc/diskstats describe the whole host, even from an
unprivileged container. The launcher reads the pod's log and turns any two samples into the load
between them:

  busy cores  CPU time that was neither idle nor iowait, nice included (sandboxes may run niced)
  disk        the busiest whole disk: the share of time it had I/O in flight, its MiB/s and its IOPS
  PSI         the share of time some task was stalled waiting for CPU, IO or memory
"""

import argparse
import json
import re
import sys
import time

SAMPLE_PREFIX = "@@baseline-perf-sample "
INTERVAL_S = 1.0  # between two samples
IDLE_WINDOW_S = 10  # how long the node is watched before the run
# An idle c3-highmem-192-metal node shows about 0.1 busy cores, disk 2% and PSI under 1%. Past any
# of these, the node is busy: a run on it may measure contention rather than one actor's cost.
IDLE_MAX_CORES = 2.0
IDLE_MAX_DISK_PCT = 10.0
IDLE_MAX_PSI_PCT = 5.0
PSI_RESOURCES = ("io", "cpu", "memory")
_WHOLE_DISK = re.compile(r"nvme\d+n\d+|sd[a-z]+|vd[a-z]+|xvd[a-z]+")  # not partitions, dm, loop or ram
_DISK_FIELDS = (3, 5, 7, 9, 12)  # of /proc/diskstats: reads, sectors read, writes, sectors written, ms with I/O


# ── In the sampler pod ───────────────────────────────────────────────────────


def read_sample() -> dict:
  """The node's counters now: CPU jiffies, PSI stall totals (µs), and each whole disk's I/O."""
  sample = {"t": time.time(), "m": time.monotonic()}
  with open("/proc/stat", encoding="ascii") as f:
    stat = f.read().splitlines()
  sample["cpu"] = [int(x) for x in stat[0].split()[1:9]]  # user nice system idle iowait irq softirq steal
  sample["ncpu"] = sum(1 for line in stat if re.match(r"cpu\d", line))
  sample["psi"] = {}
  for resource in PSI_RESOURCES:
    try:
      with open(f"/proc/pressure/{resource}", encoding="ascii") as f:
        totals = dict(re.findall(r"^(some|full) .*total=(\d+)", f.read(), re.M))
    except OSError:  # a kernel without PSI
      continue
    sample["psi"][resource] = [int(totals.get("some", 0)), int(totals.get("full", 0))]
  sample["disks"] = {}
  with open("/proc/diskstats", encoding="ascii") as f:
    for line in f:
      p = line.split()
      if len(p) >= 14 and _WHOLE_DISK.fullmatch(p[2]):
        sample["disks"][p[2]] = [int(p[i]) for i in _DISK_FIELDS]
  return sample


def main(argv=None) -> int:
  """The sampler pod's loop: a sample every --interval seconds until the pod is deleted."""
  parser = argparse.ArgumentParser(prog="sampler")
  parser.add_argument("--interval", type=float, default=INTERVAL_S)
  parser.add_argument("--samples", type=int, default=0, help="stop after this many (default: never)")
  args = parser.parse_args(argv)
  count, due = 0, time.monotonic()
  while True:
    print(SAMPLE_PREFIX + json.dumps(read_sample(), separators=(",", ":")), flush=True)
    count += 1
    if count == args.samples:
      return 0
    due += args.interval
    time.sleep(max(0.0, due - time.monotonic()))


# ── In the launcher ──────────────────────────────────────────────────────────


def parse(lines) -> list[dict]:
  """The samples in the sampler pod's log lines, in order."""
  samples = []
  for line in lines:
    if line.startswith(SAMPLE_PREFIX):
      try:
        samples.append(json.loads(line[len(SAMPLE_PREFIX):]))
      except ValueError:  # cut off when the pod was deleted
        pass
  return samples


def load(a: dict, b: dict) -> dict:
  """The node's load from sample a to sample b.

  {"seconds", "busy_cores", "ncpu", "disk", "disk_pct", "disk_mibps", "disk_iops", "psi": {resource: %}}: the disk
  numbers are the busiest whole disk's; /proc/diskstats counts sectors of 512 bytes on every device.
  """
  seconds = max(b["m"] - a["m"], 1e-3)
  cpu = [y - x for x, y in zip(a["cpu"], b["cpu"])]
  total = sum(cpu)
  io = {name: [y - x for x, y in zip(a["disks"][name], v)] for name, v in b["disks"].items() if name in a["disks"]}
  disk = max(io, key=lambda name: io[name][4], default="")
  reads, read_sectors, writes, written_sectors, busy_ms = io[disk] if disk else (0, 0, 0, 0, 0)
  return {
      "seconds": seconds,
      "busy_cores": (total - cpu[3] - cpu[4]) / total * b["ncpu"] if total else 0.0,
      "ncpu": b["ncpu"],
      "disk": disk,
      "disk_pct": 100 * busy_ms / (1000 * seconds),
      "disk_mibps": (read_sectors + written_sectors) * 512 / 2**20 / seconds,
      "disk_iops": (reads + writes) / seconds,
      "psi": {r: 100 * (b["psi"][r][0] - a["psi"][r][0]) / (1e6 * seconds) for r in b["psi"] if r in a["psi"]},
  }


def window(samples, start: float, end: float) -> dict | None:
  """The load between the first and last samples taken from start to end (Unix seconds); None if under two."""
  inside = [s for s in samples if start <= s["t"] <= end]
  return load(inside[0], inside[-1]) if len(inside) >= 2 else None


def busy_reasons(node_load: dict) -> list[str]:
  """What makes the load too much for an idle node, e.g. ['6.3 busy cores ≥ 2']; [] if it's idle."""
  reasons = []
  if node_load["busy_cores"] >= IDLE_MAX_CORES:
    reasons.append(f"{node_load['busy_cores']:.1f} busy cores ≥ {IDLE_MAX_CORES:g}")
  if node_load["disk_pct"] >= IDLE_MAX_DISK_PCT:
    reasons.append(f"disk {node_load['disk_pct']:.0f}% ≥ {IDLE_MAX_DISK_PCT:g}%")
  for resource, pct in node_load["psi"].items():
    if pct >= IDLE_MAX_PSI_PCT:
      reasons.append(f"PSI {resource} {pct:.0f}% ≥ {IDLE_MAX_PSI_PCT:g}%")
  return reasons


def describe(node_load: dict) -> str:
  """'0.8 busy cores · disk 4% · PSI io 0%', adding disk IOPS/MiB/s and PSI cpu/memory when active."""
  parts = [f"{node_load['busy_cores']:.1f} busy cores"]
  if node_load["disk"]:
    disk = f"disk {node_load['disk_pct']:.0f}%"
    iops, mib = node_load.get("disk_iops", 0.0), node_load.get("disk_mibps", 0.0)
    if node_load["disk_pct"] >= 5 and (iops >= 10 or mib >= 1):
      disk += f" ({iops:,.0f} IOPS, {mib:,.0f} MiB/s)"
    parts.append(disk)
  psi = node_load["psi"]
  shown = [f"{r} {psi[r]:.0f}%" for r in PSI_RESOURCES if r in psi and (r == "io" or psi[r] >= 1)]
  parts.append("PSI " + ", ".join(shown) if shown else "no PSI")
  return " · ".join(parts)


if __name__ == "__main__":  # the sampler pod runs this file's source as __main__
  sys.exit(main(sys.argv[1:]))
