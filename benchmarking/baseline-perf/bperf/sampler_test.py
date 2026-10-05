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

"""Tests for the sampler: the node's counters and what they say about its load."""

import contextlib
import io
import unittest

# Plain imports, not google3 ones: these run with plain python3.
from bperf import sampler


def _sample(t, cpu, psi_io=0, disks=None):
  return {
      "t": t,
      "m": t,
      "cpu": cpu,
      "ncpu": 4,
      "psi": {"io": [psi_io, psi_io], "cpu": [0, 0], "memory": [0, 0]},
      "disks": disks if disks is not None else {"nvme0n1": [0, 0, 0, 0, 0]},
  }


class LoadTest(unittest.TestCase):

  def test_busy_cores_count_nice_but_not_iowait(self):
    # 10 s of 4 CPUs at 100 Hz: user 300, nice 500, system 200, idle 2800, iowait 200 jiffies.
    a = _sample(100, [0] * 8)
    b = _sample(
        110,
        [300, 500, 200, 2800, 200, 0, 0, 0],
        psi_io=500_000,
        disks={"nvme0n1": [0, 0, 9, 72, 1500]},
    )
    load = sampler.load(a, b)
    self.assertAlmostEqual(load["seconds"], 10)
    self.assertAlmostEqual(load["busy_cores"], 1.0)
    self.assertEqual(load["disk"], "nvme0n1")
    self.assertAlmostEqual(
        load["disk_pct"], 15.0
    )  # 1.5 s of the 10 with I/O in flight
    self.assertAlmostEqual(load["psi"]["io"], 5.0)  # 0.5 s of the 10 stalled
    self.assertEqual(load["psi"]["cpu"], 0)

  def test_the_busiest_disk_counts(self):
    a = _sample(
        0,
        [0] * 8,
        disks={"nvme0n1": [0, 0, 0, 0, 100], "nvme1n1": [0, 0, 0, 0, 0]},
    )
    b = _sample(
        1,
        [0] * 8,
        disks={"nvme0n1": [0, 0, 0, 0, 200], "nvme1n1": [0, 0, 0, 0, 900]},
    )
    load = sampler.load(a, b)
    self.assertEqual(load["disk"], "nvme1n1")
    self.assertAlmostEqual(load["disk_pct"], 90.0)
    self.assertEqual(load["busy_cores"], 0.0)  # no CPU time passed at all

  def test_disk_throughput_and_iops(self):
    # 10 s: 100 reads and 50 writes, 100 MiB (204,800 sectors) each way.
    a = _sample(0, [0] * 8)
    b = _sample(10, [0] * 8, disks={"nvme0n1": [100, 204800, 50, 204800, 2000]})
    load = sampler.load(a, b)
    self.assertAlmostEqual(load["disk_mibps"], 20.0)
    self.assertAlmostEqual(load["disk_iops"], 15.0)
    self.assertEqual(load["ncpu"], 4)
    no_disk = sampler.load(_sample(0, [0] * 8, disks={}), _sample(1, [0] * 8))
    self.assertEqual(
        (no_disk["disk"], no_disk["disk_pct"], no_disk["disk_mibps"]),
        ("", 0, 0),
    )

  def test_window_uses_the_samples_inside(self):
    samples = [_sample(t, [0, 0, 0, 400 * t, 0, 0, 0, 0]) for t in range(10)]
    self.assertEqual(
        sampler.window(samples, 2.5, 7.5)["seconds"], 4
    )  # samples 3 to 7
    self.assertIsNone(sampler.window(samples, 2.5, 3.5))  # one sample only

  def test_busy_reasons_and_describe(self):
    idle = {
        "seconds": 10,
        "busy_cores": 0.09,
        "disk": "nvme0n1",
        "disk_pct": 2.0,
        "psi": {"io": 0.35, "cpu": 0.02, "memory": 0.0},
    }
    self.assertEqual(sampler.busy_reasons(idle), [])
    self.assertEqual(
        sampler.describe(idle), "0.1 busy cores · disk 2% · PSI io 0%"
    )
    busy = dict(
        idle,
        busy_cores=6.3,
        disk_pct=41.0,
        psi={"io": 12.0, "cpu": 3.0, "memory": 0.0},
    )
    self.assertEqual(
        sampler.busy_reasons(busy),
        ["6.3 busy cores ≥ 2", "disk 41% ≥ 10%", "PSI io 12% ≥ 5%"],
    )
    self.assertEqual(
        sampler.describe(busy), "6.3 busy cores · disk 41% · PSI io 12%, cpu 3%"
    )
    self.assertEqual(
        sampler.describe(dict(busy, disk_iops=300.0, disk_mibps=40.0)),
        "6.3 busy cores · disk 41% (300 IOPS, 40 MiB/s) · PSI io 12%, cpu 3%",
    )
    self.assertEqual(
        sampler.describe(dict(idle, disk="", psi={})), "0.1 busy cores · no PSI"
    )


class SamplingTest(unittest.TestCase):

  def test_main_prints_samples_that_parse(self):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
      self.assertEqual(sampler.main(["--samples=2", "--interval=0"]), 0)
    samples = sampler.parse(out.getvalue().splitlines())
    self.assertEqual(len(samples), 2)
    self.assertEqual(len(samples[0]["cpu"]), 8)
    self.assertGreater(samples[0]["ncpu"], 0)
    self.assertGreaterEqual(sampler.load(*samples)["busy_cores"], 0)

  def test_parse_skips_other_and_cut_off_lines(self):
    lines = [
        sampler.SAMPLE_PREFIX + '{"t": 1}',
        "Traceback (most recent call last):",
        sampler.SAMPLE_PREFIX + '{"t": 2',
    ]
    self.assertEqual(sampler.parse(lines), [{"t": 1}])


if __name__ == "__main__":
  unittest.main()
