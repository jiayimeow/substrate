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

"""Tests for the report's formatting and the header's facts."""

import unittest

# Plain imports, not google3 ones: these run with plain python3.
from bperf import cluster_info
from bperf import report


class ReportTest(unittest.TestCase):

  def test_headline(self):
    setup = [
        ("cluster", ["k8s v1.35.8-gke.1036000"]),
        ("machine", ["c3-highmem-192-metal", "bare metal"]),
        (
            "disk",
            [
                "hyperdisk-balanced 100 GB",
                "3,600 IOPS",
                "290 MiB/s",
                "no local SSD",
            ],
        ),
        ("version", ["substrate v0.1.0"]),
        ("commit", ["fa6d949685a6318940a9a0195c867c864009b820-dirty"]),
    ]
    self.assertEqual(
        report.headline(setup),
        [
            "substrate v0.1.0 · commit fa6d9496 (dirty)",
            "c3-highmem-192-metal, bare metal · k8s 1.35",
            (
                "disk  hyperdisk-balanced 100 GB · 3,600 IOPS · 290 MiB/s · no"
                " local SSD"
            ),
        ],
    )
    self.assertEqual(report.headline([]), [])
    unknown = [("disk", ["unknown (gcloud is not on PATH)"])]
    self.assertEqual(
        report.headline(unknown), ["disk  unknown (gcloud is not on PATH)"]
    )

  def test_headline_names_each_commit_when_they_differ(self):
    setup = [("commit", ["ate-api fa6d94968", "worker 0123abcd5-dirty"])]
    self.assertEqual(
        report.headline(setup),
        ["commits differ: ate-api fa6d9496, worker 0123abcd (dirty)"],
    )

  def test_node_lines(self):
    idle = {
        "busy_cores": 0.1,
        "disk": "nvme0n1",
        "disk_pct": 2.0,
        "psi": {"io": 0.3},
    }
    during = dict(idle, busy_cores=1.1, disk_pct=5.0)
    self.assertEqual(
        report.node_lines({"before": idle, "during": during}),
        [
            "node idle: 0.1 busy cores · disk 2% · PSI io 0%",
            "node during the run: 1.1 busy cores · disk 5% · PSI io 0%",
        ],
    )
    self.assertEqual(
        report.node_lines({"before": dict(idle, busy_cores=6.3)}),
        ["⚠ node busy before the run: 6.3 busy cores · disk 2% · PSI io 0%"],
    )
    self.assertEqual(report.node_lines({"error": "no node"}), [])
    self.assertEqual(report.node_lines(None), [])

  def test_short_phase(self):
    upload = "atelet (outside ateom: snapshot upload, …)"
    self.assertEqual(report.short_phase(upload), "snapshot upload")
    self.assertEqual(report.short_phase(upload, "Pause"), "atelet")
    self.assertEqual(report.short_phase("download", "Pause"), "local copy")
    self.assertEqual(report.short_phase("download"), "download")

  def test_breakdown_lines(self):
    tree = {
        "name": "ResumeActor",
        "ms": 100.0,
        "children": [
            {"name": "download", "ms": 60.0},
            {"name": "vm_restore", "ms": 30.0},
            {"name": "teardown", "ms": 6.0},
        ],
    }
    rows = [{"resume": 100.0, "resume_tree": tree}]
    self.assertEqual(
        report.breakdown_lines("resume", rows, "Resume", "Suspend"),
        [
            " Resume  100 ms",
            "   download          60 ms   60%  ████████████",
            "   vm_restore        30 ms   30%  ██████",
            "   other             10 ms   10%  ██",
        ],
    )
    self.assertEqual(
        report.breakdown_lines(
            "resume", [{"resume": 1.0, "resume_tree": None}], "R", "Suspend"
        ),
        [],
    )
    colored = report.breakdown_lines(
        "resume", rows, "Resume", "Suspend", color=True
    )
    self.assertIn(report.YELLOW, colored[1])
    self.assertIn(report.CYAN, colored[2])
    self.assertTrue(colored[3].startswith(report.DIM))

  def test_snapshot_line(self):
    sizes = {
        "cycles": [{
            "logical": 8_500_000_000,
            "populated": 272_000_000,
            "memory_populated": 242_000_000,
            "upper": 30_000_000,
        }],
        "stored": 75_000_000,
    }
    self.assertEqual(
        report._snapshot_line(sizes),
        " Snapshot  8,500 MB image · 272 MB populated (242 MB memory, 30 MB"
        " rootfs upper) · 75 MB in GCS",
    )


class ClusterInfoTest(unittest.TestCase):

  def test_image_name(self):
    ref = (
        "us-docker.pkg.dev/p/r/atelet:v0.1.0-gke.1@sha256:" + "22a827f5a332" * 5
    )
    self.assertEqual(cluster_info.image_name(ref), "atelet")
    ko = "gcr.io/p/ateom-microvm-" + "0123456789abcdef" * 2 + ":latest"
    self.assertEqual(cluster_info.image_name(ko), "ateom-microvm")

  def test_disk_items(self):
    vm = {
        "disks": [
            {"boot": True, "type": "PERSISTENT", "source": "https://x/disks/n"}
        ]
    }
    hyperdisk = {
        "type": "https://x/zones/z/diskTypes/hyperdisk-balanced",
        "sizeGb": "100",
        "provisionedIops": "3600",
        "provisionedThroughput": "290",
    }
    self.assertEqual(
        cluster_info.disk_items(vm, hyperdisk),
        [
            "hyperdisk-balanced 100 GB",
            "3,600 IOPS",
            "290 MiB/s",
            "no local SSD",
        ],
    )
    ssd = {"type": "SCRATCH", "interface": "NVME", "diskSizeGb": "375"}
    with_ssds = {"disks": vm["disks"] + [ssd, ssd]}
    pd = {"type": "https://x/zones/z/diskTypes/pd-balanced", "sizeGb": "200"}
    self.assertEqual(
        cluster_info.disk_items(with_ssds, pd),
        ["pd-balanced 200 GB", "2 local SSDs (750 GB)"],
    )


if __name__ == "__main__":
  unittest.main()
