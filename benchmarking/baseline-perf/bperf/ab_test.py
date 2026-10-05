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

"""Tests for ab.py: the run order, a run's numbers, and the comparison."""

import unittest

# Plain imports, not google3 ones: these run with plain python3.
from bperf import ab
from bperf import logs


def _call(ms, trace=""):
  return {"client_ms": ms, "trace_id": trace}


def _path(park, cold, cycles):
  """A runner path; cycles are (resume, exec, park) ms."""
  return {
      "park": park,
      "cold": {"resume": _call(cold), "suspend": _call(1, f"{park}-cold")},
      "cycles": [
          {
              "cycle": f"c{i}",
              "resume": _call(r),
              "exec_wall": e,
              "suspend": _call(p, f"{park}-{i}"),
          }
          for i, (r, e, p) in enumerate(cycles)
      ],
  }


def _upload(trace, name, logical, populated):
  fields = [
      ("msg", "Compressed zstd upload"),
      ("trace_id", trace),
      ("object", f"b/x/snapshots/7/{name}.zstd"),
      ("logical_bytes", logical),
      ("populated_bytes", populated),
  ]
  return logs.Record(fields, component="atelet")


class AbTest(unittest.TestCase):

  def test_order(self):
    self.assertEqual(ab.order(2), ["A", "B", "A", "B"])

  def test_parse_spec(self):
    self.assertEqual(
        ab.parse_spec("atelet=gcr.io/p/atelet:v1@sha256:66"),
        {"atelet": "gcr.io/p/atelet:v1@sha256:66"},
    )
    self.assertEqual(ab.parse_spec(" atelet = img "), {"atelet": "img"})
    self.assertEqual(ab.parse_spec("stock"), {})
    self.assertEqual(ab.parse_spec("atelet=stock"), {"atelet": "stock"})
    self.assertEqual(
        ab.parse_spec("kernel=./vmlinux,rootfs=./rootfs.img"),
        {"guest": "kernel=./vmlinux,rootfs=./rootfs.img"},
    )
    self.assertEqual(
        ab.parse_spec("guest=./my-guest/"), {"guest": "./my-guest/"}
    )

  def test_parse_spec_says_what_is_wrong(self):
    for spec, words in (
        ("gcr.io/p/atelet:v1", "KEY=VALUE"),
        ("atelet=", "KEY=VALUE"),
        ("=img", "KEY=VALUE"),
        ("", "KEY=VALUE"),
        ("bogus=x", "unknown key 'bogus'"),
        ("atelet=a,atelet=b", "'atelet' is set twice"),
        ("guest=dir,kernel=./vmlinux", "either guest="),
    ):
      with self.assertRaisesRegex(ValueError, words, msg=spec):
        ab.parse_spec(spec)

  def test_arms_a_key_one_arm_sets_is_stock_in_the_other(self):
    self.assertEqual(
        ab.arms(["atelet=img"]),
        {"A": {"atelet": "stock"}, "B": {"atelet": "img"}},
    )
    self.assertEqual(
        ab.arms(["kernel=./vmlinux"]),
        {"A": {"guest": "stock"}, "B": {"guest": "kernel=./vmlinux"}},
    )
    self.assertEqual(
        ab.arms(["stock", "atelet=img"]),
        {"A": {"atelet": "stock"}, "B": {"atelet": "img"}},
    )
    self.assertEqual(
        ab.arms(["atelet=a", "atelet=b"]),
        {"A": {"atelet": "a"}, "B": {"atelet": "b"}},
    )

  def test_arms_must_differ(self):
    for specs in (
        ["stock"],
        ["atelet=stock"],
        ["stock", "stock"],
        ["atelet=stock", "stock"],
        ["atelet=img", "atelet=img"],
    ):
      with self.assertRaisesRegex(ValueError, "same setup", msg=specs):
        ab.arms(specs)

  def test_resolve_and_describe(self):
    stock = {
        "atelet": "gcr.io/p/atelet:v1@sha256:f79efcec06d2",
        "guest": "kata-clh",
    }
    self.assertEqual(
        ab.resolve({"atelet": "stock"}, stock), {"atelet": stock["atelet"]}
    )
    self.assertEqual(
        ab.resolve({"atelet": "gcr.io/p/atelet:v2"}, stock),
        {"atelet": "gcr.io/p/atelet:v2"},
    )
    self.assertEqual(
        ab.describe({"atelet": "stock"}, stock),
        "atelet=atelet:v1@f79efcec (stock)",
    )
    self.assertEqual(
        ab.describe({"atelet": "gcr.io/p/atelet:v2"}, stock), "atelet=atelet:v2"
    )
    self.assertEqual(
        ab.describe(
            {"atelet": "~/substrate"},
            stock,
            {"atelet": "gcr.io/p/atelet:bperf-b@sha256:662323fd2209"},
        ),
        "atelet=atelet:bperf-b@662323fd (~/substrate)",
    )
    self.assertEqual(
        ab.describe({"guest": "stock"}, stock), "guest=kata-clh (stock)"
    )

  def test_template_name(self):
    self.assertEqual(ab.template_name("B"), "swebench-astropy-7336-ab-b")

  def test_short_image(self):
    ref = "gcr.io/p/ate-images/atelet:v0.1.0-bperf-mtime@sha256:662323fd2209da767d"
    self.assertEqual(ab.short_image(ref), "atelet:v0.1.0-bperf-mtime@662323fd")
    self.assertEqual(ab.short_image("gcr.io/p/atelet:dev"), "atelet:dev")

  def test_atelet_commit(self):
    differ = [(
        "commit",
        [
            "ate-api fa6d9496aaaa-dirty",
            "atelet fa6d9496aaaa",
            "worker fa6d9496aaaa-dirty",
        ],
    )]
    self.assertEqual(ab.atelet_commit(differ), "fa6d9496")
    self.assertEqual(
        ab.atelet_commit([("commit", ["fa6d9496aaaa-dirty"])]),
        "fa6d9496 (dirty)",
    )
    self.assertEqual(
        ab.atelet_commit(
            [("commit", ["unknown (couldn't run --version in the pods)"])]
        ),
        "?",
    )

  def test_metrics(self):
    cycles = [
        (400, 2000, 300),
        (500, 2500, 350),
        (450, 2200, 320),
        (480, 2600, 330),
    ]
    result = {
        "paths": [_path("Pause", 1300, cycles), _path("Suspend", 1250, cycles)]
    }
    traces = ["Suspend-cold"] + [f"Suspend-{i}" for i in range(4)]
    records = [
        _upload(t, "rootfs-upper.tar", (i + 1) * 1_000_000, 10)
        for i, t in enumerate(traces)
    ]
    records += [
        _upload(t, "memory-ranges", 8_000_000_000, 30_000_000) for t in traces
    ]
    sizes = {
        "cycles": [
            {"logical": 1, "populated": 5},
            {"logical": 1, "populated": 30_000_010},
        ],
        "stored": 111,
    }
    m = ab.metrics(result, records, sizes)
    self.assertEqual(m["Pause cold"], 1300)
    self.assertEqual(m["Pause resume"], 465)
    self.assertEqual(m["Suspend exec"], 2350)
    self.assertEqual(m["Suspend park"], 325)
    self.assertEqual(m["Pause exec_total"], 9300)
    self.assertEqual(m["Pause exec_1"], 2000)
    self.assertEqual(
        m["uppers"], [1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000]
    )
    self.assertEqual(m["upper_total"], 15_000_000)
    self.assertEqual(m["upper_last"], 5_000_000)
    self.assertEqual(m["memory_populated"], 30_000_000)
    self.assertEqual((m["populated"], m["stored"]), (30_000_010, 111))

  def test_metrics_skip_a_path_missing_a_cycle(self):
    m = ab.metrics(
        {"paths": [_path("Pause", 1300, [(400, 2000, 300)])]}, [], None
    )
    self.assertEqual(m, {"Pause cold": 1300})

  def test_row(self):
    self.assertEqual(
        ab.row("x", [100, 110], [80, 90], "ms").split(),
        [
            "x",
            "105",
            "ms",
            "100-110",
            "85",
            "ms",
            "80-90",
            "-20",
            "ms",
            "-19%",
            "*",
        ],
    )
    overlap = ab.row("x", [100, 110], [105, 120], "ms")
    self.assertFalse(overlap.endswith("*"))
    self.assertIn("+7.5 MB", ab.row("x", [10e6], [17.5e6], "MB"))
    self.assertEqual(ab.row("x", [], [1], "ms"), "")

  def test_table_lines(self):
    by_arm = {
        "A": [{"Pause cold": 1300, "upper_total": 70e6, "uppers": [2e6, 68e6]}],
        "B": [{"Pause cold": 1200, "upper_total": 30e6, "uppers": [0, 30e6]}],
    }
    lines = ab.table_lines(by_arm, ("Pause",))
    self.assertIn("Mean (range) of 1 run ", lines[0])
    self.assertIn(" Pause path (checkpoint kept on the node)", lines)
    self.assertTrue(
        any(
            line.split()[:2] == ["Cold", "start"] and "-100 ms" in line
            for line in lines
        )
    )
    self.assertIn("   rootfs upper per suspend, B: 0.0 · 30.0 MB", lines)

  def test_labels_fit_their_column(self):
    for label, _ in ab.PATH_ROWS + ab.SIZE_ROWS:
      self.assertLessEqual(len(f"  {label}"), 26, label)  # ROW's first field

  def test_row_color_keeps_alignment_and_highlights_delta(self):
    plain_win = ab.row("  Exec, median", [100, 110], [80, 90], "ms")
    color_win = ab.row("  Exec, median", [100, 110], [80, 90], "ms", color=True)
    self.assertIn("\033[1m\033[32m", color_win)
    color_reg = ab.row("  Exec, median", [80, 90], [100, 110], "ms", color=True)
    self.assertIn("\033[1m\033[31m", color_reg)
    # Stripping ANSI codes leaves the exact plain row.
    stripped = (
        color_win.replace("\033[1m", "")
        .replace("\033[2m", "")
        .replace("\033[32m", "")
        .replace("\033[0m", "")
    )
    self.assertEqual(stripped, plain_win)


if __name__ == "__main__":
  unittest.main()
