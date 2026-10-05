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

"""Tests for common.py's progress output."""

import io
import os
import unittest
from unittest import mock

# Plain imports, not google3 ones: these run with plain python3.
from bperf import common


class _Terminal(io.StringIO):
  """Output that says it's a terminal."""

  def isatty(self):
    return True


class LogTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    for patcher in (
        mock.patch.object(common, "_status_shown", False),
        mock.patch.dict(os.environ, {"TERM": "xterm"}),
        mock.patch(
            "shutil.get_terminal_size", return_value=os.terminal_size((80, 24))
        ),
    ):
      patcher.start()
      self.addCleanup(patcher.stop)

  def _log(self, out, *msgs):
    """What common.log prints to out for msgs."""
    with mock.patch("sys.stdout", out):
      for msg in msgs:
        common.log(msg)
    return out.getvalue()

  def test_status_lines_take_each_others_place_on_a_terminal(self):
    got = self._log(
        _Terminal(),
        "[4/6] Pause path...",
        common.STATUS_PREFIX + "  Cycle 1: resume...",
        common.STATUS_PREFIX + "  Cycle 1: resume 315 ms · exec...",
        "  ✔ done",
    )
    self.assertEqual(
        got,
        "[4/6] Pause path...\n"
        "  Cycle 1: resume..."
        "\r\033[K  Cycle 1: resume 315 ms · exec..."
        "\r\033[K  ✔ done\n",
    )

  def test_other_output_leaves_status_lines_out(self):
    got = self._log(io.StringIO(), "a", common.STATUS_PREFIX + "b", "c")
    self.assertEqual(got, "a\nc\n")

  def test_a_dumb_terminal_leaves_status_lines_out(self):
    with mock.patch.dict(os.environ, {"TERM": "dumb"}):
      got = self._log(_Terminal(), "a", common.STATUS_PREFIX + "b", "c")
    self.assertEqual(got, "a\nc\n")

  def test_a_status_line_fits_in_one_row(self):
    with mock.patch(
        "shutil.get_terminal_size", return_value=os.terminal_size((10, 24))
    ):
      got = self._log(_Terminal(), common.STATUS_PREFIX + "x" * 50)
    self.assertEqual(got, "x" * 9)


if __name__ == "__main__":
  unittest.main()
