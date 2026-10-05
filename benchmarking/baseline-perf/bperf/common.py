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
"""What every part of baseline-perf shares: names, the shape of a run, errors and output.

The runner pod gets this file along with runner.py (see pod.py), so it imports
only the standard library.
"""

import os
import shutil
import sys

# The atespace of the template and the actor, and the runner pod's namespace.
NAMESPACE = "benchmark-workloads"
SYSTEM_NAMESPACE = os.environ.get("ATE_NAMESPACE") or "ate-system"  # where Substrate runs
TEMPLATE = "swebench-astropy-7336"
ACTOR_NAME = "perf-eval-actor"
RUNNER_POD = "baseline-perf-runner"
SAMPLER_POD = "baseline-perf-sampler"  # on the worker's node, to check that it's idle

# The runner's last line of output: this prefix, then its results as JSON.
RESULT_PREFIX = "@@baseline-perf-result "
# A runner line that says what it's doing right now, which a terminal shows in place (see log).
STATUS_PREFIX = "@@baseline-perf-status "

# How a cycle parks the actor -> what the report calls that path, in run order.
PATHS = {
    "Pause": "Pause path (checkpoint kept on the node)",
    "Suspend": "Suspend path (snapshot to GCS)",
}

# The 21-step trace as 4 cycles, split like the boomer client's generateDynamicChunks(21, 4):
# (name, first step, last step).
CHUNKS = (
    ("Cycle 1 (Steps 1-6)", 1, 6),
    ("Cycle 2 (Steps 7-11)", 7, 11),
    ("Cycle 3 (Steps 12-16) [CPU Control]", 12, 16),
    ("Cycle 4 (Steps 17-21)", 17, 21),
)

PARKED_CALLS = ("/ResumeActor", "/SuspendActor", "/PauseActor")  # the calls the report breaks down
ATE_TIMEOUT_S = 180  # one kubectl-ate call, or one ate-api call from the runner
WIDTH = 88  # the progress output's width
FIRST_PATH_STEP = 4  # the progress step of the first path, after preflight, the idle check and the pod


class Error(Exception):
  """A step failed; the message says what went wrong and what to do."""


_status_shown = False  # the terminal's last line is a status line, still without its newline


def log(msg: str = "") -> None:
  """Prints a line of progress.

  A status line (STATUS_PREFIX, then the text), which the runner prints and the
  launcher relays, says what's happening right now. A terminal shows it in place
  of the previous one, and the next line of either kind takes its place. Output
  that isn't a terminal leaves status lines out.
  """
  global _status_shown
  status = msg.startswith(STATUS_PREFIX)
  if status and not (sys.stdout.isatty() and os.environ.get("TERM") != "dumb"):
    return
  if _status_shown:
    sys.stdout.write("\r\033[K")  # back to the start of the line, and erase it
  if status:
    columns = shutil.get_terminal_size((WIDTH, 24)).columns
    sys.stdout.write(msg[len(STATUS_PREFIX):][:columns - 1])  # one row, or \r can't get back to its start
    sys.stdout.flush()
  else:
    print(msg, flush=True)
  _status_shown = status


def ms(value: float | None) -> str:
  """'123 ms', or '–' for None."""
  return "–" if value is None else f"{value:.0f} ms"


def steps_total(parks) -> int:
  """The progress output's step count: preflight, the idle check, the pod, a step per path, cleanup."""
  return FIRST_PATH_STEP + len(parks)
