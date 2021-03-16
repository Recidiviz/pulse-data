# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Util functions common to all tests."""
import os


def print_visible_header_label(label_text: str) -> None:
    """Prints three long lines of '*' characters, followed by a text label on
    its own line. Used to print a very visible header between other debug
    printouts.
    """
    for _ in range(0, 3):
        print("*" * 200)
    print(f"\n{label_text}")


def in_docker():
    """ Returns: True if running in a Docker container, else False """
    if not os.path.exists("/proc/1/cgroup"):
        return False
    with open("/proc/1/cgroup", "rt") as ifh:
        return "docker" in ifh.read()


def is_running_in_ci() -> bool:
    """Returns True if this code is running in the CI environment, False
    otherwise.
    """
    print(os.environ)  # For debugging purposes
    return os.environ.get("CI") == "true"
