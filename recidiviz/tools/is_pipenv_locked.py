# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Checks whether the pipenv is locked"""
import logging
import sys

from pipenv.core import project


def is_locked() -> bool:
    lock_hash = project.get_lockfile_hash()
    pipfile_hash = project.calculate_pipfile_hash()
    if lock_hash == pipfile_hash:
        return True
    logging.error(
        "Pipfile.lock (%s) is out of date (expected %s).",
        lock_hash[-6:],
        pipfile_hash[-6:],
    )
    return False


def main() -> None:
    sys.exit(not is_locked())


if __name__ == "__main__":
    main()
