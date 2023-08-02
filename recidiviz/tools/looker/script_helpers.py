# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Helpers for LookML generation scripts."""

import os


def remove_lookml_files_from(directory: str) -> None:
    """
    Removes all LookML files from the given directory
    from raw_data subdirectories (e.g. views/raw_data/)
    """
    for path, _, filenames in os.walk(directory):
        for file in filenames:
            if file.endswith(".lkml"):
                os.remove(os.path.join(path, file))
