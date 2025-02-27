# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

"""Utilities for working with fixture data in unit testing."""

from recidiviz.common.local_file_paths import filepath_relative_to_caller


def as_filepath(filename: str, subdir: str = "fixtures") -> str:
    """Returns the filepath for the fixture file.

    Assumes the |filename| is in the |subdir| subdirectory relative to the
    caller's directory.
    """
    return filepath_relative_to_caller(filename, subdir, caller_depth=2)
