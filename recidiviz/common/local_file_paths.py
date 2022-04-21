# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Helpers for building paths to local files."""

import inspect
import os


def filepath_relative_to_caller(
    filename: str, relative_subdir: str, caller_depth: int = 1
) -> str:
    """Returns the filepath for the file at a path relative to the caller's directory.
    The |caller_depth| indicates how deep into the stack we want to look for the 'caller'
    of this function. Defaults to 1, which is the direct caller.
    """
    frame = inspect.stack()[caller_depth]
    module = inspect.getmodule(frame[0])

    if module is None:
        raise ValueError("Module is unexpectedly None")

    caller_filepath = module.__file__
    assert caller_filepath is not None

    if caller_filepath is None:
        raise ValueError(f"No file associated with {module}.")

    return os.path.abspath(
        os.path.join(caller_filepath, "..", relative_subdir, filename)
    )
