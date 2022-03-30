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
"""Utils for interacting with the operating system's file system."""
import os
from typing import Iterable, Set


def get_all_files_recursive(directory_path: str) -> Set[str]:
    """Returns all paths to files in the provided directory.

    Will return an empty set if the directory is empty, or if the provided path is a
    file and not a directory.
    """
    return set(
        os.path.join(path, file_name)
        for path, _directory_names, file_names in os.walk(directory_path)
        for file_name in file_names
    )


def delete_files(file_paths: Iterable[str], delete_empty_dirs: bool = False) -> None:
    """Deletes all files specified by |file_paths|.

    If |delete_empty_dirs| is True, will delete any directories that stored the
    deleted files if they are empty following the file deletion.
    """
    dir_deletion_candidate = set()
    for path in file_paths:
        os.remove(path)
        dir_deletion_candidate.add(os.path.normpath(os.path.dirname(path)))

    if delete_empty_dirs:
        # Reverse sort to place paths deepest in tree first.
        for dir_path in reversed(sorted(dir_deletion_candidate)):
            if os.path.exists(dir_path) and not os.listdir(dir_path):
                os.removedirs(dir_path)
