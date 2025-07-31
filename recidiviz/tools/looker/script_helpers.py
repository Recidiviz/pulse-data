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
import hashlib
import os

from recidiviz.tools.looker.constants import (
    GENERATED_LOOKML_ROOT_PATH,
    GENERATED_SUBDIR_NAME,
    VIEWS_DIR,
)


def hash_directory(path: str) -> str:
    """
    Computes a SHA-256 hash for the contents of a directory, including file paths
    and file contents. This ensures that changes such as renames, moves, or content
    modifications are reflected in the hash.
    Args:
        path (str): The path to the directory to be hashed.
    Returns:
        str: The hexadecimal representation of the SHA-256 hash of the directory.
    """

    hash_obj = hashlib.sha256()

    for root, _dirs, files in sorted(os.walk(path)):
        for file in sorted(files):
            file_path = os.path.join(root, file)
            # Include file paths in hash to detect renames/moves
            relative_path = os.path.relpath(file_path, path)
            hash_obj.update(relative_path.encode())

            with open(file_path, "rb") as f:
                while chunk := f.read(8192):
                    hash_obj.update(chunk)

    return hash_obj.hexdigest()


def hash_generated_directory() -> str:
    """
    Computes and returns a hash value for the contents of the directory
    specified by the `GENERATED_ROOT_PATH` constant. This function utilizes
    the `hash_directory` method to generate the hash.
    Returns:
        str: A string representation of the hash value for the directory contents.
    """
    return hash_directory(path=GENERATED_LOOKML_ROOT_PATH)


def remove_lookml_files_from(directory: str) -> None:
    """
    Removes all LookML files from the given directory
    """
    for path, _, filenames in os.walk(directory):
        for file in filenames:
            if file.endswith(".lkml") or file.endswith(".lookml"):
                os.remove(os.path.join(path, file))


def get_generated_views_path(output_dir: str, module_name: str) -> str:
    """
    Returns the path to the generated views directory for a given module name.
    "module" is a loose term and can be any string that represents a logical grouping.
    TODO(#45676) Refactor looker repo so that all generated files have a common generated/ root
    directory. ex `views/{module_name}/generated/` -> `generated/views/{module_name}/`
    """
    return os.path.join(output_dir, VIEWS_DIR, module_name, GENERATED_SUBDIR_NAME)
