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
"""Test that all directories containing Python are valid modules."""

import os
import unittest

import recidiviz


class TestPackages(unittest.TestCase):
    """Test that all directories containing Python are valid modules."""

    def test_all_python_directories_are_module(self) -> None:
        python_root_dir = os.path.dirname(recidiviz.__file__)

        def has_python_files(dir_path: str) -> bool:
            for item in os.listdir(dir_path):
                if item.endswith(".py"):
                    return True
                child_path = os.path.join(dir_path, item)
                if os.path.isdir(child_path) and has_python_files(child_path):
                    return True
            return False

        bad_packages = []
        for path, _directory_names, file_names in os.walk(python_root_dir):
            if "__init__.py" in file_names:
                continue
            if not has_python_files(path):
                continue

            bad_packages.append(path)

        if bad_packages:
            bad_packages_str = "\n * ".join(bad_packages)
            self.fail(
                f"Found python code directories with no __init__.py file:\n{bad_packages_str}",
            )
