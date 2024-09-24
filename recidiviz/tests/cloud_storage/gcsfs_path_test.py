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
"""Tests for classes in gcsfs_path.py"""
import unittest

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


class TestGcsfsPath(unittest.TestCase):
    """Tests for classes in gcsfs_path.py"""

    def test_file_path_extensions(self) -> None:
        path = GcsfsFilePath.from_absolute_path("gs://recidiviz-456-bucket/my_file.txt")
        self.assertEqual("txt", path.extension)
        self.assertFalse(path.has_zip_extension)

        path = GcsfsFilePath.from_absolute_path(
            "gs://recidiviz-456-bucket/path/to/my_file.txt"
        )
        self.assertEqual("txt", path.extension)
        self.assertFalse(path.has_zip_extension)

        path = GcsfsFilePath.from_absolute_path(
            "gs://recidiviz-456-bucket/path/to/my_file.zip"
        )
        self.assertEqual("zip", path.extension)
        self.assertTrue(path.has_zip_extension)

        path = GcsfsFilePath.from_absolute_path(
            "gs://recidiviz-456-bucket/path/to/my_file.ZIP"
        )
        self.assertEqual("ZIP", path.extension)
        # File system extensions are case-sensitive - this is not considered a ZIP file.
        self.assertFalse(path.has_zip_extension)

    def test_base_file_name(self) -> None:
        path = GcsfsFilePath.from_absolute_path("gs://recidiviz-456-bucket/my_file.txt")

        self.assertEqual("my_file.txt", path.file_name)
        self.assertEqual("my_file", path.base_file_name)
