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
"""Utility methods for working with justice counts files in tests."""

import os

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


def gcs_path(filepath: str) -> GcsfsFilePath:
    return GcsfsFilePath.from_absolute_path(
        os.path.join("gs://justice_counts", filepath)
    )


def prepare_files(fs: FakeGCSFileSystem, manifest_filepath: str) -> GcsfsFilePath:
    """Makes the file system aware of all files for the report and returns the manifest filepath."""
    directory = os.path.dirname(manifest_filepath)
    for file_name in os.listdir(directory):
        path = os.path.join(directory, file_name)
        fs.test_add_path(gcs_path(path), path)
    return gcs_path(manifest_filepath)
