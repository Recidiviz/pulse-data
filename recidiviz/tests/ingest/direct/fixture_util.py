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
"""Utility methods for working with fixture files specific to direct ingest"""

import os
from typing import Union

from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import filename_parts_from_path
from recidiviz.ingest.direct.errors import DirectIngestError, DirectIngestErrorType
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest import fixtures

def add_direct_ingest_path(
        fs: FakeGCSFileSystem, path: Union[GcsfsFilePath, GcsfsDirectoryPath], has_fixture: bool = True,
        fail_handle_file_call: bool = False) -> None:
    local_path = None
    if has_fixture and isinstance(path, GcsfsFilePath):
        local_path = _get_fixture_for_direct_ingest_path(path)
    fs.test_add_path(path, local_path, fail_handle_file_call)

def _get_fixture_for_direct_ingest_path(path: GcsfsFilePath) -> str:
    """Gets the path to the fixture file based on the input path.

    If `path` is normalized, strips it to just `file_tag`, `filename_suffix`, and `extension`. Takes `path` and uses
    it as a relative path from `recidiviz/tests/ingest` to get the associated fixture.
    """
    relative_path = path.abs_path()

    try:
        directory_path, _ = os.path.split(path.abs_path())
        parts = filename_parts_from_path(path)
        suffix = f'_{parts.filename_suffix}' if parts.filename_suffix else ''
        relative_path = os.path.join(directory_path, f'{parts.file_tag}{suffix}.{parts.extension}')
    except DirectIngestError as e:
        if e.error_type != DirectIngestErrorType.INPUT_ERROR:
            raise
        # Otherwise, assume it failed because it is not a normalized path so we can just use the `fixture_filename`
        # directly.

    return fixtures.file_path_from_relative_path(relative_path)
