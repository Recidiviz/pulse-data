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
from typing import Optional, Union

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.types.errors import (
    DirectIngestError,
    DirectIngestErrorType,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest.direct import direct_ingest_fixtures


def add_direct_ingest_path(
    fs: GCSFileSystem,
    path: Union[GcsfsFilePath, GcsfsDirectoryPath],
    region_code: str,
    has_fixture: bool = True,
    fail_handle_file_call: bool = False,
) -> None:
    if not isinstance(fs, FakeGCSFileSystem):
        raise ValueError(
            "add_direct_ingest_path can only be called on FakeGCSFileSystems"
        )
    local_path = None
    if has_fixture and isinstance(path, GcsfsFilePath):
        local_path = _get_fixture_for_direct_ingest_path(path, region_code=region_code)
    fs.test_add_path(path, local_path, fail_handle_file_call)


def _get_fixture_for_direct_ingest_path(path: GcsfsFilePath, region_code: str) -> str:
    """Gets the path to the fixture file based on the input path.

    If `path` is normalized, strips it to just `file_tag`, `filename_suffix`, and
    `extension`. Then generates a path in the regions ingest fixtures directory
    at recidiviz/tests/ingest/direct/direct_ingest_fixtures/us_xx/
    """
    try:
        parts = filename_parts_from_path(path)
        suffix = f"_{parts.filename_suffix}" if parts.filename_suffix else ""
        file_name = f"{parts.file_tag}{suffix}.{parts.extension}"
    except DirectIngestError as e:
        if e.error_type != DirectIngestErrorType.INPUT_ERROR:
            raise
        # Otherwise, assume it failed because it is not a normalized path so we can just
        # use the path file name directly.
        file_name = path.file_name

    # TODO(#10301): Move the fixture files used by parser / integration tests to ingest view subdir
    return direct_ingest_fixture_path(region_code=region_code, file_name=file_name)


def direct_ingest_fixture_path(
    *,
    region_code: str,
    file_type: Optional[GcsfsDirectIngestFileType] = None,
    file_tag: Optional[str] = None,
    file_name: str,
) -> str:
    region_fixtures_directory_path = os.path.join(
        os.path.dirname(direct_ingest_fixtures.__file__), region_code.lower()
    )

    if file_type is None:
        directory_path = region_fixtures_directory_path
    elif file_type in (
        GcsfsDirectIngestFileType.RAW_DATA,
        GcsfsDirectIngestFileType.INGEST_VIEW,
    ):
        if file_tag is None:
            raise ValueError(f"File tag cannot be none for file_type [{file_type}]")
        directory_path = os.path.join(
            region_fixtures_directory_path, file_type.value.lower(), file_tag
        )
    else:
        raise ValueError(f"Unexpected file_type [{file_type}]")

    return os.path.join(directory_path, file_name)
