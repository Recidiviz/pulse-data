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
from enum import Enum
from typing import Optional, Union

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
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
    return direct_ingest_fixture_path(
        region_code=region_code,
        file_name=file_name,
        fixture_file_type=DirectIngestFixtureDataFileType.EXTRACT_AND_MERGE_INPUT,
    )


class DirectIngestFixtureDataFileType(Enum):
    """Enumerates the different types of fixture files used as input to ingest tests."""

    # Fixture files that contain mocked raw data that is used as inputs to ingest view
    # tests.
    RAW = "RAW"

    # Fixture files that contain expected ingest view results for ingest view tests.
    INGEST_VIEW_RESULTS = "INGEST_VIEW_RESULTS"

    # Fixture files that contain enum raw text values for use in enum mappings tests.
    ENUM_RAW_TEXT = "ENUM_RAW_TEXT"

    # Fixture files that contain ingest view results in CSV form that are used as inputs
    # to parser and controller extract and merge integration tests.
    # TODO(#10301): Move the fixture files used by parser / integration tests to ingest
    #  view subdir and delete this enum?
    EXTRACT_AND_MERGE_INPUT = "EXTRACT_AND_MERGE_INPUT"

    def fixture_directory_for_region_code(
        self, region_code: str, file_tag: Optional[str] = None
    ) -> str:
        region_fixtures_directory_path = os.path.join(
            os.path.dirname(direct_ingest_fixtures.__file__), region_code.lower()
        )

        if self is DirectIngestFixtureDataFileType.EXTRACT_AND_MERGE_INPUT:
            return region_fixtures_directory_path

        if self is DirectIngestFixtureDataFileType.RAW:
            if file_tag is None:
                raise ValueError(
                    f"File tag cannot be none for fixture file type [{self}]"
                )
            subdir = os.path.join("raw", file_tag)
        elif self is DirectIngestFixtureDataFileType.INGEST_VIEW_RESULTS:
            if file_tag is None:
                raise ValueError(
                    f"File tag cannot be none for fixture file type [{self}]"
                )
            subdir = os.path.join("ingest_view", file_tag)
        elif self is DirectIngestFixtureDataFileType.ENUM_RAW_TEXT:
            subdir = "enum_raw_text"
        else:
            raise ValueError(f"Unexpected fixture file type [{self}]")

        return os.path.join(region_fixtures_directory_path, subdir)


def direct_ingest_fixture_path(
    *,
    region_code: str,
    fixture_file_type: DirectIngestFixtureDataFileType,
    file_tag: Optional[str] = None,
    file_name: str,
) -> str:
    return os.path.join(
        fixture_file_type.fixture_directory_for_region_code(
            region_code=region_code, file_tag=file_tag
        ),
        file_name,
    )
