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
import datetime
import os
from enum import Enum
from typing import Iterable, Iterator, List, Optional, Tuple

import pandas as pd

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_raw_file_path,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.types.errors import (
    DirectIngestError,
    DirectIngestErrorType,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewRawFileDependency,
)
from recidiviz.tests.ingest.direct import direct_ingest_fixtures
from recidiviz.utils import csv


def _direct_ingest_raw_file_path(
    *,
    bucket_path: GcsfsBucketPath,
    filename: str,
    should_normalize: bool,
    dt: Optional[datetime.datetime],
) -> GcsfsFilePath:
    file_path_str = filename

    if should_normalize:
        file_path_str = to_normalized_unprocessed_raw_file_path(
            original_file_path=file_path_str, dt=dt
        )

    file_path = GcsfsFilePath.from_directory_and_file_name(
        dir_path=bucket_path,
        file_name=file_path_str,
    )
    if not isinstance(file_path, GcsfsFilePath):
        raise ValueError(
            f"Expected type GcsfsFilePath, found {type(file_path)} for path: {file_path.abs_path()}"
        )
    return file_path


def add_direct_ingest_path(
    *,
    fs: GCSFileSystem,
    region_code: str,
    bucket_path: GcsfsBucketPath,
    filename: str,
    should_normalize: bool,
    dt: Optional[datetime.datetime] = None,
    has_fixture: bool = True,
    fail_handle_file_call: bool = False,
) -> GcsfsFilePath:

    path = _direct_ingest_raw_file_path(
        bucket_path=bucket_path,
        filename=filename,
        should_normalize=should_normalize,
        dt=dt,
    )

    if not isinstance(fs, FakeGCSFileSystem):
        raise ValueError(
            "add_direct_ingest_path can only be called on FakeGCSFileSystems"
        )
    local_path = None
    if has_fixture and isinstance(path, GcsfsFilePath):
        local_path = _get_fixture_for_direct_ingest_path(path, region_code=region_code)
    fs.test_add_path(path, local_path, fail_handle_file_call)
    return path


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
    # TODO(#15801): Move the fixture files used by parser / integration tests to ingest
    #  view subdir and delete this enum?
    EXTRACT_AND_MERGE_INPUT = "EXTRACT_AND_MERGE_INPUT"

    def fixture_directory_for_region_code(
        self, region_code: str, subdir_name: Optional[str] = None
    ) -> str:
        region_fixtures_directory_path = os.path.join(
            os.path.dirname(direct_ingest_fixtures.__file__), region_code.lower()
        )

        if self is DirectIngestFixtureDataFileType.EXTRACT_AND_MERGE_INPUT:
            return region_fixtures_directory_path

        if self is DirectIngestFixtureDataFileType.RAW:
            if subdir_name is None:
                raise ValueError(
                    f"subdir_name cannot be none for fixture file type [{self}]"
                )
            subdir = os.path.join("raw", subdir_name)
        elif self is DirectIngestFixtureDataFileType.INGEST_VIEW_RESULTS:
            if subdir_name is None:
                raise ValueError(
                    f"subdir_name cannot be none for fixture file type [{self}]"
                )
            subdir = os.path.join("ingest_view", subdir_name)
        elif self is DirectIngestFixtureDataFileType.ENUM_RAW_TEXT:
            subdir = "enum_raw_text"
        else:
            raise ValueError(f"Unexpected fixture file type [{self}]")

        return os.path.join(region_fixtures_directory_path, subdir)


def replace_empty_with_null(
    values: Iterable[Tuple[str, ...]]
) -> Iterator[Tuple[Optional[str], ...]]:
    """Replaces empty string values in tuple with null."""
    for row in values:
        yield tuple(value or None for value in row)


def load_dataframe_from_path(
    raw_fixture_path: str, fixture_columns: List[str]
) -> pd.DataFrame:
    """Given a raw fixture path and a list of fixture columns, load the raw data into a dataframe."""
    mock_data = csv.get_rows_as_tuples(raw_fixture_path)
    values = replace_empty_with_null(mock_data)
    return pd.DataFrame(values, columns=fixture_columns)


def direct_ingest_fixture_path(
    *,
    region_code: str,
    fixture_file_type: DirectIngestFixtureDataFileType,
    file_name: str,
) -> str:
    if fixture_file_type == DirectIngestFixtureDataFileType.RAW:
        raise ValueError(
            f"Unexpected fixture_file_type {fixture_file_type} - use "
            f"ingest_view_raw_table_dependency_fixture_path() instead."
        )
    if fixture_file_type == DirectIngestFixtureDataFileType.INGEST_VIEW_RESULTS:
        raise ValueError(
            f"Unexpected fixture_file_type {fixture_file_type} - use "
            f"ingest_view_results_fixture_path() instead."
        )
    return _direct_ingest_fixture_path(
        region_code=region_code,
        fixture_file_type=fixture_file_type,
        subdir_name=None,
        file_name=file_name,
    )


def _direct_ingest_fixture_path(
    *,
    region_code: str,
    fixture_file_type: DirectIngestFixtureDataFileType,
    subdir_name: Optional[str],
    file_name: str,
) -> str:
    return os.path.join(
        fixture_file_type.fixture_directory_for_region_code(
            region_code=region_code, subdir_name=subdir_name
        ),
        file_name,
    )


def ingest_view_raw_table_dependency_fixture_path(
    region_code: str,
    raw_file_dependency_config: DirectIngestViewRawFileDependency,
    file_name: str,
) -> str:
    return _direct_ingest_fixture_path(
        region_code=region_code,
        fixture_file_type=DirectIngestFixtureDataFileType.RAW,
        subdir_name=raw_file_dependency_config.raw_table_dependency_arg_name,
        file_name=file_name,
    )


def ingest_view_results_fixture_path(
    region_code: str,
    ingest_view_name: str,
    file_name: str,
) -> str:
    return _direct_ingest_fixture_path(
        region_code=region_code,
        fixture_file_type=DirectIngestFixtureDataFileType.INGEST_VIEW_RESULTS,
        subdir_name=ingest_view_name,
        file_name=file_name,
    )
