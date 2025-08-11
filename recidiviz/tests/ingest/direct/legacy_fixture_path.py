# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
from typing import Optional

import attr

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_raw_file_path,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.types.errors import (
    DirectIngestError,
    DirectIngestErrorType,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest.direct import direct_ingest_fixtures
from recidiviz.utils.types import assert_type


class DirectIngestFixtureDataFileType(Enum):
    """Enumerates the different types of fixture files used as input to ingest tests."""

    # Fixture files that contain mocked raw data that is used as inputs to ingest view
    # tests.
    RAW = "raw"

    # Fixture files that contain expected ingest view results for ingest view tests.
    INGEST_VIEW_RESULTS = "ingest_view"

    # Fixture files that contain ingest view results in CSV form that are used as inputs
    # ingest pipeline integration tests.
    # TODO(#22059): Have ingest pipeline integration tests use raw fixtures
    EXTRACT_AND_MERGE_INPUT = "extract_and_merge_input"

    # Fixture files that contain data in the format of the `us_xx_state`/`state` dataset
    STATE_DATA = "state"

    # Fixture files that contain data in the format of the `state` dataset
    NORMALIZED_STATE_DATA = "normalized_state"


# TODO(#39686) Delete this class when all tests use updated fixture paths
@attr.define
class DirectIngestTestFixturePath:
    """Class storing information about an ingest test fixture file."""

    region_code: str
    fixture_file_type: DirectIngestFixtureDataFileType
    subdir_name: Optional[str]
    file_name: str

    def full_path(self) -> str:
        """Returns the absolute path to this fixture file."""
        return os.path.join(
            self._fixture_directory(),
            self.file_name,
        )

    def test_name(self) -> str:
        """Returns the name of the test associated with this fixture file (ingest view
        tests only).
        """
        if self.fixture_file_type not in (
            DirectIngestFixtureDataFileType.RAW,
            DirectIngestFixtureDataFileType.INGEST_VIEW_RESULTS,
        ):
            raise ValueError(
                f"Unexpected fixture type [{self.fixture_file_type}]. Can only infer "
                f"the test name from the path for RAW and INGEST_VIEW_RESULTS fixtures."
            )
        return self.file_name.removesuffix(".csv")

    def raw_table_dependency_arg_name(self) -> str:
        """The name of the raw table dependency for a RAW fixture file. This is the text
        inside the brackets of a raw table reference in an ingest view query (e.g.
        "myFileTag" or "myFileTag@ALL"). It corresponds to a subidirectory name the
        fixture file lives in.
        """
        if self.fixture_file_type is not DirectIngestFixtureDataFileType.RAW:
            raise ValueError(
                f"Unexpected fixture type [{self.fixture_file_type}]. Can only infer "
                f"the raw file tag from the path for RAW fixtures."
            )
        return assert_type(self.subdir_name, str)

    def ingest_view_name(self) -> str:
        if (
            self.fixture_file_type
            is DirectIngestFixtureDataFileType.INGEST_VIEW_RESULTS
        ):
            return assert_type(self.subdir_name, str)
        if (
            self.fixture_file_type
            is DirectIngestFixtureDataFileType.EXTRACT_AND_MERGE_INPUT
        ):
            return self.file_name.removesuffix(".csv")

        raise ValueError(
            f"Unexpected fixture type [{self.fixture_file_type}]. Can only infer the "
            f"ingest view name from the path for INGEST_VIEW_RESULTS and "
            f"EXTRACT_AND_MERGE_INPUT fixtures."
        )

    @classmethod
    def for_ingest_view_test_results_fixture(
        cls,
        *,
        region_code: str,
        ingest_view_name: str,
        file_name: str,
    ) -> "DirectIngestTestFixturePath":
        return cls(
            region_code=region_code,
            fixture_file_type=DirectIngestFixtureDataFileType.INGEST_VIEW_RESULTS,
            subdir_name=ingest_view_name,
            file_name=file_name,
        )

    @classmethod
    def for_state_data_fixture(
        cls,
        *,
        region_code: str,
        table_id: str,
        file_name: str,
    ) -> "DirectIngestTestFixturePath":
        return cls(
            region_code=region_code,
            fixture_file_type=DirectIngestFixtureDataFileType.STATE_DATA,
            subdir_name=table_id,
            file_name=file_name,
        )

    @classmethod
    def for_normalized_state_data_fixture(
        cls,
        *,
        region_code: str,
        table_id: str,
        file_name: str,
    ) -> "DirectIngestTestFixturePath":
        return cls(
            region_code=region_code,
            fixture_file_type=DirectIngestFixtureDataFileType.NORMALIZED_STATE_DATA,
            subdir_name=table_id,
            file_name=file_name,
        )

    @classmethod
    def for_extract_and_merge_fixture(
        cls,
        *,
        region_code: str,
        file_name: str,
    ) -> "DirectIngestTestFixturePath":
        return cls(
            region_code=region_code,
            fixture_file_type=DirectIngestFixtureDataFileType.EXTRACT_AND_MERGE_INPUT,
            subdir_name=None,
            file_name=file_name,
        )

    @classmethod
    def fixtures_root(cls) -> str:
        """The root directory with all ingest test fixture files."""
        return os.path.dirname(direct_ingest_fixtures.__file__)

    @classmethod
    def fixtures_root_for_region(cls, region_code: str) -> str:
        """The root directory with all ingest test fixture files for a given region."""
        return os.path.join(cls.fixtures_root(), region_code.lower())

    @classmethod
    def from_path(cls, fixture_path: str) -> "DirectIngestTestFixturePath":
        """Parses a DirectIngestTestFixturePath from a fixture file absolute path."""
        if not os.path.exists(fixture_path):
            raise ValueError(f"Fixture path does not exist: {fixture_path}")

        if os.path.isdir(fixture_path):
            raise ValueError(
                f"Fixture path should be a file, not a directory: {fixture_path}"
            )

        relative_path = os.path.relpath(fixture_path, cls.fixtures_root())
        parts = relative_path.split(os.sep)

        region_code = parts[0]

        if len(parts) == 2:
            return cls(
                region_code=region_code,
                fixture_file_type=DirectIngestFixtureDataFileType.EXTRACT_AND_MERGE_INPUT,
                subdir_name=None,
                file_name=parts[1],
            )
        fixture_file_type = DirectIngestFixtureDataFileType(parts[1])

        if len(parts) != 4:
            raise ValueError(
                f"Expected 4 parts in relative path {relative_path}, found "
                f"{len(parts)}: {parts}"
            )
        return cls(
            region_code=region_code,
            fixture_file_type=fixture_file_type,
            subdir_name=parts[2],
            file_name=parts[3],
        )

    def _fixture_directory(self) -> str:
        """Returns the directory this fixture file is in."""
        region_fixtures_directory_path = self.fixtures_root_for_region(self.region_code)

        if (
            self.fixture_file_type
            is DirectIngestFixtureDataFileType.EXTRACT_AND_MERGE_INPUT
        ):
            return region_fixtures_directory_path

        if self.fixture_file_type is DirectIngestFixtureDataFileType.RAW:
            return os.path.join(
                region_fixtures_directory_path,
                self.fixture_file_type.value,
                self.raw_table_dependency_arg_name(),
            )

        if (
            self.fixture_file_type
            is DirectIngestFixtureDataFileType.INGEST_VIEW_RESULTS
        ):
            return os.path.join(
                region_fixtures_directory_path,
                self.fixture_file_type.value,
                self.ingest_view_name(),
            )

        if self.fixture_file_type in (
            DirectIngestFixtureDataFileType.STATE_DATA,
            DirectIngestFixtureDataFileType.NORMALIZED_STATE_DATA,
        ):
            return os.path.join(
                region_fixtures_directory_path,
                self.fixture_file_type.value,
                assert_type(self.subdir_name, str),
            )

        raise ValueError(f"Unexpected fixture file type [{self}]")


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
    return DirectIngestTestFixturePath.for_extract_and_merge_fixture(
        region_code=region_code,
        file_name=file_name,
    ).full_path()


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
