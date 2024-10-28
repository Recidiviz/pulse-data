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
"""Tests for utilities for raw file sandbox imports."""

from unittest import TestCase
from unittest.mock import ANY, MagicMock, call, create_autospec, patch

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import import (
    legacy_import_raw_files_to_bq_sandbox,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.fakes.fake_ingest_raw_file_import_controller import (
    FakeDirectIngestRegionRawFileConfig,
)
from recidiviz.tests.utils.fake_region import fake_region


class ImportRawFilesToBQSandboxTest(TestCase):
    """test for the import_raw_files_to_bq_sandbox function in ingest_utils.py"""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.metadata_patcher.start().return_value = "test-project"
        self.region_patcher = patch(
            "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import.get_direct_ingest_region",
            MagicMock(
                return_value=fake_region(
                    region_code=StateCode.US_XX.value.lower(),
                    environment="staging",
                    region_module=fake_regions,
                )
            ),
        )
        self.region_patcher.start()
        self.file_manager_patch = patch(
            "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import.LegacyDirectIngestRawFileImportManager"
        )
        self.file_manager_mock = self.file_manager_patch.start().return_value
        self.file_manager_mock.region_raw_file_config = (
            FakeDirectIngestRegionRawFileConfig("US_XX")
        )
        self.mock_big_query_client = create_autospec(BigQueryClient)
        self.mock_gcsfs = create_autospec(GCSFileSystem)

    def tearDown(self) -> None:
        self.metadata_patcher.stop()
        self.region_patcher.stop()
        self.file_manager_patch.stop()

    def test_import_raw_files_to_bq_sandbox_plain(
        self,
    ) -> None:
        # Arrange
        path1 = GcsfsFilePath(
            "bar-bucket",
            "unprocessed_2019-08-12T00:00:00:000000_raw_tagBasicData.csv",
        )
        path2 = GcsfsFilePath(
            "bar-bucket",
            "unprocessed_2019-08-12T00:00:00:000000_raw_tagMoreBasicData.csv",
        )
        files_to_import = [path1, path2]

        # Act
        legacy_import_raw_files_to_bq_sandbox(
            state_code=StateCode.US_XX,
            sandbox_dataset_prefix="foo",
            allow_incomplete_configs=False,
            files_to_import=files_to_import,
            big_query_client=self.mock_big_query_client,
            fs=self.mock_gcsfs,
        )

        # Assert
        self.file_manager_mock.import_raw_file_to_big_query.assert_has_calls(
            [call(path1, ANY), call(path2, ANY)]
        )

    def test_import_raw_files_to_bq_sandbox_filter(
        self,
    ) -> None:
        # Arrange
        path1 = GcsfsFilePath(
            "bar-bucket",
            "unprocessed_2019-08-12T00:00:00:000000_raw_tagBasicData.csv",
        )
        path2 = GcsfsFilePath(
            "bar-bucket",
            "unprocessed_2019-08-12T00:00:00:000000_raw_tagMoreBasicData.csv",
        )
        files_to_import = [path1, path2]

        # Act
        legacy_import_raw_files_to_bq_sandbox(
            state_code=StateCode.US_XX,
            sandbox_dataset_prefix="foo",
            allow_incomplete_configs=False,
            files_to_import=files_to_import,
            big_query_client=self.mock_big_query_client,
            fs=self.mock_gcsfs,
        )

        # Assert
        self.file_manager_mock.import_raw_file_to_big_query.assert_has_calls(
            [call(path2, ANY)]
        )
