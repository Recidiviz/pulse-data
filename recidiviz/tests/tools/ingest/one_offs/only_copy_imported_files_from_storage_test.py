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
"""Unit tests for the only_copy_imported_files_from_storage script"""


import datetime
from unittest import TestCase
from unittest.mock import patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.ingest.one_offs.only_copy_imported_files_from_storage import (
    CopyableRawDataFile,
)


class TestCopyableRawDataFile(TestCase):
    """Unit tests for the CopyableRawDataFile class"""

    def setUp(self) -> None:
        self.gcs_cache_patcher = patch(
            "recidiviz.tools.ingest.one_offs.only_copy_imported_files_from_storage._get_gcs_cache"
        )
        self.gcs_cache_mock = self.gcs_cache_patcher.start()
        self.metadata_patcher = patch(
            "recidiviz.utils.metadata.project_id",
            return_value="recidiviz-staging",
        )
        self.metadata_patcher.start()

    def tearDown(self) -> None:
        self.gcs_cache_patcher.stop()
        self.metadata_patcher.stop()

    def test_storage_path_no_collision(self) -> None:
        self.gcs_cache_mock.return_value = {
            datetime.date(2025, 2, 28): set(
                [
                    (
                        filename_parts_from_path(
                            GcsfsFilePath.from_absolute_path(
                                "gs://recidiviz-staging-direct-ingest-state-storage/us_tn/raw/2025/02/28/processed_2025-02-28T09:12:50:112401_raw_VantagePointAssessments.csv"
                            )
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "gs://recidiviz-staging-direct-ingest-state-storage/us_tn/raw/2025/02/28/processed_2025-02-28T09:12:50:112401_raw_VantagePointAssessments.csv"
                        ),
                    ),
                ]
            )
        }
        file = CopyableRawDataFile.build(
            file_tag="VantagePointAssessments",
            update_datetime=datetime.datetime.fromisoformat(
                "2025-02-28T09:12:50:112401"
            ).replace(tzinfo=datetime.UTC),
        )

        assert file.storage_path == file.supposed_storage_path(
            StateCode.US_TN, DirectIngestInstance.PRIMARY
        )

    def test_storage_path_collision(self) -> None:
        self.gcs_cache_mock.return_value = {
            datetime.date(2025, 2, 28): set(
                [
                    (
                        filename_parts_from_path(
                            GcsfsFilePath.from_absolute_path(
                                "gs://recidiviz-staging-direct-ingest-state-storage/us_tn/raw/2025/02/28/processed_2025-02-28T09:12:50:112401_raw_VantagePointAssessments.csv"
                            )
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "gs://recidiviz-staging-direct-ingest-state-storage/us_tn/raw/2025/02/28/processed_2025-02-28T09:12:50:112401_raw_VantagePointAssessments.csv"
                        ),
                    ),
                    (
                        filename_parts_from_path(
                            GcsfsFilePath.from_absolute_path(
                                "gs://recidiviz-staging-direct-ingest-state-storage/us_tn/raw/2025/02/28/processed_2025-02-28T09:12:50:112401_raw_VantagePointAssessments-(1).csv"
                            )
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "gs://recidiviz-staging-direct-ingest-state-storage/us_tn/raw/2025/02/28/processed_2025-02-28T09:12:50:112401_raw_VantagePointAssessments-(1).csv"
                        ),
                    ),
                    (
                        filename_parts_from_path(
                            GcsfsFilePath.from_absolute_path(
                                "gs://recidiviz-staging-direct-ingest-state-storage/us_tn/raw/2025/02/28/processed_2025-02-28T09:12:50:112401_raw_VantagePointAssessments-(2).csv"
                            )
                        ),
                        GcsfsFilePath.from_absolute_path(
                            "gs://recidiviz-staging-direct-ingest-state-storage/us_tn/raw/2025/02/28/processed_2025-02-28T09:12:50:112401_raw_VantagePointAssessments-(2).csv"
                        ),
                    ),
                ]
            )
        }
        file = CopyableRawDataFile.build(
            file_tag="VantagePointAssessments",
            update_datetime=datetime.datetime.fromisoformat(
                "2025-02-28T09:12:50:112401"
            ).replace(tzinfo=datetime.UTC),
        )

        assert file.storage_path != file.supposed_storage_path(
            StateCode.US_TN, DirectIngestInstance.PRIMARY
        )

        assert file.storage_path == GcsfsFilePath.from_absolute_path(
            "gs://recidiviz-staging-direct-ingest-state-storage/us_tn/raw/2025/02/28/processed_2025-02-28T09:12:50:112401_raw_VantagePointAssessments-(2).csv"
        )

    def test_path_creation(self) -> None:
        file = CopyableRawDataFile(
            file_tag="VantagePointAssessments",
            update_datetime=datetime.datetime.fromisoformat(
                "2025-02-28T09:12:50:112401"
            ).replace(tzinfo=datetime.UTC),
            storage_path=GcsfsFilePath.from_absolute_path(
                "gs://recidiviz-staging-direct-ingest-state-storage/us_tn/raw/2025/02/28/processed_2025-02-28T09:12:50:112401_raw_VantagePointAssessments-(2).csv"
            ),
        )

        assert file.ingest_bucket_path(
            state_code=StateCode.US_TN,
            raw_data_instance=DirectIngestInstance.SECONDARY,
        ) == GcsfsFilePath.from_absolute_path(
            "gs://recidiviz-staging-direct-ingest-state-us-tn-secondary/unprocessed_2025-02-28T09:12:50:112401_raw_VantagePointAssessments-(2).csv"
        )
