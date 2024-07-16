# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for python logic for managing and handling raw file metadata"""
import datetime
from unittest import TestCase

from recidiviz.airflow.dags.raw_data.file_metadata_tasks import (
    split_by_pre_import_normalization_type,
)
from recidiviz.airflow.dags.raw_data.metadata import (
    IMPORT_READY_FILES,
    REQUIRES_NORMALIZATION_FILES,
    REQUIRES_NORMALIZATION_FILES_BQ_METADATA,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.types.raw_data_import_types import (
    ImportReadyFile,
    RawBigQueryFileMetadataSummary,
    RawGCSFileMetadataSummary,
    RequiresNormalizationFile,
)
from recidiviz.tests.ingest.direct import fake_regions


class SplitByPreImportNormalizationTest(TestCase):
    """Unit tests for split_by_pre_import_normalization_type"""

    def test_no_files(self) -> None:
        results = split_by_pre_import_normalization_type.function(
            "US_XX", [], fake_regions
        )
        assert results[IMPORT_READY_FILES] == []
        assert results[REQUIRES_NORMALIZATION_FILES_BQ_METADATA] == []
        assert results[REQUIRES_NORMALIZATION_FILES] == []

    def test_splits_output_correctly(self) -> None:
        inputs = [
            RawBigQueryFileMetadataSummary(
                file_id=1,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadataSummary(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath(bucket_name="bucket", blob_name="blob.csv"),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadataSummary(
                file_id=2,
                file_tag="tagCustomLineTerminatorNonUTF8",
                gcs_files=[
                    RawGCSFileMetadataSummary(
                        gcs_file_id=2,
                        file_id=2,
                        path=GcsfsFilePath(
                            bucket_name="bucket", blob_name="blob1_1.csv"
                        ),
                    ),
                    RawGCSFileMetadataSummary(
                        gcs_file_id=3,
                        file_id=2,
                        path=GcsfsFilePath(
                            bucket_name="bucket", blob_name="blob1_2.csv"
                        ),
                    ),
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
        ]
        results = split_by_pre_import_normalization_type.function(
            "US_XX", [i.serialize() for i in inputs], fake_regions
        )

        assert [
            RawBigQueryFileMetadataSummary.deserialize(r)
            for r in results[REQUIRES_NORMALIZATION_FILES_BQ_METADATA]
        ] == inputs[1:]
        assert {
            RequiresNormalizationFile.deserialize(file).path
            for file in results[REQUIRES_NORMALIZATION_FILES]
        } == {gcs_file.path.abs_path() for gcs_file in inputs[1].gcs_files}
        assert [
            ImportReadyFile.deserialize(r) for r in results[IMPORT_READY_FILES]
        ] == [ImportReadyFile.from_bq_metadata(inputs[0])]
