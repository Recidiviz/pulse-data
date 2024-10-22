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
"""Tests for filtering_tasks"""

import datetime
from typing import List
from unittest import TestCase

from recidiviz.airflow.dags.raw_data.filtering_tasks import (
    filter_chunking_results_by_errors,
    filter_header_results_by_processing_errors,
)
from recidiviz.airflow.dags.raw_data.metadata import CHUNKING_ERRORS, CHUNKING_RESULTS
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.cloud_storage.types import CsvChunkBoundary
from recidiviz.ingest.direct.types.raw_data_import_types import (
    PreImportNormalizationType,
    RawBigQueryFileMetadata,
    RawFileProcessingError,
    RawGCSFileMetadata,
    RequiresPreImportNormalizationFile,
)
from recidiviz.utils.airflow_types import BatchedTaskInstanceOutput


class FilterChunkingResultsTest(TestCase):
    """Tests for filter_chunking_results_by_errors"""

    def test_empty(self) -> None:
        assert filter_chunking_results_by_errors.function(
            ['{"results": [], "errors": []}']
        ) == {CHUNKING_RESULTS: [], CHUNKING_ERRORS: []}

    @staticmethod
    def _generate_chunk_boundaries(count: int) -> List[CsvChunkBoundary]:
        """Generates a list of arbitrary CsvChunkBoundary objects based on the specified count."""
        return [
            CsvChunkBoundary(start_inclusive=i, end_exclusive=i + 1, chunk_num=i)
            for i in range(count)
        ]

    def test_non_blocking(self) -> None:
        chunks = [
            RequiresPreImportNormalizationFile(
                path=GcsfsFilePath.from_absolute_path(
                    f"test_bucket/unprocessed_2024-01-02T00:00:00:000000_raw_tag_{i}.csv"
                ),
                chunk_boundaries=self._generate_chunk_boundaries(count=4),
                pre_import_normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
            )
            for i in range(5)
        ]

        assert filter_chunking_results_by_errors.function(
            [BatchedTaskInstanceOutput(results=chunks, errors=[]).serialize()]
        ) == {
            CHUNKING_RESULTS: [c.serialize() for c in chunks],
            CHUNKING_ERRORS: [],
        }

    def test_blocking(self) -> None:
        non_blocked_chunks = [
            RequiresPreImportNormalizationFile(
                path=GcsfsFilePath.from_absolute_path(
                    f"test_bucket/unprocessed_2024-01-02T00:00:00:000000_raw_tag_{i}.csv"
                ),
                chunk_boundaries=self._generate_chunk_boundaries(count=4),
                pre_import_normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
            )
            for i in range(5)
        ]
        blocked_chunk = RequiresPreImportNormalizationFile(
            path=GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2024-01-04T00:00:00:000000_raw_tag_1.csv"
            ),
            chunk_boundaries=self._generate_chunk_boundaries(count=4),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
        )

        chunks = [*non_blocked_chunks, blocked_chunk]

        errors = [
            RawFileProcessingError(
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-03T00:00:00:000000_raw_tag_1.csv"
                ),
                temporary_file_paths=None,
                error_msg="Oops!",
            )
        ]

        result = filter_chunking_results_by_errors.function(
            [BatchedTaskInstanceOutput(results=chunks, errors=errors).serialize()]
        )

        assert result[CHUNKING_RESULTS] == [c.serialize() for c in non_blocked_chunks]
        assert len(result[CHUNKING_ERRORS]) == 2
        chunking_error = list(
            sorted(
                [
                    RawFileProcessingError.deserialize(error)
                    for error in result[CHUNKING_ERRORS]
                ],
                key=lambda x: x.parts.utc_upload_datetime,
            )
        )
        assert chunking_error[0] == errors[0]
        assert chunking_error[1].original_file_path == blocked_chunk.path


class FilterHeaderResultsTest(TestCase):
    """Tests for filter_chunking_results_by_errors"""

    file_paths = [
        GcsfsFilePath.from_absolute_path(
            "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagBasicData.csv"
        ),
        GcsfsFilePath.from_absolute_path(
            "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagBasicData.csv"
        ),
        GcsfsFilePath.from_absolute_path(
            "test_bucket/unprocessed_2021-01-03T00:00:00:000000_raw_tagBasicData.csv"
        ),
        GcsfsFilePath.from_absolute_path(
            "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagBasicerData.csv"
        ),
    ]
    file_ids_to_headers = {
        1: ["ID", "Name", "Age"],
        2: ["ID", "Name", "Age"],
        3: ["ID", "Name", "Age"],
        4: ["ID", "Name", "Age"],
    }
    bq_metadata = [
        RawBigQueryFileMetadata(
            file_id=1,
            file_tag="tagBasicData",
            gcs_files=[
                RawGCSFileMetadata(gcs_file_id=1, file_id=1, path=file_paths[0])
            ],
            update_datetime=datetime.datetime(2021, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
        ),
        RawBigQueryFileMetadata(
            file_id=2,
            file_tag="tagBasicData",
            gcs_files=[
                RawGCSFileMetadata(gcs_file_id=1, file_id=1, path=file_paths[1])
            ],
            update_datetime=datetime.datetime(2021, 1, 2, 0, 0, 0, tzinfo=datetime.UTC),
        ),
        RawBigQueryFileMetadata(
            file_id=3,
            file_tag="tagBasicData",
            gcs_files=[
                RawGCSFileMetadata(gcs_file_id=2, file_id=2, path=file_paths[2])
            ],
            update_datetime=datetime.datetime(2021, 1, 3, 1, 1, 1, tzinfo=datetime.UTC),
        ),
        RawBigQueryFileMetadata(
            file_id=4,
            file_tag="tagBasicerData",
            gcs_files=[
                RawGCSFileMetadata(gcs_file_id=3, file_id=3, path=file_paths[3])
            ],
            update_datetime=datetime.datetime(2021, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
        ),
    ]

    def test_empty(self) -> None:
        assert filter_header_results_by_processing_errors([], {}, []) == (
            {},
            [],
        )

    def test_non_blocking(self) -> None:
        assert filter_header_results_by_processing_errors(
            self.bq_metadata, self.file_ids_to_headers, []
        ) == (self.file_ids_to_headers, [])

    def test_blocking(self) -> None:
        blocking_errors = [
            RawFileProcessingError(
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagBasicData.csv"
                ),
                temporary_file_paths=None,
                error_msg="Oops!",
            )
        ]
        file_ids_to_headers = {
            1: ["ID", "Name", "Age"],
            3: ["ID", "Name", "Age"],
            4: ["ID", "Name", "Age"],
        }

        results, skipped_errors = filter_header_results_by_processing_errors(
            self.bq_metadata, file_ids_to_headers, blocking_errors
        )

        assert results == {
            1: ["ID", "Name", "Age"],
            4: ["ID", "Name", "Age"],
        }

        assert len(skipped_errors) == 1
        assert skipped_errors[0].original_file_path == GcsfsFilePath.from_absolute_path(
            "test_bucket/unprocessed_2021-01-03T00:00:00:000000_raw_tagBasicData.csv"
        )

    def test_dont_duplicate_errors(self) -> None:
        blocking_errors = [
            RawFileProcessingError(
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagBasicData.csv"
                ),
                temporary_file_paths=None,
                error_msg="Oops!",
            ),
            RawFileProcessingError(
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2021-01-03T00:00:00:000000_raw_tagBasicData.csv"
                ),
                temporary_file_paths=None,
                error_msg="Double Oops!",
            ),
        ]
        file_ids_to_headers = {
            1: ["ID", "Name", "Age"],
            4: ["ID", "Name", "Age"],
        }

        results, skipped_errors = filter_header_results_by_processing_errors(
            self.bq_metadata, file_ids_to_headers, blocking_errors
        )

        assert results == {
            1: ["ID", "Name", "Age"],
            4: ["ID", "Name", "Age"],
        }

        assert len(skipped_errors) == 0
