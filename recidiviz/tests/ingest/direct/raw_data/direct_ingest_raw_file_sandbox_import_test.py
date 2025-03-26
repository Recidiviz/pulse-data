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

import struct
from unittest import TestCase
from unittest.mock import ANY, MagicMock, call, create_autospec, patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.cloud_storage.types import CsvChunkBoundary
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import import (
    _do_pre_import_normalization,
    _import_bq_metadata_to_sandbox,
    _pre_import_norm_for_path,
    _validate_checksums,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    AppendReadyFile,
    AppendSummary,
    ImportReadyFile,
    PreImportNormalizationType,
    PreImportNormalizedCsvChunkResult,
    RawBigQueryFileMetadata,
    RawFileBigQueryLoadConfig,
    RawGCSFileMetadata,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    get_region_raw_file_config,
)
from recidiviz.tests.ingest.direct import fake_regions


class ImportRawFilesToBQSandboxTest(TestCase):
    """Integration tests for import_raw_files_to_sandbox"""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.metadata_patcher.start().return_value = "test-project"
        self.region_raw_file_config = get_region_raw_file_config("US_XX", fake_regions)
        self.mock_big_query_client = create_autospec(BigQueryClient)
        self.mock_gcsfs = create_autospec(GCSFileSystem)

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_validate_checksums(self) -> None:
        with self.assertRaisesRegex(
            struct.error, r"required argument is not an integer"
        ):
            _validate_checksums(
                fs=self.mock_gcsfs,
                path=create_autospec(GcsfsFilePath),
                chunk_results=[],
            )

        path = GcsfsFilePath.from_absolute_path("gs://test/test.csv")

        self.mock_gcsfs.get_crc32c.return_value = "AAAACg=="

        _validate_checksums(
            fs=self.mock_gcsfs,
            path=path,
            chunk_results=[
                PreImportNormalizedCsvChunkResult(
                    input_file_path=path,
                    output_file_path=path,
                    chunk_boundary=CsvChunkBoundary(
                        start_inclusive=0, end_exclusive=10, chunk_num=0
                    ),
                    crc32c=10,
                )
            ],
        )

        self.mock_gcsfs.get_crc32c.assert_called_once()

    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import.DirectIngestRawFilePreImportNormalizer"
    )
    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import.GcsfsCsvChunkBoundaryFinder"
    )
    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import._validate_checksums"
    )
    def test_pre_import_norm_for_path(
        self,
        validate_mock: MagicMock,
        boundary_mock: MagicMock,
        normalizer_mock: MagicMock,
    ) -> None:
        path = GcsfsFilePath.from_absolute_path(
            "gs://test/unprocessed_2024-01-25T16:35:33:617135_raw_tagMoreBasicData.csv"
        )

        boundary_mock().get_chunks_for_gcs_path.return_value = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=10, chunk_num=0),
            CsvChunkBoundary(start_inclusive=10, end_exclusive=20, chunk_num=1),
            CsvChunkBoundary(start_inclusive=20, end_exclusive=30, chunk_num=2),
        ]

        _pre_import_norm_for_path(
            fs=self.mock_gcsfs,
            path=path,
            raw_file_config=self.region_raw_file_config.raw_file_configs[
                "tagMoreBasicData"
            ],
            pre_import_norm_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            sandbox_dataset_prefix="test",
        )

        boundary_mock().get_chunks_for_gcs_path.assert_called_once()
        normalizer_mock().normalize_chunk_for_import.assert_has_calls(
            [call(ANY), call(ANY), call(ANY)]
        )
        validate_mock.assert_called_once()

    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import._pre_import_norm_for_path"
    )
    def test_do_pre_import_normalization(
        self,
        do_import_mock: MagicMock,
    ) -> None:

        gcs_files = [
            RawGCSFileMetadata(
                path=GcsfsFilePath.from_absolute_path(
                    "gs://test/unprocessed_2024-01-25T16:35:33:617135_raw_tagMoreBasicData-0.csv"
                ),
                gcs_file_id=1,
                file_id=1,
            ),
            RawGCSFileMetadata(
                path=GcsfsFilePath.from_absolute_path(
                    "gs://test/unprocessed_2024-01-25T16:35:33:617135_raw_tagMoreBasicData-1.csv"
                ),
                gcs_file_id=2,
                file_id=1,
            ),
        ]

        bq_metadata = RawBigQueryFileMetadata.from_gcs_files(gcs_files)
        raw_file_config = self.region_raw_file_config.raw_file_configs[
            "tagMoreBasicData"
        ]

        _do_pre_import_normalization(
            fs=self.mock_gcsfs,
            raw_file_config=raw_file_config,
            bq_metadata=bq_metadata,
            load_config=RawFileBigQueryLoadConfig.from_raw_file_config(raw_file_config),
            pre_import_norm_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            sandbox_dataset_prefix="test",
        )

        do_import_mock.assert_has_calls(
            [
                call(
                    fs=self.mock_gcsfs,
                    path=gcs_files[0].path,
                    raw_file_config=raw_file_config,
                    pre_import_norm_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
                    sandbox_dataset_prefix="test",
                ),
                call(
                    fs=self.mock_gcsfs,
                    path=gcs_files[1].path,
                    raw_file_config=raw_file_config,
                    pre_import_norm_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
                    sandbox_dataset_prefix="test",
                ),
            ]
        )

    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import._validate_headers"
    )
    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import._do_pre_import_normalization"
    )
    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import.DirectIngestRawFileLoadManager"
    )
    def test_import_bq_metadata_to_sandbox_simple(
        self,
        load_mock: MagicMock,
        pre_import_mock: MagicMock,
        headers_mock: MagicMock,
    ) -> None:
        gcs_files = [
            RawGCSFileMetadata(
                path=GcsfsFilePath.from_absolute_path(
                    "gs://test/unprocessed_2024-01-25T16:35:33:617135_raw_tagMoreBasicData-0.csv"
                ),
                gcs_file_id=1,
                file_id=1,
            )
        ]

        bq_metadata = RawBigQueryFileMetadata.from_gcs_files(gcs_files)

        headers_mock.return_value = ["col", "col2", "col3"]
        load_mock().load_and_prep_paths.return_value = AppendReadyFile(
            import_ready_file=create_autospec(ImportReadyFile),
            append_ready_table_address=create_autospec(BigQueryAddress),
            raw_rows_count=1,
        )
        load_mock().append_to_raw_data_table.return_value = AppendSummary(
            file_id=1, historical_diffs_active=False
        )

        _import_bq_metadata_to_sandbox(
            fs=self.mock_gcsfs,
            bq_client=self.mock_big_query_client,
            region_config=self.region_raw_file_config,
            bq_metadata=bq_metadata,
            sandbox_dataset_prefix="test",
            infer_schema_from_csv=False,
            skip_blocking_validations=False,
            persist_intermediary_tables=False,
            skip_raw_data_migrations=False,
        )

        headers_mock.assert_called_once()
        pre_import_mock.assert_not_called()
        load_mock().load_and_prep_paths.assert_called_once()
        load_mock().append_to_raw_data_table.assert_called_once()

    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import._validate_headers"
    )
    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import._do_pre_import_normalization"
    )
    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import.DirectIngestRawFileLoadManager"
    )
    def test_import_bq_metadata_to_sandbox_chunked(
        self,
        load_mock: MagicMock,
        pre_import_mock: MagicMock,
        headers_mock: MagicMock,
    ) -> None:
        gcs_files = [
            RawGCSFileMetadata(
                path=GcsfsFilePath.from_absolute_path(
                    "gs://test/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-1.csv"
                ),
                gcs_file_id=1,
                file_id=1,
            ),
            RawGCSFileMetadata(
                path=GcsfsFilePath.from_absolute_path(
                    "gs://test/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-2.csv"
                ),
                gcs_file_id=2,
                file_id=1,
            ),
            RawGCSFileMetadata(
                path=GcsfsFilePath.from_absolute_path(
                    "gs://test/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-3.csv"
                ),
                gcs_file_id=3,
                file_id=1,
            ),
        ]

        bq_metadata = RawBigQueryFileMetadata.from_gcs_files(gcs_files)

        headers_mock.return_value = ["col", "col2", "col3"]
        load_mock().load_and_prep_paths.return_value = AppendReadyFile(
            import_ready_file=create_autospec(ImportReadyFile),
            append_ready_table_address=create_autospec(BigQueryAddress),
            raw_rows_count=1,
        )
        load_mock().append_to_raw_data_table.return_value = AppendSummary(
            file_id=1, historical_diffs_active=False
        )

        _import_bq_metadata_to_sandbox(
            fs=self.mock_gcsfs,
            bq_client=self.mock_big_query_client,
            region_config=self.region_raw_file_config,
            bq_metadata=bq_metadata,
            sandbox_dataset_prefix="test",
            infer_schema_from_csv=False,
            skip_blocking_validations=False,
            persist_intermediary_tables=False,
            skip_raw_data_migrations=False,
        )

        headers_mock.assert_called_once()
        pre_import_mock.assert_called_once()
        load_mock().load_and_prep_paths.assert_called_once()
        load_mock().append_to_raw_data_table.assert_called_once()

    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import._validate_headers"
    )
    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import._do_pre_import_normalization"
    )
    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import.DirectIngestRawFileLoadManager"
    )
    def test_import_bq_metadata_to_sandbox_pre_import(
        self,
        load_mock: MagicMock,
        pre_import_mock: MagicMock,
        headers_mock: MagicMock,
    ) -> None:
        gcs_files = [
            RawGCSFileMetadata(
                path=GcsfsFilePath.from_absolute_path(
                    "gs://test/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv"
                ),
                gcs_file_id=1,
                file_id=1,
            )
        ]

        bq_metadata = RawBigQueryFileMetadata.from_gcs_files(gcs_files)

        headers_mock.return_value = ["col", "col2", "col3"]
        load_mock().load_and_prep_paths.return_value = AppendReadyFile(
            import_ready_file=create_autospec(ImportReadyFile),
            append_ready_table_address=create_autospec(BigQueryAddress),
            raw_rows_count=1,
        )
        load_mock().append_to_raw_data_table.return_value = AppendSummary(
            file_id=1, historical_diffs_active=False
        )

        _import_bq_metadata_to_sandbox(
            fs=self.mock_gcsfs,
            bq_client=self.mock_big_query_client,
            region_config=self.region_raw_file_config,
            bq_metadata=bq_metadata,
            sandbox_dataset_prefix="test",
            infer_schema_from_csv=False,
            skip_blocking_validations=False,
            persist_intermediary_tables=False,
            skip_raw_data_migrations=False,
        )

        headers_mock.assert_called_once()
        pre_import_mock.assert_called_once()
        load_mock().load_and_prep_paths.assert_called_once()
        load_mock().append_to_raw_data_table.assert_called_once()

    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import._validate_headers"
    )
    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import._do_pre_import_normalization"
    )
    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import.DirectIngestRawFileLoadManager"
    )
    def test_import_bq_metadata_to_infer_columns(
        self,
        load_mock: MagicMock,
        pre_import_mock: MagicMock,
        headers_mock: MagicMock,
    ) -> None:
        gcs_files = [
            RawGCSFileMetadata(
                path=GcsfsFilePath.from_absolute_path(
                    "gs://test/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv"
                ),
                gcs_file_id=1,
                file_id=1,
            )
        ]

        bq_metadata = RawBigQueryFileMetadata.from_gcs_files(gcs_files)

        headers_mock.return_value = ["col", "col2", "col3"]
        load_mock().load_and_prep_paths.return_value = AppendReadyFile(
            import_ready_file=create_autospec(ImportReadyFile),
            append_ready_table_address=create_autospec(BigQueryAddress),
            raw_rows_count=1,
        )
        load_mock().append_to_raw_data_table.return_value = AppendSummary(
            file_id=1, historical_diffs_active=False
        )

        _import_bq_metadata_to_sandbox(
            fs=self.mock_gcsfs,
            bq_client=self.mock_big_query_client,
            region_config=self.region_raw_file_config,
            bq_metadata=bq_metadata,
            sandbox_dataset_prefix="test",
            infer_schema_from_csv=True,
            skip_blocking_validations=False,
            persist_intermediary_tables=False,
            skip_raw_data_migrations=False,
        )

        headers_mock.assert_called_once()
        pre_import_mock.assert_called_once()
        load_mock().load_and_prep_paths.assert_called_once()
        load_mock().append_to_raw_data_table.assert_has_calls(
            [call(ANY, persist_intermediary_tables=True)]
        )

    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import._validate_headers"
    )
    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import._do_pre_import_normalization"
    )
    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import.DirectIngestRawFileLoadManager"
    )
    def test_import_bq_metadata_persist_tables(
        self,
        load_mock: MagicMock,
        pre_import_mock: MagicMock,
        headers_mock: MagicMock,
    ) -> None:
        gcs_files = [
            RawGCSFileMetadata(
                path=GcsfsFilePath.from_absolute_path(
                    "gs://test/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv"
                ),
                gcs_file_id=1,
                file_id=1,
            )
        ]

        bq_metadata = RawBigQueryFileMetadata.from_gcs_files(gcs_files)

        headers_mock.return_value = ["col", "col2", "col3"]
        load_mock().load_and_prep_paths.return_value = AppendReadyFile(
            import_ready_file=create_autospec(ImportReadyFile),
            append_ready_table_address=create_autospec(BigQueryAddress),
            raw_rows_count=1,
        )
        load_mock().append_to_raw_data_table.return_value = AppendSummary(
            file_id=1, historical_diffs_active=False
        )

        _import_bq_metadata_to_sandbox(
            fs=self.mock_gcsfs,
            bq_client=self.mock_big_query_client,
            region_config=self.region_raw_file_config,
            bq_metadata=bq_metadata,
            sandbox_dataset_prefix="test",
            infer_schema_from_csv=False,
            skip_blocking_validations=False,
            persist_intermediary_tables=True,
            skip_raw_data_migrations=False,
        )

        headers_mock.assert_called_once()
        pre_import_mock.assert_called_once()
        load_mock().load_and_prep_paths.assert_has_calls(
            [
                call(
                    ANY,
                    temp_table_prefix="test",
                    skip_blocking_validations=False,
                    persist_intermediary_tables=True,
                    skip_raw_data_migrations=False,
                )
            ]
        )
        load_mock().append_to_raw_data_table.assert_has_calls(
            [call(ANY, persist_intermediary_tables=True)]
        )

    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import._validate_headers"
    )
    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import._do_pre_import_normalization"
    )
    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_sandbox_import.DirectIngestRawFileLoadManager"
    )
    def test_import_bq_metadata_skip_migrations(
        self,
        load_mock: MagicMock,
        pre_import_mock: MagicMock,
        headers_mock: MagicMock,
    ) -> None:
        gcs_files = [
            RawGCSFileMetadata(
                path=GcsfsFilePath.from_absolute_path(
                    "gs://test/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv"
                ),
                gcs_file_id=1,
                file_id=1,
            )
        ]

        bq_metadata = RawBigQueryFileMetadata.from_gcs_files(gcs_files)

        headers_mock.return_value = ["col", "col2", "col3"]
        load_mock().load_and_prep_paths.return_value = AppendReadyFile(
            import_ready_file=create_autospec(ImportReadyFile),
            append_ready_table_address=create_autospec(BigQueryAddress),
            raw_rows_count=1,
        )
        load_mock().append_to_raw_data_table.return_value = AppendSummary(
            file_id=1, historical_diffs_active=False
        )

        _import_bq_metadata_to_sandbox(
            fs=self.mock_gcsfs,
            bq_client=self.mock_big_query_client,
            region_config=self.region_raw_file_config,
            bq_metadata=bq_metadata,
            sandbox_dataset_prefix="test",
            infer_schema_from_csv=False,
            skip_blocking_validations=False,
            persist_intermediary_tables=False,
            skip_raw_data_migrations=True,
        )

        headers_mock.assert_called_once()
        pre_import_mock.assert_called_once()
        load_mock().load_and_prep_paths.assert_has_calls(
            [
                call(
                    ANY,
                    temp_table_prefix="test",
                    skip_blocking_validations=False,
                    persist_intermediary_tables=False,
                    skip_raw_data_migrations=True,
                )
            ]
        )
        load_mock().append_to_raw_data_table.assert_has_calls(
            [call(ANY, persist_intermediary_tables=False)]
        )
