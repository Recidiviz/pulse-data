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
"""Tests for raw_data_import_types.py"""
import datetime
import unittest
from typing import Any, Type

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import CsvChunkBoundary
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.csv import (
    DEFAULT_CSV_ENCODING,
    DEFAULT_CSV_LINE_TERMINATOR,
)
from recidiviz.common.constants.operations.direct_ingest_raw_data_import_session import (
    DirectIngestRawDataImportSessionStatus,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawDataClassification,
    RawDataFileUpdateCadence,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    AppendReadyFile,
    AppendReadyFileBatch,
    AppendSummary,
    ImportReadyFile,
    PreImportNormalizationType,
    PreImportNormalizedCsvChunkResult,
    PreImportNormalizedFileResult,
    RawBigQueryFileImportSummary,
    RawBigQueryFileMetadata,
    RawBigQueryFileProcessedTime,
    RawDataAppendImportError,
    RawFileLoadAndPrepError,
    RawFileProcessingError,
    RawGCSFileMetadata,
    RequiresPreImportNormalizationFile,
    RequiresPreImportNormalizationFileChunk,
)
from recidiviz.utils.airflow_types import BatchedTaskInstanceOutput


class PreImportNormalizationTypeTest(unittest.TestCase):
    """Tests for PreImportNormalizationType.required_pre_import_normalization_type"""

    def setUp(self) -> None:
        self.sparse_config = DirectIngestRawFileConfig(
            file_tag="myFile",
            file_path="/path/to/myFile.yaml",
            file_description="This is a raw data file",
            data_classification=RawDataClassification.SOURCE,
            columns=[],
            custom_line_terminator=DEFAULT_CSV_LINE_TERMINATOR,
            primary_key_cols=[],
            supplemental_order_by_clause="",
            encoding=DEFAULT_CSV_ENCODING,
            separator=",",
            ignore_quotes=False,
            always_historical_export=True,
            no_valid_primary_keys=False,
            import_chunk_size_rows=10,
            infer_columns_from_config=False,
            table_relationships=[],
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
        )

    def test_no_line_term(self) -> None:
        test_config = attr.evolve(self.sparse_config, custom_line_terminator=None)
        assert (
            PreImportNormalizationType.required_pre_import_normalization_type(
                test_config
            )
            is None
        )

    def test_all_these_encodings_work(self) -> None:
        for encoding in ["uTf-8", "latin-1", "cp819", "UTF_16-BE", "utF-32be"]:
            test_config = attr.evolve(self.sparse_config, encoding=encoding)
            assert (
                PreImportNormalizationType.required_pre_import_normalization_type(
                    test_config
                )
                is None
            )

    def test_all_these_encodings_require_translation(self) -> None:
        for encoding in ["WINDOWS-1252", "cp1252", "US-ASCII"]:
            test_config = attr.evolve(self.sparse_config, encoding=encoding)
            assert (
                PreImportNormalizationType.required_pre_import_normalization_type(
                    test_config
                )
                == PreImportNormalizationType.ENCODING_UPDATE_ONLY
            )

    def test_all_of_these_line_term_require_normalization(self) -> None:
        for line_term in ["†", "†\n", "‡", "‡\n", "n", ","]:
            test_config = attr.evolve(
                self.sparse_config, custom_line_terminator=line_term
            )
            assert (
                PreImportNormalizationType.required_pre_import_normalization_type(
                    test_config
                )
                == PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE
            )

    def test_none_of_these_sep_require_normalization(self) -> None:
        for line_term in [",", "|", "\t", "?"]:
            test_config = attr.evolve(self.sparse_config, separator=line_term)
            assert (
                PreImportNormalizationType.required_pre_import_normalization_type(
                    test_config
                )
                is None
            )

    def test_two_byte_sep(self) -> None:
        # ‡ is 2 bytes in utf-8
        test_config = attr.evolve(self.sparse_config, separator="‡", encoding="UTF-8")
        assert (
            PreImportNormalizationType.required_pre_import_normalization_type(
                test_config
            )
            == PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE
        )

    def test_two_byte_sep_ii(self) -> None:
        # ‡ is only 1 bytes in windows-1252 but since we have to convert it to utf-8
        # we want to make sure we measure it's size in utf-8 (which is 2 bytes)
        test_config = attr.evolve(
            self.sparse_config, separator="‡", encoding="WINDOWS-1252"
        )
        assert (
            PreImportNormalizationType.required_pre_import_normalization_type(
                test_config
            )
            == PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE
        )


class TestSerialization(unittest.TestCase):
    """Test serialization and deserialization methods for raw data import types"""

    def test_raw_file_processing_error(self) -> None:
        no_temp = RawFileProcessingError(
            original_file_path=GcsfsFilePath.from_absolute_path("path/to/file.csv"),
            temporary_file_paths=None,
            error_msg="oooooo!",
        )
        self._validate_serialization(no_temp, RawFileProcessingError)
        with_temp = attr.evolve(
            no_temp,
            temporary_file_paths=[
                GcsfsFilePath.from_absolute_path("path/to/file_one.csv"),
                GcsfsFilePath.from_absolute_path("path/to/file_two.csv"),
            ],
        )
        self._validate_serialization(with_temp, RawFileProcessingError)

    def test_raw_file_load_and_prep_error(self) -> None:
        no_temp = RawFileLoadAndPrepError(
            file_id=1,
            file_tag="tag",
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
            original_file_paths=[
                GcsfsFilePath.from_absolute_path("path/to/file_one.csv"),
                GcsfsFilePath.from_absolute_path("path/to/file_two.csv"),
            ],
            pre_import_normalized_file_paths=None,
            error_msg="aaaaaaaaa",
            temp_table=BigQueryAddress(dataset_id="d", table_id="t"),
        )

        self._validate_serialization(no_temp, RawFileLoadAndPrepError)
        with_temp = attr.evolve(
            no_temp,
            pre_import_normalized_file_paths=[
                GcsfsFilePath.from_absolute_path("path/to/file_one.csv"),
                GcsfsFilePath.from_absolute_path("path/to/file_two.csv"),
            ],
        )
        self._validate_serialization(with_temp, RawFileLoadAndPrepError)

    def test_raw_data_import_append_error(self) -> None:
        error = RawDataAppendImportError(
            file_id=1,
            raw_temp_table=BigQueryAddress(dataset_id="data", table_id="set"),
            error_msg="eeeeeeeee",
        )

        self._validate_serialization(error, RawDataAppendImportError)

    def test_requires_pre_import_normalization_file_chunk(self) -> None:
        chunk_boundary = CsvChunkBoundary(
            start_inclusive=0, end_exclusive=100, chunk_num=0
        )
        original = RequiresPreImportNormalizationFileChunk(
            path=GcsfsFilePath.from_absolute_path("path/to/file.csv"),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=chunk_boundary,
            headers=["id", "name", "age"],
        )

        self._validate_serialization(original, RequiresPreImportNormalizationFileChunk)

    def test_requires_pre_import_normalization_file(self) -> None:
        chunk_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=100, chunk_num=0),
            CsvChunkBoundary(start_inclusive=100, end_exclusive=200, chunk_num=1),
        ]
        original = RequiresPreImportNormalizationFile(
            path=GcsfsFilePath.from_absolute_path("path/to/file.csv"),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundaries=chunk_boundaries,
            headers=["id", "name", "age"],
        )

        self._validate_serialization(original, RequiresPreImportNormalizationFile)

    def test_normalized_csv_chunk_result(self) -> None:
        chunk_boundary = CsvChunkBoundary(
            start_inclusive=0, end_exclusive=100, chunk_num=0
        )
        original = PreImportNormalizedCsvChunkResult(
            input_file_path=GcsfsFilePath.from_absolute_path("path/to/file.csv"),
            output_file_path=GcsfsFilePath.from_absolute_path("path/to/result.csv"),
            chunk_boundary=chunk_boundary,
            crc32c=0xFFFFFFFF,
        )

        self._validate_serialization(original, PreImportNormalizedCsvChunkResult)

    def test_import_ready_normalized_file(self) -> None:
        original = PreImportNormalizedFileResult(
            input_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file"),
            output_file_paths=[GcsfsFilePath.from_absolute_path("temp_bucket/file_0")],
        )

        self._validate_serialization(original, PreImportNormalizedFileResult)

    def test_import_ready_file(self) -> None:
        original = ImportReadyFile(
            file_id=1,
            file_tag="fake_tag",
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
            pre_import_normalized_file_paths=[
                GcsfsFilePath(bucket_name="bucket_temp", blob_name="blob.csv")
            ],
            original_file_paths=[
                GcsfsFilePath(bucket_name="bucket", blob_name="blob.csv")
            ],
        )
        self._validate_serialization(original, ImportReadyFile)

    def test_append_ready_file(self) -> None:
        original = AppendReadyFile(
            import_ready_file=ImportReadyFile(
                file_id=1,
                file_tag="fake_tag",
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
                pre_import_normalized_file_paths=[
                    GcsfsFilePath(bucket_name="bucket_temp", blob_name="blob.csv")
                ],
                original_file_paths=[
                    GcsfsFilePath(bucket_name="bucket", blob_name="blob.csv")
                ],
            ),
            append_ready_table_address=BigQueryAddress(
                dataset_id="dataset", table_id="table"
            ),
            raw_rows_count=3,
        )
        self._validate_serialization(original, AppendReadyFile)

    def test_append_summary(self) -> None:
        original = AppendSummary(
            file_id=1,
            net_new_or_updated_rows=2,
            deleted_rows=3,
            historical_diffs_active=True,
        )
        self._validate_serialization(original, AppendSummary)

    def test_append_ready_file_batches(self) -> None:
        original = AppendReadyFileBatch(
            append_ready_files_by_tag={
                "tag_a": [
                    AppendReadyFile(
                        import_ready_file=ImportReadyFile(
                            file_id=1,
                            file_tag="tag_a",
                            update_datetime=datetime.datetime(
                                2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                            ),
                            pre_import_normalized_file_paths=[
                                GcsfsFilePath(
                                    bucket_name="bucket_temp", blob_name="blob1.csv"
                                )
                            ],
                            original_file_paths=[
                                GcsfsFilePath(
                                    bucket_name="bucket", blob_name="blob.csv"
                                )
                            ],
                        ),
                        append_ready_table_address=BigQueryAddress(
                            dataset_id="dataset", table_id="table1"
                        ),
                        raw_rows_count=3,
                    ),
                    AppendReadyFile(
                        import_ready_file=ImportReadyFile(
                            file_id=2,
                            file_tag="tag_a",
                            update_datetime=datetime.datetime(
                                2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                            ),
                            pre_import_normalized_file_paths=[
                                GcsfsFilePath(
                                    bucket_name="bucket_temp", blob_name="blob2.csv"
                                )
                            ],
                            original_file_paths=[
                                GcsfsFilePath(
                                    bucket_name="bucket", blob_name="blob2.csv"
                                )
                            ],
                        ),
                        append_ready_table_address=BigQueryAddress(
                            dataset_id="dataset", table_id="table2"
                        ),
                        raw_rows_count=3,
                    ),
                ],
                "tag_b": [
                    AppendReadyFile(
                        import_ready_file=ImportReadyFile(
                            file_id=3,
                            file_tag="tag_b",
                            update_datetime=datetime.datetime(
                                2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                            ),
                            original_file_paths=[
                                GcsfsFilePath(
                                    bucket_name="bucket", blob_name="blob3.csv"
                                )
                            ],
                            pre_import_normalized_file_paths=None,
                        ),
                        append_ready_table_address=BigQueryAddress(
                            dataset_id="dataset", table_id="table3"
                        ),
                        raw_rows_count=3,
                    ),
                ],
            }
        )
        self._validate_serialization(original, AppendReadyFileBatch)

    def test_raw_gcs_file_metadata(self) -> None:
        original = RawGCSFileMetadata(
            gcs_file_id=1,
            file_id=2,
            path=GcsfsFilePath(bucket_name="bucket", blob_name="blob.csv"),
        )
        self._validate_serialization(original, RawGCSFileMetadata)
        original_two = RawGCSFileMetadata(
            gcs_file_id=1,
            file_id=None,
            path=GcsfsFilePath(bucket_name="bucket", blob_name="blob.csv"),
        )
        self._validate_serialization(original_two, RawGCSFileMetadata)

    def test_raw_bq_file_metadata(self) -> None:
        original = RawBigQueryFileMetadata(
            file_tag="tag1",
            file_id=1,
            gcs_files=[
                RawGCSFileMetadata(
                    gcs_file_id=1,
                    file_id=1,
                    path=GcsfsFilePath(bucket_name="bucket", blob_name="blob.csv"),
                )
            ],
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
        )
        self._validate_serialization(original, RawBigQueryFileMetadata)

    def test_import_summary(self) -> None:
        original = RawBigQueryFileImportSummary(
            file_id=1,
            import_status=DirectIngestRawDataImportSessionStatus.FAILED_UNKNOWN,
            raw_rows=1,
            historical_diffs_active=True,
            net_new_or_updated_rows=None,
            deleted_rows=1,
        )
        self._validate_serialization(original, RawBigQueryFileImportSummary)

    def test_update_ready_metadata(self) -> None:
        original = RawBigQueryFileProcessedTime(
            file_id=1,
            file_processed_time=datetime.datetime(
                2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
            ),
        )
        self._validate_serialization(original, RawBigQueryFileProcessedTime)

    def test_task_result(self) -> None:
        result = PreImportNormalizedFileResult(
            input_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file"),
            output_file_paths=[GcsfsFilePath.from_absolute_path("temp_bucket/file_0")],
        )
        original = BatchedTaskInstanceOutput[
            PreImportNormalizedFileResult, RawFileProcessingError
        ](results=[result], errors=[])

        serialized = original.serialize()
        deserialized = BatchedTaskInstanceOutput.deserialize(
            json_str=serialized,
            result_cls=PreImportNormalizedFileResult,
            error_cls=RawFileProcessingError,
        )

        self.assertEqual(original, deserialized)

    def _validate_serialization(self, obj: Any, obj_type: Type) -> None:
        serialized = obj.serialize()
        deserialized = obj_type.deserialize(serialized)

        self.assertEqual(obj, deserialized)
