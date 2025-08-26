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
import re
from typing import Dict, List
from unittest import TestCase
from unittest.mock import MagicMock, patch

import attrs
from more_itertools import one

from recidiviz.airflow.dags.raw_data.concurrency_utils import MAX_BQ_LOAD_AIRFLOW_TASKS
from recidiviz.airflow.dags.raw_data.file_metadata_tasks import (
    _build_file_imports_for_errors,
    _build_file_imports_for_results,
    _reconcile_file_imports_and_bq_metadata,
    coalesce_import_ready_files,
    coalesce_results_and_errors,
    get_files_to_import_this_run,
    split_by_pre_import_normalization_type,
)
from recidiviz.airflow.dags.raw_data.metadata import (
    APPEND_READY_FILE_BATCHES,
    BQ_METADATA_TO_IMPORT_IN_FUTURE_RUNS,
    BQ_METADATA_TO_IMPORT_THIS_RUN,
    FILE_IMPORTS,
    HAS_FILE_IMPORT_ERRORS,
    IMPORT_READY_FILES,
    PROCESSED_PATHS_TO_RENAME,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_METADATA,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_SCHEMA,
    SKIPPED_FILE_ERRORS,
)
from recidiviz.airflow.dags.raw_data.utils import get_direct_ingest_region_raw_config
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    AppendReadyFile,
    AppendReadyFileBatch,
    AppendSummary,
    ImportReadyFile,
    RawBigQueryFileMetadata,
    RawDataAppendImportError,
    RawFileBigQueryLoadConfig,
    RawFileImport,
    RawFileLoadAndPrepError,
    RawFileProcessingError,
    RawGCSFileMetadata,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.utils.airflow_types import BatchedTaskInstanceOutput
from recidiviz.utils.types import assert_type


class SplitByPreImportNormalizationTest(TestCase):
    """Unit tests for split_by_pre_import_normalization_type"""

    def setUp(self) -> None:
        self.region_module_patch = patch(
            "recidiviz.airflow.dags.raw_data.utils.direct_ingest_regions_module",
            fake_regions,
        )
        self.region_module_patch.start()
        super().setUp()

    def tearDown(self) -> None:
        self.region_module_patch.stop()
        super().tearDown()

    def test_no_files(self) -> None:
        results = split_by_pre_import_normalization_type.function("US_XX", [], {})
        assert results[IMPORT_READY_FILES] == []
        assert results[REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_METADATA] == []
        assert results[REQUIRES_PRE_IMPORT_NORMALIZATION_FILES] == []

    def test_splits_output_correctly(self) -> None:
        inputs = [
            RawBigQueryFileMetadata(
                file_id=1,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath(bucket_name="bucket", blob_name="blob.csv"),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=2,
                file_tag="tagCustomLineTerminatorNonUTF8",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=2,
                        file_id=2,
                        path=GcsfsFilePath(
                            bucket_name="bucket", blob_name="blob1_1.csv"
                        ),
                    ),
                    RawGCSFileMetadata(
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
        file_ids_to_headers: Dict[str, List[str]] = {
            "1": ["col1"],
            "2": ["col1a"],
        }
        results = split_by_pre_import_normalization_type.function(
            "US_XX", [i.serialize() for i in inputs], file_ids_to_headers
        )

        assert [
            RawBigQueryFileMetadata.deserialize(r)
            for r in results[REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_METADATA]
        ] == inputs[1:]
        assert {
            GcsfsFilePath.from_absolute_path(path)
            for path in results[REQUIRES_PRE_IMPORT_NORMALIZATION_FILES]
        } == {gcs_file.path for gcs_file in inputs[1].gcs_files}
        assert [
            ImportReadyFile.deserialize(r) for r in results[IMPORT_READY_FILES]
        ] == [
            ImportReadyFile.from_bq_metadata_and_load_config(
                inputs[0],
                RawFileBigQueryLoadConfig(
                    schema_fields=[
                        RawDataTableBigQuerySchemaBuilder.raw_file_column_as_bq_field(
                            column="col1", description=""
                        )
                    ],
                    skip_leading_rows=1,
                ),
            ),
        ]
        assert {
            file_id: RawFileBigQueryLoadConfig.deserialize(schema)
            for file_id, schema in results[
                REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_SCHEMA
            ].items()
        } == {
            2: RawFileBigQueryLoadConfig(
                schema_fields=[
                    RawDataTableBigQuerySchemaBuilder.raw_file_column_as_bq_field(
                        column="col1a", description=""
                    )
                ],
                skip_leading_rows=0,
            )
        }

    def test_skips_files_missing_headers(self) -> None:
        inputs = [
            RawBigQueryFileMetadata(
                file_id=1,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath(bucket_name="bucket", blob_name="blob.csv"),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=2,
                file_tag="tagCustomLineTerminatorNonUTF8",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=2,
                        file_id=2,
                        path=GcsfsFilePath(
                            bucket_name="bucket", blob_name="blob1_1.csv"
                        ),
                    ),
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=3,
                file_tag="tagCustomLineTerminatorNonUTF8",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=2,
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
        file_paths_to_headers: Dict[str, List[str]] = {
            "1": ["col1"],
            "2": ["col1a"],
        }
        results = split_by_pre_import_normalization_type.function(
            "US_XX", [i.serialize() for i in inputs], file_paths_to_headers
        )

        assert [
            RawBigQueryFileMetadata.deserialize(r)
            for r in results[REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_METADATA]
        ] == inputs[1:2]
        assert {
            GcsfsFilePath.from_absolute_path(path)
            for path in results[REQUIRES_PRE_IMPORT_NORMALIZATION_FILES]
        } == {gcs_file.path for gcs_file in inputs[1].gcs_files}
        assert [
            ImportReadyFile.deserialize(r) for r in results[IMPORT_READY_FILES]
        ] == [
            ImportReadyFile.from_bq_metadata_and_load_config(
                inputs[0],
                RawFileBigQueryLoadConfig(
                    schema_fields=[
                        RawDataTableBigQuerySchemaBuilder.raw_file_column_as_bq_field(
                            column="col1", description=""
                        )
                    ],
                    skip_leading_rows=1,
                ),
            ),
        ]
        assert {
            file_id: RawFileBigQueryLoadConfig.deserialize(schema)
            for file_id, schema in results[
                REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_SCHEMA
            ].items()
        } == {
            2: RawFileBigQueryLoadConfig(
                schema_fields=[
                    RawDataTableBigQuerySchemaBuilder.raw_file_column_as_bq_field(
                        column="col1a", description=""
                    )
                ],
                skip_leading_rows=0,
            )
        }


class GetFilesToImportThisRunTest(TestCase):
    """Tests for get_files_to_import_this_run task"""

    def setUp(self) -> None:
        fs = MagicMock()
        fs.get_file_size.side_effect = (
            lambda x: int(x.abs_path().split("_")[-1]) * 1024 * 1024 * 100
        )
        self.fs_patcher = patch(
            "recidiviz.airflow.dags.raw_data.file_metadata_tasks.GcsfsFactory.build",
            return_value=fs,
        ).start()

    def tearDown(self) -> None:
        self.fs_patcher.stop()

    def test_empty(self) -> None:
        assert get_files_to_import_this_run.function(
            raw_data_instance=DirectIngestInstance.PRIMARY, serialized_bq_metadata=[]
        ) == {
            BQ_METADATA_TO_IMPORT_THIS_RUN: [],
            BQ_METADATA_TO_IMPORT_IN_FUTURE_RUNS: [],
        }

    def test_less_than_max(self) -> None:
        oldest = RawBigQueryFileMetadata(
            file_id=1,
            file_tag="tagBasicData",
            gcs_files=[
                RawGCSFileMetadata(
                    gcs_file_id=1,
                    file_id=1,
                    path=GcsfsFilePath(bucket_name="bucket", blob_name="blob_1"),
                )
            ],
            update_datetime=datetime.datetime(2024, 1, 3, 1, 1, 1, tzinfo=datetime.UTC),
        ).serialize()

        newest = RawBigQueryFileMetadata(
            file_id=3,
            file_tag="tagCustomLineTerminatorNonUTF8",
            gcs_files=[
                RawGCSFileMetadata(
                    gcs_file_id=2,
                    file_id=2,
                    path=GcsfsFilePath(bucket_name="bucket", blob_name="blob_1"),
                ),
            ],
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
        ).serialize()

        bq_metadata = [newest, oldest]

        result = get_files_to_import_this_run.function(
            raw_data_instance=DirectIngestInstance.PRIMARY,
            serialized_bq_metadata=bq_metadata,
        )

        assert len(result[BQ_METADATA_TO_IMPORT_THIS_RUN]) == len(bq_metadata)
        assert set(result[BQ_METADATA_TO_IMPORT_THIS_RUN]) == {oldest, newest}

        assert len(result[BQ_METADATA_TO_IMPORT_IN_FUTURE_RUNS]) == 0
        assert set(result[BQ_METADATA_TO_IMPORT_IN_FUTURE_RUNS]) == set()

        result = get_files_to_import_this_run.function(
            raw_data_instance=DirectIngestInstance.SECONDARY,
            serialized_bq_metadata=bq_metadata,
        )

        assert len(result[BQ_METADATA_TO_IMPORT_THIS_RUN]) == len(bq_metadata)
        assert set(result[BQ_METADATA_TO_IMPORT_THIS_RUN]) == {oldest, newest}

        assert len(result[BQ_METADATA_TO_IMPORT_IN_FUTURE_RUNS]) == 0
        assert set(result[BQ_METADATA_TO_IMPORT_IN_FUTURE_RUNS]) == set()

    def test_split_sort(self) -> None:
        file = RawBigQueryFileMetadata(
            file_id=1,
            file_tag="tagBasicData",
            gcs_files=[
                RawGCSFileMetadata(
                    gcs_file_id=1,
                    file_id=1,
                    path=GcsfsFilePath(bucket_name="bucket", blob_name="blob_50"),
                ),
                RawGCSFileMetadata(
                    gcs_file_id=1,
                    file_id=1,
                    path=GcsfsFilePath(bucket_name="bucket", blob_name="blob_50"),
                ),
            ],
            update_datetime=datetime.datetime(1900, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
        )

        bq_metadata = list(
            reversed(
                [
                    attrs.evolve(
                        file,
                        update_datetime=datetime.datetime(
                            1900 + i, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                        ),
                    ).serialize()
                    for i in range(500)
                ]
            )
        )

        result = get_files_to_import_this_run.function(
            raw_data_instance=DirectIngestInstance.PRIMARY,
            serialized_bq_metadata=bq_metadata,
        )

        assert len(result[BQ_METADATA_TO_IMPORT_THIS_RUN]) == len(bq_metadata)
        assert len(result[BQ_METADATA_TO_IMPORT_IN_FUTURE_RUNS]) == 0

        result = get_files_to_import_this_run.function(
            raw_data_instance=DirectIngestInstance.SECONDARY,
            serialized_bq_metadata=bq_metadata,
        )

        assert len(result[BQ_METADATA_TO_IMPORT_THIS_RUN]) == 300

        result_years = {
            RawBigQueryFileMetadata.deserialize(bq_str).update_datetime.year
            for bq_str in result[BQ_METADATA_TO_IMPORT_THIS_RUN]
        }
        assert result_years == set(range(1900, 1900 + 300))

        # should be 1 of the older file and 1 of the newer file
        assert len(result[BQ_METADATA_TO_IMPORT_IN_FUTURE_RUNS]) == 200
        defer_years = {
            RawBigQueryFileMetadata.deserialize(bq_str).update_datetime.year
            for bq_str in result[BQ_METADATA_TO_IMPORT_IN_FUTURE_RUNS]
        }
        assert defer_years == set(range(1900 + 300, 1900 + 500))


class CoalesceImportReadyFilesTest(TestCase):
    """Tests for coalesce_import_ready_files task"""

    def test_empty(self) -> None:
        assert not coalesce_import_ready_files.function(
            [], '{"results": [], "errors": []}'
        )

    def test_pre_import_skipped(self) -> None:
        assert not coalesce_import_ready_files.function([], None)

    def test_failed_upstream(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "If there are no import ready files, we expect an empty list; however since"
                "we found None, that means the task failed to run upstream so let's fail "
                "loud."
            ),
        ):
            coalesce_import_ready_files.function(None, None)

    def test_single_per_batch(self) -> None:
        files = [
            ImportReadyFile(
                file_id=1,
                file_tag="tag",
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
                original_file_paths=[GcsfsFilePath.from_absolute_path("test/123.csv")],
                pre_import_normalized_file_paths=None,
                bq_load_config=RawFileBigQueryLoadConfig(
                    schema_fields=[], skip_leading_rows=1
                ),
            ),
            ImportReadyFile(
                file_id=1,
                file_tag="tag",
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
                original_file_paths=[GcsfsFilePath.from_absolute_path("test/123.csv")],
                pre_import_normalized_file_paths=[
                    GcsfsFilePath.from_absolute_path("test/456.csv")
                ],
                bq_load_config=RawFileBigQueryLoadConfig(
                    schema_fields=[], skip_leading_rows=1
                ),
            ),
        ]

        output = BatchedTaskInstanceOutput[ImportReadyFile, RawFileProcessingError](
            errors=[], results=files
        )

        serialized_files = [f.serialize() for f in files]

        assert coalesce_import_ready_files.function(
            serialized_files, output.serialize()
        ) == [[f] for f in [*serialized_files, *serialized_files]]

    def test_multiple_per_batch(self) -> None:
        file = ImportReadyFile(
            file_id=1,
            file_tag="tag",
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
            original_file_paths=[GcsfsFilePath.from_absolute_path("test/123.csv")],
            pre_import_normalized_file_paths=None,
            bq_load_config=RawFileBigQueryLoadConfig(
                schema_fields=[], skip_leading_rows=1
            ),
        ).serialize()

        output = BatchedTaskInstanceOutput[ImportReadyFile, RawFileProcessingError](
            errors=[], results=[]
        )

        serialized_files = [file for _ in range(MAX_BQ_LOAD_AIRFLOW_TASKS * 3)]

        assert coalesce_import_ready_files.function(
            serialized_files, output.serialize()
        ) == [[file, file, file] for _ in range(MAX_BQ_LOAD_AIRFLOW_TASKS)]


class CoalesceResultsAndErrorsTest(TestCase):
    """Tests for coalesce_results_and_errors"""

    def setUp(self) -> None:
        self.pruning_patch = patch(
            "recidiviz.airflow.dags.raw_data.file_metadata_tasks.automatic_raw_data_pruning_enabled_for_state_and_instance"
        )
        self.pruning_mock = self.pruning_patch.start()
        self.pruning_mock.return_value = False
        self.region_module_patch = patch(
            "recidiviz.airflow.dags.raw_data.utils.direct_ingest_regions_module",
            fake_regions,
        )
        self.region_module_patch.start()

    def tearDown(self) -> None:
        self.pruning_patch.stop()
        self.region_module_patch.stop()

    def test_none(self) -> None:
        serialized_empty_batched_task_instance = BatchedTaskInstanceOutput(
            errors=[], results=[]
        ).serialize()

        assert coalesce_results_and_errors.function(
            region_code="US_XX",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            serialized_bq_metadata=[],
            serialized_header_verification_errors=[],
            serialized_chunking_errors=[],
            serialized_pre_import_normalization_result=serialized_empty_batched_task_instance,
            serialized_load_prep_results=[],
            serialized_append_batches={
                SKIPPED_FILE_ERRORS: [],
                APPEND_READY_FILE_BATCHES: [],
            },
            serialized_append_result=[],
        ) == {
            FILE_IMPORTS: [],
            HAS_FILE_IMPORT_ERRORS: False,
            PROCESSED_PATHS_TO_RENAME: [],
        }
        assert coalesce_results_and_errors.function(
            region_code="US_XX",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            serialized_bq_metadata=None,
            serialized_header_verification_errors=None,
            serialized_chunking_errors=None,
            serialized_pre_import_normalization_result=None,
            serialized_load_prep_results=None,
            serialized_append_batches=None,
            serialized_append_result=None,
        ) == {
            FILE_IMPORTS: [],
            HAS_FILE_IMPORT_ERRORS: False,
            PROCESSED_PATHS_TO_RENAME: [],
        }

    def test_build_file_imports_for_results_all_matching(self) -> None:
        summaries = [
            AppendSummary(file_id=1, historical_diffs_active=False),
            AppendSummary(file_id=2, historical_diffs_active=False),
            AppendSummary(
                file_id=3,
                historical_diffs_active=True,
                net_new_or_updated_rows=1,
                deleted_rows=2,
            ),
        ]
        append_ready_file_batches = [
            AppendReadyFileBatch(
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
                                    GcsfsFilePath(bucket_name="temp", blob_name="a.csv")
                                ],
                                original_file_paths=[
                                    GcsfsFilePath(
                                        bucket_name="original", blob_name="a.csv"
                                    )
                                ],
                                bq_load_config=RawFileBigQueryLoadConfig(
                                    schema_fields=[], skip_leading_rows=1
                                ),
                            ),
                            append_ready_table_address=BigQueryAddress(
                                dataset_id="temp", table_id="tag_a__1"
                            ),
                            raw_rows_count=3,
                        ),
                        AppendReadyFile(
                            import_ready_file=ImportReadyFile(
                                file_id=2,
                                file_tag="tag_a",
                                update_datetime=datetime.datetime(
                                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                                ),
                                pre_import_normalized_file_paths=[
                                    GcsfsFilePath(bucket_name="temp", blob_name="a.csv")
                                ],
                                original_file_paths=[
                                    GcsfsFilePath(
                                        bucket_name="original", blob_name="a.csv"
                                    )
                                ],
                                bq_load_config=RawFileBigQueryLoadConfig(
                                    schema_fields=[], skip_leading_rows=1
                                ),
                            ),
                            append_ready_table_address=BigQueryAddress(
                                dataset_id="temp", table_id="tag_a__2"
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
                                pre_import_normalized_file_paths=[
                                    GcsfsFilePath(bucket_name="temp", blob_name="b.csv")
                                ],
                                original_file_paths=[
                                    GcsfsFilePath(
                                        bucket_name="original", blob_name="b.csv"
                                    )
                                ],
                                bq_load_config=RawFileBigQueryLoadConfig(
                                    schema_fields=[], skip_leading_rows=1
                                ),
                            ),
                            append_ready_table_address=BigQueryAddress(
                                dataset_id="temp", table_id="tag_b__3"
                            ),
                            raw_rows_count=3,
                        ),
                    ],
                }
            )
        ]

        file_imports, files = _build_file_imports_for_results(
            summaries, append_ready_file_batches
        )

        append_ready_files = [
            file
            for batch in append_ready_file_batches[0].append_ready_files_by_tag.values()
            for file in batch
        ]

        for i, summary in enumerate(file_imports.values()):
            assert summary == RawFileImport.from_load_results(
                append_ready_files[i], summaries[i]
            )

        assert set(files) == set(
            path
            for metadata in append_ready_files
            for path in metadata.import_ready_file.original_file_paths
        )

    def test_build_file_imports_for_results_missing(self) -> None:
        summaries = [
            AppendSummary(file_id=1, historical_diffs_active=False),
            AppendSummary(file_id=2, historical_diffs_active=False),
            AppendSummary(
                file_id=4,
                historical_diffs_active=True,
                net_new_or_updated_rows=1,
                deleted_rows=2,
            ),
        ]
        append_ready_file_batches = [
            AppendReadyFileBatch(
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
                                    GcsfsFilePath(bucket_name="temp", blob_name="a.csv")
                                ],
                                original_file_paths=[
                                    GcsfsFilePath(
                                        bucket_name="original", blob_name="a.csv"
                                    )
                                ],
                                bq_load_config=RawFileBigQueryLoadConfig(
                                    schema_fields=[], skip_leading_rows=1
                                ),
                            ),
                            append_ready_table_address=BigQueryAddress(
                                dataset_id="temp", table_id="tag_a__1"
                            ),
                            raw_rows_count=3,
                        ),
                        AppendReadyFile(
                            import_ready_file=ImportReadyFile(
                                file_id=2,
                                file_tag="tag_a",
                                update_datetime=datetime.datetime(
                                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                                ),
                                pre_import_normalized_file_paths=[
                                    GcsfsFilePath(bucket_name="temp", blob_name="a.csv")
                                ],
                                original_file_paths=[
                                    GcsfsFilePath(
                                        bucket_name="original", blob_name="a.csv"
                                    )
                                ],
                                bq_load_config=RawFileBigQueryLoadConfig(
                                    schema_fields=[], skip_leading_rows=1
                                ),
                            ),
                            append_ready_table_address=BigQueryAddress(
                                dataset_id="temp", table_id="tag_a__2"
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
                                pre_import_normalized_file_paths=[
                                    GcsfsFilePath(bucket_name="temp", blob_name="b.csv")
                                ],
                                original_file_paths=[
                                    GcsfsFilePath(
                                        bucket_name="original", blob_name="b.csv"
                                    )
                                ],
                                bq_load_config=RawFileBigQueryLoadConfig(
                                    schema_fields=[], skip_leading_rows=1
                                ),
                            ),
                            append_ready_table_address=BigQueryAddress(
                                dataset_id="temp", table_id="tag_b__3"
                            ),
                            raw_rows_count=3,
                        ),
                    ],
                }
            )
        ]

        file_imports, files = _build_file_imports_for_results(
            summaries, append_ready_file_batches
        )

        append_ready_files = [
            file
            for batch in append_ready_file_batches[0].append_ready_files_by_tag.values()
            for file in batch
        ]

        assert 3 not in file_imports
        assert 4 not in file_imports

        assert len(file_imports) == 2
        for i, summary in enumerate(file_imports.values()):
            assert summary == RawFileImport.from_load_results(
                append_ready_files[i], summaries[i]
            )

        assert set(files) == set(
            path
            for metadata in append_ready_files[:2]
            for path in metadata.import_ready_file.original_file_paths
        )

    def test_build_file_imports_for_errors_bq_errors(self) -> None:
        load_errors = [
            # no temp files, no temp table
            RawFileLoadAndPrepError(
                file_id=1,
                file_tag="tagBasicData",
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
                pre_import_normalized_file_paths=None,
                original_file_paths=[],
                temp_table=None,
                error_msg="yikes!",
            ),
            # temp files
            RawFileLoadAndPrepError(
                file_id=2,
                file_tag="tagBasicData",
                update_datetime=datetime.datetime(
                    2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                ),
                pre_import_normalized_file_paths=[
                    GcsfsFilePath.from_absolute_path("temp/needs-clean.csv")
                ],
                original_file_paths=[],
                temp_table=None,
                error_msg="yikes!",
            ),
            # temp table
            RawFileLoadAndPrepError(
                file_id=3,
                file_tag="tagBasicData",
                update_datetime=datetime.datetime(
                    2024, 1, 3, 1, 1, 1, tzinfo=datetime.UTC
                ),
                pre_import_normalized_file_paths=None,
                original_file_paths=[],
                temp_table=BigQueryAddress.from_str("temp.table"),
                error_msg="yikes!",
            ),
        ]
        append_errors = [
            RawDataAppendImportError(
                file_id=4,
                raw_temp_table=BigQueryAddress.from_str("temp.table2"),
                error_msg="yikes!",
            ),
        ]

        raw_region_config = get_direct_ingest_region_raw_config("US_XX")

        file_imports = _build_file_imports_for_errors(
            raw_region_config,
            DirectIngestInstance.PRIMARY,
            [],
            [],
            load_errors,
            append_errors,
        )

        assert len(file_imports) == 4
        assert (
            one({summary.import_status for summary in file_imports.values()})
            == DirectIngestRawFileImportStatus.FAILED_LOAD_STEP
        )

    def test_build_file_imports_for_errors_processing_errors_single_chunk_fail_all_fail(
        self,
    ) -> None:
        bq_metadata = [
            RawBigQueryFileMetadata(
                file_id=1,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath(bucket_name="bucket", blob_name="blob.csv"),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=2,
                file_tag="tagCustomLineTerminatorNonUTF8",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=2,
                        file_id=2,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag.csv"
                        ),
                    ),
                    RawGCSFileMetadata(
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

        processing_errors = [
            RawFileProcessingError(
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag.csv"
                ),
                temporary_file_paths=[
                    GcsfsFilePath(bucket_name="temp", blob_name="temp_blob1_1_1.csv"),
                    GcsfsFilePath(bucket_name="temp", blob_name="temp_blob1_1_2.csv"),
                ],
                error_msg="yike!",
            )
        ]

        raw_region_config = get_direct_ingest_region_raw_config("US_XX")

        file_imports = _build_file_imports_for_errors(
            raw_region_config,
            DirectIngestInstance.PRIMARY,
            bq_metadata,
            processing_errors,
            [],
            [],
        )

        assert len(file_imports) == 1
        assert 2 in file_imports
        assert (
            file_imports[2].import_status
            == DirectIngestRawFileImportStatus.FAILED_PRE_IMPORT_NORMALIZATION_STEP
        )

    def test_build_file_imports_for_errors_processing_errors_both(self) -> None:
        bq_metadata = [
            RawBigQueryFileMetadata(
                file_id=1,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath(bucket_name="bucket", blob_name="blob.csv"),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=2,
                file_tag="tagCustomLineTerminatorNonUTF8",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=2,
                        file_id=2,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag_1.csv"
                        ),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
        ]

        processing_errors = [
            RawFileProcessingError(
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag_1.csv"
                ),
                temporary_file_paths=[
                    GcsfsFilePath(bucket_name="temp", blob_name="temp_blob1_1_1.csv"),
                ],
                error_msg="yike 1!",
            ),
            RawFileProcessingError(
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag_1.csv"
                ),
                temporary_file_paths=[
                    GcsfsFilePath(bucket_name="temp", blob_name="temp_blob1_1_2.csv"),
                ],
                error_msg="yike 2!",
            ),
            RawFileProcessingError(
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag_1.csv"
                ),
                temporary_file_paths=[
                    GcsfsFilePath(bucket_name="temp", blob_name="temp_blob1_1_3.csv"),
                ],
                error_msg="yike 3!",
            ),
        ]

        raw_region_config = get_direct_ingest_region_raw_config("US_XX")

        file_imports = _build_file_imports_for_errors(
            raw_region_config,
            DirectIngestInstance.PRIMARY,
            bq_metadata,
            processing_errors,
            [],
            [],
        )

        assert len(file_imports) == 1
        assert 2 in file_imports
        assert (
            file_imports[2].import_status
            == DirectIngestRawFileImportStatus.FAILED_PRE_IMPORT_NORMALIZATION_STEP
        )
        assert "yike 1!" in assert_type(file_imports[2].error_message, str)
        assert "yike 2!" in assert_type(file_imports[2].error_message, str)
        assert "yike 3!" in assert_type(file_imports[2].error_message, str)

    def test_build_file_imports_for_errors_processing_pruning_propagated(
        self,
    ) -> None:
        self.pruning_mock.return_value = True
        bq_metadata = [
            RawBigQueryFileMetadata(
                file_id=1,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagBasicData.csv"
                        ),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=2,
                file_tag="tagCustomLineTerminatorNonUTF8",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=2,
                        file_id=2,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv"
                        ),
                    ),
                    RawGCSFileMetadata(
                        gcs_file_id=3,
                        file_id=2,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv"
                        ),
                    ),
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=3,
                file_tag="multipleColPrimaryKeyHistorical",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_multipleColPrimaryKeyHistorical.csv"
                        ),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 25, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=4,
                file_tag="multipleColPrimaryKeyHistorical",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_multipleColPrimaryKeyHistorical.csv"
                        ),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 26, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
        ]

        processing_errors = [
            RawFileProcessingError(
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagBasicData.csv"
                ),
                temporary_file_paths=[
                    GcsfsFilePath(bucket_name="temp", blob_name="temp_blob1_1_1.csv"),
                    GcsfsFilePath(bucket_name="temp", blob_name="temp_blob1_1_2.csv"),
                ],
                error_msg="yike!",
            )
        ]

        load_errors = [
            RawFileLoadAndPrepError(
                file_id=2,
                file_tag="tagCustomLineTerminatorNonUTF8",
                update_datetime=datetime.datetime(
                    2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                ),
                pre_import_normalized_file_paths=[
                    GcsfsFilePath.from_absolute_path("temp/needs-clean.csv")
                ],
                original_file_paths=[
                    GcsfsFilePath.from_absolute_path(
                        "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagCustomLineTerminatorNonUTF8.csv"
                    )
                ],
                temp_table=None,
                error_msg="yikes!",
            ),
        ]

        append_errors = [
            RawDataAppendImportError(
                file_id=3,
                raw_temp_table=BigQueryAddress(
                    dataset_id="temp",
                    table_id="multipleColPrimaryKeyHistorical__1__transformed",
                ),
                error_msg="yike!",
            )
        ]

        result = coalesce_results_and_errors.function(
            region_code="US_XX",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            serialized_bq_metadata=[m.serialize() for m in bq_metadata],
            serialized_header_verification_errors=[],
            serialized_chunking_errors=[],
            serialized_pre_import_normalization_result=BatchedTaskInstanceOutput(
                results=[], errors=processing_errors
            ).serialize(),
            serialized_load_prep_results=[],
            serialized_append_batches={
                APPEND_READY_FILE_BATCHES: [],
                SKIPPED_FILE_ERRORS: [e.serialize() for e in load_errors],
            },
            serialized_append_result=[
                BatchedTaskInstanceOutput(results=[], errors=append_errors).serialize(),
            ],
        )

        assert result[HAS_FILE_IMPORT_ERRORS] is True
        file_imports = result[FILE_IMPORTS]
        assert isinstance(file_imports, list)
        assert len(file_imports) == len(bq_metadata)
        summaries = [RawFileImport.deserialize(s) for s in file_imports]
        assert summaries[0].file_id == 3
        assert summaries[0].historical_diffs_active is True
        assert (
            summaries[0].import_status
            == DirectIngestRawFileImportStatus.FAILED_LOAD_STEP
        )
        assert summaries[1].file_id == 2
        assert summaries[1].historical_diffs_active is True
        assert (
            summaries[1].import_status
            == DirectIngestRawFileImportStatus.FAILED_LOAD_STEP
        )
        assert summaries[2].file_id == 1
        assert summaries[2].historical_diffs_active is True
        assert (
            summaries[2].import_status
            == DirectIngestRawFileImportStatus.FAILED_PRE_IMPORT_NORMALIZATION_STEP
        )
        assert summaries[3].file_id == 4
        assert summaries[3].historical_diffs_active is True
        assert (
            summaries[3].import_status == DirectIngestRawFileImportStatus.FAILED_UNKNOWN
        )

    def test_reconcile_file_imports_and_bq_metadata_all_accounted_for(self) -> None:
        bq_metadata = [
            RawBigQueryFileMetadata(
                file_id=1,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath(bucket_name="bucket", blob_name="blob.csv"),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=2,
                file_tag="tagCustomLineTerminatorNonUTF8",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=2,
                        file_id=2,
                        path=GcsfsFilePath(
                            bucket_name="bucket", blob_name="blob1_1.csv"
                        ),
                    ),
                    RawGCSFileMetadata(
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
            RawBigQueryFileMetadata(
                file_id=3,
                file_tag="tagCustomLineTerminatorNonUTF8",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=4,
                        file_id=3,
                        path=GcsfsFilePath(
                            bucket_name="bucket", blob_name="blob2_1.csv"
                        ),
                    ),
                    RawGCSFileMetadata(
                        gcs_file_id=5,
                        file_id=3,
                        path=GcsfsFilePath(
                            bucket_name="bucket", blob_name="blob2_2.csv"
                        ),
                    ),
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
        ]

        successful_imports = {
            1: RawFileImport(
                file_id=1,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
                error_message=None,
            ),
            2: RawFileImport(
                file_id=2,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
                error_message=None,
            ),
        }

        failed_imports = {
            3: RawFileImport(
                file_id=3,
                import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
                historical_diffs_active=False,
                error_message="failure!",
            )
        }

        raw_region_config = get_direct_ingest_region_raw_config("US_XX")

        missing_file_imports = _reconcile_file_imports_and_bq_metadata(
            raw_region_config,
            DirectIngestInstance.PRIMARY,
            bq_metadata,
            successful_imports,
            failed_imports,
        )

        assert not missing_file_imports

    def test__reconcile_file_imports_and_bq_metadata_some_missing(self) -> None:
        bq_metadata = [
            RawBigQueryFileMetadata(
                file_id=1,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath(bucket_name="bucket", blob_name="blob.csv"),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=2,
                file_tag="tagCustomLineTerminatorNonUTF8",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=2,
                        file_id=2,
                        path=GcsfsFilePath(
                            bucket_name="bucket", blob_name="blob1_1.csv"
                        ),
                    ),
                    RawGCSFileMetadata(
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
            RawBigQueryFileMetadata(
                file_id=3,
                file_tag="tagCustomLineTerminatorNonUTF8",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=4,
                        file_id=3,
                        path=GcsfsFilePath(
                            bucket_name="bucket", blob_name="blob2_1.csv"
                        ),
                    ),
                    RawGCSFileMetadata(
                        gcs_file_id=5,
                        file_id=3,
                        path=GcsfsFilePath(
                            bucket_name="bucket", blob_name="blob2_2.csv"
                        ),
                    ),
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
        ]

        successful_imports = {
            1: RawFileImport(
                file_id=1,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
                error_message=None,
            ),
        }

        raw_region_config = get_direct_ingest_region_raw_config("US_XX")

        missing_file_imports = _reconcile_file_imports_and_bq_metadata(
            raw_region_config,
            DirectIngestInstance.PRIMARY,
            bq_metadata,
            successful_imports,
            {},
        )

        assert len(missing_file_imports) == 2
        assert missing_file_imports[0].file_id == 2
        assert missing_file_imports[1].file_id == 3
        assert (
            one({summary.import_status for summary in missing_file_imports})
            == DirectIngestRawFileImportStatus.FAILED_UNKNOWN
        )
        for missing_file_import in missing_file_imports:
            assert missing_file_import.error_message == (
                f"Could not locate a success or failure for this file_id "
                f"[{missing_file_import.file_id}] despite it being marked for import. This"
                f"is likely indicative that a DAG-level failure occurred during"
                f"this import run."
            )
