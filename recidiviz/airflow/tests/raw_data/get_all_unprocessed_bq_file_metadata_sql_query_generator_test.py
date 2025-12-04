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
"""Unit tests for GetAllUnprocessedBQFileMetadataSqlQueryGenerator"""
import datetime
from typing import List
from unittest.mock import Mock, call, create_autospec, patch

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from more_itertools import one

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.get_all_unprocessed_bq_file_metadata_sql_query_generator import (
    GetAllUnprocessedBQFileMetadataSqlQueryGenerator,
)
from recidiviz.airflow.dags.raw_data.get_all_unprocessed_gcs_file_metadata_sql_query_generator import (
    GetAllUnprocessedGCSFileMetadataSqlQueryGenerator,
)
from recidiviz.airflow.dags.raw_data.write_file_processed_time_to_bq_file_metadata_sql_query_generator import (
    WriteFileProcessedTimeToBQFileMetadataSqlQueryGenerator,
)
from recidiviz.airflow.tests.raw_data.raw_data_test_utils import (
    FakeStateRawFileChunkingMetadataFactory,
)
from recidiviz.airflow.tests.test_utils import CloudSqlQueryGeneratorUnitTest
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawBigQueryFileProcessedTime,
    RawDataFilesSkippedError,
    RawGCSFileMetadata,
)
from recidiviz.persistence.database.schema.operations.schema import OperationsBase
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.utils.types import assert_type


class TestGetAllUnprocessedBQFileMetadataSqlQueryGenerator(
    CloudSqlQueryGeneratorUnitTest
):
    """Unit tests for GetAllUnprocessedBQFilesSqlQueryGenerator"""

    metas = [OperationsBase]

    def setUp(self) -> None:
        super().setUp()
        self.region_module_patch = patch(
            "recidiviz.airflow.dags.raw_data.utils.direct_ingest_regions_module",
            fake_regions,
        )
        self.region_module_patch.start()
        self.chunking_metadata_patcher = patch(
            "recidiviz.airflow.dags.raw_data.get_all_unprocessed_bq_file_metadata_sql_query_generator.StateRawFileChunkingMetadataFactory",
            FakeStateRawFileChunkingMetadataFactory,
        )
        self.chunking_metadata_patcher.start()

        self.mock_pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.generator = GetAllUnprocessedBQFileMetadataSqlQueryGenerator(
            region_code="US_XX",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            get_all_unprocessed_gcs_file_metadata_task_id="test_id",
        )
        self.raw_gcs_generator = GetAllUnprocessedGCSFileMetadataSqlQueryGenerator(
            region_code="US_XX",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            list_normalized_unprocessed_gcs_file_paths_task_id="test_id",
        )
        self.mock_operator = create_autospec(CloudSqlQueryOperator)
        self.mock_context = create_autospec(Context)

    def tearDown(self) -> None:
        self.region_module_patch.stop()
        self.chunking_metadata_patcher.stop()
        return super().tearDown()

    def _validate_results(
        self, _results: List[str], _inputs: List[RawGCSFileMetadata]
    ) -> None:
        metadata = [RawBigQueryFileMetadata.deserialize(result) for result in _results]

        assert {p.path for m in metadata for p in m.gcs_files} == {
            i.path for i in _inputs
        }

        persisted_gcs_records = self.mock_pg_hook.get_records(
            "select file_id, array_agg(normalized_file_name) from direct_ingest_raw_gcs_file_metadata group by file_id order by file_id;"
        )

        assert len(persisted_gcs_records) == len(metadata)

        # make sure we correctly persisted file_id
        for i, record in enumerate(persisted_gcs_records):
            assert record[0] == metadata[i].file_id
            ps = [f.path.abs_path() for f in metadata[i].gcs_files]
            for name in record[1]:
                assert f"testing/{name}" in ps

    def _insert_rows_into_gcs(self, paths: List[str]) -> List[RawGCSFileMetadata]:
        p = [GcsfsFilePath.from_absolute_path(path) for path in paths]
        results = self.mock_pg_hook.get_records(
            # pylint: disable=protected-access
            self.raw_gcs_generator._register_new_files_sql_query(p)
        )

        return [
            RawGCSFileMetadata.from_gcs_metadata_table_row(result, p[i])
            for i, result in enumerate(results)
        ]

    def test_no_files(self) -> None:
        self.mock_operator.xcom_pull.return_value = []
        mock_postgres = create_autospec(PostgresHook)
        results = self.generator.execute_postgres_query(
            self.mock_operator, mock_postgres, self.mock_context
        )

        assert results == []
        mock_postgres.get_records.assert_not_called()

    def test_all_pre_registered(self) -> None:

        # first, not registered at all
        paths = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_file_tag_first.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_file_tag_first.csv",
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_file_tag_first.csv",
        ]

        inputs = self._insert_rows_into_gcs(paths)

        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in inputs]
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )
        metadata_results = [
            RawBigQueryFileMetadata.deserialize(result) for result in results
        ]

        assert len(metadata_results) == len(inputs)

        for i, result in enumerate(metadata_results):
            assert isinstance(result.file_id, int)
            assert len(result.gcs_files) == 1
            assert result.gcs_files[0].path == inputs[i].path
            inputs[i].file_id = result.file_id

        # then, all registered
        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in inputs]
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )
        metadata_results = [
            RawBigQueryFileMetadata.deserialize(result) for result in results
        ]

        assert len(metadata_results) == len(inputs)

        for i, result in enumerate(metadata_results):
            assert result.file_id == inputs[i].file_id
            assert len(result.gcs_files) == 1
            assert result.gcs_files[0].path == inputs[i].path

    def test_filter_already_processsed(self) -> None:

        # first, not registered at all
        paths = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_file_tag_first.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_file_tag_first.csv",
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_file_tag_first.csv",
        ]

        inputs = self._insert_rows_into_gcs(paths)

        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in inputs]
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )
        metadata_results = [
            RawBigQueryFileMetadata.deserialize(result) for result in results
        ]

        assert len(metadata_results) == len(inputs)

        for i, result in enumerate(metadata_results):
            assert isinstance(result.file_id, int)
            assert len(result.gcs_files) == 1
            assert result.gcs_files[0].path == inputs[i].path
            inputs[i].file_id = result.file_id

        # mark as processed

        self.mock_operator.xcom_pull.return_value = [
            RawBigQueryFileProcessedTime(
                file_id=assert_type(i.file_id, int),
                file_processed_time=datetime.datetime.now(tz=datetime.UTC),
            ).serialize()
            for i in inputs
        ]

        WriteFileProcessedTimeToBQFileMetadataSqlQueryGenerator(
            "fake_task_id"
        ).execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        # if we try to reimport them, we wont because we have already processed them!
        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in inputs]
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )
        metadata_results = [
            RawBigQueryFileMetadata.deserialize(result) for result in results
        ]

        assert len(metadata_results) == 0

    def test_no_recognized_paths(self) -> None:
        inputs = [
            RawGCSFileMetadata(
                gcs_file_id=1,
                file_id=None,
                path=GcsfsFilePath.from_absolute_path(
                    "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagThatDoesNotExist.csv",
                ),
            ),
            RawGCSFileMetadata(
                gcs_file_id=3,
                file_id=None,
                path=GcsfsFilePath.from_absolute_path(
                    "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagThatIReallyPromiseDoesNotExist.csv",
                ),
            ),
        ]
        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in inputs]
        with self.assertLogs("raw_data", level="INFO") as logs:
            results = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        assert results == []

        assert len(logs.output) == 1
        self.assertRegex(
            logs.output[0],
            r"Found unrecognized file tags that we will skip marking in direct_ingest_raw_big_query_file_metadata: \[.*,.*\]",
        )

    def test_none_pre_registered_no_chunks(self) -> None:
        paths = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagBasicData.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagBasicData-1.csv",
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_tagBasicData.csv",
        ]
        inputs = self._insert_rows_into_gcs(paths)

        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in inputs]
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        self._validate_results(results, inputs)

    def test_some_pre_registered_no_chunks(self) -> None:
        paths_batch_one = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagBasicData.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagBasicData-1.csv",
        ]
        paths_batch_two = [
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_tagBasicData.csv",
        ]
        inputs_batch_one = self._insert_rows_into_gcs(paths_batch_one)

        self.mock_operator.xcom_pull.return_value = [
            i.serialize() for i in inputs_batch_one
        ]
        results_one = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )
        self._validate_results(results_one, inputs_batch_one)

        inputs_batch_two = inputs_batch_one + self._insert_rows_into_gcs(
            paths_batch_two
        )

        self.mock_operator.xcom_pull.return_value = [
            i.serialize() for i in inputs_batch_two
        ]
        results_two = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )
        self._validate_results(results_two, inputs_batch_two)

    def test_simple_chunks(self) -> None:
        paths = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-1.csv",
            "testing/processed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-2.csv",
            "testing/processed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-3.csv",
        ]
        inputs = self._insert_rows_into_gcs(paths)

        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in inputs]
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        # they all have the same file_id
        assert len({r[0] for r in results}) == 1

        self._validate_results(results, inputs)

    def test_wrong_number_chunks(self) -> None:
        paths = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-1.csv",
            "testing/processed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-2.csv",
        ]
        inputs = self._insert_rows_into_gcs(paths)

        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in inputs]
        with self.assertLogs("raw_data", level="ERROR") as logs:
            results = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        assert len(results) == 0
        assert len(logs.output) == 1

        self.assertRegex(
            logs.output[0],
            r"ERROR:raw_data:Skipping grouping for \[tagChunkedFile\]; Expected \[3\] chunks, but found \[2\]",
        )

        new_paths = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-3.csv",
            "testing/processed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-4.csv",
        ]
        new_inputs = inputs + self._insert_rows_into_gcs(new_paths)

        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in new_inputs]
        with self.assertLogs("raw_data", level="ERROR") as logs:
            results = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        assert len(results) == 0
        assert len(logs.output) == 1

        self.assertRegex(
            logs.output[0],
            r"ERROR:raw_data:Skipping grouping for \[tagChunkedFile\]; Expected \[3\] chunks, but found \[4\]",
        )

    def test_incorrect_chunk_count_subsequent_fail(self) -> None:
        paths = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagBasicData.csv",
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-1.csv",
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-2.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFile-1.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFile-2.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFile-3.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFileTwo-1.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFileTwo-2.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFileTwo-3.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFileTwo-4.csv",
        ]
        inputs = self._insert_rows_into_gcs(paths)

        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in inputs]
        with self.assertLogs("raw_data", level="ERROR") as logs:
            results = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        assert len(results) == 2
        assert len(logs.output) == 2

        self.assertRegex(
            logs.output[0],
            r"ERROR:raw_data:Skipping grouping for \[tagChunkedFile\]; Expected \[3\] chunks, but found \[2\]",
        )
        self.assertRegex(
            logs.output[1],
            r"ERROR:raw_data:Skipping import for file_id \[2\], file_tag \[tagChunkedFile\]: path \[.*\] was previously skipped",
        )

    def test_multiple_chunks(self) -> None:
        paths = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-1.csv",
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-2.csv",
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-3.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFile-1.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFile-2.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFile-3.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFileTwo-1.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFileTwo-2.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFileTwo-3.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFileTwo-4.csv",
        ]
        inputs = self._insert_rows_into_gcs(paths)

        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in inputs]
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        metadata = [
            RawBigQueryFileMetadata.deserialize(result_str) for result_str in results
        ]

        assert len({meta.file_id for meta in metadata}) == 3

        self._validate_results(results, inputs)

    def test_chunks_change(self) -> None:
        # simple case first
        paths = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagBasicData.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFile-1.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFile-2.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFile-3.csv",
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_tagChunkedFile-1.csv",
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_tagChunkedFile-2.csv",
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_tagChunkedFile-3.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFileTwo-1.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFileTwo-2.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFileTwo-3.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFileTwo-4.csv",
        ]
        inputs = self._insert_rows_into_gcs(paths)

        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in inputs]
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        metadata = [
            RawBigQueryFileMetadata.deserialize(result_str) for result_str in results
        ]

        assert len({meta.file_id for meta in metadata}) == 4

        self._validate_results(results, inputs)

        all_gcs_files = [gcs_file for m in metadata for gcs_file in m.gcs_files]

        print(all_gcs_files)

        excluded_chunk = one(
            filter(
                lambda x: x.path.blob_name
                == "unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFile-2.csv",
                all_gcs_files,
            )
        )

        all_gcs_files_minus_chunk = list(
            filter(lambda x: x != excluded_chunk, all_gcs_files)
        )

        self.mock_operator.xcom_pull.return_value = [
            i.serialize() for i in all_gcs_files_minus_chunk
        ]
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        metadata = [
            RawBigQueryFileMetadata.deserialize(result_str) for result_str in results
        ]

        metadata_file_ids = {meta.file_id for meta in metadata}
        # we excluded that chunk as incomplete & the one after since it blocks
        assert len(metadata_file_ids) == 2
        assert excluded_chunk.file_id not in metadata_file_ids

    def test_uploaded_old_data(self) -> None:
        # first, upload new data
        new_paths = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_file_tag_first.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_file_tag_first.csv",
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_file_tag_first.csv",
        ]

        inputs = self._insert_rows_into_gcs(new_paths)

        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in inputs]
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )
        metadata_results = [
            RawBigQueryFileMetadata.deserialize(result) for result in results
        ]

        assert len(metadata_results) == len(inputs)

        for i, result in enumerate(metadata_results):
            assert isinstance(result.file_id, int)
            assert len(result.gcs_files) == 1
            assert result.gcs_files[0].path == inputs[i].path
            inputs[i].file_id = result.file_id

        # mark as processed

        self.mock_operator.xcom_pull.return_value = [
            RawBigQueryFileProcessedTime(
                file_id=assert_type(metadata.file_id, int),
                file_processed_time=datetime.datetime.now(tz=datetime.UTC),
            ).serialize()
            for metadata in metadata_results
        ]

        WriteFileProcessedTimeToBQFileMetadataSqlQueryGenerator(
            "fake_task_id"
        ).execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        # then, try to upload an old file
        old_file = [
            "testing/unprocessed_2024-01-24T16:35:33:617135_raw_file_tag_first.csv",
        ]
        old_input = self._insert_rows_into_gcs(old_file)

        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in old_input]
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )
        metadata_results = [
            RawBigQueryFileMetadata.deserialize(result) for result in results
        ]

        assert len(metadata_results) == 0

        # but uploading a newer file should work
        newer_file = [
            "testing/unprocessed_2024-01-28T16:35:33:617135_raw_file_tag_first.csv",
        ]
        newer_input = self._insert_rows_into_gcs(newer_file)

        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in newer_input]
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )
        metadata_results = [
            RawBigQueryFileMetadata.deserialize(result) for result in results
        ]

        assert len(metadata_results) == len(newer_file)
        for i, result in enumerate(metadata_results):
            assert isinstance(result.file_id, int)
            assert len(result.gcs_files) == 1
            assert result.gcs_files[0].path == newer_input[i].path

    def test_deleted_file_tag_but_didnt_deprecate(self) -> None:
        # first, upload new data
        new_path = (
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_file_tag_first.csv"
        )

        inputs = self._insert_rows_into_gcs([new_path])

        self.mock_operator.xcom_pull.return_value = [i.serialize() for i in inputs]
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )
        metadata_results = [
            RawBigQueryFileMetadata.deserialize(result) for result in results
        ]

        assert len(metadata_results) == len(inputs)

        for i, result in enumerate(metadata_results):
            assert isinstance(result.file_id, int)
            assert len(result.gcs_files) == 1
            assert result.gcs_files[0].path == inputs[i].path
            inputs[i].file_id = result.file_id

        # then, try to import them again

        new_gcs_path = GcsfsFilePath.from_absolute_path(new_path)
        already_seen_gcs_metadata = [
            RawGCSFileMetadata.from_gcs_metadata_table_row(metadata_row, new_gcs_path)
            for metadata_row in self.mock_pg_hook.get_records(
                # pylint: disable=protected-access
                self.raw_gcs_generator._get_existing_files_by_path_sql_query(
                    [new_gcs_path]
                )
            )
        ]

        self.mock_operator.xcom_pull.return_value = [
            i.serialize() for i in already_seen_gcs_metadata
        ]

        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        empty_raw_region_config = Mock()
        empty_raw_region_config.raw_file_tags = set()

        # pylint: disable=protected-access
        self.generator._region_raw_file_config = empty_raw_region_config

        expected_skipped_error = RawDataFilesSkippedError(
            file_paths=[GcsfsFilePath.from_absolute_path(new_path)],
            file_tag="file_tag_first",
            update_datetime=datetime.datetime.fromisoformat(
                "2024-01-25T16:35:33:617135Z"
            ),
            skipped_message="Skipping import for file_id [1], file_tag [file_tag_first]: file_tag [file_tag_first] no longer exists!",
        )

        with self.assertLogs("raw_data", level="ERROR") as logs:
            results = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

            assert len(results) == 0
            assert len(logs.output) == 1

            self.assertRegex(
                logs.output[0],
                r"ERROR:raw_data:Skipping import for file_id \[1\] as \[file_tag_first\] is no longer a valid file tag",
            )

            self.mock_operator.xcom_push.assert_has_calls(
                [
                    call(
                        context=self.mock_context,
                        key="skipped_file_errors",
                        value=[expected_skipped_error.serialize()],
                    )
                ]
            )
