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
from typing import List, Optional, Tuple
from unittest.mock import create_autospec

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.get_all_unprocessed_bq_file_metadata_sql_query_generator import (
    GetAllUnprocessedBQFileMetadataSqlQueryGenerator,
)
from recidiviz.airflow.dags.raw_data.get_all_unprocessed_gcs_file_metadata_sql_query_generator import (
    GetAllUnprocessedGCSFileMetadataSqlQueryGenerator,
)
from recidiviz.airflow.dags.raw_data.types import GCSMetadataRow
from recidiviz.airflow.tests.test_utils import CloudSqlQueryGeneratorUnitTest
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
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

        self.mock_pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.generator = GetAllUnprocessedBQFileMetadataSqlQueryGenerator(
            region_code="US_XX",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            get_all_unprocessed_gcs_file_metadata_task_id="test_id",
            region_module_override=fake_regions,
        )
        self.raw_gcs_generator = GetAllUnprocessedGCSFileMetadataSqlQueryGenerator(
            region_code="US_XX",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            list_normalized_unprocessed_gcs_file_paths_task_id="test_id",
        )
        self.mock_operator = create_autospec(CloudSqlQueryOperator)
        self.mock_context = create_autospec(Context)

    def _validate_results(
        self, _results: List[Tuple[int, List[str]]], _inputs: List
    ) -> None:

        for i, result in enumerate(_results):
            assert isinstance(result[0], int)

        assert {p for r in _results for p in r[1]} == {i[2] for i in _inputs}

        persisted_gcs_records = self.mock_pg_hook.get_records(
            "select file_id, array_agg(normalized_file_name) from direct_ingest_raw_gcs_file_metadata group by file_id order by file_id;"
        )

        assert len(persisted_gcs_records) == len(_results)

        # make sure we correctly persisted file_id
        for i, record in enumerate(persisted_gcs_records):
            assert record[0] == _results[i][0]
            for name in record[1]:
                assert f"testing/{name}" in _results[i][1]

    def _insert_rows_into_gcs(
        self, paths: List[str]
    ) -> List[Tuple[int, Optional[int]]]:
        records: List[GCSMetadataRow] = self.mock_pg_hook.get_records(
            # pylint: disable=protected-access
            self.raw_gcs_generator._register_new_files_sql_query(
                [GcsfsFilePath.from_absolute_path(path) for path in paths]
            )
        )
        return [(record.gcs_file_id, record.file_id) for record in records]

    def test_no_files(self) -> None:
        self.mock_operator.xcom_pull.return_value = []
        mock_postgres = create_autospec(PostgresHook)
        results = self.generator.execute_postgres_query(
            self.mock_operator, mock_postgres, self.mock_context
        )

        assert results == []
        mock_postgres.get_records.assert_not_called()

    def test_all_pre_registered(self) -> None:
        inputs = [
            [
                1,
                2,
                "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag.csv",
            ],
            [
                3,
                4,
                "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag-1.csv",
            ],
            [
                5,
                6,
                "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag.csv",
            ],
        ]

        self.mock_operator.xcom_pull.return_value = inputs
        mock_postgres = create_autospec(PostgresHook)
        results = self.generator.execute_postgres_query(
            self.mock_operator, mock_postgres, self.mock_context
        )

        assert len(results) == len(inputs)

        for i, result in enumerate(results):
            assert result[0] == inputs[i][1]
            assert len(result[1]) == 1
            assert result[1][0] in assert_type(inputs[i][2], str)

        mock_postgres.get_records.assert_not_called()

    def test_no_recognized_paths(self) -> None:
        inputs = [
            [
                1,
                None,
                "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagThatDoesNotExist.csv",
            ],
            [
                3,
                None,
                "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagThatIReallyPromiseDoesNotExist.csv",
            ],
        ]
        self.mock_operator.xcom_pull.return_value = inputs
        mock_postgres = create_autospec(PostgresHook)
        with self.assertLogs("raw_data", level="INFO") as logs:
            _ = self.generator.execute_postgres_query(
                self.mock_operator, mock_postgres, self.mock_context
            )

        assert len(logs.output) == 1
        self.assertRegex(
            logs.output[0],
            r"Found unrecognized file tags that we will skip marking in direct_ingest_raw_big_query_file_metadata: \[.*,.*\]",
        )
        mock_postgres.get_records.assert_not_called()

    def test_none_pre_registered_no_chunks(self) -> None:
        paths = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagBasicData.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagBasicData-1.csv",
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_tagBasicData.csv",
        ]
        inputs = [
            [*ids, paths[i]] for i, ids in enumerate(self._insert_rows_into_gcs(paths))
        ]

        self.mock_operator.xcom_pull.return_value = inputs
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
        inputs_batch_one = [
            [*ids, paths_batch_one[i]]
            for i, ids in enumerate(self._insert_rows_into_gcs(paths_batch_one))
        ]

        self.mock_operator.xcom_pull.return_value = inputs_batch_one
        results_one = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )
        self._validate_results(results_one, inputs_batch_one)

        inputs_batch_two = inputs_batch_one + [
            [*ids, paths_batch_two[i]]
            for i, ids in enumerate(self._insert_rows_into_gcs(paths_batch_two))
        ]

        self.mock_operator.xcom_pull.return_value = inputs_batch_two
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
        inputs = [
            [*ids, paths[i]] for i, ids in enumerate(self._insert_rows_into_gcs(paths))
        ]

        self.mock_operator.xcom_pull.return_value = inputs
        with self.assertLogs("raw_data", level="INFO") as logs:
            results = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        # they all have the same file_id
        assert len({r[0] for r in results}) == 1

        self._validate_results(results, inputs)

        assert len(logs.output) == 1
        self.assertRegex(
            logs.output[0],
            r"INFO:raw_data:Found 3\/3 paths for tagChunkedFile on 2024-01-25 -- grouping \[.*\]",
        )

    def test_wrong_number_chunks(self) -> None:
        paths = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-1.csv",
            "testing/processed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-2.csv",
        ]
        inputs = [
            [*ids, paths[i]] for i, ids in enumerate(self._insert_rows_into_gcs(paths))
        ]

        self.mock_operator.xcom_pull.return_value = inputs
        with self.assertLogs("raw_data", level="ERROR") as logs:
            results = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        assert len(results) == 0
        assert len(logs.output) == 1

        self.assertRegex(
            logs.output[0],
            r"ERROR:raw_data:Skipping grouping for tagChunkedFile on 2024-01-25, found 2 but expected 3 paths: \[.*\]",
        )

        new_paths = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-3.csv",
            "testing/processed_2024-01-25T16:35:33:617135_raw_tagChunkedFile-4.csv",
        ]
        new_inputs = inputs + [
            [*ids, new_paths[i]]
            for i, ids in enumerate(self._insert_rows_into_gcs(new_paths))
        ]

        self.mock_operator.xcom_pull.return_value = new_inputs
        with self.assertLogs("raw_data", level="ERROR") as logs:
            results = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        assert len(results) == 0
        assert len(logs.output) == 1

        self.assertRegex(
            logs.output[0],
            r"ERROR:raw_data:Skipping grouping for tagChunkedFile on 2024-01-25, found 4 but expected 3 paths: \[.*\]",
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
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFileTwo-3.csv",
        ]
        inputs = [
            [*ids, paths[i]] for i, ids in enumerate(self._insert_rows_into_gcs(paths))
        ]

        self.mock_operator.xcom_pull.return_value = inputs
        with self.assertLogs("raw_data", level="ERROR") as logs:
            results = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        assert len(results) == 2
        assert len(logs.output) == 2

        self.assertRegex(
            logs.output[0],
            r"ERROR:raw_data:Skipping grouping for tagChunkedFile on 2024-01-25, found 2 but expected 3 paths: \[.*\]",
        )
        self.assertRegex(
            logs.output[1],
            r"ERROR:raw_data:Skipping grouping for tagChunkedFile on 2024-01-26 as we "
            r"could not successfully group tagChunkedFile on 2024-01-25 and we want to "
            r"guarantee upload order in big query",
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
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_tagChunkedFileTwo-3.csv",
        ]
        inputs = [
            [*ids, paths[i]] for i, ids in enumerate(self._insert_rows_into_gcs(paths))
        ]

        self.mock_operator.xcom_pull.return_value = inputs
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        assert len({r[0] for r in results}) == 3

        self._validate_results(results, inputs)
