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
"""Unit tests for GetAllUnprocessedGCSFileMetadataSqlQueryGenerator"""
import datetime
from typing import Any, List
from unittest.mock import create_autospec

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from freezegun import freeze_time

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.get_all_unprocessed_gcs_file_metadata_sql_query_generator import (
    GetAllUnprocessedGCSFileMetadataSqlQueryGenerator,
)
from recidiviz.airflow.tests.test_utils import CloudSqlQueryGeneratorUnitTest
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawGCSFileMetadataSummary,
)
from recidiviz.persistence.database.schema.operations.schema import OperationsBase


class TestGetAllUnprocessedGCSFileMetadataSqlQueryGenerator(
    CloudSqlQueryGeneratorUnitTest
):
    """Unit tests for GetAllUnprocessedGCSFileMetadataSqlQueryGenerator"""

    metas = [OperationsBase]

    def setUp(self) -> None:
        super().setUp()

        self.mock_pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.generator = GetAllUnprocessedGCSFileMetadataSqlQueryGenerator(
            region_code="US_XX",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            list_normalized_unprocessed_gcs_file_paths_task_id="test_id",
        )
        self.mock_operator = create_autospec(CloudSqlQueryOperator)
        self.mock_context = create_autospec(Context)

    def test_no_files(self) -> None:
        self.mock_operator.xcom_pull.return_value = []
        mock_postgres = create_autospec(PostgresHook)
        results = self.generator.execute_postgres_query(
            self.mock_operator, mock_postgres, self.mock_context
        )

        assert results == []
        mock_postgres.get_records.assert_not_called()

    def test_none_pre_registered(self) -> None:
        normalized_fns = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag.csv",
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag-1.csv",
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag-2.csv",
        ]

        self.mock_operator.xcom_pull.return_value = normalized_fns
        frozen = datetime.datetime(2023, 1, 26, 0, 0, 0, 0, tzinfo=datetime.UTC)
        with freeze_time(frozen):
            results = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        for i, result in enumerate(results):
            gcs = RawGCSFileMetadataSummary.deserialize(result)
            assert gcs.file_id is None
            assert gcs.path.abs_path() == normalized_fns[i]

        full_results: List[Any] = self.mock_pg_hook.get_records(
            "SELECT gcs_file_id, region_code, raw_data_instance, file_tag, normalized_file_name, update_datetime, file_discovery_time FROM direct_ingest_raw_gcs_file_metadata"
        )

        for full_result in full_results:
            assert isinstance(full_result[0], int)
            assert full_result[1] == "US_XX"
            assert full_result[2] == DirectIngestInstance.PRIMARY.value
            assert full_result[3] == "test_file_tag"
            assert full_result[5] == datetime.datetime.fromisoformat(
                "2024-01-25T16:35:33:617135"
            ).replace(tzinfo=datetime.UTC)
            assert full_result[6] == frozen

    def test_some_pre_registered(self) -> None:
        normalized_fns_batch_1 = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_test_file_tag_with_suffix-1.csv",
        ]

        normalized_fns_batch_2 = [
            *normalized_fns_batch_1,
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_test_file_tag.csv",
        ]

        self.mock_operator.xcom_pull.return_value = normalized_fns_batch_1
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        for i, result in enumerate(results):
            gcs = RawGCSFileMetadataSummary.deserialize(result)
            assert gcs.file_id is None
            assert gcs.path.abs_path() == normalized_fns_batch_1[i]

        self.mock_operator.xcom_pull.return_value = normalized_fns_batch_2
        results = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        for i, result in enumerate(results):
            gcs = RawGCSFileMetadataSummary.deserialize(result)
            assert gcs.file_id is None
            assert gcs.path.abs_path() == normalized_fns_batch_2[i]

    def test_all_preregistered(self) -> None:
        normalized_fns = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_test_file_tag_with_suffix-1.csv",
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_test_file_tag.csv",
        ]

        self.mock_operator.xcom_pull.return_value = normalized_fns

        def _validate(_results: List[str]) -> None:
            for i, result in enumerate(_results):
                gcs = RawGCSFileMetadataSummary.deserialize(result)
                assert gcs.file_id is None
                assert gcs.path.abs_path() == normalized_fns[i]

        results_1 = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )
        _validate(results_1)
        results_2 = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )
        _validate(results_2)

    def test_idempotent(self) -> None:
        normalized_fns = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_test_file_tag_with_suffix-1.csv",
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_test_file_tag.csv",
        ]
        self.mock_operator.xcom_pull.return_value = normalized_fns

        for _ in range(5):
            results = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

            for i, result in enumerate(results):
                gcs = RawGCSFileMetadataSummary.deserialize(result)
                assert gcs.file_id is None
                assert gcs.path.abs_path() == normalized_fns[i]

        results = self.mock_pg_hook.get_records(
            "SELECT * FROM direct_ingest_raw_gcs_file_metadata"
        )
        assert len(results) == 3

    def test_with_bq_file_ids(self) -> None:
        normalized_fns = [
            "testing/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag.csv",
            "testing/unprocessed_2024-01-26T16:35:33:617135_raw_test_file_tag_with_suffix-1.csv",
            "testing/unprocessed_2024-01-27T16:35:33:617135_raw_test_file_tag.csv",
        ]
        self.mock_operator.xcom_pull.return_value = normalized_fns

        mock_postgres = create_autospec(PostgresHook)
        mock_postgres.get_records.return_value = [
            (i, i, fn.split("/")[-1]) for i, fn in enumerate(normalized_fns)
        ]

        results = self.generator.execute_postgres_query(
            self.mock_operator, mock_postgres, self.mock_context
        )

        for i, result in enumerate(results):
            gcs = RawGCSFileMetadataSummary.deserialize(result)
            assert gcs.file_id == i
            assert gcs.gcs_file_id == i
            assert gcs.path.abs_path() == normalized_fns[i]
