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
"""Tests for WriteFileProcessedTimeToBQFileMetadataSqlQueryGenerator"""
import datetime
from typing import List
from unittest.mock import create_autospec

from airflow.models import DagRun
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.write_file_processed_time_to_bq_file_metadata_sql_query_generator import (
    WriteFileProcessedTimeToBQFileMetadataSqlQueryGenerator,
)
from recidiviz.airflow.dags.utils.cloud_sql import postgres_formatted_datetime_with_tz
from recidiviz.airflow.tests.test_utils import CloudSqlQueryGeneratorUnitTest
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileProcessedTime,
)
from recidiviz.persistence.database.schema.operations.schema import OperationsBase


class WriteImportSessionSqlQueryGeneratorTest(CloudSqlQueryGeneratorUnitTest):
    """Unit tests for WriteImportSessionSqlQueryGenerator"""

    metas = [OperationsBase]
    mock_operator = create_autospec(CloudSqlQueryOperator)
    mock_context = create_autospec(Context)

    def setUp(self) -> None:
        super().setUp()
        self.generator = WriteFileProcessedTimeToBQFileMetadataSqlQueryGenerator(
            write_import_session_task_id="task_id",
        )
        self.mock_pg_hook = PostgresHook(postgres_conn_id=self.conn_id)

    def test_write_no_updates(self) -> None:
        self.mock_operator.xcom_pull.return_value = []
        mock_hook = create_autospec(PostgresHook)
        self.generator.execute_postgres_query(
            self.mock_operator, mock_hook, self.mock_context
        )

        mock_hook.run.assert_not_called()

    def _seed_bq_metadata(self, updates: List[RawBigQueryFileProcessedTime]) -> None:
        dt = datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC)
        values = [
            f"({session.file_id},'US_XX','PRIMARY','tag_a','{postgres_formatted_datetime_with_tz(dt)}',False)"
            for session in updates
        ]
        self.mock_pg_hook.run(
            f""" INSERT INTO direct_ingest_raw_big_query_file_metadata (file_id, region_code, raw_data_instance, file_tag, update_datetime, is_invalidated)
            VALUES {','.join(values)}
            """
        )

    def test_write_sessions(self) -> None:
        start_time = datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC)

        dag_run = create_autospec(DagRun)
        dag_run.start_date = start_time
        self.mock_context.__getitem__.return_value = dag_run

        updates = [
            RawBigQueryFileProcessedTime(
                file_id=1,
                file_processed_time=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileProcessedTime(
                file_id=2,
                file_processed_time=datetime.datetime(
                    2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileProcessedTime(
                file_id=3,
                file_processed_time=datetime.datetime(
                    2024, 1, 3, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileProcessedTime(
                file_id=4,
                file_processed_time=datetime.datetime(
                    2024, 1, 4, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileProcessedTime(
                file_id=5,
                file_processed_time=datetime.datetime(
                    2024, 1, 5, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
        ]

        self._seed_bq_metadata(updates)

        self.mock_operator.xcom_pull.return_value = [u.serialize() for u in updates]

        self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        persisted_records = self.mock_pg_hook.get_records(
            "select file_id, file_processed_time from direct_ingest_raw_big_query_file_metadata order by file_id;"
        )

        for i, record in enumerate(persisted_records):
            assert record[0] == updates[i].file_id
            assert record[1] == updates[i].file_processed_time
