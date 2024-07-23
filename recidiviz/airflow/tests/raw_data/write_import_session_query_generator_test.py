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
"""Unit tests for WriteImportSessionSqlQueryGenerator"""
import datetime
from typing import List, NamedTuple
from unittest.mock import create_autospec

from airflow.models import DagRun
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from freezegun import freeze_time

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.write_import_session_query_generator import (
    WriteImportSessionSqlQueryGenerator,
)
from recidiviz.airflow.dags.utils.cloud_sql import postgres_formatted_datetime_with_tz
from recidiviz.airflow.tests.test_utils import CloudSqlQueryGeneratorUnitTest
from recidiviz.common.constants.operations.direct_ingest_raw_data_import_session import (
    DirectIngestRawDataImportSessionStatus,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    ImportSessionSummary,
    RawBigQueryFileProcessedTime,
)
from recidiviz.persistence.database.schema.operations.schema import OperationsBase
from recidiviz.utils.types import assert_type

ImportSession = NamedTuple(
    "ImportSession",
    [
        ("import_session_id", int),
        ("file_id", int),
        ("import_status", str),
        ("import_start", datetime.datetime),
        ("import_end", datetime.datetime),
        ("region_code", str),
        ("raw_data_instance", str),
        ("historical_diffs_active", bool),
    ],
)


class WriteImportSessionSqlQueryGeneratorTest(CloudSqlQueryGeneratorUnitTest):
    """Unit tests for WriteImportSessionSqlQueryGenerator"""

    metas = [OperationsBase]
    mock_operator = create_autospec(CloudSqlQueryOperator)
    mock_context = create_autospec(Context)

    def setUp(self) -> None:
        super().setUp()
        self.generator = WriteImportSessionSqlQueryGenerator(
            region_code="US_XX",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            coalesce_results_and_errors_task_id="task_id",
        )
        self.mock_pg_hook = PostgresHook(postgres_conn_id=self.conn_id)

    def test_write_no_sessions(self) -> None:
        self.mock_operator.xcom_pull.return_value = []
        mock_hook = create_autospec(PostgresHook)
        result = self.generator.execute_postgres_query(
            self.mock_operator, mock_hook, self.mock_context
        )

        assert not result
        assert mock_hook.get_records.assert_not_called

    def _seed_bq_metadata(self, import_sessions: List[ImportSessionSummary]) -> None:
        dt = datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC)
        values = [
            f"({session.file_id},'US_XX','PRIMARY','tag_a','{postgres_formatted_datetime_with_tz(dt)}',False)"
            for session in import_sessions
        ]
        self.mock_pg_hook.run(
            f""" INSERT INTO direct_ingest_raw_big_query_file_metadata (file_id, region_code, raw_data_instance, file_tag, update_datetime, is_invalidated)
            VALUES {','.join(values)}
            """
        )

    def test_write_sessions(self) -> None:
        start_time = datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC)
        end_time = datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC)

        dag_run = create_autospec(DagRun)
        dag_run.start_date = start_time
        self.mock_context.__getitem__.return_value = dag_run

        import_sessions = [
            ImportSessionSummary(
                file_id=1,
                import_status=DirectIngestRawDataImportSessionStatus.SUCCEEDED,
                historical_diffs_active=True,
                raw_rows=2,
                net_new_or_updated_rows=2,
                deleted_rows=2,
            ),
            ImportSessionSummary(
                file_id=2,
                import_status=DirectIngestRawDataImportSessionStatus.SUCCEEDED,
                historical_diffs_active=False,
                raw_rows=1,
            ),
            ImportSessionSummary(
                file_id=3,
                import_status=DirectIngestRawDataImportSessionStatus.FAILED_LOAD_STEP,
                historical_diffs_active=True,
            ),
            ImportSessionSummary(
                file_id=4,
                import_status=DirectIngestRawDataImportSessionStatus.FAILED_PRE_IMPORT_NORMALIZATION_STEP,
                historical_diffs_active=True,
            ),
            ImportSessionSummary(
                file_id=5,
                import_status=DirectIngestRawDataImportSessionStatus.FAILED_UNKNOWN,
                historical_diffs_active=True,
            ),
        ]

        self._seed_bq_metadata(import_sessions)

        self.mock_operator.xcom_pull.return_value = [
            session.serialize() for session in import_sessions
        ]

        with freeze_time(end_time):
            result = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        assert len(result) == 2
        update_metadata = [
            RawBigQueryFileProcessedTime.deserialize(result_str)
            for result_str in result
        ]

        assert update_metadata[0].file_id == 1
        assert update_metadata[1].file_id == 2

        persisted_records = [
            ImportSession(*row)
            for row in assert_type(
                self.mock_pg_hook.get_records(
                    """SELECT 
                import_session_id, 
                file_id, 
                import_status, 
                import_start, 
                import_end, 
                region_code, 
                raw_data_instance, 
                historical_diffs_active 
            FROM direct_ingest_raw_data_import_session 
            ORDER BY file_id;"""
                ),
                list,
            )
        ]

        for i, record in enumerate(persisted_records):
            assert isinstance(record.import_session_id, int)
            assert record.file_id == import_sessions[i].file_id
            assert record.import_status == import_sessions[i].import_status.value
            assert record.import_start == start_time
            assert record.import_end == end_time
            assert record.region_code == "US_XX"
            assert record.raw_data_instance == "PRIMARY"
            assert (
                record.historical_diffs_active
                == import_sessions[i].historical_diffs_active
            )
