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
"""A CloudSQLQueryGenerator that writes import session info to the operations db"""
import datetime
from typing import List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.metadata import IMPORT_SUMMARIES
from recidiviz.airflow.dags.utils.cloud_sql import (
    postgres_formatted_current_datetime_utc_str,
    postgres_formatted_datetime_with_tz,
)
from recidiviz.common.constants.operations.direct_ingest_raw_data_import_session import (
    DirectIngestRawDataImportSessionStatus,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileImportSummary,
    RawBigQueryFileProcessedTime,
)
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

ADD_IMPORT_SESSION_SQL_QUERY = """
INSERT INTO direct_ingest_raw_data_import_session (file_id, import_status, import_start, import_end, region_code, raw_data_instance, historical_diffs_active, raw_rows, net_new_or_updated_rows, deleted_rows)
VALUES {values}
RETURNING file_id, import_status, import_end;"""


class WriteImportSessionSqlQueryGenerator(CloudSqlQueryGenerator[List[str]]):
    """Custom query generator that writes import summaries to the import sessions
    operations table
    """

    def __init__(
        self,
        region_code: str,
        raw_data_instance: DirectIngestInstance,
        coalesce_results_and_errors_task_id: str,
    ) -> None:
        super().__init__()
        self._region_code = region_code
        self._raw_data_instance = raw_data_instance
        self._coalesce_results_and_errors_task_id = coalesce_results_and_errors_task_id

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[str]:

        import_summaries = [
            RawBigQueryFileImportSummary.deserialize(import_summary_str)
            for import_summary_str in operator.xcom_pull(
                context,
                key=IMPORT_SUMMARIES,
                task_ids=self._coalesce_results_and_errors_task_id,
            )
        ]

        if not import_summaries:
            return []

        dag_run = context["dag_run"]
        if not dag_run:
            raise ValueError(
                "Dag run not passed to task. Should be automatically set due to "
                "function being a task."
            )

        # TODO(#30169) is there a scale where we would not want to insert them all at
        # once and instead do batches?
        records = postgres_hook.get_records(
            self._create_insert_into_import_session_sql_query(
                import_summaries, assert_type(dag_run.start_date, datetime.datetime)
            ),
        )

        bq_update_ready_metadata = [
            RawBigQueryFileProcessedTime.from_new_import_session_row(record)
            for record in records
            if DirectIngestRawDataImportSessionStatus(record[1])
            == DirectIngestRawDataImportSessionStatus.SUCCEEDED
        ]

        return [
            bq_update_ready.serialize() for bq_update_ready in bq_update_ready_metadata
        ]

    def _import_session_row_from_summary(
        self,
        import_summary: RawBigQueryFileImportSummary,
        import_start: datetime.datetime,
    ) -> str:
        # n.b. the order of the values in this column MUST match the order of the columns
        # specified in ADD_IMPORT_SESSION_SQL_QUERY
        row = [
            import_summary.file_id,
            f"'{import_summary.import_status.value}'",
            f"'{postgres_formatted_datetime_with_tz(import_start)}'",
            f"'{postgres_formatted_current_datetime_utc_str()}'",
            f"'{self._region_code}'",
            f"'{self._raw_data_instance.value}'",
            import_summary.historical_diffs_active,
            import_summary.raw_rows,
            import_summary.net_new_or_updated_rows,
            import_summary.deleted_rows,
        ]
        row_as_str = [str(value) if not value is None else "NULL" for value in row]

        return f"({','.join(row_as_str)})"

    def _create_insert_into_import_session_sql_query(
        self,
        import_summaries: List[RawBigQueryFileImportSummary],
        import_start: datetime.datetime,
    ) -> str:

        import_session_strings = ",".join(
            self._import_session_row_from_summary(import_summary, import_start)
            for import_summary in import_summaries
        )

        return StrictStringFormatter().format(
            ADD_IMPORT_SESSION_SQL_QUERY, values=import_session_strings
        )
