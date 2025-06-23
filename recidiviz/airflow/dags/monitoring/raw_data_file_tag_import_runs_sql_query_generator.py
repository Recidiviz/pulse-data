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
"""A cloudsql generator for retrieving recent file import runs at the file tag level"""
import datetime
import logging
from typing import List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.monitoring.file_tag_import_run_summary import (
    FileTagImportRunSummary,
)
from recidiviz.airflow.dags.monitoring.job_run import JobRunState
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.utils.cloud_sql import (
    postgres_formatted_current_datetime_utc_str,
)
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
    DirectIngestRawFileImportStatusBucket,
)
from recidiviz.utils.string import StrictStringFormatter

GET_RECENT_FILE_TAG_IMPORTS_QUERY_TEMPLATE = """
WITH imports_in_lookback as (
    SELECT 
        dag_run_id,
        import_run_start,
        region_code, 
        raw_data_instance,
        import_run_id
    FROM direct_ingest_raw_file_import_run
    WHERE 
        EXTRACT(epoch FROM '{current_datetime}'::timestamp with time zone - import_run_start) <= {lookback_in_seconds}
        AND raw_data_instance = 'PRIMARY'
), recent_file_imports as (
    SELECT 
        ir.dag_run_id,
        ir.import_run_start,
        ir.region_code, 
        ir.raw_data_instance,
        fi.file_id,
        CASE
            WHEN fi.import_status in {pending_statuses} THEN {pending_value}
            WHEN fi.import_status in {success_statuses} THEN {success_value}
            WHEN fi.import_status in {failed_statuses} THEN {failed_value}
            ELSE {unknown_value}
        END as import_state,
        import_status,
        fi.error_message,
        bq.file_tag,
        bq.update_datetime
    FROM imports_in_lookback as ir
    INNER JOIN direct_ingest_raw_file_import as fi
    ON ir.import_run_id = fi.import_run_id
    INNER JOIN direct_ingest_raw_big_query_file_metadata as bq
    ON fi.file_id = bq.file_id
)
SELECT
    import_run_start,
    region_code, 
    raw_data_instance,
    file_tag,
    MAX(import_state) as file_tag_import_state,
    -- we filter down to just FAILED statuses since they are the details we care about
    json_agg(
        json_build_object(
            'file_id', file_id,
            'update_datetime', update_datetime,
            'failed_file_import_status', import_status,
            'error_message', error_message
        )
    ) FILTER (WHERE import_status in {failed_statuses}) as failed_file_import_runs
FROM recent_file_imports
GROUP BY (
    dag_run_id, 
    import_run_start,
    region_code, 
    raw_data_instance,
    file_tag
);
"""


class RawDataFileTagImportRunSqlQueryGenerator(CloudSqlQueryGenerator[List[str]]):
    """A cloudsql generator that builds file-tag level import summaries for each raw
    data import DAG run.
    """

    def __init__(self, *, lookback: datetime.timedelta) -> None:
        super().__init__()
        self._lookback = lookback

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[str]:

        file_tag_import_run_summaries = [
            FileTagImportRunSummary.from_db_row(
                import_run_start=file_tag_import_run_summary[0],
                state_code_str=file_tag_import_run_summary[1],
                raw_data_instance_str=file_tag_import_run_summary[2],
                file_tag=file_tag_import_run_summary[3],
                file_tag_import_state_int=file_tag_import_run_summary[4],
                failed_file_import_runs_json=file_tag_import_run_summary[5],
            )
            for file_tag_import_run_summary in postgres_hook.get_records(
                self._get_recent_file_import_runs_sql_query()
            )
        ]

        if not file_tag_import_run_summaries:
            logging.error(
                "Found no import run summaries for last [%s] seconds",
                int(self._lookback.total_seconds()),
            )

        return [
            file_tag_import_run_summary.serialize()
            for file_tag_import_run_summary in file_tag_import_run_summaries
        ]

    @staticmethod
    def _statuses_as_str(statuses: frozenset[DirectIngestRawFileImportStatus]) -> str:
        status_str = ",".join([f"'{status.value}'" for status in statuses])
        return f"({status_str})"

    def _get_recent_file_import_runs_sql_query(self) -> str:
        return StrictStringFormatter().format(
            GET_RECENT_FILE_TAG_IMPORTS_QUERY_TEMPLATE,
            current_datetime=postgres_formatted_current_datetime_utc_str(),
            lookback_in_seconds=int(self._lookback.total_seconds()),
            pending_statuses=self._statuses_as_str(
                DirectIngestRawFileImportStatusBucket.in_progress_statuses()
            ),
            pending_value=JobRunState.PENDING.value,
            success_statuses=self._statuses_as_str(
                DirectIngestRawFileImportStatusBucket.succeeded_statuses()
            ),
            success_value=JobRunState.SUCCESS.value,
            failed_statuses=self._statuses_as_str(
                DirectIngestRawFileImportStatusBucket.failed_statuses()
            ),
            failed_value=JobRunState.FAILED.value,
            unknown_value=JobRunState.UNKNOWN.value,
        )
