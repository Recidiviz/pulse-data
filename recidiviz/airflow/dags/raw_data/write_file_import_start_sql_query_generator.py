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
"""Class for starting an import run and opening file import objects in the operations db"""
import datetime
import logging
from typing import Dict, List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.metadata import (
    BQ_METADATA_TO_IMPORT_IN_FUTURE_RUNS,
    BQ_METADATA_TO_IMPORT_THIS_RUN,
    IMPORT_RUN_ID,
)
from recidiviz.airflow.dags.utils.cloud_sql import postgres_formatted_datetime_with_tz
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import RawBigQueryFileMetadata
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

ADD_IMPORT_RUN_SQL_QUERY = """
INSERT INTO direct_ingest_raw_file_import_run (dag_run_id, import_run_start, region_code, raw_data_instance)
VALUES {value}
RETURNING import_run_id;"""

ADD_FILE_IMPORT_SQL_QUERY = """
INSERT INTO direct_ingest_raw_file_import (file_id, import_run_id, import_status, region_code, raw_data_instance)
VALUES {values};"""


class WriteImportStartCloudSqlGenerator(CloudSqlQueryGenerator[Dict[str, int]]):
    """Class for starting an import run and opening file import objects in the operations db"""

    def __init__(
        self,
        region_code: str,
        raw_data_instance: DirectIngestInstance,
        files_to_import_this_run_task_id: str,
    ) -> None:
        super().__init__()
        self._region_code = region_code
        self._raw_data_instance = raw_data_instance
        self._files_to_import_this_run_task_id = files_to_import_this_run_task_id

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> Dict[str, int]:

        bq_file_metadata_to_import_this_run = [
            RawBigQueryFileMetadata.deserialize(bq_metadata_str)
            for bq_metadata_str in operator.xcom_pull(
                context=context,
                task_ids=self._files_to_import_this_run_task_id,
                key=BQ_METADATA_TO_IMPORT_THIS_RUN,
            )
        ]
        bq_file_metadata_to_import_in_future_runs = [
            RawBigQueryFileMetadata.deserialize(bq_metadata_str)
            for bq_metadata_str in operator.xcom_pull(
                context=context,
                task_ids=self._files_to_import_this_run_task_id,
                key=BQ_METADATA_TO_IMPORT_IN_FUTURE_RUNS,
            )
        ]

        if (
            not bq_file_metadata_to_import_this_run
            and bq_file_metadata_to_import_in_future_runs
        ):
            raise ValueError(
                "We should never defer files to run until future imports without having files to run during this import!"
            )

        if not bq_file_metadata_to_import_this_run:
            logging.info("Found no metadata to import this run; returning.")
            return {}

        dag_run = context["dag_run"]
        if not dag_run:
            raise ValueError(
                "Dag run not passed to task. Should be automatically set due to "
                "function being a task."
            )
        dag_run_start_time = assert_type(dag_run.start_date, datetime.datetime)
        dag_run_id = assert_type(dag_run.run_id, str)

        import_run_id_result = postgres_hook.get_records(
            self._create_insert_into_import_run_sql_query(
                dag_run_id, dag_run_start_time
            ),
        )

        if len(import_run_id_result) != 1:
            raise ValueError("Expected only a single row from import run id query")

        import_run_id = assert_type(import_run_id_result[0][0], int)

        postgres_hook.get_records(
            self._create_file_import_sql_query(
                import_run_id=import_run_id,
                bq_metadata=bq_file_metadata_to_import_this_run,
                status=DirectIngestRawFileImportStatus.STARTED,
            )
        )

        if bq_file_metadata_to_import_in_future_runs:
            # TODO(#30169) is there a scale where we would not want to insert them all at
            # once and instead do batches?
            postgres_hook.get_records(
                self._create_file_import_sql_query(
                    import_run_id=import_run_id,
                    bq_metadata=bq_file_metadata_to_import_in_future_runs,
                    status=DirectIngestRawFileImportStatus.DEFERRED,
                )
            )

        return {IMPORT_RUN_ID: import_run_id}

    def _file_import_row_from_metadata(
        self,
        *,
        import_run_id: int,
        bq_metadata_item: RawBigQueryFileMetadata,
        status: DirectIngestRawFileImportStatus,
    ) -> str:
        # n.b. the order of the values in this column MUST match the order of the columns
        # specified in ADD_FILE_IMPORT_SQL_QUERY
        row = [
            f"{assert_type(bq_metadata_item.file_id, int)}",
            f"{import_run_id}",
            f"'{status.value}'",
            f"'{self._region_code}'",
            f"'{self._raw_data_instance.value}'",
        ]

        return f"({','.join(row)})"

    def _create_file_import_sql_query(
        self,
        *,
        import_run_id: int,
        bq_metadata: List[RawBigQueryFileMetadata],
        status: DirectIngestRawFileImportStatus,
    ) -> str:
        start_file_import_rows = ",\n ".join(
            [
                self._file_import_row_from_metadata(
                    import_run_id=import_run_id,
                    bq_metadata_item=bq_metadata_item,
                    status=status,
                )
                for bq_metadata_item in bq_metadata
            ]
        )

        return StrictStringFormatter().format(
            ADD_FILE_IMPORT_SQL_QUERY, values=start_file_import_rows
        )

    def _create_insert_into_import_run_sql_query(
        self,
        dag_run_id: str,
        dag_run_start_time: datetime.datetime,
    ) -> str:
        import_run_row = f"('{dag_run_id}', '{postgres_formatted_datetime_with_tz(dag_run_start_time)}', '{self._region_code.upper()}', '{self._raw_data_instance.value}')"

        return StrictStringFormatter().format(
            ADD_IMPORT_RUN_SQL_QUERY, value=import_run_row
        )
