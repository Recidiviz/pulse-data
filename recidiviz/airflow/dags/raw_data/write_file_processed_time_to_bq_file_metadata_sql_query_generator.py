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
"""A CloudSQLQueryGenerator that writes file_processed_time to the operations db"""
from typing import List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.utils.cloud_sql import postgres_formatted_datetime_with_tz
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileProcessedTime,
)
from recidiviz.utils.string import StrictStringFormatter

UPDATE_BQ_METADATA_FILE_PROCESSED_TIME = """
UPDATE direct_ingest_raw_big_query_file_metadata AS m SET file_processed_time = v.file_processed_time::timestamp with time zone
FROM ( VALUES
    {values}
) as v(file_id, file_processed_time)
WHERE m.file_id = v.file_id
"""


class WriteFileProcessedTimeToBQFileMetadataSqlQueryGenerator(
    CloudSqlQueryGenerator[None]
):
    """A CloudSQLQueryGenerator that writes file_processed_time to the operations db"""

    def __init__(
        self,
        write_import_session_task_id: str,
    ) -> None:
        super().__init__()
        self._write_import_session_task_id = write_import_session_task_id

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> None:

        processed_times_to_set = [
            RawBigQueryFileProcessedTime.deserialize(metadata_str)
            for metadata_str in operator.xcom_pull(
                context,
                key="return_value",
                task_ids=self._write_import_session_task_id,
            )
        ]

        if not processed_times_to_set:
            return

        postgres_hook.run(
            self._update_file_processed_time_sql_query(processed_times_to_set)
        )

    @staticmethod
    def _update_file_processed_time_sql_query(
        processed_times_to_set: List[RawBigQueryFileProcessedTime],
    ) -> str:
        values = ",".join(
            [
                f"({processed_time_to_set.file_id}, '{postgres_formatted_datetime_with_tz(processed_time_to_set.file_processed_time)}')"
                for processed_time_to_set in processed_times_to_set
            ]
        )
        return StrictStringFormatter().format(
            UPDATE_BQ_METADATA_FILE_PROCESSED_TIME, values=values
        )
