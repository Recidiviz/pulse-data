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
"""A CloudSQLQueryGenerator that writes import info to the operations db"""
import datetime
from typing import Dict, List, NamedTuple, Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.metadata import FILE_IMPORTS, IMPORT_RUN_ID
from recidiviz.airflow.dags.utils.cloud_sql import postgres_formatted_datetime_with_tz
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileProcessedTime,
    RawFileImport,
)
from recidiviz.utils.string import StrictStringFormatter

# n.b. the order of the fields here MUST match the select order of the columns listed
# in GET_FILE_IMPORTS_FOR_IMPORT_RUN_ID
FileImport = NamedTuple(
    "FileImport",
    [
        ("file_import_id", int),
        ("import_run_id", int),
        ("file_id", int),
        ("import_status", str),
    ],
)


GET_FILE_IMPORTS_FOR_IMPORT_RUN_ID = """
SELECT file_import_id, import_run_id, file_id, import_status
FROM direct_ingest_raw_file_import
WHERE import_run_id = {import_run_id}
AND import_status != 'DEFERRED'
"""


UPDATE_FILE_IMPORT_BY_FILE_IMPORT_ID = """
UPDATE direct_ingest_raw_file_import AS o
SET 
  import_status = v.import_status::direct_ingest_file_import_status,
  historical_diffs_active = v.historical_diffs_active::boolean,
  raw_rows = v.raw_rows::numeric,
  net_new_or_updated_rows = v.net_new_or_updated_rows::numeric,
  deleted_rows = v.deleted_rows::numeric,
  error_message = v.error_message::text
FROM ( VALUES
  {file_import_rows}
) AS v(file_import_id, import_status, historical_diffs_active, raw_rows, net_new_or_updated_rows, deleted_rows, error_message)
WHERE o.file_import_id = v.file_import_id
RETURNING o.file_import_id, o.import_run_id, o.file_id, o.import_status;"""


WRITE_IMPORT_RUN_END = """
UPDATE direct_ingest_raw_file_import_run SET import_run_end = '{import_run_end}'
WHERE import_run_id = {import_run_id};"""


class WriteImportCompletionsSqlQueryGenerator(CloudSqlQueryGenerator[List[str]]):
    """Custom query generator that writes import summaries to the import
    operations table
    """

    def __init__(
        self,
        region_code: str,
        raw_data_instance: DirectIngestInstance,
        coalesce_results_and_errors_task_id: str,
        write_import_start_task_id: str,
    ) -> None:
        super().__init__()
        self._region_code = region_code
        self._raw_data_instance = raw_data_instance
        self._coalesce_results_and_errors_task_id = coalesce_results_and_errors_task_id
        self._write_import_start_task_id = write_import_start_task_id

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[str]:

        import_run_id_xcom: Dict[str, int] = operator.xcom_pull(
            context,
            task_ids=self._write_import_start_task_id,
        )

        if not import_run_id_xcom or not isinstance(
            import_run_id := import_run_id_xcom.get(IMPORT_RUN_ID), int
        ):
            raise ValueError(
                f"Could not retrieve import_run_id from upstream; found: [{import_run_id_xcom}]"
            )

        existing_file_imports = [
            FileImport(*file_import)
            for file_import in postgres_hook.get_records(
                self._create_get_import_current_file_imports_for_import_run_id_sql_query(
                    import_run_id
                )
            )
        ]

        file_import_results = [
            RawFileImport.deserialize(file_import_str)
            for file_import_str in operator.xcom_pull(
                context,
                key=FILE_IMPORTS,
                task_ids=self._coalesce_results_and_errors_task_id,
            )
        ]

        # TODO(#40150) add validation here that all statuses we are matching to file_imports
        # are STARTED? so as not to overwrite import statuses? what implications does this
        # have (if we do or do not do this) for if we retry/clear airflow tasks in the
        # raw data import DAG?
        existing_file_import_to_file_import_result = (
            self._match_existing_file_import_to_result(
                existing_file_imports, file_import_results
            )
        )

        # TODO(#40149) is there a scale where we would not want to insert them all at
        # once and instead do batches?
        updated_file_imports = [
            FileImport(*file_import)
            for file_import in postgres_hook.get_records(
                self._create_update_file_import_run_sql_query(
                    existing_file_import_to_file_import_result
                ),
            )
        ]

        import_run_end = datetime.datetime.now(tz=datetime.UTC)
        postgres_hook.run(
            self._create_write_import_run_end_sql_query(import_run_id, import_run_end)
        )

        bq_update_ready_metadata = [
            RawBigQueryFileProcessedTime.from_file_id_and_import_run_end(
                updated_file_import.file_id, import_run_end
            )
            for updated_file_import in updated_file_imports
            if DirectIngestRawFileImportStatus(updated_file_import.import_status)
            == DirectIngestRawFileImportStatus.SUCCEEDED
        ]

        return [
            bq_update_ready.serialize() for bq_update_ready in bq_update_ready_metadata
        ]

    def _update_file_import_from_result(
        self,
        existing_file_import: FileImport,
        file_import_result: Optional[RawFileImport],
    ) -> str:
        """Builds a row to insert into PG from a file import result."""
        # n.b. the order of the values in this column MUST match the order of the columns
        # specified in UPDATE_FILE_IMPORT_BY_FILE_IMPORT_ID::v
        row = []
        if not file_import_result:
            row = [
                existing_file_import.file_import_id,
                f"'{DirectIngestRawFileImportStatus.FAILED_UNKNOWN.value}'",
                None,
                None,
                None,
                None,
                "'Could not find a RawFileImport object associated with the file_import_id, "
                "despite it being marked as STARTED. This likely means that there was a "
                "DAG-level failure occurred during this import run.'",
            ]
        else:
            row = [
                existing_file_import.file_import_id,
                f"'{file_import_result.import_status.value}'",
                file_import_result.historical_diffs_active,
                file_import_result.raw_rows,
                file_import_result.net_new_or_updated_rows,
                file_import_result.deleted_rows,
                (
                    f"'{file_import_result.error_message_quote_safe()}'"
                    if file_import_result.error_message
                    else None
                ),
            ]
        row_as_str = [str(value) if value is not None else "NULL" for value in row]
        return f"({','.join(row_as_str)})"

    def _create_update_file_import_run_sql_query(
        self,
        existing_file_import_to_file_import_result: Dict[
            FileImport, Optional[RawFileImport]
        ],
    ) -> str:

        file_import_rows = ",".join(
            self._update_file_import_from_result(
                existing_file_import, file_import_result
            )
            for existing_file_import, file_import_result in existing_file_import_to_file_import_result.items()
        )

        return StrictStringFormatter().format(
            UPDATE_FILE_IMPORT_BY_FILE_IMPORT_ID, file_import_rows=file_import_rows
        )

    @staticmethod
    def _create_get_import_current_file_imports_for_import_run_id_sql_query(
        import_run_id: int,
    ) -> str:
        return StrictStringFormatter().format(
            GET_FILE_IMPORTS_FOR_IMPORT_RUN_ID, import_run_id=import_run_id
        )

    @staticmethod
    def _match_existing_file_import_to_result(
        existing_file_imports: List[FileImport],
        file_import_results: List[RawFileImport],
    ) -> Dict[FileImport, Optional[RawFileImport]]:
        imported_file_id_to_file_import = {
            file_import.file_id: file_import for file_import in existing_file_imports
        }

        existing_file_import_to_file_import_result: Dict[
            FileImport, Optional[RawFileImport]
        ] = {file_import: None for file_import in existing_file_imports}

        for file_import_result in file_import_results:
            if file_import_result.file_id not in imported_file_id_to_file_import:
                raise ValueError(
                    f"Found file_id [{file_import_result.file_id}] that does not have a corresponding file import object"
                )
            existing_file_import_to_file_import_result[
                imported_file_id_to_file_import[file_import_result.file_id]
            ] = file_import_result

        return existing_file_import_to_file_import_result

    @staticmethod
    def _create_write_import_run_end_sql_query(
        import_run_id: int, import_run_end: datetime.datetime
    ) -> str:
        return StrictStringFormatter().format(
            WRITE_IMPORT_RUN_END,
            import_run_id=import_run_id,
            import_run_end=postgres_formatted_datetime_with_tz(import_run_end),
        )
