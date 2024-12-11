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
"""Unit tests for WriteImportCompletionsSqlQueryGenerator"""
import datetime
from typing import Any, Dict, List, NamedTuple, Optional
from unittest.mock import create_autospec

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from freezegun import freeze_time

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.metadata import FILE_IMPORTS, IMPORT_RUN_ID
from recidiviz.airflow.dags.raw_data.write_import_completions_query_generator import (
    WriteImportCompletionsSqlQueryGenerator,
)
from recidiviz.airflow.dags.utils.cloud_sql import postgres_formatted_datetime_with_tz
from recidiviz.airflow.tests.test_utils import CloudSqlQueryGeneratorUnitTest
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileProcessedTime,
    RawFileImport,
)
from recidiviz.persistence.database.schema.operations.schema import OperationsBase
from recidiviz.utils.types import assert_type

FileImport = NamedTuple(
    "FileImport",
    [
        ("file_import_id", int),
        ("file_id", int),
        ("import_status", str),
        ("region_code", str),
        ("raw_data_instance", str),
        ("raw_rows", int),
        ("net_new_or_updated_rows", int),
        ("deleted_rows", int),
        ("historical_diffs_active", bool),
        ("error_message", str),
    ],
)


class WriteImportCompletionsSqlQueryGeneratorTest(CloudSqlQueryGeneratorUnitTest):
    """Unit tests for WriteImportCompletionsSqlQueryGenerator"""

    metas = [OperationsBase]
    mock_operator = create_autospec(CloudSqlQueryOperator)

    def setUp(self) -> None:
        super().setUp()
        self.generator = WriteImportCompletionsSqlQueryGenerator(
            region_code="US_XX",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            coalesce_results_and_errors_task_id="task_id",
            write_import_start_task_id="task_id_2",
        )
        self.mock_pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.mock_context = create_autospec(Context)
        self.mock_operator = create_autospec(CloudSqlQueryOperator)
        self.mock_operator.xcom_pull.side_effect = self._xcom_pull
        self.import_run_id_xcom: Optional[Dict[str, Optional[int]]] = None
        self.file_imports: List[RawFileImport] = []

    def _xcom_pull(
        self, _context: Context, key: str = "return_value", **_kwargs: Any
    ) -> Any:
        if key == "return_value":
            return self.import_run_id_xcom
        if key == FILE_IMPORTS:
            return [s.serialize() for s in self.file_imports]

        raise ValueError(f"Unexpected key: {key}")

    def _seed_bq_metadata(self, file_imports: List[RawFileImport]) -> None:
        dt = datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC)
        values = [
            f"({summary.file_id},'US_XX','PRIMARY','tag_a','{postgres_formatted_datetime_with_tz(dt)}',False)"
            for summary in file_imports
        ]
        self.mock_pg_hook.run(
            f""" INSERT INTO direct_ingest_raw_big_query_file_metadata (file_id, region_code, raw_data_instance, file_tag, update_datetime, is_invalidated)
            VALUES {','.join(values)}
            """
        )

    def _seed_deferred(self, file_ids: list[int], import_run_id: int) -> None:
        dt = datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC)
        bq_values = [
            f"({file_id},'US_XX','PRIMARY','tag_a','{postgres_formatted_datetime_with_tz(dt)}',False)"
            for file_id in file_ids
        ]
        self.mock_pg_hook.run(
            f""" INSERT INTO direct_ingest_raw_big_query_file_metadata (file_id, region_code, raw_data_instance, file_tag, update_datetime, is_invalidated)
            VALUES {','.join(bq_values)}
            """
        )
        import_values = ",".join(
            [
                f"({file_id}, {import_run_id}, 'DEFERRED', 'US_XX', 'PRIMARY')"
                for file_id in file_ids
            ]
        )
        self.mock_pg_hook.run(
            f"""INSERT INTO direct_ingest_raw_file_import (file_id, import_run_id, import_status, region_code, raw_data_instance)
                VALUES {import_values};
        """
        )

    def _seed_import_run(self, file_imports: List[RawFileImport]) -> int:
        dt = datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC)
        import_run_id_result = assert_type(
            self.mock_pg_hook.get_records(
                f""" INSERT INTO direct_ingest_raw_file_import_run (dag_run_id, import_run_start, region_code, raw_data_instance)
                VALUES ('abc_123', '{postgres_formatted_datetime_with_tz(dt)}', 'US_XX', 'PRIMARY')
                RETURNING import_run_id;
            """
            ),
            list,
        )

        import_run_id = assert_type(import_run_id_result[0][0], int)
        rows = ",".join(
            [
                f"({s.file_id}, {import_run_id}, 'STARTED', 'US_XX', 'PRIMARY')"
                for s in file_imports
            ]
        )
        self.mock_pg_hook.run(
            f"""INSERT INTO direct_ingest_raw_file_import (file_id, import_run_id, import_status, region_code, raw_data_instance)
                VALUES {rows};
        """
        )

        self.import_run_id_xcom = {IMPORT_RUN_ID: import_run_id}

        return import_run_id

    def test_write_no_import_written(self) -> None:
        mock_hook = create_autospec(PostgresHook)
        with self.assertRaisesRegex(
            ValueError,
            r"Could not retrieve import_run_id from upstream; found: \[.*\]",
        ):
            _ = self.generator.execute_postgres_query(
                self.mock_operator, mock_hook, self.mock_context
            )

    def test_write_import_id_is_none(self) -> None:
        mock_hook = create_autospec(PostgresHook)
        self.import_run_id_xcom = {IMPORT_RUN_ID: None}
        with self.assertRaisesRegex(
            ValueError,
            r"Could not retrieve import_run_id from upstream; found: \[.*\]",
        ):
            _ = self.generator.execute_postgres_query(
                self.mock_operator, mock_hook, self.mock_context
            )

    def test_write_results(self) -> None:
        end_time = datetime.datetime(2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC)

        self.file_imports = [
            RawFileImport(
                file_id=1,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=True,
                raw_rows=2,
                net_new_or_updated_rows=2,
                deleted_rows=2,
                error_message=None,
            ),
            RawFileImport(
                file_id=2,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
                raw_rows=2,
                error_message=None,
            ),
            RawFileImport(
                file_id=3,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
                raw_rows=1,
                error_message=None,
            ),
            RawFileImport(
                file_id=4,
                import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
                historical_diffs_active=True,
                error_message="""FAILED_LOAD_STEP for [tagBasic] for [recidiviz-staging-direct-ingest-state-us-testing/unprocessed_2021-11-29T00:00:00:000000_raw_tagBasic.csv] failed with:

400 Syntax error: Unclosed identifier literal at [4:180]; reason: invalidQuery, location: query, message: Syntax error: Unclosed identifier literal at [4:180]

Location: US
Job ID: <some-job-id>

Traceback (most recent call last):
  File "/home/airflow/gcs/dags/recidiviz/airflow/dags/raw_data/bq_load_tasks.py", line 112, in load_and_prep_paths_for_batch
    succeeded_loads.append(future.result())
                           ^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/concurrent/futures/_base.py", line 449, in result
    return self.__get_result()
           ^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/concurrent/futures/_base.py", line 401, in __get_result
    raise self._exception
  File "/opt/python3.11/lib/python3.11/concurrent/futures/thread.py", line 58, in run
    result = self.fn(*self.args, **self.kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/gcs/dags/recidiviz/ingest/direct/raw_data/direct_ingest_raw_file_load_manager.py", line 346, in load_and_prep_paths
    self.validator.run_raw_data_temp_table_validations(
  File "/home/airflow/gcs/dags/recidiviz/ingest/direct/raw_data/direct_ingest_raw_table_pre_import_validator.py", line 182, in run_raw_data_temp_table_validations
    failures = self._execute_validation_queries_concurrently(validations_to_run)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/gcs/dags/recidiviz/ingest/direct/raw_data/direct_ingest_raw_table_pre_import_validator.py", line 160, in _execute_validation_queries_concurrently
    bq_query_job_result_to_list_of_row_dicts(f.result())
                                             ^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/concurrent/futures/_base.py", line 449, in result
    return self.__get_result()
           ^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/concurrent/futures/_base.py", line 401, in __get_result
    raise self._exception
  File "/opt/python3.11/lib/python3.11/concurrent/futures/thread.py", line 58, in run
    result = self.fn(*self.args, **self.kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/google/cloud/bigquery/job/query.py", line 1590, in result
    do_get_result()
  File "/opt/python3.11/lib/python3.11/site-packages/google/api_core/retry/retry_unary.py", line 293, in retry_wrapped_func
    return retry_target(
           ^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/google/api_core/retry/retry_unary.py", line 153, in retry_target
    _retry_error_helper(
  File "/opt/python3.11/lib/python3.11/site-packages/google/api_core/retry/retry_base.py", line 212, in _retry_error_helper
    raise final_exc from source_exc
  File "/opt/python3.11/lib/python3.11/site-packages/google/api_core/retry/retry_unary.py", line 144, in retry_target
    result = target()
             ^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/google/cloud/bigquery/job/query.py", line 1579, in do_get_result
    super(QueryJob, self).result(retry=retry, timeout=timeout)
  File "/opt/python3.11/lib/python3.11/site-packages/google/cloud/bigquery/job/base.py", line 971, in result
    return super(_AsyncJob, self).result(timeout=timeout, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/site-packages/google/api_core/future/polling.py", line 261, in result
    raise self._exception
google.api_core.exceptions.BadRequest: 400 Syntax error: Unclosed identifier literal at [4:180]; reason: invalidQuery, location: query, message: Syntax error: Unclosed identifier literal at [4:180]""",
            ),
            RawFileImport(
                file_id=5,
                import_status=DirectIngestRawFileImportStatus.FAILED_VALIDATION_STEP,
                historical_diffs_active=True,
                error_message="""
FAILED_VALIDATION_STEP for [tagBasic] for [recidiviz-staging-direct-ingest-state-testing/unprocessed_2022-03-23T12:00:17:064931_raw_tagBasic.csv] failed with:

1 pre-import validation(s) failed for file [tagBasic]. If you wish [tagBasic] to be permanently excluded from any validation,  please add the validation_type and exemption_reason to import_blocking_validation_exemptions for a table-wide exemption or to import_blocking_column_validation_exemptions for a column-specific exemption in the raw file config.
Error: Found column [COL1] on raw file [tagBasic] with only null values.
Validation type: NONNULL_VALUES
Validation query: 
SELECT COL1
FROM recidiviz-staging.tagBasic.tagBasic
WHERE COL1 IS NOT NULL
LIMIT 1
""",
            ),
            RawFileImport(
                file_id=6,
                import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
                historical_diffs_active=True,
                error_message="""FAILED_LOAD_STEP for [tagBasic] for [recidiviz-staging-direct-ingest-state-testing/unprocessed_2021-11-29T00:00:00:000000_raw_tagBasic.csv] failed with:

known_values for CustodyLevel must not be empty
Traceback (most recent call last):
  File "/home/airflow/gcs/dags/recidiviz/airflow/dags/raw_data/bq_load_tasks.py", line 112, in load_and_prep_paths_for_batch
    succeeded_loads.append(future.result())
                           ^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/concurrent/futures/_base.py", line 449, in result
    return self.__get_result()
           ^^^^^^^^^^^^^^^^^^^
  File "/opt/python3.11/lib/python3.11/concurrent/futures/_base.py", line 401, in __get_result
    raise self._exception
  File "/opt/python3.11/lib/python3.11/concurrent/futures/thread.py", line 58, in run
    result = self.fn(*self.args, **self.kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/gcs/dags/recidiviz/ingest/direct/raw_data/direct_ingest_raw_file_load_manager.py", line 346, in load_and_prep_paths
    self.validator.run_raw_data_temp_table_validations(
  File "/home/airflow/gcs/dags/recidiviz/ingest/direct/raw_data/direct_ingest_raw_table_pre_import_validator.py", line 178, in run_raw_data_temp_table_validations
    ] = self._collect_validations_to_run(
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/gcs/dags/recidiviz/ingest/direct/raw_data/direct_ingest_raw_table_pre_import_validator.py", line 126, in _collect_validations_to_run
    col_validation_cls.create_column_validation(
  File "/home/airflow/gcs/dags/recidiviz/ingest/direct/raw_data/validations/known_values_column_validation.py", line 65, in create_column_validation
    raise ValueError(f"known_values for {column.name} must not be empty")
ValueError: known_values for tagBasic must not be empty""",
            ),
            RawFileImport(
                file_id=7,
                import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
                historical_diffs_active=True,
                error_message=(
                    "Could not locate a success or failure for this file_id "
                    "[7] despite it being marked for import. This"
                    "is likely indicative that there was a DAG-level failure "
                    "occurred during this import run."
                ),
            ),
            RawFileImport(
                file_id=8,
                import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
                historical_diffs_active=True,
                error_message="""Test here to make sure we are escaping:
                - double quotes: "
                - single quotes: '
                - backticks: `
                """,
            ),
            RawFileImport(
                file_id=9,
                import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
                historical_diffs_active=True,
                error_message="""Test here to make sure we are escaping:
                - double quotes, twice: "" .... "this is inside of double quotes"
                - single quotes, twice: '' .... 'this is inside of single quotes'
                - backticks, twice: ``, `this is inside of backticks`
                """,
            ),
        ]

        self._seed_bq_metadata(self.file_imports)
        self._seed_import_run(self.file_imports)

        with freeze_time(end_time):
            result = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        assert len(result) == 3
        update_metadata = [
            RawBigQueryFileProcessedTime.deserialize(result_str)
            for result_str in result
        ]

        assert update_metadata[0].file_id == 1
        assert update_metadata[1].file_id == 2
        assert update_metadata[2].file_id == 3

        persisted_records = [
            FileImport(*row)
            for row in assert_type(
                self.mock_pg_hook.get_records(
                    """SELECT 
                file_import_id, 
                file_id, 
                import_status, 
                region_code, 
                raw_data_instance, 
                raw_rows,
                net_new_or_updated_rows,
                deleted_rows,
                historical_diffs_active, 
                error_message
            FROM direct_ingest_raw_file_import
            ORDER BY file_id;"""
                ),
                list,
            )
        ]

        for i, record in enumerate(persisted_records):
            assert isinstance(record.file_import_id, int)
            assert record.file_id == self.file_imports[i].file_id
            assert record.import_status == self.file_imports[i].import_status.value
            assert record.region_code == "US_XX"
            assert record.raw_data_instance == "PRIMARY"
            assert (
                record.historical_diffs_active
                == self.file_imports[i].historical_diffs_active
            )
            assert record.error_message == self.file_imports[i].error_message

        import_runs = assert_type(
            self.mock_pg_hook.get_records(
                "SELECT import_run_id, import_run_end FROM direct_ingest_raw_file_import_run"
            ),
            list,
        )

        assert len(import_runs) == 1
        import_run = import_runs[0]

        assert self.import_run_id_xcom is not None
        assert import_run[0] == self.import_run_id_xcom[IMPORT_RUN_ID]
        assert import_run[1] == end_time

    def test_write_results_some_missing(self) -> None:
        end_time = datetime.datetime(2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC)

        self.file_imports = [
            RawFileImport(
                file_id=1,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=True,
                raw_rows=2,
                net_new_or_updated_rows=2,
                deleted_rows=2,
                error_message=None,
            ),
            RawFileImport(
                file_id=2,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
                raw_rows=1,
                error_message=None,
            ),
            RawFileImport(
                file_id=3,
                import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
                historical_diffs_active=True,
                error_message=None,
            ),
        ]

        missing_file_imports = [
            RawFileImport(
                file_id=4,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=True,
                raw_rows=2,
                error_message=None,
            ),
            RawFileImport(
                file_id=5,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=True,
                raw_rows=2,
                error_message=None,
            ),
        ]

        all_file_imports = [*self.file_imports, *missing_file_imports]

        self._seed_bq_metadata(all_file_imports)
        import_run_id = self._seed_import_run(all_file_imports)
        self._seed_deferred(file_ids=[6, 7, 8], import_run_id=import_run_id)

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
            FileImport(*row)
            for row in assert_type(
                self.mock_pg_hook.get_records(
                    """SELECT 
                file_import_id, 
                file_id, 
                import_status, 
                region_code, 
                raw_data_instance, 
                raw_rows,
                net_new_or_updated_rows,
                deleted_rows,
                historical_diffs_active,
                error_message
            FROM direct_ingest_raw_file_import
            ORDER BY file_id;"""
                ),
                list,
            )
        ]

        for i, record in enumerate(persisted_records):
            assert isinstance(record.file_import_id, int)
            # import summaries that were present in xcom return value
            if i < len(self.file_imports):
                assert record.file_id == self.file_imports[i].file_id
                assert record.import_status == self.file_imports[i].import_status.value
                assert record.region_code == "US_XX"
                assert record.raw_data_instance == "PRIMARY"
                assert (
                    record.historical_diffs_active
                    == self.file_imports[i].historical_diffs_active
                )
                assert record.error_message == self.file_imports[i].error_message
            # import summaries that were missing that we will just close out w/ FAILED UNKNOWN
            elif i < 5:
                assert (
                    record.file_id
                    == missing_file_imports[i - len(self.file_imports)].file_id
                )
                assert record.import_status == "FAILED_UNKNOWN"
                assert record.region_code == "US_XX"
                assert record.raw_data_instance == "PRIMARY"
                assert record.historical_diffs_active is None
                assert record.raw_rows is None
                assert record.net_new_or_updated_rows is None
                assert record.deleted_rows is None
                assert record.error_message == (
                    "Could not find a RawFileImport object associated with the file_import_id, "
                    "despite it being marked as STARTED. This likely means that there was a "
                    "DAG-level failure occurred during this import run."
                )
            else:
                assert record.import_status == "DEFERRED"
                assert record.region_code == "US_XX"
                assert record.raw_data_instance == "PRIMARY"
                assert record.historical_diffs_active is None
                assert record.raw_rows is None
                assert record.net_new_or_updated_rows is None
                assert record.deleted_rows is None
                assert record.error_message is None

        import_runs = assert_type(
            self.mock_pg_hook.get_records(
                "SELECT import_run_id, import_run_end FROM direct_ingest_raw_file_import_run"
            ),
            list,
        )

        assert len(import_runs) == 1
        import_run = import_runs[0]

        assert self.import_run_id_xcom is not None
        assert import_run[0] == self.import_run_id_xcom[IMPORT_RUN_ID]
        assert import_run[1] == end_time
