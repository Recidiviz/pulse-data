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
"""Unit tests for RawDataFileTagImportRunSqlQueryGenerator"""
import datetime
from typing import List
from unittest.mock import create_autospec

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from freezegun import freeze_time

from recidiviz.airflow.dags.monitoring.job_run import JobRunState
from recidiviz.airflow.dags.monitoring.raw_data_file_tag_import_runs_sql_query_generator import (
    FileTagImportRunSummary,
    RawDataFileTagImportRunSqlQueryGenerator,
)
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.utils.cloud_sql import postgres_formatted_datetime_with_tz
from recidiviz.airflow.tests.test_utils import CloudSqlQueryGeneratorUnitTest
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.ingest.direct.types.raw_data_import_types import RawFileImport
from recidiviz.persistence.database.schema.operations.schema import OperationsBase
from recidiviz.utils.types import assert_type

NULL = "NULL"


class RawDataFileTagImportRunSqlQueryGeneratorTest(CloudSqlQueryGeneratorUnitTest):
    """Unit tests for RawDataFileTagImportRunSqlQueryGenerator"""

    metas = [OperationsBase]
    mock_operator = create_autospec(CloudSqlQueryOperator)

    def setUp(self) -> None:
        super().setUp()
        self.generator = RawDataFileTagImportRunSqlQueryGenerator(
            lookback=datetime.timedelta(days=20)
        )
        self.mock_pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.mock_context = create_autospec(Context)
        self.mock_operator = create_autospec(CloudSqlQueryOperator)

    def _invalidate_by_id(self, file_ids: list[int]) -> None:
        self.mock_pg_hook.run(
            f"""UPDATE direct_ingest_raw_big_query_file_metadata
            SET is_invalidated = True
            WHERE file_id in ({','.join(str(file_id) for file_id in file_ids)})
            """
        )

    def _seed_bq_metadata(
        self, file_imports: List[RawFileImport], file_tag: str, dt: datetime.datetime
    ) -> None:
        """Persists bq_metadata objects."""
        values = [
            f"({summary.file_id},'US_XX','PRIMARY','{file_tag}','{postgres_formatted_datetime_with_tz(dt)}',False)"
            for summary in file_imports
        ]
        self.mock_pg_hook.run(
            f""" INSERT INTO direct_ingest_raw_big_query_file_metadata (file_id, region_code, raw_data_instance, file_tag, update_datetime, is_invalidated)
            VALUES {','.join(values)}
            """
        )

    def _seed_import_run(
        self,
        file_imports: List[RawFileImport],
        dt: datetime.datetime,
        dag_run_id: str = "abc_123",
    ) -> None:
        """Persists import_run and import objects."""
        import_run_id_result = assert_type(
            self.mock_pg_hook.get_records(
                f""" INSERT INTO direct_ingest_raw_file_import_run (dag_run_id, import_run_start, region_code, raw_data_instance)
                VALUES ('{dag_run_id}', '{postgres_formatted_datetime_with_tz(dt)}', 'US_XX', 'PRIMARY')
                RETURNING import_run_id;
            """
            ),
            list,
        )

        import_run_id = assert_type(import_run_id_result[0][0], int)
        rows = [
            [
                s.file_id,
                import_run_id,
                f"'{s.import_status.value}'",
                "'US_XX'",
                "'PRIMARY'",
                s.historical_diffs_active,
                s.raw_rows or NULL,
                (f"'{s.error_message_quote_safe()}'" if s.error_message else NULL),
            ]
            for s in file_imports
        ]

        str_rows = ",".join([f'({",".join(str(col) for col in row)})' for row in rows])
        self.mock_pg_hook.run(
            f"""INSERT INTO direct_ingest_raw_file_import (file_id, import_run_id, import_status, region_code, raw_data_instance, historical_diffs_active, raw_rows, error_message)
                VALUES {str_rows};
        """
        )

    def test_none(self) -> None:
        mock_hook = create_autospec(PostgresHook)
        assert not self.generator.execute_postgres_query(
            self.mock_operator, mock_hook, self.mock_context
        )

    def test_all_success(self) -> None:
        start_time = datetime.datetime(2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC)

        tag_a_file_imports = [
            RawFileImport(
                file_id=1,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
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
        ]

        tag_b_file_imports = [
            RawFileImport(
                file_id=4,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
                raw_rows=2,
                net_new_or_updated_rows=2,
                deleted_rows=2,
                error_message=None,
            ),
            RawFileImport(
                file_id=5,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
                raw_rows=2,
                error_message=None,
            ),
            RawFileImport(
                file_id=6,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
                raw_rows=1,
                error_message=None,
            ),
        ]

        tag_a_update_datetime = start_time - datetime.timedelta(hours=2)
        self._seed_bq_metadata(
            tag_a_file_imports, file_tag="tag_a", dt=tag_a_update_datetime
        )
        self._seed_import_run(tag_a_file_imports, dt=start_time)

        tag_b_update_datetime = start_time - datetime.timedelta(hours=2)
        self._seed_bq_metadata(
            tag_b_file_imports, file_tag="tag_b", dt=tag_b_update_datetime
        )
        self._seed_import_run(tag_b_file_imports, dt=start_time)

        with freeze_time(start_time + datetime.timedelta(hours=1)):
            results = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        assert len(results) == 2

        for result in results:
            summary = FileTagImportRunSummary.deserialize(result)

            file_imports, update_datetime = (
                (tag_a_file_imports, tag_a_update_datetime)
                if summary.file_tag == "tag_a"
                else (tag_b_file_imports, tag_b_update_datetime)
            )

            assert summary.import_run_start == start_time
            assert summary.raw_data_instance.value == "PRIMARY"
            assert summary.file_tag_import_state == JobRunState.SUCCESS

            for i, import_run in enumerate(
                sorted(summary.file_import_runs, key=lambda x: x.file_id)
            ):
                assert import_run.file_id == file_imports[i].file_id
                assert import_run.update_datetime == update_datetime
                assert import_run.error_message == file_imports[i].error_message

    def test_consecutive_dags(self) -> None:
        start_time = datetime.datetime(2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC)

        file_imports = [
            RawFileImport(
                file_id=1,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
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
        ]

        update_datetime = start_time - datetime.timedelta(hours=1)
        run_id_suffixes = ["123", "234", "345", "456", "567", "678", "789"]
        for i, suffix in enumerate(run_id_suffixes):
            for fi in file_imports:
                fi.file_id += i * 10
            self._seed_bq_metadata(
                file_imports,
                file_tag="tag_b",
                dt=update_datetime - datetime.timedelta(hours=i),
            )
            self._seed_import_run(
                file_imports,
                dt=start_time - datetime.timedelta(hours=i),
                dag_run_id=f"abc-{suffix}",
            )

        with freeze_time(start_time + datetime.timedelta(hours=1)):
            results = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        assert len(results) == len(run_id_suffixes)

        for result in results:
            summary = FileTagImportRunSummary.deserialize(result)
            assert summary.raw_data_instance.value == "PRIMARY"
            assert summary.file_tag_import_state == JobRunState.SUCCESS
            assert len(summary.file_import_runs) == 3
            for i, run in enumerate(
                sorted(summary.file_import_runs, key=lambda x: x.file_id)
            ):
                assert run.file_import_status == file_imports[i].import_status
                assert run.error_message == file_imports[i].error_message

    def test_lookback(self) -> None:
        start_time = datetime.datetime(2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC)

        file_imports = [
            RawFileImport(
                file_id=1,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
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
        ]

        update_datetime = start_time - datetime.timedelta(hours=1)
        run_id_suffixes = ["123", "234", "345"]
        for i, suffix in enumerate(run_id_suffixes):
            for fi in file_imports:
                fi.file_id += i * 10
            self._seed_bq_metadata(
                file_imports,
                file_tag="tag_b",
                dt=update_datetime - datetime.timedelta(hours=i),
            )
            self._seed_import_run(
                file_imports,
                dt=start_time - datetime.timedelta(hours=i),
                dag_run_id=f"abc-{suffix}",
            )

        with freeze_time(start_time):
            # w/ a lookback of 1 hour, we do <=, so we should capture the dag run
            # that is EXACTLY 1 hour ago, but exclude the one that is two hours ago
            generator = RawDataFileTagImportRunSqlQueryGenerator(
                lookback=datetime.timedelta(hours=1)
            )
            results = generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        assert len(results) == 2

        for result in results:
            summary = FileTagImportRunSummary.deserialize(result)
            assert summary.raw_data_instance.value == "PRIMARY"
            assert summary.file_tag_import_state == JobRunState.SUCCESS
            assert len(summary.file_import_runs) == 3
            for i, run in enumerate(
                sorted(summary.file_import_runs, key=lambda x: x.file_id)
            ):
                assert run.file_import_status == file_imports[i].import_status
                assert run.error_message == file_imports[i].error_message

    def test_single_file_tag_with_failures(self) -> None:
        start_time = datetime.datetime(2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC)

        file_imports = [
            RawFileImport(
                file_id=1,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
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
                historical_diffs_active=False,
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
                historical_diffs_active=False,
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
                historical_diffs_active=False,
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
                historical_diffs_active=False,
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
                historical_diffs_active=False,
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

        update_datetime = start_time - datetime.timedelta(hours=2)
        self._seed_bq_metadata(file_imports, file_tag="tag_a", dt=update_datetime)
        self._seed_import_run(file_imports, dt=start_time)

        with freeze_time(start_time + datetime.timedelta(hours=1)):
            result = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        assert len(result) == 1

        tag_a = FileTagImportRunSummary.deserialize(result[0])

        assert tag_a.file_tag == "tag_a"
        assert tag_a.import_run_start == start_time
        assert tag_a.raw_data_instance.value == "PRIMARY"
        assert tag_a.file_tag_import_state == JobRunState.FAILED

        for i, import_run in enumerate(
            sorted(tag_a.file_import_runs, key=lambda x: x.file_id)
        ):
            assert import_run.file_id == file_imports[i].file_id
            assert import_run.update_datetime == update_datetime
            assert import_run.file_import_status == file_imports[i].import_status
            assert import_run.error_message == file_imports[i].error_message

    def test_invalidation(self) -> None:
        start_time = datetime.datetime(2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC)

        file_imports = [
            RawFileImport(
                file_id=1,
                import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
                historical_diffs_active=False,
                raw_rows=2,
                net_new_or_updated_rows=2,
                deleted_rows=2,
                error_message="Oops! We failed!",
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
        ]

        self._seed_bq_metadata(
            file_imports,
            file_tag="tag_a",
            dt=start_time - datetime.timedelta(hours=1),
        )
        self._seed_import_run(
            file_imports,
            dt=start_time,
            dag_run_id="first-run",
        )

        with freeze_time(start_time + datetime.timedelta(hours=1)):
            results = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        assert len(results) == 1

        summary = FileTagImportRunSummary.deserialize(results[0])
        assert summary.raw_data_instance.value == "PRIMARY"
        assert summary.file_tag_import_state == JobRunState.FAILED
        assert len(summary.file_import_runs) == 3
        for i, run in enumerate(
            sorted(summary.file_import_runs, key=lambda x: x.file_id)
        ):
            assert run.file_import_status == file_imports[i].import_status
            assert run.error_message == file_imports[i].error_message

        # OKAY
        # now we invalidate the failure (file_id: 1) -- it should still show up!

        self._invalidate_by_id([1])

        with freeze_time(start_time + datetime.timedelta(hours=1)):
            results = self.generator.execute_postgres_query(
                self.mock_operator, self.mock_pg_hook, self.mock_context
            )

        assert len(results) == 1

        summary = FileTagImportRunSummary.deserialize(results[0])
        assert summary.raw_data_instance.value == "PRIMARY"
        assert summary.file_tag_import_state == JobRunState.FAILED
        assert len(summary.file_import_runs) == 3
        for i, run in enumerate(
            sorted(summary.file_import_runs, key=lambda x: x.file_id)
        ):
            assert run.file_import_status == file_imports[i].import_status
            assert run.error_message == file_imports[i].error_message
