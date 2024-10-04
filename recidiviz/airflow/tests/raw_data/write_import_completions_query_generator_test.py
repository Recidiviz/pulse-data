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

    def _seed_import_run(self, file_imports: List[RawFileImport]) -> None:
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
            ),
            RawFileImport(
                file_id=2,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
                raw_rows=2,
            ),
            RawFileImport(
                file_id=3,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
                raw_rows=1,
            ),
            RawFileImport(
                file_id=4,
                import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
                historical_diffs_active=True,
            ),
            RawFileImport(
                file_id=5,
                import_status=DirectIngestRawFileImportStatus.FAILED_PRE_IMPORT_NORMALIZATION_STEP,
                historical_diffs_active=True,
            ),
            RawFileImport(
                file_id=6,
                import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
                historical_diffs_active=True,
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
                historical_diffs_active
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
            ),
            RawFileImport(
                file_id=2,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=False,
                raw_rows=1,
            ),
            RawFileImport(
                file_id=3,
                import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
                historical_diffs_active=True,
            ),
        ]

        missing_file_imports = [
            RawFileImport(
                file_id=4,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=True,
                raw_rows=2,
            ),
            RawFileImport(
                file_id=5,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                historical_diffs_active=True,
                raw_rows=2,
            ),
        ]

        all_file_imports = [*self.file_imports, *missing_file_imports]

        self._seed_bq_metadata(all_file_imports)
        self._seed_import_run(all_file_imports)

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
                historical_diffs_active
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
            # import summaries that were missing that we will just close out w/ FAILED UNKNOWN
            else:
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
