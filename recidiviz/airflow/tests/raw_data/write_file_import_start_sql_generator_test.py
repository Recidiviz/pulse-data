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
"""Unit tests for WriteFileImportStartCloudSqlGenerator"""
import datetime
from typing import List, NamedTuple
from unittest.mock import create_autospec

from airflow.models import DagRun
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from more_itertools import one

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.metadata import IMPORT_RUN_ID
from recidiviz.airflow.dags.raw_data.write_file_import_start_sql_query_generator import (
    WriteImportStartCloudSqlGenerator,
)
from recidiviz.airflow.dags.utils.cloud_sql import postgres_formatted_datetime_with_tz
from recidiviz.airflow.tests.test_utils import CloudSqlQueryGeneratorUnitTest
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import RawBigQueryFileMetadata
from recidiviz.persistence.database.schema.operations.schema import OperationsBase
from recidiviz.utils.types import assert_type

ImportRun = NamedTuple(
    "ImportRun",
    [
        ("import_run_id", int),
        ("dag_run_id", int),
        ("import_run_start", datetime.datetime),
        ("import_run_end", datetime.datetime),
        ("region_code", str),
        ("raw_data_instance", str),
    ],
)


FileImport = NamedTuple(
    "FileImport",
    [
        ("file_import_id", int),
        ("file_id", int),
        ("import_run_id", int),
        ("import_status", str),
        ("region_code", str),
        ("raw_data_instance", str),
    ],
)


class WriteFileImportStartCloudSqlGeneratorTest(CloudSqlQueryGeneratorUnitTest):
    """Unit tests for WriteFileImportStartCloudSqlGenerator"""

    metas = [OperationsBase]
    mock_operator = create_autospec(CloudSqlQueryOperator)
    mock_context = create_autospec(Context)

    def setUp(self) -> None:
        super().setUp()
        self.generator = WriteImportStartCloudSqlGenerator(
            region_code="US_XX",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            get_all_unprocessed_bq_file_metadata_task_id="task_id",
        )
        self.mock_pg_hook = PostgresHook(postgres_conn_id=self.conn_id)

    def test_write_no_runs(self) -> None:
        self.mock_operator.xcom_pull.return_value = []
        mock_hook = create_autospec(PostgresHook)
        result = self.generator.execute_postgres_query(
            self.mock_operator, mock_hook, self.mock_context
        )

        assert not result
        assert mock_hook.get_records.assert_not_called

    def _seed_bq_metadata(self, file_tags: List[str]) -> List[RawBigQueryFileMetadata]:
        dt = datetime.datetime(2024, 1, 1, 1, 2, 1, tzinfo=datetime.UTC)
        values = [
            f"('US_XX','PRIMARY','{tag}','{postgres_formatted_datetime_with_tz(dt)}',False)"
            for tag in file_tags
        ]
        records = assert_type(
            self.mock_pg_hook.get_records(
                f""" INSERT INTO direct_ingest_raw_big_query_file_metadata (region_code, raw_data_instance, file_tag, update_datetime, is_invalidated)
            VALUES {','.join(values)}
            RETURNING file_id, file_tag, update_datetime;
            """
            ),
            list,
        )

        return [
            RawBigQueryFileMetadata(
                gcs_files=[],
                file_id=record[0],
                file_tag=record[1],
                update_datetime=record[2],
            )
            for record in records
        ]

    def test_write_file_imports(self) -> None:
        start_time = datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC)

        dag_run = create_autospec(DagRun)
        dag_run.start_date = start_time
        dag_run.run_id = "run123"
        self.mock_context.__getitem__.return_value = dag_run

        file_tags = ["tag_a", "tag_b", "tag_c", "tag_d"]

        bq_files = self._seed_bq_metadata(file_tags)

        self.mock_operator.xcom_pull.return_value = [
            bq_file.serialize() for bq_file in bq_files
        ]

        result = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        assert IMPORT_RUN_ID in result
        assert isinstance(result[IMPORT_RUN_ID], int)

        persisted_import_run = one(
            [
                ImportRun(*row)
                for row in assert_type(
                    self.mock_pg_hook.get_records(
                        """SELECT 
                import_run_id, 
                dag_run_id, 
                import_run_start, 
                import_run_end, 
                region_code, 
                raw_data_instance
            FROM direct_ingest_raw_file_import_run 
            ORDER BY import_run_id;"""
                    ),
                    list,
                )
            ]
        )

        assert persisted_import_run.import_run_id == result[IMPORT_RUN_ID]

        persisted_file_imports = [
            FileImport(*row)
            for row in assert_type(
                self.mock_pg_hook.get_records(
                    """SELECT 
                file_import_id, 
                file_id, 
                import_run_id,
                import_status, 
                region_code, 
                raw_data_instance
            FROM direct_ingest_raw_file_import
            ORDER BY file_id;"""
                ),
                list,
            )
        ]

        for i, record in enumerate(persisted_file_imports):
            assert isinstance(record.file_import_id, int)
            assert record.import_run_id == persisted_import_run.import_run_id
            assert record.file_id == bq_files[i].file_id
            assert record.import_status == DirectIngestRawFileImportStatus.STARTED.value
            assert record.region_code == "US_XX"
            assert record.raw_data_instance == "PRIMARY"

        # oop! run again!!!

        result2 = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        assert IMPORT_RUN_ID in result2
        assert isinstance(result2[IMPORT_RUN_ID], int)

        persisted_import_run2 = [
            ImportRun(*row)
            for row in assert_type(
                self.mock_pg_hook.get_records(
                    """SELECT 
                import_run_id, 
                dag_run_id, 
                import_run_start, 
                import_run_end, 
                region_code, 
                raw_data_instance
            FROM direct_ingest_raw_file_import_run 
            ORDER BY import_run_id;"""
                ),
                list,
            )
        ]

        assert persisted_import_run2[1].import_run_id == result2[IMPORT_RUN_ID]

        persisted_file_imports2 = [
            FileImport(*row)
            for row in assert_type(
                self.mock_pg_hook.get_records(
                    """SELECT 
                file_import_id, 
                file_id, 
                import_run_id,
                import_status, 
                region_code, 
                raw_data_instance
            FROM direct_ingest_raw_file_import
            ORDER BY file_id ASC, import_run_id ASC;"""
                ),
                list,
            )
        ]

        for i, record in enumerate(persisted_file_imports2):
            assert isinstance(record.file_import_id, int)
            assert record.import_run_id == persisted_import_run2[i % 2].import_run_id
            assert record.file_id == bq_files[i // 2].file_id
            assert record.import_status == DirectIngestRawFileImportStatus.STARTED.value
            assert record.region_code == "US_XX"
            assert record.raw_data_instance == "PRIMARY"
