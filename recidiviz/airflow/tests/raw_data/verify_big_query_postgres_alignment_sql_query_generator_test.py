# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for VerifyBigQueryPostgresAlignmentSQLQueryGenerator"""
import datetime
from typing import List
from unittest.mock import MagicMock, create_autospec, patch

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from google.cloud.bigquery import QueryJob
from more_itertools import one

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.metadata import SKIPPED_FILE_ERRORS
from recidiviz.airflow.dags.raw_data.verify_big_query_postgres_alignment_sql_query_generator import (
    VerifyBigQueryPostgresAlignmentSQLQueryGenerator,
)
from recidiviz.airflow.tests.test_utils import CloudSqlQueryGeneratorUnitTest
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawDataFilesSkippedError,
    RawGCSFileMetadata,
)
from recidiviz.persistence.database.schema.operations.schema import OperationsBase


class VerifyBigQueryPostgresAlignmentSQLQueryGeneratorTest(
    CloudSqlQueryGeneratorUnitTest
):
    """Unit tests for VerifyBigQueryPostgresAlignmentSQLQueryGenerator"""

    metas = [OperationsBase]
    mock_operator = create_autospec(CloudSqlQueryOperator)
    mock_context = create_autospec(Context)

    def setUp(self) -> None:
        super().setUp()
        self.mock_pg_hook = PostgresHook(postgres_conn_id=self.conn_id)

        # Reset mocks between tests
        self.mock_operator.reset_mock()
        self.mock_context.reset_mock()

        self.bq_client_patcher = patch(
            "recidiviz.airflow.dags.raw_data.verify_big_query_postgres_alignment_sql_query_generator.BigQueryClientImpl"
        )
        self.mock_bq_client_class = self.bq_client_patcher.start()
        self.mock_bq_client = MagicMock()
        self.mock_bq_client_class.return_value = self.mock_bq_client

        self.generator = VerifyBigQueryPostgresAlignmentSQLQueryGenerator(
            state_code=StateCode.US_XX,
            project_id="recidiviz-testing",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            get_all_unprocessed_bq_file_metadata_task_id="test_task_id",
        )

    def tearDown(self) -> None:
        super().tearDown()
        self.bq_client_patcher.stop()

    def _seed_bq_metadata(
        self,
        file_ids: List[int],
        file_tag: str,
        is_invalidated: bool = False,
    ) -> None:
        """Helper to seed the operations DB with BQ file metadata"""
        values = [
            f"({file_id},'US_XX','PRIMARY','{file_tag}','NOW()',{is_invalidated},'NOW()')"
            for file_id in file_ids
        ]
        self.mock_pg_hook.run(
            f"""INSERT INTO direct_ingest_raw_big_query_file_metadata
            (file_id, region_code, raw_data_instance, file_tag, update_datetime, is_invalidated, file_processed_time)
            VALUES {','.join(values)}
            """
        )

    def _create_bq_file_metadata(
        self, file_id: int, file_tag: str
    ) -> RawBigQueryFileMetadata:
        """Helper to create a RawBigQueryFileMetadata object"""
        update_datetime = datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC)
        gcs_file = RawGCSFileMetadata(
            gcs_file_id=file_id,
            file_id=file_id,
            path=GcsfsFilePath.from_absolute_path(
                f"testing/unprocessed_{update_datetime.isoformat()}_raw_{file_tag}.csv"
            ),
        )
        return RawBigQueryFileMetadata(
            gcs_files=[gcs_file],
            file_id=file_id,
            file_tag=file_tag,
            update_datetime=update_datetime,
        )

    def test_no_files_to_validate(self) -> None:
        """Test when there are no files to validate"""
        self.mock_operator.xcom_pull.return_value = []

        result = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        assert result == []

        self.mock_operator.xcom_push.assert_called_once()
        call_args = self.mock_operator.xcom_push.call_args
        assert call_args[1]["key"] == SKIPPED_FILE_ERRORS
        assert call_args[1]["value"] == []

    @patch(
        "recidiviz.airflow.dags.raw_data.verify_big_query_postgres_alignment_sql_query_generator.file_tag_exempt_from_automatic_raw_data_pruning",
        return_value=True,
    )
    def test_pruning_not_enabled(self, _mock_pruning_enabled: MagicMock) -> None:
        """Test that when pruning is not enabled, validation is skipped and all files are returned"""
        bq_files = [
            self._create_bq_file_metadata(3, "test_tag"),
            self._create_bq_file_metadata(4, "test_tag"),
            self._create_bq_file_metadata(5, "test_tag"),
        ]

        self.mock_operator.xcom_pull.return_value = [f.serialize() for f in bq_files]

        result = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        assert len(result) == 3
        returned_metadata = [RawBigQueryFileMetadata.deserialize(r) for r in result]
        assert {m.file_id for m in returned_metadata} == {3, 4, 5}

        self.mock_operator.xcom_push.assert_called_once()
        call_args = self.mock_operator.xcom_push.call_args
        assert call_args[1]["key"] == SKIPPED_FILE_ERRORS
        assert call_args[1]["value"] == []

    def test_all_files_valid(self) -> None:
        self._seed_bq_metadata([1, 2, 3], file_tag="test_tag")

        # Mock BigQuery to return file_ids 1 and 2
        # we should allow file_ids in the bq metadata table to not have
        # any corresponding rows in the big query raw data table
        mock_query_job = MagicMock(spec=QueryJob)
        mock_query_job.result.return_value = [[1], [2]]
        self.mock_bq_client.run_query_async.return_value = mock_query_job

        bq_files = [
            self._create_bq_file_metadata(4, "test_tag"),
            self._create_bq_file_metadata(5, "test_tag"),
        ]

        self.mock_operator.xcom_pull.return_value = [f.serialize() for f in bq_files]

        result = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        assert len(result) == 2
        returned_metadata = [RawBigQueryFileMetadata.deserialize(r) for r in result]
        assert {m.file_id for m in returned_metadata} == {4, 5}

        call_args = self.mock_operator.xcom_push.call_args
        assert call_args[1]["key"] == SKIPPED_FILE_ERRORS
        assert call_args[1]["value"] == []

    def test_partial_validation_failure(self) -> None:
        self._seed_bq_metadata([1, 2], file_tag="tag_a")
        self._seed_bq_metadata([11, 12], file_tag="tag_b", is_invalidated=True)

        mock_query_job = MagicMock(spec=QueryJob)
        mock_query_job.result.side_effect = [
            [[1], [2]],  # First call for tag_a
            [[11], [12]],  # Second call for tag_b
        ]
        self.mock_bq_client.run_query_async.return_value = mock_query_job

        bq_files = [
            self._create_bq_file_metadata(3, "tag_a"),
            self._create_bq_file_metadata(13, "tag_b"),
        ]

        self.mock_operator.xcom_pull.return_value = [f.serialize() for f in bq_files]

        result = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        returned_metadata = one(
            [RawBigQueryFileMetadata.deserialize(r) for r in result]
        )
        assert returned_metadata.file_tag == "tag_a"

        call_args = self.mock_operator.xcom_push.call_args
        skipped_error = one(
            [RawDataFilesSkippedError.deserialize(e) for e in call_args[1]["value"]]
        )
        assert skipped_error.file_tag == "tag_b"

    def test_no_file_ids_from_bigquery(self) -> None:
        self._seed_bq_metadata([1, 2], file_tag="test_tag")

        # Mock BigQuery to return no file_ids
        mock_query_job = MagicMock(spec=QueryJob)
        mock_query_job.result.return_value = []
        self.mock_bq_client.run_query_async.return_value = mock_query_job

        bq_files = [
            self._create_bq_file_metadata(3, "test_tag"),
            self._create_bq_file_metadata(4, "test_tag"),
        ]

        self.mock_operator.xcom_pull.return_value = [f.serialize() for f in bq_files]

        result = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        assert len(result) == 0

        call_args = self.mock_operator.xcom_push.call_args
        skipped_errors = [
            RawDataFilesSkippedError.deserialize(e) for e in call_args[1]["value"]
        ]
        assert len(skipped_errors) == 2
        assert all(e.file_tag == "test_tag" for e in skipped_errors)
