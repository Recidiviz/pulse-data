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
"""Unit tests for VerifyRawDataPruningMetadataSqlQueryGenerator"""
import datetime
from unittest.mock import create_autospec, patch

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from more_itertools import one

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.verify_raw_data_pruning_metadata_sql_query_generator import (
    RawDataPruningConfig,
    VerifyRawDataPruningMetadataSqlQueryGenerator,
)
from recidiviz.airflow.tests.test_utils import CloudSqlQueryGeneratorUnitTest
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawDataFilesSkippedError,
    RawGCSFileMetadata,
)
from recidiviz.persistence.database.schema.operations.schema import OperationsBase
from recidiviz.tests.ingest.direct import fake_regions


class TestVerifyRawDataPruningMetadataSqlQueryGenerator(CloudSqlQueryGeneratorUnitTest):
    """Unit tests for VerifyRawDataPruningMetadataSqlQueryGenerator"""

    metas = [OperationsBase]
    maxDiff = None

    def setUp(self) -> None:
        super().setUp()
        self.state_code = StateCode.US_XX
        self.raw_data_instance = DirectIngestInstance.PRIMARY

        self.region_module_patch = patch(
            "recidiviz.airflow.dags.raw_data.utils.direct_ingest_regions_module",
            fake_regions,
        )
        self.region_module_patch.start()

        self.pruning_enabled_patch = patch(
            "recidiviz.ingest.direct.raw_data.raw_data_pruning_utils.automatic_raw_data_pruning_enabled_for_state_and_instance",
            return_value=True,
        )
        self.pruning_enabled_patch.start()

        self.mock_pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.generator = VerifyRawDataPruningMetadataSqlQueryGenerator(
            state_code=self.state_code,
            raw_data_instance=self.raw_data_instance,
            get_all_unprocessed_bq_file_metadata_task_id="test_id",
        )
        self.mock_operator = create_autospec(CloudSqlQueryOperator)
        self.mock_context = create_autospec(Context)
        self.region_raw_file_config = get_region_raw_file_config(
            region_code=self.state_code.value, region_module=fake_regions
        )

    def tearDown(self) -> None:
        self.region_module_patch.stop()
        self.pruning_enabled_patch.stop()
        return super().tearDown()

    def _create_bq_metadata_for_file_being_imported(
        self, file_tag: str, file_id: int = 1
    ) -> RawBigQueryFileMetadata:
        update_datetime = datetime.datetime.now(tz=datetime.UTC)

        self._insert_bq_file_metadata(file_tag=file_tag, num_files=1, processed=False)

        gcs_file = RawGCSFileMetadata(
            gcs_file_id=file_id,
            file_id=file_id,
            path=GcsfsFilePath.from_absolute_path(
                f"testing/unprocessed_{update_datetime.isoformat()}_raw_{file_tag}.csv"
            ),
        )
        return RawBigQueryFileMetadata(
            file_id=file_id,
            file_tag=file_tag,
            gcs_files=[gcs_file],
            update_datetime=update_datetime,
        )

    def _insert_bq_file_metadata(
        self, file_tag: str, num_files: int, processed: bool = True
    ) -> None:
        for _ in range(num_files):
            self.mock_pg_hook.run(
                f"""
                INSERT INTO direct_ingest_raw_big_query_file_metadata
                (region_code, raw_data_instance, file_tag, update_datetime, is_invalidated, file_processed_time)
                VALUES ('{self.state_code.value}', '{self.raw_data_instance.value}', '{file_tag}', NOW(), FALSE, {'NOW()' if processed else 'NULL'})
                """
            )

    def _insert_pruning_metadata(
        self,
        file_tag: str,
        automatic_pruning_enabled: bool,
        primary_keys: str,
        full_historical_lookback: bool,
    ) -> None:
        self.mock_pg_hook.run(
            f"""
            INSERT INTO direct_ingest_raw_data_pruning_metadata
            (region_code, raw_data_instance, file_tag, updated_at, automatic_pruning_enabled,
             raw_file_primary_keys, raw_files_contain_full_historical_lookback)
            VALUES ('{self.state_code.value}', '{self.raw_data_instance.value}', '{file_tag}', NOW(), {automatic_pruning_enabled},
                    '{primary_keys}', {full_historical_lookback})
            """
        )

    def _get_pruning_metadata(self, file_tag: str) -> RawDataPruningConfig | None:
        raw_data_pruning_metadata_rows = self.mock_pg_hook.get_records(
            f"""
            SELECT *
            FROM direct_ingest_raw_data_pruning_metadata
            WHERE region_code = 'US_XX' AND file_tag = '{file_tag}'
            """
        )
        metadata_row = (
            one(raw_data_pruning_metadata_rows)
            if raw_data_pruning_metadata_rows
            else None
        )
        return (
            RawDataPruningConfig.from_metadata_row(metadata_row)
            if metadata_row
            else None
        )

    def _get_expected_pruning_config(self, file_tag: str) -> RawDataPruningConfig:
        return RawDataPruningConfig.from_raw_file_config(
            raw_file_config=self.region_raw_file_config.raw_file_configs[file_tag],
            state_code=self.state_code,
            raw_data_instance=self.raw_data_instance,
        )

    def test_no_files(self) -> None:
        self.mock_operator.xcom_pull.return_value = []
        errors = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        assert errors == []

    def test_new_file_tag_with_pruning_one_existing_file(self) -> None:
        pruning_enabled_file_tag = "singlePrimaryKey"

        self._insert_bq_file_metadata(file_tag=pruning_enabled_file_tag, num_files=1)

        bq_metadata = self._create_bq_metadata_for_file_being_imported(
            file_tag=pruning_enabled_file_tag,
        )
        self.mock_operator.xcom_pull.return_value = [bq_metadata.serialize()]

        errors = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        assert errors == []

        actual_metadata_config = self._get_pruning_metadata(pruning_enabled_file_tag)
        expected_metadata_config = self._get_expected_pruning_config(
            pruning_enabled_file_tag
        )

        assert actual_metadata_config == expected_metadata_config

    def test_config_change_primary_keys_one_file(self) -> None:
        pruning_enabled_file_tag = "singlePrimaryKey"

        self._insert_pruning_metadata(
            file_tag=pruning_enabled_file_tag,
            automatic_pruning_enabled=True,
            primary_keys="old_key",
            full_historical_lookback=True,
        )

        self._insert_bq_file_metadata(file_tag=pruning_enabled_file_tag, num_files=1)

        bq_metadata = self._create_bq_metadata_for_file_being_imported(
            file_tag=pruning_enabled_file_tag,
        )
        self.mock_operator.xcom_pull.return_value = [bq_metadata.serialize()]

        errors = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        assert errors == []

        actual_metadata_config = self._get_pruning_metadata(pruning_enabled_file_tag)
        expected_metadata_config = self._get_expected_pruning_config(
            pruning_enabled_file_tag
        )

        assert actual_metadata_config == expected_metadata_config

    def test_new_file_tag_with_pruning_multiple_existing_files(self) -> None:
        """Test file tags with no entry in pruning metadata table and >1 imported files.
        Should fail for pruning enabled file and pass for pruning disabled file."""
        pruning_enabled_file_tag = "singlePrimaryKey"
        pruning_disabled_file_tag = "tagInvalidCharacters"

        self._insert_bq_file_metadata(file_tag=pruning_enabled_file_tag, num_files=2)
        self._insert_bq_file_metadata(file_tag=pruning_disabled_file_tag, num_files=2)

        bq_metadata_1 = self._create_bq_metadata_for_file_being_imported(
            file_tag=pruning_enabled_file_tag,
        )
        bq_metadata_2 = self._create_bq_metadata_for_file_being_imported(
            file_tag=pruning_disabled_file_tag,
            file_id=2,
        )
        self.mock_operator.xcom_pull.return_value = [
            bq_metadata_1.serialize(),
            bq_metadata_2.serialize(),
        ]

        errors = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        # Should have errors only for pruning enabled file
        assert len(errors) == 1
        error = RawDataFilesSkippedError.deserialize(errors[0])
        assert error.file_tag == pruning_enabled_file_tag
        assert (
            rf"Pruning configuration for file_tag [{pruning_enabled_file_tag}] changed but [2] files have already been imported"
            in error.skipped_message
        )
        # Metadata should not be created for pruning enabled file
        actual_metadata_config_for_pruning_file = self._get_pruning_metadata(
            pruning_enabled_file_tag
        )
        assert actual_metadata_config_for_pruning_file is None

        actual_metadata_config = self._get_pruning_metadata(pruning_disabled_file_tag)
        expected_metadata_config = self._get_expected_pruning_config(
            pruning_disabled_file_tag
        )

        assert actual_metadata_config == expected_metadata_config

    def test_toggle_pruning_enabled_with_multiple_files_imported(self) -> None:
        """Test toggling pruning enabled/disabled with multiple files - both should fail."""
        pruning_enabled_file_tag = "singlePrimaryKey"
        pruning_disabled_file_tag = "tagInvalidCharacters"

        self._insert_pruning_metadata(
            file_tag=pruning_disabled_file_tag,
            automatic_pruning_enabled=True,  # Now disabled in config
            primary_keys="COL_1",
            full_historical_lookback=False,
        )
        self._insert_pruning_metadata(
            file_tag=pruning_enabled_file_tag,
            automatic_pruning_enabled=False,  # Now enabled in config
            primary_keys="",
            full_historical_lookback=True,
        )

        self._insert_bq_file_metadata(file_tag=pruning_disabled_file_tag, num_files=2)
        self._insert_bq_file_metadata(file_tag=pruning_enabled_file_tag, num_files=2)

        bq_metadata_1 = self._create_bq_metadata_for_file_being_imported(
            file_tag=pruning_disabled_file_tag,
        )
        bq_metadata_2 = self._create_bq_metadata_for_file_being_imported(
            file_tag=pruning_enabled_file_tag,
            file_id=2,
        )
        self.mock_operator.xcom_pull.return_value = [
            bq_metadata_1.serialize(),
            bq_metadata_2.serialize(),
        ]

        errors = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        assert len(errors) == 2
        errors_deserialized = [
            RawDataFilesSkippedError.deserialize(error) for error in errors
        ]
        error_1 = next(
            error
            for error in errors_deserialized
            if error.file_tag == pruning_disabled_file_tag
        )
        assert (
            rf"Pruning configuration for file_tag [{pruning_disabled_file_tag}] changed but [2] files have already been imported"
            in error_1.skipped_message
        )

        # Metadata should not be updated
        actual_metadata_config_for_disabled_file = self._get_pruning_metadata(
            pruning_disabled_file_tag
        )
        assert actual_metadata_config_for_disabled_file is not None
        assert (
            actual_metadata_config_for_disabled_file.automatic_pruning_enabled is True
        )
        assert actual_metadata_config_for_disabled_file.primary_keys == ["COL_1"]
        assert (
            actual_metadata_config_for_disabled_file.raw_files_contain_full_historical_lookback
            is False
        )

        error_2 = next(
            error
            for error in errors_deserialized
            if error.file_tag == pruning_enabled_file_tag
        )
        assert (
            rf"Pruning configuration for file_tag [{pruning_enabled_file_tag}] changed but [2] files have already been imported"
            in error_2.skipped_message
        )

        # Metadata should not be updated
        actual_metadata_config_for_enabled_file = self._get_pruning_metadata(
            pruning_enabled_file_tag
        )
        assert actual_metadata_config_for_enabled_file is not None
        assert (
            actual_metadata_config_for_enabled_file.automatic_pruning_enabled is False
        )
        assert actual_metadata_config_for_enabled_file.primary_keys == []
        assert (
            actual_metadata_config_for_enabled_file.raw_files_contain_full_historical_lookback
            is True
        )

    def test_pruning_disabled_config_update_with_multiple_files_imported(self) -> None:
        """When automatic pruning is disabled in the config, the metadata can be updated,
        even if multiple files have been imported."""
        pruning_disabled_file_tag = "tagInvalidCharacters"

        self._insert_pruning_metadata(
            file_tag=pruning_disabled_file_tag,
            automatic_pruning_enabled=False,
            primary_keys="",
            full_historical_lookback=True,
        )
        self._insert_bq_file_metadata(file_tag=pruning_disabled_file_tag, num_files=2)

        bq_metadata = self._create_bq_metadata_for_file_being_imported(
            file_tag=pruning_disabled_file_tag,
        )
        self.mock_operator.xcom_pull.return_value = [bq_metadata.serialize()]

        errors = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        assert errors == []

        actual_metadata_config = self._get_pruning_metadata(pruning_disabled_file_tag)
        expected_metadata_config = self._get_expected_pruning_config(
            pruning_disabled_file_tag
        )

        assert actual_metadata_config == expected_metadata_config

    def test_ignore_invalidated_files(self) -> None:
        pruning_enabled_file_tag = "singlePrimaryKey"

        self.mock_pg_hook.run(
            f"""
            INSERT INTO direct_ingest_raw_big_query_file_metadata
            (region_code, raw_data_instance, file_tag, update_datetime, is_invalidated, file_processed_time)
            VALUES ('US_XX', 'PRIMARY', '{pruning_enabled_file_tag}', NOW(), TRUE, NOW()),
                   ('US_XX', 'PRIMARY', '{pruning_enabled_file_tag}', NOW(), TRUE, NOW())

            """
        )

        bq_metadata = self._create_bq_metadata_for_file_being_imported(
            file_tag=pruning_enabled_file_tag,
        )
        self.mock_operator.xcom_pull.return_value = [bq_metadata.serialize()]

        errors = self.generator.execute_postgres_query(
            self.mock_operator, self.mock_pg_hook, self.mock_context
        )

        assert errors == []

        actual_metadata_config = self._get_pruning_metadata(pruning_enabled_file_tag)
        expected_metadata_config = self._get_expected_pruning_config(
            pruning_enabled_file_tag
        )

        assert actual_metadata_config == expected_metadata_config
