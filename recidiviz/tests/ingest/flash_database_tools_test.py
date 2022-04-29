# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for Flash Database tools"""

import datetime
import unittest
from unittest.mock import call, create_autospec, patch

from freezegun import freeze_time
from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materialization_gating_context import (
    IngestViewMaterializationGatingContext,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.flash_database_tools import (
    move_ingest_view_results_between_instances,
    move_ingest_view_results_to_backup,
    ungate_bq_materialization_for_instance,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest.direct.ingest_view_materialization.ingest_view_materialization_gating_context_test import (
    GATING_CONTEXT_PACKAGE_NAME,
    SIMPLE_CONFIG_YAML,
)


class FlashDatabaseToolsTest(unittest.TestCase):
    """tests for flash_database_tools.py"""

    def setUp(self) -> None:
        self.region_code = StateCode.US_XX
        self.mock_project_id = "recidiviz-456"
        self.mock_project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_patcher.start().return_value = self.mock_project_id

        self.mock_bq_client = create_autospec(BigQueryClient)

        def fake_dataset_ref_for_id(dataset_id: str) -> bigquery.DatasetReference:
            return bigquery.DatasetReference(self.mock_project_id, dataset_id)

        self.mock_bq_client.dataset_ref_for_id = fake_dataset_ref_for_id

        self.gcs_factory_patcher = patch(
            f"{GATING_CONTEXT_PACKAGE_NAME}.GcsfsFactory.build"
        )
        self.fake_gcs = FakeGCSFileSystem()
        self.gcs_factory_patcher.start().return_value = self.fake_gcs

    def tearDown(self) -> None:
        self.mock_project_id_patcher.stop()
        self.gcs_factory_patcher.stop()

    def set_config_yaml(self, contents: str) -> None:
        path = GcsfsFilePath.from_absolute_path(
            f"gs://{self.mock_project_id}-configs/bq_materialization_gating_config.yaml"
        )
        self.fake_gcs.upload_from_string(
            path=path, contents=contents, content_type="text/yaml"
        )

    def test_move_ingest_view_results_to_backup_primary_instance(self) -> None:
        move_to_backup_date = datetime.datetime(2022, 2, 1, 0, 0, 0)

        source_id = "us_xx_ingest_view_results_primary"
        destination_id = "us_xx_ingest_view_results_primary_2022_02_01"
        self.mock_bq_client.add_timestamp_suffix_to_dataset_id.return_value = (
            destination_id
        )

        with freeze_time(move_to_backup_date):
            move_ingest_view_results_to_backup(
                state_code=self.region_code,
                ingest_instance=DirectIngestInstance.PRIMARY,
                big_query_client=self.mock_bq_client,
            )

        self.mock_bq_client.assert_has_calls(
            [
                call.add_timestamp_suffix_to_dataset_id(dataset_id=source_id),
                call.create_dataset_if_necessary(
                    dataset_ref=DatasetReference(self.mock_project_id, destination_id),
                    default_table_expiration_ms=2592000000,
                ),
                call.copy_dataset_tables(
                    source_dataset_id=source_id,
                    destination_dataset_id=destination_id,
                ),
                call.delete_dataset(
                    dataset_ref=DatasetReference(self.mock_project_id, source_id),
                    delete_contents=True,
                ),
            ]
        )

    def test_move_ingest_view_results_to_backup_secondary_instance(self) -> None:
        move_to_backup_date = datetime.datetime(2022, 2, 1, 0, 0, 0)

        source_id = "us_xx_ingest_view_results_secondary"
        destination_id = "us_xx_ingest_view_results_secondary_2022_02_01"
        self.mock_bq_client.add_timestamp_suffix_to_dataset_id.return_value = (
            destination_id
        )

        with freeze_time(move_to_backup_date):
            move_ingest_view_results_to_backup(
                state_code=self.region_code,
                ingest_instance=DirectIngestInstance.SECONDARY,
                big_query_client=self.mock_bq_client,
            )

        self.mock_bq_client.assert_has_calls(
            [
                call.add_timestamp_suffix_to_dataset_id(dataset_id=source_id),
                call.create_dataset_if_necessary(
                    dataset_ref=DatasetReference(self.mock_project_id, destination_id),
                    default_table_expiration_ms=2592000000,
                ),
                call.copy_dataset_tables(
                    source_dataset_id=source_id,
                    destination_dataset_id=destination_id,
                ),
                call.delete_dataset(
                    dataset_ref=DatasetReference(self.mock_project_id, source_id),
                    delete_contents=True,
                ),
            ]
        )

    def test_move_ingest_view_results_primary_instance_to_secondary(self) -> None:
        move_to_date = datetime.datetime(2022, 2, 1, 0, 0, 0)

        source_id = "us_xx_ingest_view_results_primary"
        destination_id = "us_xx_ingest_view_results_secondary"
        self.mock_bq_client.add_timestamp_suffix_to_dataset_id.return_value = (
            destination_id
        )

        with freeze_time(move_to_date):
            move_ingest_view_results_between_instances(
                state_code=self.region_code,
                ingest_instance_source=DirectIngestInstance.PRIMARY,
                ingest_instance_destination=DirectIngestInstance.SECONDARY,
                big_query_client=self.mock_bq_client,
            )

        self.mock_bq_client.assert_has_calls(
            [
                call.create_dataset_if_necessary(
                    dataset_ref=DatasetReference(self.mock_project_id, destination_id),
                ),
                call.copy_dataset_tables(
                    source_dataset_id=source_id,
                    destination_dataset_id=destination_id,
                    overwrite_destination_tables=False,
                ),
                call.delete_dataset(
                    dataset_ref=DatasetReference(self.mock_project_id, source_id),
                    delete_contents=True,
                ),
            ]
        )

    def test_move_ingest_view_results_secondary_instance_to_primary(self) -> None:
        move_to_date = datetime.datetime(2022, 2, 1, 0, 0, 0)

        source_id = "us_xx_ingest_view_results_secondary"
        destination_id = "us_xx_ingest_view_results_primary"
        self.mock_bq_client.add_timestamp_suffix_to_dataset_id.return_value = (
            destination_id
        )

        with freeze_time(move_to_date):
            move_ingest_view_results_between_instances(
                state_code=self.region_code,
                ingest_instance_source=DirectIngestInstance.SECONDARY,
                ingest_instance_destination=DirectIngestInstance.PRIMARY,
                big_query_client=self.mock_bq_client,
            )

        self.mock_bq_client.assert_has_calls(
            [
                call.create_dataset_if_necessary(
                    dataset_ref=DatasetReference(self.mock_project_id, destination_id),
                ),
                call.copy_dataset_tables(
                    source_dataset_id=source_id,
                    destination_dataset_id=destination_id,
                    overwrite_destination_tables=False,
                ),
                call.delete_dataset(
                    dataset_ref=DatasetReference(self.mock_project_id, source_id),
                    delete_contents=True,
                ),
            ]
        )

    def test_ungate_bq_materialization_instance_valid(self) -> None:
        # Arrange
        self.set_config_yaml(SIMPLE_CONFIG_YAML)

        # Act
        ungate_bq_materialization_for_instance(
            state_code=self.region_code, ingest_instance=DirectIngestInstance.PRIMARY
        )

        updated_gating_context = IngestViewMaterializationGatingContext.load_from_gcs()

        # Assert
        self.assertTrue(
            updated_gating_context.is_bq_ingest_view_materialization_enabled(
                StateCode.US_XX, DirectIngestInstance.PRIMARY
            )
        )
        self.assertTrue(
            updated_gating_context.is_bq_ingest_view_materialization_enabled(
                StateCode.US_XX, DirectIngestInstance.SECONDARY
            )
        )
        for ingest_instance in DirectIngestInstance:
            self.assertFalse(
                updated_gating_context.is_bq_ingest_view_materialization_enabled(
                    StateCode.US_YY, ingest_instance
                )
            )
        for ingest_instance in DirectIngestInstance:
            self.assertTrue(
                updated_gating_context.is_bq_ingest_view_materialization_enabled(
                    StateCode.US_WW, ingest_instance
                )
            )

    def test_ungate_bq_materialization_instance_invalid_state(self) -> None:
        # Arrange
        self.set_config_yaml(SIMPLE_CONFIG_YAML)

        # Act
        with self.assertRaisesRegex(
            ValueError, f"Invalid state: {StateCode.US_AK.value}"
        ):
            ungate_bq_materialization_for_instance(
                state_code=StateCode.US_AK, ingest_instance=DirectIngestInstance.PRIMARY
            )
