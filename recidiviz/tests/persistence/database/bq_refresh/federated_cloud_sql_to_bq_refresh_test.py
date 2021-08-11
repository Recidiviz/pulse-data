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
"""Tests for federated_cloud_sql_to_bq_refresh.py."""

import importlib
import unittest
from unittest import mock
from unittest.mock import create_autospec, patch

from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryAddress
from recidiviz.big_query.view_update_manager import (
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants import states
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.persistence.database.base_schema import JailsBase, StateBase
from recidiviz.persistence.database.bq_refresh import (
    federated_cloud_sql_table_big_query_view_collector,
    federated_cloud_sql_to_bq_refresh,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_to_bq_refresh import (
    UnionedStateSegmentsViewBuilder,
    federated_bq_schema_refresh,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem

FEDERATED_REFRESH_PACKAGE_NAME = federated_cloud_sql_to_bq_refresh.__name__
FEDERATED_REFRESH_COLLECTOR_PACKAGE_NAME = (
    federated_cloud_sql_table_big_query_view_collector.__name__
)


class TestFederatedBQSchemaRefresh(unittest.TestCase):
    """Tests for federated_cloud_sql_to_bq_refresh.py."""

    def setUp(self) -> None:
        # Ensures StateCode.US_XX is properly loaded
        importlib.reload(states)

        self.mock_project_id = "recidiviz-staging"
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_metadata = self.metadata_patcher.start()
        self.mock_metadata.return_value = self.mock_project_id
        self.gcs_factory_patcher = mock.patch(
            "recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config.GcsfsFactory.build"
        )
        self.fake_gcs = FakeGCSFileSystem()
        self.gcs_factory_patcher.start().return_value = self.fake_gcs
        yaml_contents = """
    region_codes_to_exclude:
    - US_ND
    state_history_tables_to_include:
    - state_person_history
    county_columns_to_exclude:
    person:
    - full_name
    - birthdate_inferred_from_age
    """
        path = GcsfsFilePath.from_absolute_path(
            f"gs://{self.mock_project_id}-configs/cloud_sql_to_bq_config.yaml"
        )
        self.fake_gcs.upload_from_string(
            path=path, contents=yaml_contents, content_type="text/yaml"
        )

        self.mock_bq_client = create_autospec(BigQueryClientImpl)
        self.client_patcher = mock.patch(
            f"{FEDERATED_REFRESH_PACKAGE_NAME}.BigQueryClientImpl"
        )
        self.client_patcher.start().return_value = self.mock_bq_client
        self.view_update_client_patcher = mock.patch(
            "recidiviz.big_query.view_update_manager.BigQueryClientImpl"
        )
        self.view_update_client_patcher.start().return_value = self.mock_bq_client

        test_secrets = {
            # pylint: disable=protected-access
            SQLAlchemyEngineManager._get_cloudsql_instance_id_key(
                schema_type
            ): f"test-project:us-east2:{schema_type.value}-data"
            for schema_type in SchemaType
        }
        self.get_secret_patcher = mock.patch("recidiviz.utils.secrets.get_secret")

        self.get_secret_patcher.start().side_effect = test_secrets.get

    def tearDown(self) -> None:
        self.metadata_patcher.stop()
        self.get_secret_patcher.stop()
        self.client_patcher.stop()
        self.view_update_client_patcher.stop()

    def test_unioned_segments_view_unsegmented_config_crashes(self) -> None:
        config = CloudSqlToBQConfig.for_schema_type(SchemaType.JAILS)
        with self.assertRaisesRegex(ValueError, r"^Unexpected schema type \[JAILS\]$"):
            _ = UnionedStateSegmentsViewBuilder(
                config=config,
                table=JailsBase.metadata.sorted_tables[0],
                state_codes=[StateCode.US_XX],
            )

    def test_build_unioned_segments_view(self) -> None:
        config = CloudSqlToBQConfig.for_schema_type(SchemaType.STATE)
        view_builder = UnionedStateSegmentsViewBuilder(
            config=config,
            table=StateBase.metadata.tables["state_person_external_id"],
            state_codes=[StateCode.US_XX, StateCode.US_WW],
        )
        view = view_builder.build()
        expected_query = (
            "SELECT state_person_external_id.external_id,state_person_external_id.state_code,"
            "state_person_external_id.id_type,state_person_external_id.person_external_id_id,"
            "state_person_external_id.person_id FROM "
            "`recidiviz-staging.us_xx_state_regional.state_person_external_id` state_person_external_id\n"
            "UNION ALL\n"
            "SELECT state_person_external_id.external_id,state_person_external_id.state_code,"
            "state_person_external_id.id_type,state_person_external_id.person_external_id_id,"
            "state_person_external_id.person_id FROM "
            "`recidiviz-staging.us_ww_state_regional.state_person_external_id` state_person_external_id"
        )
        self.assertEqual(expected_query, view.view_query)
        self.assertEqual(
            BigQueryAddress(
                dataset_id="state_regional", table_id="state_person_external_id"
            ),
            view.materialized_address,
        )

    def test_build_unioned_segments_view_with_dataset_overrides(self) -> None:
        config = CloudSqlToBQConfig.for_schema_type(SchemaType.STATE)
        view_builder = UnionedStateSegmentsViewBuilder(
            config=config,
            table=StateBase.metadata.tables["state_person_external_id"],
            state_codes=[StateCode.US_XX, StateCode.US_WW],
        )
        view = view_builder.build(
            dataset_overrides={
                "state_regional": "my_prefix_state_regional",
                "us_xx_state_regional": "my_prefix_us_xx_state_regional",
                "us_ww_state_regional": "my_prefix_us_ww_state_regional",
            }
        )
        expected_query = (
            "SELECT state_person_external_id.external_id,state_person_external_id.state_code,"
            "state_person_external_id.id_type,state_person_external_id.person_external_id_id,"
            "state_person_external_id.person_id FROM "
            "`recidiviz-staging.my_prefix_us_xx_state_regional.state_person_external_id` state_person_external_id\n"
            "UNION ALL\n"
            "SELECT state_person_external_id.external_id,state_person_external_id.state_code,"
            "state_person_external_id.id_type,state_person_external_id.person_external_id_id,"
            "state_person_external_id.person_id FROM "
            "`recidiviz-staging.my_prefix_us_ww_state_regional.state_person_external_id` state_person_external_id"
        )
        self.assertEqual(expected_query, view.view_query)
        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_prefix_state_regional",
                table_id="state_person_external_id",
            ),
            view.materialized_address,
        )

    @patch(f"{FEDERATED_REFRESH_PACKAGE_NAME}.get_existing_direct_ingest_states")
    @patch(
        f"{FEDERATED_REFRESH_COLLECTOR_PACKAGE_NAME}.get_existing_direct_ingest_states"
    )
    @patch(
        f"{FEDERATED_REFRESH_PACKAGE_NAME}.CLOUDSQL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA",
        {
            SchemaType.OPERATIONS: {
                "operations_cloudsql_connection",
                "us_xx_operations_regional",
                "us_ww_operations_regional",
                "operations_regional",
                "operations",
            }
        },
    )
    def test_federated_cloud_sql_to_bq_refresh(
        self,
        mock_states_fn: mock.MagicMock,
        mock_states_fn_other: mock.MagicMock,
    ) -> None:
        # Arrange
        def mock_dataset_ref_for_id(dataset_id: str) -> bigquery.DatasetReference:
            return bigquery.DatasetReference.from_string(
                dataset_id, default_project=self.mock_project_id
            )

        state_codes = [StateCode.US_XX, StateCode.US_WW]
        mock_states_fn.return_value = state_codes
        mock_states_fn_other.return_value = state_codes

        self.mock_bq_client.dataset_ref_for_id = mock_dataset_ref_for_id
        self.mock_bq_client.dataset_exists.return_value = True

        # Act
        federated_bq_schema_refresh(SchemaType.OPERATIONS)

        # Assert
        self.assertEqual(
            self.mock_bq_client.create_dataset_if_necessary.mock_calls,
            [
                mock.call(
                    DatasetReference(
                        "recidiviz-staging", "operations_cloudsql_connection"
                    ),
                    None,
                ),
                mock.call(
                    DatasetReference("recidiviz-staging", "us_xx_operations_regional"),
                    None,
                ),
                mock.call(
                    DatasetReference("recidiviz-staging", "us_ww_operations_regional"),
                    None,
                ),
                mock.call(
                    DatasetReference("recidiviz-staging", "operations_regional"), None
                ),
                mock.call(
                    DatasetReference("recidiviz-staging", "operations"),
                    default_table_expiration_ms=None,
                ),
            ],
        )

        self.mock_bq_client.list_tables.assert_called_with(dataset_id="operations")
        self.mock_bq_client.backup_dataset_tables_if_dataset_exists.assert_called_with(
            dataset_id="operations"
        )
        self.mock_bq_client.copy_dataset_tables_across_regions.assert_called_with(
            source_dataset_id="operations_regional",
            destination_dataset_id="operations",
        )
        self.mock_bq_client.delete_dataset.assert_has_calls(
            [
                mock.call(
                    self.mock_bq_client.backup_dataset_tables_if_dataset_exists.return_value,
                    delete_contents=True,
                    not_found_ok=True,
                ),
            ]
        )

    @patch(f"{FEDERATED_REFRESH_PACKAGE_NAME}.get_existing_direct_ingest_states")
    @patch(
        f"{FEDERATED_REFRESH_COLLECTOR_PACKAGE_NAME}.get_existing_direct_ingest_states"
    )
    def test_federated_cloud_sql_to_bq_refresh_with_overrides(
        self, mock_states_fn: mock.MagicMock, mock_states_fn_other: mock.MagicMock
    ) -> None:
        # Arrange
        def mock_dataset_ref_for_id(dataset_id: str) -> bigquery.DatasetReference:
            return bigquery.DatasetReference.from_string(
                dataset_id, default_project=self.mock_project_id
            )

        state_codes = [StateCode.US_XX, StateCode.US_WW]
        mock_states_fn.return_value = state_codes
        mock_states_fn_other.return_value = state_codes

        self.mock_bq_client.dataset_ref_for_id = mock_dataset_ref_for_id
        self.mock_bq_client.dataset_exists.return_value = True
        self.mock_bq_client.list_tables.return_value = [
            bigquery.TableReference(
                mock_dataset_ref_for_id("my_prefix_operations"), "table_1"
            ),
            bigquery.TableReference(
                mock_dataset_ref_for_id("my_prefix_operations"), "table_2"
            ),
        ]

        # Act
        federated_bq_schema_refresh(
            SchemaType.OPERATIONS, dataset_override_prefix="my_prefix"
        )

        # Assert
        expiration_ms = TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
        self.assertEqual(
            self.mock_bq_client.create_dataset_if_necessary.mock_calls,
            [
                mock.call(
                    DatasetReference(
                        "recidiviz-staging", "my_prefix_operations_cloudsql_connection"
                    ),
                    expiration_ms,
                ),
                mock.call(
                    DatasetReference(
                        "recidiviz-staging", "my_prefix_us_xx_operations_regional"
                    ),
                    expiration_ms,
                ),
                mock.call(
                    DatasetReference(
                        "recidiviz-staging", "my_prefix_us_ww_operations_regional"
                    ),
                    expiration_ms,
                ),
                mock.call(
                    DatasetReference(
                        "recidiviz-staging", "my_prefix_operations_regional"
                    ),
                    expiration_ms,
                ),
                mock.call(
                    DatasetReference("recidiviz-staging", "my_prefix_operations"),
                    default_table_expiration_ms=expiration_ms,
                ),
            ],
        )

        self.mock_bq_client.backup_dataset_tables_if_dataset_exists.assert_called_with(
            dataset_id="my_prefix_operations"
        )
        self.mock_bq_client.list_tables.assert_called_with(
            dataset_id="my_prefix_operations"
        )
        self.mock_bq_client.delete_table.assert_has_calls(
            [
                mock.call("my_prefix_operations", "table_1"),
                mock.call("my_prefix_operations", "table_2"),
            ]
        )
        self.mock_bq_client.copy_dataset_tables_across_regions.assert_called_with(
            source_dataset_id="my_prefix_operations_regional",
            destination_dataset_id="my_prefix_operations",
        )
        self.mock_bq_client.delete_dataset.assert_has_calls(
            [
                mock.call(
                    self.mock_bq_client.backup_dataset_tables_if_dataset_exists.return_value,
                    delete_contents=True,
                    not_found_ok=True,
                ),
            ]
        )

    def test_secondary_without_prefix_raises(self) -> None:
        with self.assertRaises(ValueError):
            federated_bq_schema_refresh(
                SchemaType.STATE, direct_ingest_instance=DirectIngestInstance.SECONDARY
            )
