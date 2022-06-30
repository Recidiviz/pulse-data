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

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.view_update_manager import (
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants import states
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.base_schema import JailsBase, StateBase
from recidiviz.persistence.database.bq_refresh import (
    federated_cloud_sql_table_big_query_view_collector,
    federated_cloud_sql_to_bq_refresh,
)
from recidiviz.persistence.database.bq_refresh.bq_refresh_status_storage import (
    CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS,
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
    region_codes_to_exclude: []
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
        config = CloudSqlToBQConfig.for_schema_type(SchemaType.CASE_TRIAGE)
        with self.assertRaisesRegex(
            ValueError, r"^Unexpected schema type \[CASE_TRIAGE\]$"
        ):
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
            "SELECT state_person_external_id.person_external_id_id,"
            "state_person_external_id.external_id,state_person_external_id.state_code,"
            "state_person_external_id.id_type,state_person_external_id.person_id FROM "
            "`recidiviz-staging.us_xx_state_regional.state_person_external_id` state_person_external_id\n"
            "UNION ALL\n"
            "SELECT state_person_external_id.person_external_id_id,"
            "state_person_external_id.external_id,state_person_external_id.state_code,"
            "state_person_external_id.id_type,state_person_external_id.person_id FROM "
            "`recidiviz-staging.us_ww_state_regional.state_person_external_id` state_person_external_id"
        )
        self.assertEqual(expected_query, view.view_query)
        self.assertEqual(
            BigQueryAddress(
                dataset_id="state_regional", table_id="state_person_external_id"
            ),
            view.materialized_address,
        )

    def test_build_unioned_segments_view_with_address_overrides(self) -> None:
        config = CloudSqlToBQConfig.for_schema_type(SchemaType.STATE)
        view_builder = UnionedStateSegmentsViewBuilder(
            config=config,
            table=StateBase.metadata.tables["state_person_external_id"],
            state_codes=[StateCode.US_XX, StateCode.US_WW],
        )

        address_overrides_builder = BigQueryAddressOverrides.Builder(
            sandbox_prefix="my_prefix"
        )
        for dataset_id in [
            "state_regional",
            "us_xx_state_regional",
            "us_ww_state_regional",
        ]:
            address_overrides_builder.register_sandbox_override_for_entire_dataset(
                dataset_id
            )
        address_overrides = address_overrides_builder.build()
        view = view_builder.build(address_overrides=address_overrides)
        expected_query = (
            "SELECT state_person_external_id.person_external_id_id,"
            "state_person_external_id.external_id,state_person_external_id.state_code,"
            "state_person_external_id.id_type,state_person_external_id.person_id FROM "
            "`recidiviz-staging.my_prefix_us_xx_state_regional.state_person_external_id` state_person_external_id\n"
            "UNION ALL\n"
            "SELECT state_person_external_id.person_external_id_id,"
            "state_person_external_id.external_id,state_person_external_id.state_code,"
            "state_person_external_id.id_type,state_person_external_id.person_id FROM "
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
                "operations_v2_cloudsql_connection",
                "us_xx_operations_regional",
                "us_ww_operations_regional",
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
                        "recidiviz-staging", "operations_v2_cloudsql_connection"
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
                mock.call(
                    DatasetReference("recidiviz-staging", "cloud_sql_to_bq_refresh")
                ),
            ],
        )

        self.mock_bq_client.backup_dataset_tables_if_dataset_exists.assert_called_with(
            dataset_id="operations"
        )
        self.mock_bq_client.copy_dataset_tables_across_regions.assert_called_with(
            source_dataset_id="operations_regional",
            destination_dataset_id="operations",
            overwrite_destination_tables=True,
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
        stream_into_table_args = self.mock_bq_client.stream_into_table.call_args
        self.assertEqual(
            stream_into_table_args[0][0],
            DatasetReference("recidiviz-staging", "cloud_sql_to_bq_refresh"),
        )
        self.assertEqual(
            stream_into_table_args[0][1],
            CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS.table_id,
        )
        self.assertEqual(2, len(stream_into_table_args[0][2]))
        self.assertEqual(
            stream_into_table_args[0][2][0].get("region_code"), state_codes[0].name
        )
        self.assertEqual(stream_into_table_args[0][2][0].get("schema"), "OPERATIONS")
        self.assertEqual(
            stream_into_table_args[0][2][1].get("region_code"), state_codes[1].name
        )
        self.assertEqual(stream_into_table_args[0][2][1].get("schema"), "OPERATIONS")

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
                        "recidiviz-staging",
                        "my_prefix_operations_v2_cloudsql_connection",
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
                mock.call(
                    DatasetReference(
                        "recidiviz-staging", "my_prefix_cloud_sql_to_bq_refresh"
                    )
                ),
            ],
        )

        self.mock_bq_client.backup_dataset_tables_if_dataset_exists.assert_called_with(
            dataset_id="my_prefix_operations"
        )
        self.mock_bq_client.copy_dataset_tables_across_regions.assert_called_with(
            source_dataset_id="my_prefix_operations_regional",
            destination_dataset_id="my_prefix_operations",
            overwrite_destination_tables=True,
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
        stream_into_table_args = self.mock_bq_client.stream_into_table.call_args
        self.assertEqual(
            stream_into_table_args[0][0],
            DatasetReference("recidiviz-staging", "my_prefix_cloud_sql_to_bq_refresh"),
        )
        self.assertEqual(
            stream_into_table_args[0][1],
            CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS.table_id,
        )
        self.assertEqual(2, len(stream_into_table_args[0][2]))
        self.assertEqual(
            stream_into_table_args[0][2][0].get("region_code"), state_codes[0].name
        )
        self.assertEqual(stream_into_table_args[0][2][0].get("schema"), "OPERATIONS")
        self.assertEqual(
            stream_into_table_args[0][2][1].get("region_code"), state_codes[1].name
        )
        self.assertEqual(stream_into_table_args[0][2][1].get("schema"), "OPERATIONS")

    def test_secondary_without_prefix_raises(self) -> None:
        with self.assertRaises(ValueError):
            federated_bq_schema_refresh(
                SchemaType.STATE, direct_ingest_instance=DirectIngestInstance.SECONDARY
            )

    @patch(f"{FEDERATED_REFRESH_PACKAGE_NAME}.get_existing_direct_ingest_states")
    @patch(
        f"{FEDERATED_REFRESH_COLLECTOR_PACKAGE_NAME}.get_existing_direct_ingest_states"
    )
    @patch(
        f"{FEDERATED_REFRESH_PACKAGE_NAME}.CLOUDSQL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA",
        {
            SchemaType.OPERATIONS: {
                "operations_v2_cloudsql_connection",
                "us_ww_operations_regional",
                "us_xx_operations_regional",
                # Dataset for region that is no longer managed, should be deleted
                "us_zz_operations_regional",
            }
        },
    )
    def test_federated_cloud_sql_to_bq_refresh_excluded_region(
        self,
        mock_states_fn: mock.MagicMock,
        mock_states_fn_other: mock.MagicMock,
    ) -> None:
        # Arrange
        def mock_dataset_ref_for_id(dataset_id: str) -> bigquery.DatasetReference:
            return bigquery.DatasetReference.from_string(
                dataset_id, default_project=self.mock_project_id
            )

        yaml_contents = """
region_codes_to_exclude:
- US_WW
"""
        path = GcsfsFilePath.from_absolute_path(
            f"gs://{self.mock_project_id}-configs/cloud_sql_to_bq_config.yaml"
        )
        self.fake_gcs.upload_from_string(
            path=path, contents=yaml_contents, content_type="text/yaml"
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
                        "recidiviz-staging", "operations_v2_cloudsql_connection"
                    ),
                    None,
                ),
                mock.call(
                    DatasetReference("recidiviz-staging", "us_xx_operations_regional"),
                    None,
                ),
                mock.call(
                    DatasetReference("recidiviz-staging", "operations_regional"), None
                ),
                mock.call(
                    DatasetReference("recidiviz-staging", "operations"),
                    default_table_expiration_ms=None,
                ),
                mock.call(
                    DatasetReference("recidiviz-staging", "cloud_sql_to_bq_refresh")
                ),
            ],
        )

        self.assertEqual(
            self.mock_bq_client.delete_dataset.mock_calls,
            [
                mock.call(
                    bigquery.DatasetReference.from_string(
                        "us_zz_operations_regional",
                        default_project=self.mock_project_id,
                    ),
                    delete_contents=True,
                ),
                mock.call(
                    self.mock_bq_client.backup_dataset_tables_if_dataset_exists.return_value,
                    delete_contents=True,
                    not_found_ok=True,
                ),
            ],
        )

        stream_into_table_args = self.mock_bq_client.stream_into_table.call_args
        self.assertEqual(
            stream_into_table_args[0][0],
            DatasetReference("recidiviz-staging", "cloud_sql_to_bq_refresh"),
        )
        self.assertEqual(
            stream_into_table_args[0][1],
            CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS.table_id,
        )
        self.assertEqual(1, len(stream_into_table_args[0][2]))
        self.assertEqual(stream_into_table_args[0][2][0].get("region_code"), "US_XX")
        self.assertEqual(stream_into_table_args[0][2][0].get("schema"), "OPERATIONS")
