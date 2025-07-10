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

import unittest
from typing import Optional
from unittest import mock
from unittest.mock import create_autospec, patch

from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import (
    BigQueryClientImpl,
    BigQueryViewMaterializationResult,
)
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.constants import TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
from recidiviz.cloud_resources.resource_label import ResourceLabel
from recidiviz.persistence.database.bq_refresh import (
    federated_cloud_sql_table_big_query_view_collector,
    federated_cloud_sql_to_bq_refresh,
)
from recidiviz.persistence.database.bq_refresh.bq_refresh_status_storage import (
    CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_to_bq_refresh import (
    federated_bq_schema_refresh,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)

FEDERATED_REFRESH_PACKAGE_NAME = federated_cloud_sql_to_bq_refresh.__name__
FEDERATED_REFRESH_COLLECTOR_PACKAGE_NAME = (
    federated_cloud_sql_table_big_query_view_collector.__name__
)


class TestFederatedBQSchemaRefresh(unittest.TestCase):
    """Tests for federated_cloud_sql_to_bq_refresh.py."""

    def setUp(self) -> None:
        self.mock_project_id = "recidiviz-staging"
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_metadata = self.metadata_patcher.start()
        self.mock_metadata.return_value = self.mock_project_id
        self.mock_bq_client = create_autospec(BigQueryClientImpl)
        self.client_patcher = mock.patch(
            f"{FEDERATED_REFRESH_PACKAGE_NAME}.BigQueryClientImpl"
        )
        self.client_patcher.start().return_value = self.mock_bq_client
        self.view_update_client_patcher = mock.patch(
            "recidiviz.big_query.view_update_manager.BigQueryClientImpl"
        )
        self.view_update_client_patcher.start().return_value = self.mock_bq_client

        def fake_materialize_view_to_table(
            view: BigQueryView,
            # pylint: disable=unused-argument
            use_query_cache: bool,
            view_configuration_changed: bool,
            job_labels: Optional[list[ResourceLabel]] = None,
        ) -> BigQueryViewMaterializationResult:
            return BigQueryViewMaterializationResult(
                view_address=view.address,
                materialized_table=create_autospec(bigquery.Table),
                completed_materialization_job=create_autospec(bigquery.QueryJob),
            )

        self.mock_bq_client.materialize_view_to_table.side_effect = (
            fake_materialize_view_to_table
        )

        test_secrets = {
            # pylint: disable=protected-access
            SQLAlchemyEngineManager._get_cloudsql_instance_id_key(
                schema_type=schema_type,
                secret_prefix_override=None,
            ): f"test-project:us-east2:{schema_type.value}-data"
            for schema_type in SchemaType
            if schema_type.has_cloud_sql_instance
        }
        self.get_secret_patcher = mock.patch("recidiviz.utils.secrets.get_secret")

        self.get_secret_patcher.start().side_effect = test_secrets.get

    def tearDown(self) -> None:
        self.metadata_patcher.stop()
        self.get_secret_patcher.stop()
        self.client_patcher.stop()
        self.view_update_client_patcher.stop()

    @patch(
        f"{FEDERATED_REFRESH_PACKAGE_NAME}.CLOUDSQL_REFRESH_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED_BY_SCHEMA",
        {
            SchemaType.OPERATIONS: {
                "operations_v2_cloudsql_connection",
                "operations_regional",
            }
        },
    )
    def test_federated_cloud_sql_to_bq_refresh(self) -> None:
        # Arrange
        self.mock_bq_client.dataset_exists.return_value = True

        # Act
        federated_bq_schema_refresh(SchemaType.OPERATIONS)

        # Assert
        self.assertEqual(
            self.mock_bq_client.create_dataset_if_necessary.mock_calls,
            [
                mock.call(
                    "operations_v2_cloudsql_connection",
                    default_table_expiration_ms=None,
                ),
                mock.call("operations_regional", default_table_expiration_ms=None),
                mock.call("operations", default_table_expiration_ms=None),
                mock.call("cloud_sql_to_bq_refresh"),
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
            BigQueryAddress(
                dataset_id="cloud_sql_to_bq_refresh",
                table_id=CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS.table_id,
            ),
        )

    def test_federated_cloud_sql_to_bq_refresh_with_overrides(self) -> None:
        # Arrange
        self.mock_bq_client.dataset_exists.return_value = True
        self.mock_bq_client.list_tables.return_value = [
            bigquery.TableReference(
                DatasetReference(self.mock_project_id, "my_prefix_operations"),
                "table_1",
            ),
            bigquery.TableReference(
                DatasetReference(self.mock_project_id, "my_prefix_operations"),
                "table_2",
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
                    "my_prefix_operations_v2_cloudsql_connection",
                    default_table_expiration_ms=expiration_ms,
                ),
                mock.call(
                    "my_prefix_operations_regional",
                    default_table_expiration_ms=expiration_ms,
                ),
                mock.call(
                    "my_prefix_operations",
                    default_table_expiration_ms=expiration_ms,
                ),
                mock.call("my_prefix_cloud_sql_to_bq_refresh"),
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
            BigQueryAddress(
                dataset_id="my_prefix_cloud_sql_to_bq_refresh",
                table_id=CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS.table_id,
            ),
        )
