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

import datetime
import unittest
from typing import Optional
from unittest import mock
from unittest.mock import create_autospec, patch

from google.cloud import bigquery

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
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_to_bq_refresh import (
    federated_bq_schema_refresh,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)

_OLD_TABLE_CREATION_TIME_MS = str(
    int(
        (
            datetime.datetime.now(tz=datetime.timezone.utc)
            - datetime.timedelta(days=30)
        ).timestamp()
        * 1000
    )
)


def _table_list_item(
    *, project_id: str, dataset_id: str, table_id: str
) -> bigquery.table.TableListItem:
    return bigquery.table.TableListItem(
        {
            "tableReference": {
                "projectId": project_id,
                "datasetId": dataset_id,
                "tableId": table_id,
            },
            "creationTime": _OLD_TABLE_CREATION_TIME_MS,
        }
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
            use_declared_schema: bool = True,
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
                "operations_cloudsql_connection",
                "operations_regional",
            }
        },
    )
    def test_federated_cloud_sql_to_bq_refresh(self) -> None:
        # Arrange
        self.mock_bq_client.dataset_exists.return_value = True

        collector = federated_cloud_sql_table_big_query_view_collector.FederatedCloudSQLTableBigQueryViewCollector(
            CloudSqlToBQConfig.for_schema_type(SchemaType.OPERATIONS)
        )
        managed_table_id = collector.collect_view_builders()[0].view_id

        def fake_list_tables(
            dataset_id: str,
        ) -> list[bigquery.table.TableListItem]:
            return [
                _table_list_item(
                    project_id=self.mock_project_id,
                    dataset_id=dataset_id,
                    table_id=managed_table_id,
                ),
                _table_list_item(
                    project_id=self.mock_project_id,
                    dataset_id=dataset_id,
                    table_id="unmanaged_table",
                ),
            ]

        self.mock_bq_client.list_tables.side_effect = fake_list_tables

        # Act
        federated_bq_schema_refresh(SchemaType.OPERATIONS)

        # Assert
        self.mock_bq_client.list_tables.assert_has_calls(
            [
                mock.call("operations_cloudsql_connection"),
                mock.call("operations_regional"),
            ],
            any_order=True,
        )
        self.assertEqual(2, self.mock_bq_client.list_tables.call_count)
        # Cleanup deletes unmanaged tables by calling delete_table with just the
        # address, which distinguishes these calls from the not_found_ok=True calls
        # made elsewhere in the refresh to delete and recreate managed views.
        cleanup_delete_calls = [
            call
            for call in self.mock_bq_client.delete_table.mock_calls
            if call.kwargs == {}
        ]
        self.assertCountEqual(
            [
                mock.call(
                    BigQueryAddress(
                        dataset_id="operations_cloudsql_connection",
                        table_id="unmanaged_table",
                    )
                ),
                mock.call(
                    BigQueryAddress(
                        dataset_id="operations_regional",
                        table_id="unmanaged_table",
                    )
                ),
            ],
            cleanup_delete_calls,
        )
        self.assertEqual(
            self.mock_bq_client.create_dataset_if_necessary.mock_calls,
            [
                mock.call(
                    "operations_cloudsql_connection",
                    default_table_expiration_ms=None,
                ),
                mock.call("operations_regional", default_table_expiration_ms=None),
                mock.call("operations", default_table_expiration_ms=None),
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
                    "my_prefix_operations_cloudsql_connection",
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
                mock.call(
                    "my_prefix_cloud_sql_to_bq_refresh",
                    default_table_expiration_ms=expiration_ms,
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
            BigQueryAddress(
                dataset_id="my_prefix_cloud_sql_to_bq_refresh",
                table_id=CLOUD_SQL_TO_BQ_REFRESH_STATUS_ADDRESS.table_id,
            ),
        )

        # A sandboxed run never cleans up unmanaged views/tables: list_tables is
        # only ever called as part of that cleanup, and delete_table is only called
        # by cleanup with just the address (other delete_table calls, made when
        # recreating a changed view, always pass not_found_ok=True).
        self.mock_bq_client.list_tables.assert_not_called()
        cleanup_delete_calls = [
            call
            for call in self.mock_bq_client.delete_table.mock_calls
            if call.kwargs == {}
        ]
        self.assertEqual([], cleanup_delete_calls)
