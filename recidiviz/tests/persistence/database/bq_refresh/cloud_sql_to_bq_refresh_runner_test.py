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
"""Tests for cloud_sql_to_bq_refresh_runner.py."""

import unittest
from unittest import mock
from unittest.mock import create_autospec

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.persistence.database.bq_refresh import cloud_sql_to_bq_refresh_runner
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.schema_utils import SchemaType

CLOUD_SQL_BQ_EXPORT_RUNNER_PACKAGE_NAME = cloud_sql_to_bq_refresh_runner.__name__


class CloudSqlToBQExportManagerTest(unittest.TestCase):
    """Tests for cloud_sql_to_bq_refresh_runner.py."""

    def setUp(self) -> None:
        self.bq_refresh_patcher = mock.patch(
            f"{CLOUD_SQL_BQ_EXPORT_RUNNER_PACKAGE_NAME}.bq_refresh"
        )
        self.mock_bq_refresh = self.bq_refresh_patcher.start()

        self.mock_client = create_autospec(BigQueryClient)

        self.cloud_sql_to_gcs_export_patcher = mock.patch(
            f"{CLOUD_SQL_BQ_EXPORT_RUNNER_PACKAGE_NAME}.cloud_sql_to_gcs_export"
        )
        self.mock_cloud_sql_to_gcs_export = self.cloud_sql_to_gcs_export_patcher.start()

        self.fake_table_name = "first_table"

        self.export_config_patcher = mock.patch(
            f"{CLOUD_SQL_BQ_EXPORT_RUNNER_PACKAGE_NAME}.CloudSqlToBQConfig.for_schema_type"
        )
        self.mock_bq_refresh_config_fn = self.export_config_patcher.start()
        self.mock_bq_refresh_config = create_autospec(CloudSqlToBQConfig)
        self.mock_bq_refresh_config_fn.return_value = self.mock_bq_refresh_config

    def tearDown(self) -> None:
        self.bq_refresh_patcher.stop()
        self.cloud_sql_to_gcs_export_patcher.stop()
        self.export_config_patcher.stop()

    def test_export_table_then_load_table_succeeds(self) -> None:
        """Test that export_table_then_load_table passes the client, table, and config
        to bq_refresh.refresh_bq_table_from_gcs_export_synchronous if the export succeeds.
        """

        cloud_sql_to_bq_refresh_runner.export_table_then_load_table(
            self.mock_client, self.fake_table_name, SchemaType.STATE
        )

        self.mock_cloud_sql_to_gcs_export.export_table.assert_called_with(
            self.fake_table_name, self.mock_bq_refresh_config
        )

        self.mock_bq_refresh.refresh_bq_table_from_gcs_export_synchronous.assert_called_with(
            self.mock_client, self.fake_table_name, self.mock_bq_refresh_config
        )

    def test_export_table_then_load_table_export_fails(self) -> None:
        """Test that export_table_then_load_table does not pass args to load the table
        if export fails and raises an error.
        """
        self.mock_cloud_sql_to_gcs_export.export_table.return_value = False

        with self.assertRaises(ValueError):
            cloud_sql_to_bq_refresh_runner.export_table_then_load_table(
                self.mock_client, "random-table", SchemaType.STATE
            )

        self.mock_bq_refresh.assert_not_called()
