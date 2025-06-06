# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for cloud_sql_to_bq_refresh_control.py."""
import unittest
from unittest import mock
from unittest.mock import MagicMock

from mock import create_autospec
from parameterized import parameterized

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.persistence.database.bq_refresh import cloud_sql_to_bq_refresh_control
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_control import (
    RefreshBQDatasetSuccessPersister,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCPEnvironment
from recidiviz.utils.metadata import local_project_id_override

REFRESH_CONTROL_PACKAGE_NAME = cloud_sql_to_bq_refresh_control.__name__


@mock.patch(
    f"{REFRESH_CONTROL_PACKAGE_NAME}.BigQueryClientImpl",
    create_autospec(BigQueryClientImpl),
)
class ExecuteCloudSqlToBQRefreshTest(unittest.TestCase):
    """Tests the execute_cloud_sql_to_bq_refresh function"""

    def setUp(self) -> None:
        self.mock_refresh_bq_dataset_persister = create_autospec(
            RefreshBQDatasetSuccessPersister
        )
        self.mock_refresh_bq_dataset_persister_patcher = mock.patch(
            f"{REFRESH_CONTROL_PACKAGE_NAME}.RefreshBQDatasetSuccessPersister"
        )
        self.mock_refresh_bq_dataset_persister_patcher.start().return_value = (
            self.mock_refresh_bq_dataset_persister
        )
        self.environment_patcher = mock.patch(
            "recidiviz.utils.environment.get_gcp_environment",
            return_value=GCPEnvironment.PRODUCTION.value,
        )
        self.environment_patcher.start()

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.mock_refresh_bq_dataset_persister_patcher.stop()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        mock_federated_bq.return_value = []

        def mock_record_success(
            # pylint: disable=unused-argument
            schema_type: SchemaType,
            runtime_sec: int,
        ) -> None:
            return None

        self.mock_refresh_bq_dataset_persister.record_success_in_bq.side_effect = (
            mock_record_success
        )

        cloud_sql_to_bq_refresh_control.execute_cloud_sql_to_bq_refresh(
            schema_type=SchemaType.OPERATIONS
        )

        mock_federated_bq.assert_called_once_with(
            schema_type=SchemaType.OPERATIONS,
            dataset_override_prefix=None,
        )

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_bq_refresh_invalid_schema_type(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        with self.assertRaisesRegex(ValueError, r"Unsupported schema type*"):
            cloud_sql_to_bq_refresh_control.execute_cloud_sql_to_bq_refresh(
                schema_type=SchemaType.JUSTICE_COUNTS
            )
        mock_federated_bq.assert_not_called()
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh_federated_throws(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        mock_federated_bq.side_effect = ValueError("some error")

        with self.assertRaises(ValueError):
            cloud_sql_to_bq_refresh_control.execute_cloud_sql_to_bq_refresh(
                schema_type=SchemaType.OPERATIONS
            )
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh_bq_success_throws(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        mock_federated_bq.return_value = None

        self.mock_refresh_bq_dataset_persister.record_success_in_bq.side_effect = (
            ValueError("some error")
        )

        with self.assertRaises(ValueError):
            cloud_sql_to_bq_refresh_control.execute_cloud_sql_to_bq_refresh(
                schema_type=SchemaType.OPERATIONS
            )
        mock_federated_bq.assert_called()

    @parameterized.expand(
        [
            ("operations", SchemaType.OPERATIONS),
            ("case_triage", SchemaType.CASE_TRIAGE),
        ]
    )
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh_args_check(
        self,
        _name: str,
        schema_called_with: SchemaType,
        mock_federated_bq: mock.MagicMock,
    ) -> None:
        mock_federated_bq.return_value = ["foo_table"]

        cloud_sql_to_bq_refresh_control.execute_cloud_sql_to_bq_refresh(
            schema_type=schema_called_with,
        )

        mock_federated_bq.assert_called_with(
            schema_type=schema_called_with,
            dataset_override_prefix=None,
        )
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_called_with(
            schema_type=schema_called_with,
            runtime_sec=mock.ANY,
        )


class TestRefreshBQDatasetSuccessPersister(unittest.TestCase):
    def test_persist(self) -> None:
        mock_client = MagicMock()
        with local_project_id_override(GCP_PROJECT_STAGING):
            persister = RefreshBQDatasetSuccessPersister(bq_client=mock_client)

            # Just shouldn't crash
            persister.record_success_in_bq(
                schema_type=SchemaType.OPERATIONS,
                runtime_sec=100,
            )
