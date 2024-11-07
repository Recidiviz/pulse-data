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
"""Tests for execute_update_all_managed_views.py"""
import unittest
from unittest import mock
from unittest.mock import MagicMock, patch

from recidiviz.utils.environment import (
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
    GCPEnvironment,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.view_registry.execute_update_all_managed_views import (
    AllViewsUpdateSuccessPersister,
    execute_update_all_managed_views,
)


class TestAllViewsUpdateSuccessPersister(unittest.TestCase):
    def test_persist(self) -> None:
        mock_client = MagicMock()
        with local_project_id_override(GCP_PROJECT_STAGING):
            persister = AllViewsUpdateSuccessPersister(bq_client=mock_client)

            # Just shouldn't crash
            persister.record_success_in_bq(
                deployed_builders=[], dataset_override_prefix=None, runtime_sec=100
            )


class TestExecuteUpdateAllManagedViews(unittest.TestCase):
    """Tests the execute_update_all_managed_views function."""

    def setUp(self) -> None:
        self.all_views_update_success_persister_patcher = patch(
            "recidiviz.view_registry.execute_update_all_managed_views.AllViewsUpdateSuccessPersister"
        )
        self.all_views_update_success_persister_constructor = (
            self.all_views_update_success_persister_patcher.start()
        )
        self.mock_all_views_update_success_persister = (
            self.all_views_update_success_persister_constructor.return_value
        )
        self.environment_patcher = mock.patch(
            "recidiviz.utils.environment.get_gcp_environment",
            return_value=GCPEnvironment.PRODUCTION.value,
        )
        self.environment_patcher.start()

        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = GCP_PROJECT_PRODUCTION

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.all_views_update_success_persister_patcher.stop()
        self.project_id_patcher.stop()

    @mock.patch(
        "recidiviz.view_registry.execute_update_all_managed_views.deployed_view_builders",
    )
    @mock.patch(
        "recidiviz.view_registry.execute_update_all_managed_views.BigQueryClientImpl"
    )
    @mock.patch(
        "recidiviz.view_registry.execute_update_all_managed_views.create_managed_dataset_and_deploy_views_for_view_builders"
    )
    def test_execute_update_all_managed_views(
        self,
        mock_create: MagicMock,
        _mock_bq_client: MagicMock,
        _mock_view_builders: MagicMock,
    ) -> None:
        execute_update_all_managed_views(sandbox_prefix=None)
        mock_create.assert_called()
        self.mock_all_views_update_success_persister.record_success_in_bq.assert_called_with(
            deployed_builders=mock.ANY,
            dataset_override_prefix=None,
            runtime_sec=mock.ANY,
        )

    @mock.patch(
        "recidiviz.view_registry.execute_update_all_managed_views.deployed_view_builders",
    )
    @mock.patch(
        "recidiviz.view_registry.execute_update_all_managed_views.BigQueryClientImpl"
    )
    @mock.patch(
        "recidiviz.view_registry.execute_update_all_managed_views.create_managed_dataset_and_deploy_views_for_view_builders"
    )
    def test_execute_update_all_managed_views_with_sandbox_prefix(
        self,
        mock_create: MagicMock,
        _mock_bq_client: MagicMock,
        _mock_view_builders: MagicMock,
    ) -> None:
        execute_update_all_managed_views(sandbox_prefix="test_prefix")
        mock_create.assert_called()
        self.mock_all_views_update_success_persister.record_success_in_bq.assert_called_with(
            deployed_builders=mock.ANY,
            dataset_override_prefix="test_prefix",
            runtime_sec=mock.ANY,
        )
