#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests for the admin panel workflows endpoints."""

import datetime
from http import HTTPStatus
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask, url_for
from flask_smorest import Api

from recidiviz.admin_panel.all_routes import admin_panel_blueprint
from recidiviz.admin_panel.routes.workflows import workflows_blueprint
from recidiviz.persistence.database.schema.workflows.schema import OpportunityStatus
from recidiviz.workflows.types import FullOpportunityConfig

TEST_WORKFLOW_TYPE = "usIdSLD"
TEST_OFFSET = 17
TEST_STATUS = OpportunityStatus.INACTIVE
TEST_CONFIG_ID = 6264


def generate_config(
    config_id: int, created_at: datetime.datetime, is_active: bool = True
) -> FullOpportunityConfig:
    return FullOpportunityConfig(
        id=config_id,
        state_code="US_ID",
        opportunity_type="usIdSLD",
        display_name="display",
        methodology_url="url",
        initial_header="header",
        denial_reasons={"DENY": "Denied"},
        eligible_criteria_copy={},
        ineligible_criteria_copy={},
        dynamic_eligibility_text="text",
        call_to_action="do something",
        snooze={},
        is_alert=False,
        sidebar_components=["someComponent"],
        denial_text="Deny",
        created_at=created_at,
        created_by="Mary",
        description="A config",
        status=OpportunityStatus.ACTIVE if is_active else OpportunityStatus.INACTIVE,
        feature_variant="feature_variant",
    )


@pytest.mark.usefixtures("snapshottest_snapshot")
class WorkflowsAdminPanelEndpointTests(TestCase):
    """Test for the workflows admin panel Flask routes."""

    def setUp(self) -> None:
        # Set up app
        self.app = Flask(__name__)
        self.app.register_blueprint(admin_panel_blueprint)
        api = Api(
            self.app,
            spec_kwargs={
                "title": "default",
                "version": "1.0.0",
                "openapi_version": "3.1.0",
            },
        )

        api.register_blueprint(workflows_blueprint, url_prefix="/admin/workflows")
        self.client = self.app.test_client()

        with self.app.test_request_context():
            self.enabled_states_url = url_for("workflows.EnabledStatesAPI")
            self.opportunities_url = url_for(
                "workflows.OpportunitiesAPI", state_code_str="US_ID"
            )
            self.opportunity_configuration_url = url_for(
                "workflows.OpportunityConfigurationsAPI",
                state_code_str="US_ID",
                opportunity_type=TEST_WORKFLOW_TYPE,
            )
            self.opportunity_configuration_url_with_query_params = url_for(
                "workflows.OpportunityConfigurationsAPI",
                state_code_str="US_ID",
                opportunity_type=TEST_WORKFLOW_TYPE,
                offset=TEST_OFFSET,
                status=TEST_STATUS.name,
            )
            self.single_opportunity_configuration_url = url_for(
                "workflows.OpportunitySingleConfigurationAPI",
                state_code_str="US_ID",
                opportunity_type=TEST_WORKFLOW_TYPE,
                config_id=TEST_CONFIG_ID,
            )

    ########
    # GET /workflows/enabled_state_codes
    ########

    @patch(
        "recidiviz.admin_panel.routes.workflows.get_workflows_enabled_states",
    )
    def test_enabled_states(self, mock_enabled_states: MagicMock) -> None:
        mock_enabled_states.return_value = ["US_AK", "US_HI"]

        response = self.client.get(self.enabled_states_url)

        self.assertEqual(HTTPStatus.OK, response.status_code)
        self.assertEqual(
            [{"code": "US_AK", "name": "Alaska"}, {"code": "US_HI", "name": "Hawaii"}],
            response.json,
        )

    ########
    # GET /workflows/<state_code>/opportunities
    ########

    @patch(
        "recidiviz.admin_panel.routes.workflows.WorkflowsQuerier",
    )
    @patch(
        "recidiviz.admin_panel.routes.workflows.get_workflows_enabled_states",
    )
    def test_get_opportunities(
        self, mock_enabled_states: MagicMock, mock_querier: MagicMock
    ) -> None:
        mock_enabled_states.return_value = ["US_ID"]

        mock_config = [
            {
                "state_code": "US_ID",
                "name": "Opportunity Name",
                "system": "INCARCERATION",
                "url": "url-path",
                "completion_event": "event_id",
                "experiment_id": "experiment_id",
                "last_updated_at": datetime.datetime(2024, 4, 15),
                "last_updated_by": "bob",
            }
        ]

        mock_querier.return_value.get_opportunities.return_value = mock_config

        expected_response = [
            {
                "stateCode": "US_ID",
                "name": "Opportunity Name",
                "system": "INCARCERATION",
                "url": "url-path",
                "completionEvent": "event_id",
                "experimentId": "experiment_id",
                "lastUpdatedBy": "bob",
                "lastUpdatedAt": "2024-04-15 00:00:00",
            }
        ]

        response = self.client.get(self.opportunities_url)

        self.assertEqual(HTTPStatus.OK, response.status_code)
        self.assertEqual(expected_response, response.json)

    ########
    # GET /workflows/<state_code>/<opportunity_type>/configurations
    ########
    @patch(
        "recidiviz.admin_panel.routes.workflows.WorkflowsQuerier",
    )
    @patch(
        "recidiviz.admin_panel.routes.workflows.get_workflows_enabled_states",
    )
    def test_get_configs_for_opportunity(
        self, mock_enabled_states: MagicMock, mock_querier: MagicMock
    ) -> None:
        mock_enabled_states.return_value = ["US_ID"]
        mock_configs = [
            generate_config(1, datetime.datetime(2024, 5, 12), is_active=True),
            generate_config(2, datetime.datetime(2024, 5, 13), is_active=False),
            generate_config(3, datetime.datetime(2024, 5, 15), is_active=True),
        ]

        mock_querier.return_value.get_configs_for_type.return_value = mock_configs

        response = self.client.get(self.opportunity_configuration_url)

        self.assertEqual(HTTPStatus.OK, response.status_code)
        self.assertEqual(3, len(response.json))  # type: ignore[arg-type]
        self.snapshot.assert_match(  # type: ignore[attr-defined]
            response.json, name="test_get_configs_for_opportunity"
        )

    @patch(
        "recidiviz.admin_panel.routes.workflows.WorkflowsQuerier",
    )
    @patch(
        "recidiviz.admin_panel.routes.workflows.get_workflows_enabled_states",
    )
    def test_get_configs_for_opportunity_passes_query_params(
        self, mock_enabled_states: MagicMock, mock_querier: MagicMock
    ) -> None:
        mock_enabled_states.return_value = ["US_ID"]

        response = self.client.get(self.opportunity_configuration_url_with_query_params)

        self.assertEqual(HTTPStatus.OK, response.status_code)

        mock_querier.return_value.get_configs_for_type.assert_called_with(
            TEST_WORKFLOW_TYPE,
            offset=TEST_OFFSET,
            status=TEST_STATUS,
        )

    @patch(
        "recidiviz.admin_panel.routes.workflows.WorkflowsQuerier",
    )
    @patch(
        "recidiviz.admin_panel.routes.workflows.get_workflows_enabled_states",
    )
    def test_get_single_config_by_id(
        self, mock_enabled_states: MagicMock, mock_querier: MagicMock
    ) -> None:
        mock_enabled_states.return_value = ["US_ID"]

        response = self.client.get(self.single_opportunity_configuration_url)

        self.assertEqual(HTTPStatus.OK, response.status_code)

        mock_querier.return_value.get_config_for_id.assert_called_with(
            TEST_WORKFLOW_TYPE,
            TEST_CONFIG_ID,
        )

    @patch(
        "recidiviz.admin_panel.routes.workflows.WorkflowsQuerier",
    )
    @patch(
        "recidiviz.admin_panel.routes.workflows.get_workflows_enabled_states",
    )
    def test_get_single_config_by_id_returns_bad_request_on_failure(
        self, mock_enabled_states: MagicMock, mock_querier: MagicMock
    ) -> None:
        mock_enabled_states.return_value = ["US_ID"]

        mock_querier.return_value.get_config_for_id.return_value = None

        response = self.client.get(self.single_opportunity_configuration_url)

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
