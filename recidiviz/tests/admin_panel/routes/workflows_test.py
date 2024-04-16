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

from flask import Flask, url_for
from flask_smorest import Api

from recidiviz.admin_panel.all_routes import admin_panel_blueprint
from recidiviz.admin_panel.routes.workflows import workflows_blueprint


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
    # GET /outliers/enabled_state_codes
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
