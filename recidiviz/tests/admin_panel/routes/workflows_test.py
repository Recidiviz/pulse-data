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
import json
from http import HTTPStatus
from typing import Any, Dict
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pytest
import responses
from flask import Flask, url_for
from flask_smorest import Api
from freezegun import freeze_time

from recidiviz.admin_panel.all_routes import admin_panel_blueprint
from recidiviz.admin_panel.routes.workflows import workflows_blueprint
from recidiviz.persistence.database.schema.workflows.schema import OpportunityStatus
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
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
        denial_reasons=[{"key": "DENY", "text": "Denied"}],
        eligible_criteria_copy=[],
        ineligible_criteria_copy=[],
        dynamic_eligibility_text="text",
        call_to_action="do something",
        subheading="this is what the policy does",
        snooze={"default_snooze_days": 30, "max_snooze_days": 180},
        is_alert=False,
        priority="NORMAL",
        sidebar_components=["someComponent"],
        denial_text="Deny",
        created_at=created_at,
        created_by="Mary",
        variant_description="A config",
        revision_description="for testing",
        status=OpportunityStatus.ACTIVE if is_active else OpportunityStatus.INACTIVE,
        feature_variant="feature_variant",
        eligibility_date_text="date text",
        hide_denial_revert=True,
        tooltip_eligibility_text="Eligible",
        tab_groups=[],
        compare_by=[
            {
                "field": "eligibilityDate",
                "sort_direction": "asc",
                "undefined_behavior": "undefinedFirst",
            }
        ],
        notifications=[],
        zero_grants_tooltip="example tooltip",
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

        self.headers: Dict[str, Dict[Any, Any]] = {"x-goog-iap-jwt-assertion": {}}

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
            self.single_opportunity_configuration_deactivate_url = url_for(
                "workflows.OpportunitySingleConfigurationDeactivateAPI",
                state_code_str="US_ID",
                opportunity_type=TEST_WORKFLOW_TYPE,
                config_id=TEST_CONFIG_ID,
            )
            self.single_opportunity_configuration_activate_url = url_for(
                "workflows.OpportunitySingleConfigurationActivateAPI",
                state_code_str="US_ID",
                opportunity_type=TEST_WORKFLOW_TYPE,
                config_id=TEST_CONFIG_ID,
            )
            self.single_opportunity_configuration_promote_url = url_for(
                "workflows.OpportunitySingleConfigurationPromoteAPI",
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
                "opportunity_type": "Opportunity Name",
                "system_type": "INCARCERATION",
                "url_section": "url-path",
                "completion_event": "event_id",
                "experiment_id": "experiment_id",
                "homepage_position": 1,
                "last_updated_at": datetime.datetime(2024, 4, 15),
                "last_updated_by": "bob",
            },
            {
                "state_code": "US_ID",
                "opportunity_type": "Unprovisioned Opp",
                "system_type": "INCARCERATION",
                "url_section": "other-url-path",
                "completion_event": "event_id",
                "experiment_id": "experiment_id",
                "homepage_position": None,
                "last_updated_at": None,
                "last_updated_by": None,
            },
        ]

        mock_querier.return_value.get_opportunities.return_value = mock_config

        expected_response = [
            {
                "stateCode": "US_ID",
                "opportunityType": "Opportunity Name",
                "systemType": "INCARCERATION",
                "urlSection": "url-path",
                "completionEvent": "event_id",
                "experimentId": "experiment_id",
                "homepagePosition": 1,
                "lastUpdatedBy": "bob",
                "lastUpdatedAt": "2024-04-15 00:00:00",
            },
            {
                "stateCode": "US_ID",
                "opportunityType": "Unprovisioned Opp",
                "systemType": "INCARCERATION",
                "urlSection": "other-url-path",
                "completionEvent": "event_id",
                "experimentId": "experiment_id",
                "homepagePosition": None,
                "lastUpdatedAt": None,
                "lastUpdatedBy": None,
            },
        ]

        response = self.client.get(self.opportunities_url)

        self.assertEqual(HTTPStatus.OK, response.status_code)
        self.assertEqual(expected_response, response.json)

    ########
    # GET /workflows/<state_code>/opportunities/<opportunity_type>/configurations
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

    ########
    # POST /workflows/<state_code>/opportunities/<opportunity_type>/configurations
    ########
    @patch(
        "recidiviz.admin_panel.routes.workflows.get_authenticated_user_email",
    )
    @patch(
        "recidiviz.admin_panel.routes.workflows.WorkflowsQuerier",
    )
    @patch(
        "recidiviz.admin_panel.routes.workflows.get_workflows_enabled_states",
    )
    def test_post_new_config(
        self,
        mock_enabled_states: MagicMock,
        mock_querier: MagicMock,
        mock_get_email: MagicMock,
    ) -> None:
        mock_enabled_states.return_value = ["US_ID"]
        mock_get_email.return_value = ("e@mail.com", None)

        config_fields = generate_config(-1, datetime.datetime(9, 9, 9))

        req_body = {
            "stateCode": "US_ID",
            "variantDescription": config_fields.variant_description,
            "revisionDescription": config_fields.revision_description,
            "featureVariant": config_fields.feature_variant,
            "displayName": config_fields.display_name,
            "methodologyUrl": config_fields.methodology_url,
            "isAlert": config_fields.is_alert,
            "priority": config_fields.priority,
            "initialHeader": config_fields.initial_header,
            "denialReasons": config_fields.denial_reasons,
            "eligibleCriteriaCopy": config_fields.eligible_criteria_copy,
            "ineligibleCriteriaCopy": config_fields.ineligible_criteria_copy,
            "dynamicEligibilityText": config_fields.dynamic_eligibility_text,
            "callToAction": config_fields.call_to_action,
            "subheading": config_fields.subheading,
            "denialText": config_fields.denial_text,
            "snooze": {"defaultSnoozeDays": 30, "maxSnoozeDays": 180},
            "sidebarComponents": config_fields.sidebar_components,
            "eligibilityDateText": config_fields.eligibility_date_text,
            "hideDenialRevert": config_fields.hide_denial_revert,
            "tooltipEligibilityText": config_fields.tooltip_eligibility_text,
            "tabGroups": config_fields.tab_groups,
            "compareBy": [
                {
                    "field": "eligibilityDate",
                    "sortDirection": "asc",
                    "undefinedBehavior": "undefinedFirst",
                }
            ],
            "notifications": config_fields.notifications,
            "zeroGrantsTooltip": config_fields.zero_grants_tooltip,
        }

        mock_querier.return_value.add_config.return_value = TEST_CONFIG_ID

        with freeze_time(datetime.datetime(10, 10, 10)):
            response = self.client.post(
                self.opportunity_configuration_url,
                json=req_body,
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(TEST_CONFIG_ID, response.json)
            mock_querier.return_value.add_config.assert_called_with(
                TEST_WORKFLOW_TYPE,
                created_by="e@mail.com",
                created_at=datetime.datetime.now(),
                variant_description=req_body["variantDescription"],
                revision_description=req_body["revisionDescription"],
                feature_variant=req_body["featureVariant"],
                display_name=req_body["displayName"],
                methodology_url=req_body["methodologyUrl"],
                is_alert=req_body["isAlert"],
                priority=req_body["priority"],
                initial_header=req_body["initialHeader"],
                denial_reasons=req_body["denialReasons"],
                eligible_criteria_copy=req_body["eligibleCriteriaCopy"],
                ineligible_criteria_copy=req_body["ineligibleCriteriaCopy"],
                dynamic_eligibility_text=req_body["dynamicEligibilityText"],
                eligibility_date_text=req_body["eligibilityDateText"],
                hide_denial_revert=req_body["hideDenialRevert"],
                tooltip_eligibility_text=req_body["tooltipEligibilityText"],
                call_to_action=req_body["callToAction"],
                subheading=req_body["subheading"],
                denial_text=req_body["denialText"],
                snooze={"default_snooze_days": 30, "max_snooze_days": 180},
                sidebar_components=req_body["sidebarComponents"],
                tab_groups=req_body["tabGroups"],
                compare_by=[
                    {
                        "field": "eligibilityDate",
                        "sort_direction": "asc",
                        "undefined_behavior": "undefinedFirst",
                    }
                ],
                notifications=req_body["notifications"],
                staging_id=None,
                zero_grants_tooltip=req_body["zeroGrantsTooltip"],
            )

    ########
    # GET /workflows/<state_code>/opportunities/<opportunity_type>/configurations/<id>
    ########
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

    ########
    # POST /workflows/<state_code>/opportunities/<opportunity_type>/configurations/<id>/deactivate
    ########

    @patch(
        "recidiviz.admin_panel.routes.workflows.WorkflowsQuerier",
    )
    @patch(
        "recidiviz.admin_panel.routes.workflows.get_workflows_enabled_states",
    )
    def test_deactivate_config(
        self, mock_enabled_states: MagicMock, mock_querier: MagicMock
    ) -> None:
        mock_enabled_states.return_value = ["US_ID"]

        response = self.client.post(
            self.single_opportunity_configuration_deactivate_url
        )

        self.assertEqual(HTTPStatus.OK, response.status_code)

        mock_querier.return_value.deactivate_config.assert_called_with(
            TEST_WORKFLOW_TYPE,
            TEST_CONFIG_ID,
        )

    # if WorkflowsQuerier.deactivate_config throws a ValueError, the route should
    # return BAD_REQUEST with the error message
    @patch(
        "recidiviz.admin_panel.routes.workflows.WorkflowsQuerier",
        side_effect=ValueError("Config does not exist"),
    )
    @patch(
        "recidiviz.admin_panel.routes.workflows.get_workflows_enabled_states",
    )
    def test_deactivate_config_error(
        self, mock_enabled_states: MagicMock, _mock_querier: MagicMock
    ) -> None:
        mock_enabled_states.return_value = ["US_ID"]

        response = self.client.post(
            self.single_opportunity_configuration_deactivate_url
        )

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
        self.assertEqual("Config does not exist", json.loads(response.data)["message"])

    ########
    # POST /workflows/<state_code>/opportunities/<opportunity_type>/configurations/<id>/activate
    ########

    @patch(
        "recidiviz.admin_panel.routes.workflows.WorkflowsQuerier",
    )
    @patch(
        "recidiviz.admin_panel.routes.workflows.get_workflows_enabled_states",
    )
    def test_activate_config(
        self, mock_enabled_states: MagicMock, mock_querier: MagicMock
    ) -> None:
        mock_enabled_states.return_value = ["US_ID"]

        response = self.client.post(self.single_opportunity_configuration_activate_url)

        self.assertEqual(HTTPStatus.OK, response.status_code)

        mock_querier.return_value.activate_config.assert_called_with(
            TEST_WORKFLOW_TYPE,
            TEST_CONFIG_ID,
        )

    # if WorkflowsQuerier.activate_config throws a ValueError, the route should
    # return BAD_REQUEST with the error message
    @patch(
        "recidiviz.admin_panel.routes.workflows.WorkflowsQuerier",
        side_effect=ValueError("Config does not exist"),
    )
    @patch(
        "recidiviz.admin_panel.routes.workflows.get_workflows_enabled_states",
    )
    def test_activate_config_error(
        self, mock_enabled_states: MagicMock, _mock_querier: MagicMock
    ) -> None:
        mock_enabled_states.return_value = ["US_ID"]

        response = self.client.post(self.single_opportunity_configuration_activate_url)

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
        self.assertEqual("Config does not exist", json.loads(response.data)["message"])

    ########
    # POST /workflows/<state_code_str>/opportunities/<opportunity_type>/configurations/<int:config_id>/promote
    ########

    @patch("recidiviz.admin_panel.utils.fetch_id_token")
    @patch("recidiviz.admin_panel.utils.in_gcp")
    @patch("recidiviz.admin_panel.utils.get_secret")
    @patch(
        "recidiviz.admin_panel.routes.workflows.get_workflows_enabled_states",
    )
    @patch(
        "recidiviz.admin_panel.routes.workflows.WorkflowsQuerier",
    )
    @patch(
        "recidiviz.admin_panel.routes.workflows.get_authenticated_user_email",
    )
    def test_promote_configuration_success(
        self,
        mock_get_email: MagicMock,
        mock_querier: MagicMock,
        mock_enabled_states: MagicMock,
        mock_get_secret: MagicMock,
        in_gcp_mock: MagicMock,
        fetch_id_token_mock: MagicMock,
    ) -> None:
        mock_enabled_states.return_value = ["US_ID"]
        mock_get_email.return_value = ("email@fake.com", None)

        test_token = "test-token-value"
        in_gcp_mock.return_value = True
        fetch_id_token_mock.return_value = test_token
        mock_get_secret.return_value = "audience"

        mock_querier.return_value.get_config_for_id.return_value = generate_config(
            TEST_CONFIG_ID, datetime.datetime(2024, 5, 12), is_active=True
        )

        with self.app.test_request_context(), responses.RequestsMock() as rsps:
            rsps.post(
                f"https://admin-panel-prod.recidiviz.org/admin/workflows/US_ID/opportunities/{TEST_WORKFLOW_TYPE}/configurations",
                status=200,
                match=[
                    responses.matchers.header_matcher(
                        {"Authorization": f"Bearer {test_token}"}
                    ),
                    responses.matchers.json_params_matcher(
                        {
                            "stateCode": "US_ID",
                            "displayName": "display",
                            "methodologyUrl": "url",
                            "initialHeader": "header",
                            "denialReasons": [{"key": "DENY", "text": "Denied"}],
                            "eligibleCriteriaCopy": [],
                            "ineligibleCriteriaCopy": [],
                            "dynamicEligibilityText": "text",
                            "eligibilityDateText": "date text",
                            "hideDenialRevert": True,
                            "tooltipEligibilityText": "Eligible",
                            "callToAction": "do something",
                            "subheading": "this is what the policy does",
                            "snooze": {"defaultSnoozeDays": 30, "maxSnoozeDays": 180},
                            "isAlert": False,
                            "priority": "NORMAL",
                            "sidebarComponents": ["someComponent"],
                            "denialText": "Deny",
                            "tabGroups": [],
                            "compareBy": [
                                {
                                    "field": "eligibilityDate",
                                    "sortDirection": "asc",
                                    "undefinedBehavior": "undefinedFirst",
                                }
                            ],
                            "notifications": [],
                            "variantDescription": "A config",
                            "revisionDescription": "for testing",
                            "featureVariant": "feature_variant",
                            "stagingId": TEST_CONFIG_ID,
                            "createdBy": "email@fake.com",
                            "zeroGrantsTooltip": "example tooltip",
                        }
                    ),
                ],
            )

            result = self.client.post(
                self.single_opportunity_configuration_promote_url,
                headers=self.headers,
            )
            self.assertEqual(result.status_code, HTTPStatus.OK)
            self.assertEqual(
                json.loads(result.data),
                f"Configuration {TEST_CONFIG_ID} successfully promoted to production",
            )
            mock_get_secret.assert_called_once_with(
                "iap_client_id", GCP_PROJECT_PRODUCTION
            )

    @patch(
        "recidiviz.admin_panel.routes.workflows.WorkflowsQuerier",
    )
    @patch(
        "recidiviz.admin_panel.routes.workflows.get_workflows_enabled_states",
    )
    def test_promote_config_bad_request(
        self, mock_enabled_states: MagicMock, mock_querier: MagicMock
    ) -> None:
        mock_enabled_states.return_value = ["US_ID"]

        mock_querier.return_value.get_config_for_id.return_value = None

        response = self.client.post(self.single_opportunity_configuration_promote_url)

        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
        self.assertEqual(
            f"No config matching opportunity_type='{TEST_WORKFLOW_TYPE}' config_id={TEST_CONFIG_ID}",
            json.loads(response.data)["message"],
        )
