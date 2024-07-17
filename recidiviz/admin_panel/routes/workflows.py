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
"""Admin panel routes for configuring workflows settings."""
import datetime
import logging
from http import HTTPStatus
from typing import Any, Dict, List

from flask.views import MethodView
from flask_smorest import Blueprint, abort

from recidiviz.admin_panel.admin_stores import fetch_state_codes
from recidiviz.admin_panel.line_staff_tools.workflows_api_schemas import (
    OpportunityConfigurationRequestSchema,
    OpportunityConfigurationResponseSchema,
    OpportunityConfigurationsQueryArgs,
    OpportunitySchema,
    StateCodeSchema,
)
from recidiviz.auth.helpers import get_authenticated_user_email
from recidiviz.calculator.query.state.views.outliers.workflows_enabled_states import (
    get_workflows_enabled_states,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.workflows.querier.querier import WorkflowsQuerier
from recidiviz.workflows.types import FullOpportunityConfig, FullOpportunityInfo

workflows_blueprint = Blueprint("workflows", "workflows")


def refine_state_code(state_code_str: str) -> StateCode:
    if state_code_str not in get_workflows_enabled_states():
        raise ValueError(
            f"Cannot retrieve opportunities for invalid state: {state_code_str}"
        )

    return StateCode(state_code_str.upper())


@workflows_blueprint.route("enabled_state_codes")
class EnabledStatesAPI(MethodView):
    """Endpoint to list all states with Workflows enabled."""

    @workflows_blueprint.response(HTTPStatus.OK, StateCodeSchema(many=True))
    def get(self) -> List[dict]:
        states = fetch_state_codes(
            [
                StateCode[state_code_str]
                for state_code_str in get_workflows_enabled_states()
                if StateCode.is_state_code(state_code_str)
            ]
        )
        return states


@workflows_blueprint.route("<state_code_str>/opportunities")
class OpportunitiesAPI(MethodView):
    """Endpoint to list all opportunities in a state."""

    @workflows_blueprint.response(HTTPStatus.OK, OpportunitySchema(many=True))
    def get(self, state_code_str: str) -> List[FullOpportunityInfo]:
        state_code = refine_state_code(state_code_str)
        opportunities = WorkflowsQuerier(state_code).get_opportunities()
        return opportunities


@workflows_blueprint.route(
    "<state_code_str>/opportunities/<opportunity_type>/configurations"
)
class OpportunityConfigurationsAPI(MethodView):
    """Implementation of <state_code_str>/opportunities/<opportunity_type>/configurations endpoints."""

    @workflows_blueprint.arguments(
        OpportunityConfigurationsQueryArgs,
        location="query",
        error_status_code=HTTPStatus.BAD_REQUEST,
    )
    @workflows_blueprint.response(
        HTTPStatus.OK, OpportunityConfigurationResponseSchema(many=True)
    )
    def get(
        self,
        query_args: Dict[str, Any],
        state_code_str: str,
        opportunity_type: str,
    ) -> List[FullOpportunityConfig]:
        """Endpoint to list configs for a given workflow type."""
        offset = query_args.get("offset", 0)
        status = query_args.get("status", None)

        state_code = refine_state_code(state_code_str)
        configs = WorkflowsQuerier(state_code).get_configs_for_type(
            opportunity_type, offset=offset, status=status
        )
        return configs

    @workflows_blueprint.arguments(
        OpportunityConfigurationRequestSchema,
        location="json",
        error_status_code=HTTPStatus.BAD_REQUEST,
    )
    @workflows_blueprint.response(HTTPStatus.OK)
    def post(
        self,
        body_args: Dict[str, Any],
        state_code_str: str,
        opportunity_type: str,
    ) -> int:
        """Endpoint to create a new config."""
        state_code = refine_state_code(state_code_str)
        user_email, error_str = get_authenticated_user_email()
        if error_str:
            logging.error("Error determining logged-in user: %s", error_str)
            abort(HTTPStatus.BAD_REQUEST, message=error_str)

        new_config_id = WorkflowsQuerier(state_code).add_config(
            opportunity_type,
            created_by=user_email,
            created_at=datetime.datetime.now(),
            description=body_args["description"],
            feature_variant=body_args.get("feature_variant"),
            display_name=body_args["display_name"],
            methodology_url=body_args["methodology_url"],
            is_alert=body_args["is_alert"],
            initial_header=body_args.get("initial_header"),
            denial_reasons=body_args["denial_reasons"],
            eligible_criteria_copy=body_args["eligible_criteria_copy"],
            ineligible_criteria_copy=body_args["ineligible_criteria_copy"],
            dynamic_eligibility_text=body_args["dynamic_eligibility_text"],
            eligibility_date_text=body_args.get("eligibility_date_text"),
            hide_denial_revert=body_args["hide_denial_revert"],
            tooltip_eligibility_text=body_args.get("tooltip_eligibility_text"),
            call_to_action=body_args["call_to_action"],
            subheading=body_args.get("subheading"),
            denial_text=body_args.get("denial_text"),
            snooze=body_args.get("snooze"),
            sidebar_components=body_args["sidebar_components"],
            tab_groups=body_args.get("tab_groups"),
            compare_by=body_args.get("compare_by"),
            notifications=body_args["notifications"],
        )
        return new_config_id


@workflows_blueprint.route(
    "<state_code_str>/opportunities/<opportunity_type>/configurations/<int:config_id>"
)
class OpportunitySingleConfigurationAPI(MethodView):
    """Endpoint to retrieve a config given an id."""

    @workflows_blueprint.response(
        HTTPStatus.OK, OpportunityConfigurationResponseSchema()
    )
    def get(
        self,
        state_code_str: str,
        opportunity_type: str,
        config_id: int,
    ) -> FullOpportunityConfig:
        state_code = refine_state_code(state_code_str)
        config = WorkflowsQuerier(state_code).get_config_for_id(
            opportunity_type, config_id
        )

        if config is None:
            abort(
                HTTPStatus.BAD_REQUEST,
                message=f"No config matching {opportunity_type=} {config_id=}",
            )

        return config


@workflows_blueprint.route(
    "<state_code_str>/opportunities/<opportunity_type>/configurations/<int:config_id>/deactivate"
)
class OpportunitySingleConfigurationDeactivateAPI(MethodView):
    "Endpoint to deactivate a config given an id."

    @workflows_blueprint.response(HTTPStatus.OK)
    def put(self, state_code_str: str, opportunity_type: str, config_id: int) -> str:
        state_code = refine_state_code(state_code_str)

        try:
            WorkflowsQuerier(state_code).deactivate_config(opportunity_type, config_id)
        except ValueError as error:
            abort(HTTPStatus.BAD_REQUEST, message=str(error))

        return f"Configuration {str(config_id)} has been deactivated"


@workflows_blueprint.route(
    "<state_code_str>/opportunities/<opportunity_type>/configurations/<int:config_id>/activate"
)
class OpportunitySingleConfigurationActivateAPI(MethodView):
    "Endpoint to activate a config given an id."

    @workflows_blueprint.response(HTTPStatus.OK)
    def put(self, state_code_str: str, opportunity_type: str, config_id: int) -> str:
        state_code = refine_state_code(state_code_str)

        try:
            active_config_id = WorkflowsQuerier(state_code).activate_config(
                opportunity_type, config_id
            )
        except ValueError as error:
            abort(HTTPStatus.BAD_REQUEST, message=str(error))

        return f"Created new activate config {str(active_config_id)} from deactived config{str(config_id)}"
