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

import requests
from flask.views import MethodView
from flask_smorest import Blueprint, abort

from recidiviz.admin_panel.admin_stores import fetch_state_codes
from recidiviz.admin_panel.line_staff_tools.workflows_api_schemas import (
    OpportunityConfigurationRequestSchema,
    OpportunityConfigurationResponseSchema,
    OpportunityConfigurationsQueryArgs,
    OpportunityRequestSchema,
    OpportunitySchema,
    StateCodeSchema,
)
from recidiviz.admin_panel.utils import auth_header_for_request_to_prod
from recidiviz.auth.helpers import get_authenticated_user_email
from recidiviz.calculator.query.state.views.outliers.workflows_enabled_states import (
    get_workflows_enabled_states,
)
from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import snake_to_camel
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


@workflows_blueprint.route("<state_code_str>/opportunities/<opportunity_type>")
class OpportunityAPI(MethodView):
    """Endpoint to to create or update an opportunity."""

    @workflows_blueprint.arguments(
        OpportunityRequestSchema,
        location="json",
        error_status_code=HTTPStatus.BAD_REQUEST,
    )
    @workflows_blueprint.response(HTTPStatus.OK)
    def post(
        self,
        body_args: Dict[str, Any],
        state_code_str: str,
        opportunity_type: str,
    ) -> None:
        state_code = refine_state_code(state_code_str)
        updated_by, error_str = get_authenticated_user_email()
        if error_str:
            logging.error("Error determining logged-in user: %s", error_str)
            abort(HTTPStatus.BAD_REQUEST, message=error_str)

        WorkflowsQuerier(state_code).update_opportunity(
            opportunity_type=opportunity_type,
            updated_by=updated_by,
            updated_at=datetime.datetime.now(),
            gating_feature_variant=body_args.get("gating_feature_variant"),
            homepage_position=body_args["homepage_position"],
        )


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

        # Use the email in the request dict if provided, otherwise infer the email
        created_by = (
            user_email
            if body_args.get("created_by") is None
            else str(body_args.get("created_by"))
        )

        new_config_id = WorkflowsQuerier(state_code).add_config(
            opportunity_type,
            created_by=created_by,
            created_at=datetime.datetime.now(),
            variant_description=body_args["variant_description"],
            revision_description=body_args["revision_description"],
            feature_variant=body_args.get("feature_variant"),
            display_name=body_args["display_name"],
            methodology_url=body_args["methodology_url"],
            is_alert=body_args["is_alert"],
            priority=body_args["priority"],
            initial_header=body_args.get("initial_header"),
            denial_reasons=body_args["denial_reasons"],
            eligible_criteria_copy=body_args["eligible_criteria_copy"],
            ineligible_criteria_copy=body_args["ineligible_criteria_copy"],
            dynamic_eligibility_text=body_args["dynamic_eligibility_text"],
            eligibility_date_text=body_args.get("eligibility_date_text"),
            hide_denial_revert=body_args["hide_denial_revert"],
            tooltip_eligibility_text=body_args.get("tooltip_eligibility_text"),
            call_to_action=body_args.get("call_to_action"),
            subheading=body_args.get("subheading"),
            denial_text=body_args.get("denial_text"),
            snooze=body_args.get("snooze"),
            sidebar_components=body_args["sidebar_components"],
            tab_groups=body_args.get("tab_groups"),
            compare_by=body_args.get("compare_by"),
            notifications=body_args["notifications"],
            staging_id=body_args.get("staging_id"),
            zero_grants_tooltip=body_args.get("zero_grants_tooltip"),
            denied_tab_title=body_args.get("denied_tab_title"),
            denial_adjective=body_args.get("denial_adjective"),
            denial_noun=body_args.get("denial_noun"),
            supports_submitted=body_args["supports_submitted"],
            submitted_tab_title=body_args.get("submitted_tab_title"),
            empty_tab_copy=body_args["empty_tab_copy"],
            tab_preface_copy=body_args["tab_preface_copy"],
            subcategory_headings=body_args["subcategory_headings"],
            subcategory_orderings=body_args["subcategory_orderings"],
            mark_submitted_options_by_tab=body_args["mark_submitted_options_by_tab"],
            oms_criteria_header=body_args.get("oms_criteria_header"),
            non_oms_criteria_header=body_args.get("non_oms_criteria_header"),
            non_oms_criteria=body_args["non_oms_criteria"],
            highlight_cases_on_homepage=body_args["highlight_cases_on_homepage"],
            highlighted_case_cta_copy=body_args.get("highlighted_case_cta_copy"),
            overdue_opportunity_callout_copy=body_args.get(
                "overdue_opportunity_callout_copy"
            ),
            snooze_companion_opportunity_types=body_args.get(
                "snooze_companion_opportunity_types"
            ),
            case_notes_title=body_args.get("case_notes_title"),
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

    @workflows_blueprint.response(
        HTTPStatus.OK, OpportunityConfigurationResponseSchema()
    )
    def post(
        self, state_code_str: str, opportunity_type: str, config_id: int
    ) -> FullOpportunityConfig:
        state_code = refine_state_code(state_code_str)

        try:
            WorkflowsQuerier(state_code).deactivate_config(opportunity_type, config_id)
        except ValueError as error:
            abort(HTTPStatus.BAD_REQUEST, message=str(error))

        config = WorkflowsQuerier(state_code).get_config_for_id(
            opportunity_type, config_id
        )
        if config is None:
            raise RuntimeError("Couldn't fetch deactivated config")
        return config


@workflows_blueprint.route(
    "<state_code_str>/opportunities/<opportunity_type>/configurations/<int:config_id>/activate"
)
class OpportunitySingleConfigurationActivateAPI(MethodView):
    "Endpoint to activate a config given an id."

    @workflows_blueprint.response(
        HTTPStatus.OK, OpportunityConfigurationResponseSchema()
    )
    def post(
        self, state_code_str: str, opportunity_type: str, config_id: int
    ) -> FullOpportunityConfig:
        state_code = refine_state_code(state_code_str)

        try:
            active_config_id = WorkflowsQuerier(state_code).activate_config(
                opportunity_type, config_id
            )
        except ValueError as error:
            abort(HTTPStatus.BAD_REQUEST, message=str(error))

        config = WorkflowsQuerier(state_code).get_config_for_id(
            opportunity_type, active_config_id
        )
        if config is None:
            raise RuntimeError("Couldn't fetch activated config")
        return config


@workflows_blueprint.route(
    "<state_code_str>/opportunities/<opportunity_type>/configurations/<int:config_id>/promote"
)
class OpportunitySingleConfigurationPromoteAPI(MethodView):
    """Endpoint to promote a configuration to production given an id."""

    @workflows_blueprint.response(HTTPStatus.OK)
    def post(self, state_code_str: str, opportunity_type: str, config_id: int) -> str:
        """
        Promotes the staging Configuration with id: config_id for opportunity: opportunity_type
        to production by making a POST request to <state_code_str>/opportunities/<opportunity_type>/configurations
        in production.
        """
        state_code = refine_state_code(state_code_str)
        config = WorkflowsQuerier(state_code).get_config_for_id(
            opportunity_type, config_id
        )

        if config is None:
            abort(
                HTTPStatus.BAD_REQUEST,
                message=f"No config matching {opportunity_type=} {config_id=}",
            )

        # Filter out nulled optional fields
        config_dict = {k: v for (k, v) in config.to_dict().items() if v is not None}

        # The created_by of the promoted config should be the email of the user,
        # and if this wasn't specified here, we'd get the staging CR service account.
        user_email, error_str = get_authenticated_user_email()
        if error_str:
            logging.error("Error determining logged-in user: %s", error_str)
        config_dict["created_by"] = user_email

        # Remove these fields because the new request body should follow the OpportunityConfigurationRequestSchema type
        config_dict.pop("opportunity_type")
        config_dict.pop("status")
        config_dict.pop("created_at")

        # Hydrate the staging_id field in production to identify which configuration
        # in staging the new configuration in production corresponds to
        config_dict["staging_id"] = config_dict.pop("id")

        # Make request
        response = requests.post(
            f"https://admin-panel-prod.recidiviz.org/admin/workflows/{state_code_str.upper()}/opportunities/{opportunity_type}/configurations",
            headers=auth_header_for_request_to_prod(),
            json=convert_nested_dictionary_keys(
                config_dict,
                snake_to_camel,
            ),
            timeout=60,
        )

        response.raise_for_status()
        return f"Configuration {str(config_id)} successfully promoted to production"
