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

from http import HTTPStatus
from typing import Any, Dict, List

from flask.views import MethodView
from flask_smorest import Blueprint, abort

from recidiviz.admin_panel.admin_stores import fetch_state_codes
from recidiviz.admin_panel.line_staff_tools.workflows_api_schemas import (
    OpportunityConfigurationSchema,
    OpportunityConfigurationsQueryArgs,
    OpportunitySchema,
    StateCodeSchema,
)
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


@workflows_blueprint.route("<state_code_str>/<opportunity_type>/configurations")
class OpportunityConfigurationsAPI(MethodView):
    """Endpoint to list configs for a given workflow type."""

    @workflows_blueprint.arguments(
        OpportunityConfigurationsQueryArgs,
        location="query",
        error_status_code=HTTPStatus.BAD_REQUEST,
    )
    @workflows_blueprint.response(
        HTTPStatus.OK, OpportunityConfigurationSchema(many=True)
    )
    def get(
        self,
        query_args: Dict[str, Any],
        state_code_str: str,
        opportunity_type: str,
    ) -> List[FullOpportunityConfig]:
        offset = query_args.get("offset", 0)
        status = query_args.get("status", None)

        state_code = refine_state_code(state_code_str)
        configs = WorkflowsQuerier(state_code).get_configs_for_type(
            opportunity_type, offset=offset, status=status
        )
        return configs


@workflows_blueprint.route(
    "<state_code_str>/<opportunity_type>/configurations/<int:config_id>"
)
class OpportunitySingleConfigurationAPI(MethodView):
    """Endpoint to retrieve a config given an id."""

    @workflows_blueprint.response(HTTPStatus.OK, OpportunityConfigurationSchema())
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
