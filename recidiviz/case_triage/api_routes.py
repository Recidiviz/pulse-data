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
"""Implements API routes for the Case Triage app."""
from datetime import date, datetime
from functools import wraps
from http import HTTPStatus
from typing import Optional, Callable, List, Any, Dict

from flask import Blueprint, current_app, g, jsonify, Response
from flask_sqlalchemy_session import current_session
from flask_wtf.csrf import generate_csrf

from recidiviz.case_triage.analytics import CaseTriageSegmentClient
from recidiviz.case_triage.api_schemas import (
    load_api_schema,
    CaseUpdateSchema,
    PolicyRequirementsSchema,
)
from recidiviz.case_triage.case_updates.interface import (
    CaseUpdatesInterface,
    DemoCaseUpdatesInterface,
)
from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.demo_helpers import DEMO_FROZEN_DATE
from recidiviz.case_triage.exceptions import CaseTriageBadRequestException
from recidiviz.case_triage.querier.querier import (
    CaseTriageQuerier,
    DemoCaseTriageQuerier,
    PersonDoesNotExistError,
)
from recidiviz.case_triage.state_utils.requirements import policy_requirements_for_state


def _should_see_demo() -> bool:
    """Returns true if the user who is logged in is not a parole officer, is not
    impersonating a parole officer, and is allowed to see demo data."""
    return getattr(g, "current_user", None) is None and g.can_see_demo_data


def require_current_user(route: Callable) -> Callable:
    @wraps(route)
    def decorated(*args: List[Any], **kwargs: Dict[str, Any]) -> Any:
        if not getattr(g, "current_user", None):
            raise CaseTriageBadRequestException(
                code="not_allowed",
                description="A user must be associated with this request",
            )

        return route(*args, **kwargs)

    return decorated


def create_api_blueprint(
    segment_client: Optional[CaseTriageSegmentClient] = None,
) -> Blueprint:
    """Creates Blueprint object that is parameterized with a SegmentClient."""
    api = Blueprint("api", __name__)

    @api.route("/clients")
    def _get_clients() -> str:
        demo_timedelta_shift = None
        if _should_see_demo():
            clients = DemoCaseTriageQuerier.clients_for_demo_user(
                current_session, g.email
            )
            demo_timedelta_shift = date.today() - DEMO_FROZEN_DATE
        else:
            clients = CaseTriageQuerier.clients_for_officer(
                current_session,
                g.current_user,
            )

        return jsonify([client.to_json(demo_timedelta_shift) for client in clients])

    @api.route("/opportunities")
    def _get_opportunities() -> str:
        if _should_see_demo():
            return jsonify([])

        now = datetime.now()
        return jsonify(
            [
                opportunity.to_json()
                for opportunity in CaseTriageQuerier.opportunities_for_officer(
                    current_session, g.current_user
                )
                if opportunity.opportunity_active_at_time(now)
            ]
        )

    @api.route("/bootstrap")
    def _get_bootstrap() -> str:
        return jsonify(
            {
                "csrf": generate_csrf(current_app.secret_key),
                "segmentUserId": g.segment_user_id,
            }
        )

    @api.route("/policy_requirements_for_state", methods=["POST"])
    def _get_policy_requirements_for_state() -> str:
        """Returns policy requirements for a given state. Expects input in the form:
        {
            state: str,
        }
        """
        data = load_api_schema(PolicyRequirementsSchema)

        return jsonify(policy_requirements_for_state(data["state"]).to_json())

    @api.route("/record_client_action", methods=["POST"])
    def _record_client_action() -> Response:
        """ Records individual clients actions. Expects JSON body of CaseUpdateSchema """
        data = load_api_schema(CaseUpdateSchema)

        person_external_id = data["person_external_id"]

        try:
            if _should_see_demo():
                client = DemoCaseTriageQuerier.etl_client_with_id(person_external_id)
            else:
                client = CaseTriageQuerier.etl_client_with_id_and_state_code(
                    current_session,
                    person_external_id,
                    g.current_user.state_code,
                )
        except PersonDoesNotExistError as e:
            raise CaseTriageBadRequestException(
                code="bad_request",
                description={
                    "personExternalId": ["does not correspond to a known person"]
                },
            ) from e

        actions = data["actions"]
        user_initiated_actions = [CaseUpdateActionType(a) for a in actions]
        other_text = data.get("other_text", None)

        if _should_see_demo():
            demo_time = datetime(
                year=DEMO_FROZEN_DATE.year,
                month=DEMO_FROZEN_DATE.month,
                day=DEMO_FROZEN_DATE.day,
            )
            DemoCaseUpdatesInterface.update_case_for_person(
                session=current_session,
                user_email=g.email,
                client=client,
                actions=user_initiated_actions,
                other_text=other_text,
                action_ts=demo_time,
            )
        else:
            old_case = CaseTriageQuerier.case_for_client_and_officer(
                current_session,
                client,
                g.current_user,
            )
            CaseUpdatesInterface.update_case_for_person(
                current_session,
                g.current_user,
                client,
                user_initiated_actions,
                other_text,
            )

            if segment_client:
                old_action_types = [
                    CaseUpdateActionType(action.action_type)
                    for action in old_case.in_progress_officer_actions()
                ]
                segment_client.track_person_case_updated(
                    g.current_user,
                    client,
                    old_action_types,
                    user_initiated_actions,
                )

        return jsonify(
            {
                "status": "ok",
                "status_code": HTTPStatus.OK,
            }
        )

    return api
