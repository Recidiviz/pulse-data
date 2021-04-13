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
import json
import os
from datetime import datetime
from http import HTTPStatus
from typing import Optional

from flask import Blueprint, current_app, g, jsonify, request
from flask_sqlalchemy_session import current_session
from flask_wtf.csrf import generate_csrf

from recidiviz.case_triage.analytics import CaseTriageSegmentClient
from recidiviz.case_triage.case_updates.interface import CaseUpdatesInterface
from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.exceptions import CaseTriageBadRequestException
from recidiviz.case_triage.querier.querier import (
    CaseTriageQuerier,
    PersonDoesNotExistError,
)
from recidiviz.case_triage.state_utils.requirements import policy_requirements_for_state


def _is_non_officer_user() -> bool:
    """Returns true if the user who is logged in is not a parole officer, and is not
    impersonating a parole officer."""
    return getattr(g, "current_user", None) is None


def create_api_blueprint(
    segment_client: Optional[CaseTriageSegmentClient] = None,
) -> Blueprint:
    """Creates Blueprint object that is parameterized with a SegmentClient."""
    api = Blueprint("api", __name__)

    def get_clients() -> str:
        if _is_non_officer_user() and g.can_see_demo_data:
            fixture_path = os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.realpath(__file__)),
                    "./fixtures/dummy_clients.json",
                )
            )
            with open(fixture_path) as f:
                return jsonify({"clients": json.load(f), "isDemoData": True})

        return jsonify(
            {
                "clients": [
                    client.to_json()
                    for client in CaseTriageQuerier.clients_for_officer(
                        current_session,
                        g.current_user,
                    )
                ],
                "isDemoData": False,
            }
        )

    api.route("/clients")(get_clients)

    def get_opportunities() -> str:
        if _is_non_officer_user():
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

    api.route("/opportunities")(get_opportunities)

    def get_bootstrap() -> str:
        return jsonify(
            {
                "csrf": generate_csrf(current_app.secret_key),
                "segmentUserId": g.segment_user_id,
            }
        )

    api.route("/bootstrap")(get_bootstrap)

    def get_policy_requirements_for_state() -> str:
        """Returns policy requirements for a given state. Expects input in the form:
        {
            state: str,
        }
        """
        if not request.json:
            raise CaseTriageBadRequestException(
                code="missing_body",
                description="A json body must be provided for the request",
            )
        if "state" not in request.json:
            raise CaseTriageBadRequestException(
                code="missing_arg",
                description="`state` keyword is missing from request",
            )
        state = request.json["state"]

        # NOTE: This will have to be further abstracted and generalized once we understand
        # the requirements for additional states.
        if state != "US_ID":
            raise CaseTriageBadRequestException(
                code="invalid_arg",
                description=f"Invalid state ({state}) provided",
            )

        return jsonify(policy_requirements_for_state(state).to_json())

    api.route("/policy_requirements_for_state", methods=["POST"])(
        get_policy_requirements_for_state
    )

    def post_record_client_action() -> str:
        """Records individual clients actions. Expects JSON body of the form:
        {
            personExternalId: str,
            actions: List[str],
            otherText: Optional[str],
        }
        `actions` must be non-empty and only contain approved actions
        """
        if not getattr(g, "current_user", None):
            raise CaseTriageBadRequestException(
                code="not_allowed",
                description="A user must be associated with the record-action request",
            )
        if not request.json:
            raise CaseTriageBadRequestException(
                code="missing_body",
                description="A json body must be provided for the request",
            )
        if "personExternalId" not in request.json:
            raise CaseTriageBadRequestException(
                code="missing_arg",
                description="`personExternalId` keyword is missing from request",
            )
        if "actions" not in request.json:
            raise CaseTriageBadRequestException(
                code="missing_arg",
                description="`actions` keyword is missing from request",
            )

        person_external_id = request.json["personExternalId"]
        actions = request.json["actions"]
        other_text = request.json.get("otherText")

        if not isinstance(actions, list):
            raise CaseTriageBadRequestException(
                code="improper_type",
                description="`actions` must be a list of CaseUpdateActionTypes",
            )

        try:
            user_initiated_actions = [CaseUpdateActionType(a) for a in actions]
        except ValueError as e:
            raise CaseTriageBadRequestException(
                code="improper_type",
                description="`actions` must be a list of CaseUpdateActionTypes",
            ) from e

        state_code = g.current_user.state_code
        try:
            client = CaseTriageQuerier.etl_client_with_id_and_state_code(
                current_session,
                person_external_id,
                state_code,
            )
        except PersonDoesNotExistError as e:
            raise CaseTriageBadRequestException(
                code="invalid_arg",
                description=f"`personExternalId` does not correspond to a known person: {person_external_id}",
            ) from e

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

    api.route("/record_client_action", methods=["POST"])(post_record_client_action)

    return api
