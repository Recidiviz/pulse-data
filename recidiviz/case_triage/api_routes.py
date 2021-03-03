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
from http import HTTPStatus

from flask import Blueprint, current_app, g, jsonify, request, session
from flask_sqlalchemy_session import current_session
from flask_wtf.csrf import generate_csrf

from recidiviz.case_triage.case_updates.interface import CaseUpdatesInterface
from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.exceptions import CaseTriageBadRequestException
from recidiviz.case_triage.querier.querier import (
    CaseTriageQuerier,
    PersonDoesNotExistError,
)
from recidiviz.case_triage.state_utils.requirements import policy_requirements_for_state
from recidiviz.case_triage.util import SESSION_ADMIN_KEY


api = Blueprint("api", __name__)


@api.route("/clients")
def get_clients() -> str:
    if session.get(SESSION_ADMIN_KEY) and not getattr(g, "current_user", None):
        fixture_path = os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "./fixtures/dummy_clients.json",
            )
        )
        with open(fixture_path) as f:
            return jsonify(json.load(f))

    return jsonify(
        [
            client.to_json()
            for client in CaseTriageQuerier.clients_for_officer(
                current_session,
                g.current_user,
            )
        ]
    )


@api.route("/bootstrap")
def get_bootstrap() -> str:
    return jsonify({"csrf": generate_csrf(current_app.secret_key)})


@api.route("/policy_requirements_for_state", methods=["POST"])
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


@api.route("/record_client_action", methods=["POST"])
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

    CaseUpdatesInterface.update_case_for_person(
        current_session,
        g.current_user,
        client,
        user_initiated_actions,
        other_text,
    )
    return jsonify(
        {
            "status": "ok",
            "status_code": HTTPStatus.OK,
        }
    )
