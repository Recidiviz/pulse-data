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
import os
from functools import wraps
from http import HTTPStatus
from typing import Any, Callable, Dict, List

from flask import Blueprint, Response, current_app, g, jsonify
from flask.globals import session
from flask_sqlalchemy_session import current_session
from flask_wtf.csrf import generate_csrf

from recidiviz.case_triage.admin_flask_views import IMPERSONATED_EMAIL_KEY
from recidiviz.case_triage.analytics import CaseTriageSegmentClient
from recidiviz.case_triage.api_schemas import (
    CaseUpdateSchema,
    CreateNoteSchema,
    DeferOpportunitySchema,
    PolicyRequirementsSchema,
    PreferredContactMethodSchema,
    PreferredNameSchema,
    ReceivingSSIOrDisabilityIncomeSchema,
    ResolveNoteSchema,
    UpdateNoteSchema,
    requires_api_schema,
)
from recidiviz.case_triage.case_updates.interface import (
    CaseUpdateDoesNotExistError,
    CaseUpdatesInterface,
)
from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.client_info.interface import ClientInfoInterface
from recidiviz.case_triage.exceptions import (
    CaseTriageAuthorizationError,
    CaseTriageBadRequestException,
    CaseTriagePersonNotOnCaseloadException,
    CaseTriageSecretForbiddenException,
)
from recidiviz.case_triage.officer_notes.interface import (
    OfficerNoteDoesNotExistError,
    OfficerNotesInterface,
)
from recidiviz.case_triage.opportunities.interface import (
    OpportunitiesInterface,
    OpportunityDeferralDoesNotExistError,
)
from recidiviz.case_triage.opportunities.types import (
    OpportunityDeferralType,
    OpportunityDoesNotExistError,
)
from recidiviz.case_triage.permissions_checker import PermissionsChecker
from recidiviz.case_triage.querier.case_update_presenter import CaseUpdatePresenter
from recidiviz.case_triage.querier.querier import (
    CaseTriageQuerier,
    OfficerDoesNotExistError,
    PersonDoesNotExistError,
)
from recidiviz.case_triage.state_utils.requirements import policy_requirements_for_state
from recidiviz.case_triage.user_context import Permission
from recidiviz.persistence.database.schema.case_triage.schema import ETLClient


def load_client(person_external_id: str) -> ETLClient:
    try:
        return CaseTriageQuerier.etl_client_for_officer(
            current_session, g.user_context, person_external_id
        )
    except PersonDoesNotExistError as e:
        raise CaseTriagePersonNotOnCaseloadException from e


def route_with_permissions(
    blueprint: Blueprint, rule: str, permissions: List[Permission], **options: Any
) -> Callable:
    def inner(route: Callable) -> Callable:
        @blueprint.route(rule, **options)
        @wraps(route)
        def decorated(*args: List[Any], **kwargs: Dict[str, Any]) -> Callable:
            if g.user_context.permission not in permissions:
                raise CaseTriageAuthorizationError(
                    code="insufficient_permissions",
                    description="You do not have sufficient permissions to perform this action.",
                )
            return route(*args, **kwargs)

        return decorated

    return inner


def create_api_blueprint(
    segment_client: CaseTriageSegmentClient,
    authorization_decorator: Callable,
) -> Blueprint:
    """Creates Blueprint object that is parameterized with a SegmentClient, a requires_authorization decorator and an authorization_store."""

    api = Blueprint("api", __name__)

    @api.before_request
    @authorization_decorator
    def fetch_user_info() -> None:
        """This method both fetches the current user and (by virtue of the decorator) enforces authorization
        for all API routes.

        If the user is an admin (i.e. an approved Recidiviz employee), and the `impersonated_email` param is
        set, then they can make requests as if they were the impersonated user.
        """
        if not hasattr(g, "user_context"):
            # We expect the authorization decorator to have populated the user context.
            # However, in the case that it doesn't successfully happen, this is to check
            # for that.
            raise CaseTriageSecretForbiddenException()
        if IMPERSONATED_EMAIL_KEY in session:
            try:
                impersonated_officer = CaseTriageQuerier.officer_for_email(
                    current_session, session[IMPERSONATED_EMAIL_KEY].lower()
                )
                if g.user_context.can_impersonate(impersonated_officer):
                    g.user_context.current_user = impersonated_officer
            except OfficerDoesNotExistError as e:
                session.pop(IMPERSONATED_EMAIL_KEY)

        if not g.user_context.current_user:
            try:
                g.user_context.current_user = CaseTriageQuerier.officer_for_email(
                    current_session, g.user_context.email
                )
            except OfficerDoesNotExistError as e:
                if not g.user_context.can_see_demo_data:
                    raise CaseTriageAuthorizationError(
                        code="no_case_triage_access",
                        description="You are not authorized to access this application",
                    ) from e

    @route_with_permissions(
        api, "/clients", [Permission.READ_WRITE, Permission.READ_ONLY]
    )
    def _get_clients() -> str:
        clients = CaseTriageQuerier.clients_for_officer(current_session, g.user_context)
        return jsonify([client.to_json() for client in clients])

    @route_with_permissions(
        api, "/opportunities", [Permission.READ_WRITE, Permission.READ_ONLY]
    )
    def _get_opportunities() -> str:
        opportunity_presenters = CaseTriageQuerier.opportunities_for_officer(
            current_session, g.user_context
        )
        return jsonify(
            [opportunity.to_json() for opportunity in opportunity_presenters]
        )

    @route_with_permissions(
        api, "/bootstrap", [Permission.READ_WRITE, Permission.READ_ONLY]
    )
    def _get_bootstrap() -> str:
        return jsonify(
            {
                "csrf": generate_csrf(current_app.secret_key),
                "segmentUserId": g.user_context.segment_user_id,
                "knownExperiments": {
                    k: v for k, v in g.user_context.known_experiments.items() if v
                },
                "dashboardURL": os.getenv(
                    "DASHBOARD_URL", "https://dashboard.recidiviz.org"
                ),
                **g.user_context.access_permissions.to_json(),
            }
        )

    @route_with_permissions(
        api, "/opportunity_deferrals", [Permission.READ_WRITE], methods=["POST"]
    )
    @requires_api_schema(DeferOpportunitySchema)
    def _defer_opportunity() -> str:
        etl_client = load_client(g.api_data["person_external_id"])
        deferred_until = g.api_data["defer_until"]
        try:
            OpportunitiesInterface.defer_opportunity(
                current_session,
                g.user_context,
                etl_client,
                g.api_data["opportunity_type"],
                g.api_data["deferral_type"],
                deferred_until,
                g.api_data["request_reminder"],
            )
        except OpportunityDoesNotExistError as e:
            raise CaseTriageBadRequestException(
                "not_found", "The opportunity could not be found."
            ) from e

        segment_client.track_opportunity_deferred(
            g.user_context,
            etl_client,
            g.api_data["opportunity_type"],
            g.api_data["defer_until"],
            g.api_data["request_reminder"],
        )

        return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

    @route_with_permissions(
        api,
        "/opportunity_deferrals/<deferral_id>",
        [Permission.READ_WRITE],
        methods=["DELETE"],
    )
    def _delete_opportunity_deferral(deferral_id: str) -> Response:
        try:
            opportunity_deferral = OpportunitiesInterface.delete_opportunity_deferral(
                current_session, g.user_context, deferral_id
            )
        except OpportunityDeferralDoesNotExistError as e:
            raise CaseTriageBadRequestException(
                "not_found", "The opportunity deferral could not be found."
            ) from e

        etl_client = load_client(opportunity_deferral.person_external_id)
        segment_client.track_opportunity_deferral_deleted(
            g.user_context,
            etl_client,
            OpportunityDeferralType(opportunity_deferral.deferral_type),
            deferral_id,
        )

        return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

    @route_with_permissions(
        api,
        "/policy_requirements_for_state",
        [Permission.READ_WRITE, Permission.READ_ONLY],
        methods=["POST"],
    )
    @requires_api_schema(PolicyRequirementsSchema)
    def _get_policy_requirements_for_state() -> str:
        """Returns policy requirements for a given state. Expects input in the form:
        {
            state: str,
        }
        """
        return jsonify(policy_requirements_for_state(g.api_data["state"]).to_json())

    @route_with_permissions(
        api, "/case_updates", [Permission.READ_WRITE], methods=["POST"]
    )
    @requires_api_schema(CaseUpdateSchema)
    def _create_case_update() -> Response:
        """Records individual clients actions. Expects JSON body of CaseUpdateSchema"""
        etl_client = load_client(g.api_data["person_external_id"])

        case_update = CaseUpdatesInterface.update_case_for_person(
            current_session,
            g.user_context,
            etl_client,
            g.api_data["action_type"],
            g.api_data.get("comment", None),
        )
        segment_client.track_person_action_taken(
            g.user_context,
            etl_client,
            g.api_data["action_type"],
        )
        presenter = CaseUpdatePresenter(etl_client, case_update)
        return jsonify(presenter.to_json())

    @route_with_permissions(
        api, "/case_updates/<update_id>", [Permission.READ_WRITE], methods=["DELETE"]
    )
    def _delete_case_update(update_id: str) -> Response:
        try:
            case_update = CaseUpdatesInterface.delete_case_update(
                current_session,
                g.user_context,
                update_id,
            )
        except CaseUpdateDoesNotExistError as e:
            raise CaseTriageBadRequestException(
                "not_found", "The case update could not be found."
            ) from e

        etl_client = load_client(case_update.person_external_id)
        segment_client.track_person_action_removed(
            g.user_context,
            etl_client,
            CaseUpdateActionType(case_update.action_type),
            str(case_update.update_id),
        )
        return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

    @route_with_permissions(
        api, "/set_preferred_name", [Permission.READ_WRITE], methods=["POST"]
    )
    @requires_api_schema(PreferredNameSchema)
    def _set_preferred_name() -> Response:
        etl_client = load_client(g.api_data["person_external_id"])
        if not PermissionsChecker.is_on_caseload(etl_client, g.user_context):
            raise CaseTriagePersonNotOnCaseloadException
        ClientInfoInterface.set_preferred_name(
            current_session, g.user_context, etl_client, g.api_data["name"]
        )

        return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

    @route_with_permissions(
        api, "/set_preferred_contact_method", [Permission.READ_WRITE], methods=["POST"]
    )
    @requires_api_schema(PreferredContactMethodSchema)
    def _set_preferred_contact_method() -> Response:
        etl_client = load_client(g.api_data["person_external_id"])

        if not PermissionsChecker.is_on_caseload(etl_client, g.user_context):
            raise CaseTriagePersonNotOnCaseloadException
        ClientInfoInterface.set_preferred_contact_method(
            current_session, g.user_context, etl_client, g.api_data["contact_method"]
        )

        return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

    @route_with_permissions(
        api,
        "/set_receiving_ssi_or_disability_income",
        [Permission.READ_WRITE],
        methods=["POST"],
    )
    @requires_api_schema(ReceivingSSIOrDisabilityIncomeSchema)
    def _set_receiving_ssi_or_disability_income() -> Response:
        etl_client = load_client(g.api_data["person_external_id"])

        if not PermissionsChecker.is_on_caseload(etl_client, g.user_context):
            raise CaseTriagePersonNotOnCaseloadException
        ClientInfoInterface.set_receiving_ssi_or_disability_income(
            current_session, g.user_context, etl_client, g.api_data["mark_receiving"]
        )

        return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

    @route_with_permissions(
        api, "/create_note", [Permission.READ_WRITE], methods=["POST"]
    )
    @requires_api_schema(CreateNoteSchema)
    def _create_note() -> Response:
        etl_client = load_client(g.api_data["person_external_id"])

        if not PermissionsChecker.is_on_caseload(etl_client, g.user_context):
            raise CaseTriagePersonNotOnCaseloadException
        officer_note = OfficerNotesInterface.create_note(
            current_session, g.user_context, etl_client, g.api_data["text"]
        )
        return jsonify(officer_note.to_json())

    @route_with_permissions(
        api, "/resolve_note", [Permission.READ_WRITE], methods=["POST"]
    )
    @requires_api_schema(ResolveNoteSchema)
    def _resolve_note() -> Response:
        try:
            OfficerNotesInterface.resolve_note(
                current_session,
                g.user_context,
                g.api_data["note_id"],
                g.api_data["is_resolved"],
            )
        except OfficerNoteDoesNotExistError as e:
            raise CaseTriageBadRequestException(
                "not_found",
                f"OfficerNote with id {g.api_data['note_id']} does not exist",
            ) from e

        return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

    @route_with_permissions(
        api, "/update_note", [Permission.READ_WRITE], methods=["POST"]
    )
    @requires_api_schema(UpdateNoteSchema)
    def _update_note() -> Response:
        try:
            officer_note = OfficerNotesInterface.update_note(
                current_session,
                g.user_context,
                g.api_data["note_id"],
                g.api_data["text"],
            )
        except OfficerNoteDoesNotExistError as e:
            raise CaseTriageBadRequestException(
                "not_found",
                f"OfficerNote with id {g.api_data['note_id']} does not exist",
            ) from e

        return jsonify(officer_note.to_json())

    return api
