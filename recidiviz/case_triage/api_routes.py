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
from http import HTTPStatus

from flask import Blueprint, Response, current_app, g, jsonify
from flask_sqlalchemy_session import current_session
from flask_wtf.csrf import generate_csrf

from recidiviz.case_triage.analytics import CaseTriageSegmentClient
from recidiviz.case_triage.api_schemas import (
    CaseUpdateSchema,
    CreateNoteSchema,
    DeferOpportunitySchema,
    PolicyRequirementsSchema,
    PreferredContactMethodSchema,
    PreferredNameSchema,
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
    CaseTriageBadRequestException,
    CaseTriagePersonNotOnCaseloadException,
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
    PersonDoesNotExistError,
)
from recidiviz.case_triage.state_utils.requirements import policy_requirements_for_state
from recidiviz.persistence.database.schema.case_triage.schema import ETLClient


def _should_see_demo() -> bool:
    """Returns true if the user who is logged in is not a parole officer, is not
    impersonating a parole officer, and is allowed to see demo data."""
    return getattr(g, "current_user", None) is None and g.can_see_demo_data


def load_client(person_external_id: str) -> ETLClient:
    try:
        return CaseTriageQuerier.etl_client_for_officer(
            current_session, g.user_context, person_external_id
        )
    except PersonDoesNotExistError as e:
        raise CaseTriagePersonNotOnCaseloadException from e


def create_api_blueprint(segment_client: CaseTriageSegmentClient) -> Blueprint:
    """Creates Blueprint object that is parameterized with a SegmentClient."""
    api = Blueprint("api", __name__)

    @api.route("/clients")
    def _get_clients() -> str:
        clients = CaseTriageQuerier.clients_for_officer(current_session, g.user_context)
        return jsonify(
            [
                client.to_json(g.user_context.demo_timedelta_shift_from_today)
                for client in clients
            ]
        )

    @api.route("/opportunities")
    def _get_opportunities() -> str:
        opportunity_presenters = CaseTriageQuerier.opportunities_for_officer(
            current_session, g.user_context
        )
        now = g.user_context.now()
        return jsonify(
            [opportunity.to_json(now) for opportunity in opportunity_presenters]
        )

    @api.route("/bootstrap")
    def _get_bootstrap() -> str:
        return jsonify(
            {
                "csrf": generate_csrf(current_app.secret_key),
                "segmentUserId": g.user_context.segment_user_id,
                "knownExperiments": {
                    k: v for k, v in g.user_context.known_experiments.items() if v
                },
            }
        )

    @api.route("/opportunity_deferrals", methods=["POST"])
    @requires_api_schema(DeferOpportunitySchema)
    def _defer_opportunity() -> str:
        etl_client = load_client(g.api_data["person_external_id"])
        demo_timedelta_shift = g.user_context.demo_timedelta_shift_to_today
        deferred_until = g.api_data["defer_until"]
        if demo_timedelta_shift:
            deferred_until += demo_timedelta_shift
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

    @api.route("/opportunity_deferrals/<deferral_id>", methods=["DELETE"])
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

    @api.route("/policy_requirements_for_state", methods=["POST"])
    @requires_api_schema(PolicyRequirementsSchema)
    def _get_policy_requirements_for_state() -> str:
        """Returns policy requirements for a given state. Expects input in the form:
        {
            state: str,
        }
        """
        return jsonify(policy_requirements_for_state(g.api_data["state"]).to_json())

    @api.route("/case_updates", methods=["POST"])
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

    @api.route("/case_updates/<update_id>", methods=["DELETE"])
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

    @api.route("/set_preferred_name", methods=["POST"])
    @requires_api_schema(PreferredNameSchema)
    def _set_preferred_name() -> Response:
        etl_client = load_client(g.api_data["person_external_id"])
        if not PermissionsChecker.is_on_caseload(etl_client, g.user_context):
            raise CaseTriagePersonNotOnCaseloadException
        ClientInfoInterface.set_preferred_name(
            current_session, g.user_context, etl_client, g.api_data["name"]
        )

        return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

    @api.route("/set_preferred_contact_method", methods=["POST"])
    @requires_api_schema(PreferredContactMethodSchema)
    def _set_preferred_contact_method() -> Response:
        etl_client = load_client(g.api_data["person_external_id"])

        if not PermissionsChecker.is_on_caseload(etl_client, g.user_context):
            raise CaseTriagePersonNotOnCaseloadException
        ClientInfoInterface.set_preferred_contact_method(
            current_session, g.user_context, etl_client, g.api_data["contact_method"]
        )

        return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

    @api.route("/create_note", methods=["POST"])
    @requires_api_schema(CreateNoteSchema)
    def _create_note() -> Response:
        etl_client = load_client(g.api_data["person_external_id"])

        if not PermissionsChecker.is_on_caseload(etl_client, g.user_context):
            raise CaseTriagePersonNotOnCaseloadException
        officer_note = OfficerNotesInterface.create_note(
            current_session, g.user_context, etl_client, g.api_data["text"]
        )
        return jsonify(officer_note.to_json())

    @api.route("/resolve_note", methods=["POST"])
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

    @api.route("/update_note", methods=["POST"])
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
