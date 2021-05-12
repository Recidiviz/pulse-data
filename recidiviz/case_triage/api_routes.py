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
from http import HTTPStatus
from typing import Optional

import pytz

from flask import Blueprint, current_app, g, jsonify, Response
from flask_sqlalchemy_session import current_session
from flask_wtf.csrf import generate_csrf

from recidiviz.case_triage.analytics import CaseTriageSegmentClient
from recidiviz.case_triage.api_schemas import (
    CaseUpdateSchema,
    DeferOpportunitySchema,
    PolicyRequirementsSchema,
    requires_api_schema,
)
from recidiviz.case_triage.case_updates.interface import (
    CaseUpdateDoesNotExistError,
    CaseUpdatesInterface,
    DemoCaseUpdatesInterface,
)
from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.demo_helpers import DEMO_FROZEN_DATE, DEMO_FROZEN_DATETIME
from recidiviz.case_triage.exceptions import (
    CaseTriageBadRequestException,
)
from recidiviz.case_triage.opportunities.interface import (
    DemoOpportunitiesInterface,
    OpportunitiesInterface,
    OpportunityDeferralDoesNotExistError,
)
from recidiviz.case_triage.opportunities.types import (
    OpportunityDeferralType,
    OpportunityDoesNotExistError,
)
from recidiviz.case_triage.querier.case_update_presenter import CaseUpdatePresenter
from recidiviz.case_triage.querier.querier import (
    CaseTriageQuerier,
    DemoCaseTriageQuerier,
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
        if _should_see_demo():
            return DemoCaseTriageQuerier.etl_client_with_id(person_external_id)

        return CaseTriageQuerier.etl_client_for_officer(
            current_session, g.current_user, person_external_id
        )
    except PersonDoesNotExistError as e:
        raise CaseTriageBadRequestException(
            code="bad_request",
            description={"personExternalId": ["does not correspond to a known person"]},
        ) from e


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
            opportunity_presenters = DemoCaseTriageQuerier.opportunities_for_demo_user(
                current_session, g.email
            )
            now = DEMO_FROZEN_DATETIME
        else:
            opportunity_presenters = CaseTriageQuerier.opportunities_for_officer(
                current_session, g.current_user
            )
            now = datetime.now(tz=pytz.UTC)

        return jsonify(
            [opportunity.to_json(now) for opportunity in opportunity_presenters]
        )

    @api.route("/bootstrap")
    def _get_bootstrap() -> str:
        return jsonify(
            {
                "csrf": generate_csrf(current_app.secret_key),
                "segmentUserId": g.segment_user_id,
                "knownExperiments": {k: v for k, v in g.known_experiments.items() if v},
            }
        )

    @api.route("/opportunity_deferrals", methods=["POST"])
    @requires_api_schema(DeferOpportunitySchema)
    def _defer_opportunity() -> str:
        etl_client = load_client(g.api_data["person_external_id"])

        try:
            if _should_see_demo():
                demo_timedelta_shift = DEMO_FROZEN_DATE - date.today()

                DemoOpportunitiesInterface.defer_opportunity(
                    current_session,
                    g.email,
                    etl_client,
                    g.api_data["opportunity_type"],
                    g.api_data["deferral_type"],
                    g.api_data["defer_until"] + demo_timedelta_shift,
                    g.api_data["request_reminder"],
                )
            else:
                OpportunitiesInterface.defer_opportunity(
                    current_session,
                    g.current_user,
                    etl_client,
                    g.api_data["opportunity_type"],
                    g.api_data["deferral_type"],
                    g.api_data["defer_until"],
                    g.api_data["request_reminder"],
                )

                if segment_client:
                    segment_client.track_opportunity_deferred(
                        g.current_user,
                        etl_client,
                        g.api_data["opportunity_type"],
                        g.api_data["defer_until"],
                        g.api_data["request_reminder"],
                    )
        except OpportunityDoesNotExistError as e:
            raise CaseTriageBadRequestException(
                "not_found", "The opportunity could not be found."
            ) from e

        return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

    @api.route("/opportunity_deferrals/<deferral_id>", methods=["DELETE"])
    def _delete_opportunity_deferral(deferral_id: str) -> Response:
        try:
            if _should_see_demo():
                DemoOpportunitiesInterface.delete_opportunity_deferral(
                    current_session,
                    g.email,
                    deferral_id,
                )
            else:
                opportunity_deferral = (
                    OpportunitiesInterface.delete_opportunity_deferral(
                        current_session,
                        g.current_user,
                        deferral_id,
                    )
                )

                etl_client = load_client(opportunity_deferral.person_external_id)

                if segment_client:
                    segment_client.track_opportunity_deferral_deleted(
                        g.current_user,
                        etl_client,
                        OpportunityDeferralType(opportunity_deferral.deferral_type),
                        deferral_id,
                    )
        except OpportunityDeferralDoesNotExistError as e:
            raise CaseTriageBadRequestException(
                "not_found", "The opportunity deferral could not be found."
            ) from e

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
        """ Records individual clients actions. Expects JSON body of CaseUpdateSchema """
        etl_client = load_client(g.api_data["person_external_id"])

        if _should_see_demo():
            case_update = DemoCaseUpdatesInterface.update_case_for_person(
                current_session,
                g.email,
                etl_client,
                g.api_data["action_type"],
                g.api_data.get("comment", None),
                action_ts=DEMO_FROZEN_DATETIME,
            )
        else:
            case_update = CaseUpdatesInterface.update_case_for_person(
                current_session,
                g.current_user,
                etl_client,
                g.api_data["action_type"],
                g.api_data.get("comment", None),
            )

            if segment_client:
                segment_client.track_person_action_taken(
                    g.current_user,
                    etl_client,
                    g.api_data["action_type"],
                )
        presenter = CaseUpdatePresenter(etl_client, case_update)
        return jsonify(presenter.to_json())

    @api.route("/case_updates/<update_id>", methods=["DELETE"])
    def _delete_case_update(update_id: str) -> Response:
        try:
            if _should_see_demo():
                DemoCaseUpdatesInterface.delete_case_update(
                    current_session,
                    g.email,
                    update_id,
                )
            else:
                case_update = CaseUpdatesInterface.delete_case_update(
                    current_session,
                    g.current_user,
                    update_id,
                )

                etl_client = load_client(case_update.person_external_id)

                if segment_client:
                    segment_client.track_person_action_removed(
                        g.current_user,
                        etl_client,
                        CaseUpdateActionType(case_update.action_type),
                        str(case_update.update_id),
                    )
        except CaseUpdateDoesNotExistError as e:
            raise CaseTriageBadRequestException(
                "not_found", "The case update could not be found."
            ) from e

        return jsonify({"status": "ok", "status_code": HTTPStatus.OK})

    return api
