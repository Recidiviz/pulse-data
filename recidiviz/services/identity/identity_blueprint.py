# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""flask_smorest Blueprint and MethodView classes for the Identity Service API."""
import logging
import uuid
from http import HTTPStatus

from flask import Response, jsonify
from flask.views import MethodView
from flask_smorest import Blueprint, abort

from recidiviz.services.identity.api_schemas import (
    IdentityByQueryParametersRequestSchema,
    IdentityByUuidRequestSchema,
    IdentityHistorySchema,
    IdentitySchema,
    IdentitySearchRequestSchema,
    ImportRequestSchema,
    SearchResponseSchema,
)
from recidiviz.services.identity.authorization import CallerRole
from recidiviz.services.identity.constants import (
    IDENTITIES_BLUEPRINT_ROUTE,
    TRIGGER_IMPORT_BLUEPRINT_ROUTE,
)
from recidiviz.services.identity.querier import IdentityServiceQuerier
from recidiviz.services.identity.types import IdentitySearchRequest

identity_blueprint = Blueprint("identity", "identity")

# Roles permitted to read identities via the /identities endpoints.
IDENTITY_READ_ROLES = frozenset({CallerRole.READER, CallerRole.EDITOR})


@identity_blueprint.route(f"{IDENTITIES_BLUEPRINT_ROUTE}/<uuid:recidiviz_id>")
class IdentityByRecidivizIdAPI(MethodView):
    """Item-level endpoints for a single identity, addressed by Recidiviz ID."""

    ALLOWED_ROLES_BY_METHOD: dict[str, frozenset[CallerRole]] = {
        "GET": IDENTITY_READ_ROLES,
    }

    @identity_blueprint.arguments(
        IdentityByUuidRequestSchema,
        location="query",
        error_status_code=HTTPStatus.BAD_REQUEST,
    )
    @identity_blueprint.response(HTTPStatus.OK)
    def get(self, params: dict, recidiviz_id: uuid.UUID) -> Response:
        """Returns the identity for the given Recidiviz ID.

        A retired Recidiviz ID resolves to its surviving record. Pass `?full=true`
        to include audit history (merge/split events) and internal bookkeeping.
        """
        querier = IdentityServiceQuerier()
        identity_record = querier.get_identity(recidiviz_id, resolve_retired=True)
        if identity_record is None:
            abort(
                HTTPStatus.NOT_FOUND,
                message=f"No identity found for recidiviz_id [{recidiviz_id}]",
            )

        if params["full"]:
            history = querier.get_identity_history(identity_record)
            return jsonify(IdentityHistorySchema().dump(history))
        return jsonify(IdentitySchema().dump(identity_record))


@identity_blueprint.route(f"{IDENTITIES_BLUEPRINT_ROUTE}/search")
class IdentitySearchAPI(MethodView):
    """POST /identities/search — find identities matching search criteria."""

    ALLOWED_ROLES_BY_METHOD: dict[str, frozenset[CallerRole]] = {
        "POST": IDENTITY_READ_ROLES,
    }

    @identity_blueprint.arguments(
        IdentitySearchRequestSchema,
        location="json",
        error_status_code=HTTPStatus.BAD_REQUEST,
    )
    @identity_blueprint.response(HTTPStatus.OK)
    def post(self, params: dict) -> Response:
        """Returns identities matching the given search criteria (ANDed).

        By default only ACTIVE identities are returned; set `retired_handling`
        to RESOLVE to replace RETIRED matches with their surviving record, or
        AS_STORED to return RETIRED matches unresolved. Returns at most
        `limit` results (max 100, default 50); pass the response's
        `next_cursor` back as `cursor` to retrieve the next page.
        """
        search_request = IdentitySearchRequest(
            name=params["name"],
            tenant=params["tenant"],
            person_type=params["person_type"],
            external_id=params["external_id"],
            limit=params["limit"],
            cursor=params["cursor"],
            retired_handling=params["retired_handling"],
        )
        search_result = IdentityServiceQuerier().search(search_request)
        return jsonify(SearchResponseSchema().dump(search_result))


@identity_blueprint.route(IDENTITIES_BLUEPRINT_ROUTE)
class IdentityAPI(MethodView):
    """Collection-level endpoints for identities, looked up by alternate key."""

    ALLOWED_ROLES_BY_METHOD: dict[str, frozenset[CallerRole]] = {
        "GET": IDENTITY_READ_ROLES,
    }

    @identity_blueprint.arguments(
        IdentityByQueryParametersRequestSchema,
        location="query",
        error_status_code=HTTPStatus.BAD_REQUEST,
    )
    @identity_blueprint.response(HTTPStatus.OK)
    def get(self, params: dict) -> Response:
        """Returns the active identity for the given lookup params.

        Accepts either external_id+id_type or tenant+email_hash. A retired
        identity resolves to its surviving record. Pass `?full=true` to include
        audit history (merge/split events) and internal bookkeeping.
        """
        querier = IdentityServiceQuerier()
        if params["external_id"] is not None:
            identity_record = querier.get_by_external_id(
                params["external_id"], params["id_type"]
            )
            not_found_msg = (
                f"No identity found for external_id [{params['external_id']}] "
                f"id_type [{params['id_type'].value}]"
            )
        else:
            identity_record = querier.get_by_email_hash(
                params["email_hash"], params["tenant"]
            )
            not_found_msg = (
                f"No identity found for email_hash [{params['email_hash']}] "
                f"tenant [{params['tenant'].value}]"
            )
        if identity_record is None:
            abort(HTTPStatus.NOT_FOUND, message=not_found_msg)
        if params["full"]:
            history = querier.get_identity_history(identity_record)
            return jsonify(IdentityHistorySchema().dump(history))
        return jsonify(IdentitySchema().dump(identity_record))


@identity_blueprint.route(TRIGGER_IMPORT_BLUEPRINT_ROUTE)
class TriggerImportAPI(MethodView):
    """Endpoint the Batch Identity Clustering DAG calls after writing a tenant's
    clustering results to BigQuery, to trigger reconciliation of those results
    into the Identity Service's Postgres state."""

    ALLOWED_ROLES_BY_METHOD: dict[str, frozenset[CallerRole]] = {
        "POST": frozenset({CallerRole.IMPORTER}),
    }

    @identity_blueprint.arguments(
        ImportRequestSchema, error_status_code=HTTPStatus.BAD_REQUEST
    )
    @identity_blueprint.response(HTTPStatus.ACCEPTED)
    def post(self, params: dict) -> Response:
        """Accepts a trigger_import request for the given tenant and returns 202.

        TODO(OBT-37693): Replace with the real implementation.
        """
        tenant = params["tenant"]
        logging.info(
            "Received /trigger_import request for tenant [%s]; import not yet implemented.",
            tenant.value,
        )
        response = jsonify({})
        response.status_code = HTTPStatus.ACCEPTED
        return response
