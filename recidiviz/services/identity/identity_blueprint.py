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
)
from recidiviz.services.identity.querier import IdentityServiceQuerier

identity_blueprint = Blueprint("identity", "identity")


@identity_blueprint.route("/identity/<uuid:recidiviz_id>")
class IdentityByRecidivizIdAPI(MethodView):
    """CRUD endpoints for /identity/<recidiviz_id>."""

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


@identity_blueprint.route("/identity")
class IdentityAPI(MethodView):
    """CRUD endpoints for /identity."""

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
