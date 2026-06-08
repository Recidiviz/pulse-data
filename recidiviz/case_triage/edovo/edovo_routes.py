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
"""Flask blueprint for the Edovo course-completion inbound API.

Endpoint: POST /edovo/course-completions

Auth: HMAC-SHA256 per recidiviz.case_triage.edovo.hmac_verifier.  No Auth0 JWT
is required; the request is authenticated entirely by the shared secret.
"""
import logging
import uuid
from datetime import timezone
from decimal import Decimal
from http import HTTPStatus
from typing import Literal

from flask import Blueprint, Response, jsonify, make_response, request
from pydantic import ValidationError as PydanticValidationError

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.case_triage.edovo.course_completion_models import (
    CourseCompletionAcceptedResponse,
    CourseCompletionAlreadyCompletedResponse,
    CourseCompletionDuplicateResponse,
    CourseCompletionPersonNotFoundResponse,
    CourseCompletionRequest,
    CourseCompletionValidationErrorResponse,
    ValidationErrorDetails,
)
from recidiviz.case_triage.edovo.credit_calculator import (
    new_credits_for_completion,
    query_total_hours,
)
from recidiviz.case_triage.edovo.hmac_verifier import load_secret_and_verify
from recidiviz.case_triage.edovo.persistence import (
    AlreadyCompletedError,
    persist_completion,
)
from recidiviz.case_triage.edovo.person_resolver import (
    PersonNotFoundError,
    resolve_person_by_doc_id,
)
from recidiviz.persistence.database.sqlalchemy_flask_utils import current_session

_ConstraintLiteral = Literal[
    "gt_zero", "required", "timezone_aware", "invalid_state_code"
]

_PYDANTIC_TYPE_TO_CONSTRAINT: dict[str, _ConstraintLiteral] = {
    "missing": "required",
    "timezone_aware": "timezone_aware",
}


def _map_pydantic_error(
    exc: PydanticValidationError,
) -> CourseCompletionValidationErrorResponse:
    first = exc.errors()[0]
    field = str(first["loc"][0]) if first["loc"] else "unknown"
    error_type = first["type"]

    mapped = _PYDANTIC_TYPE_TO_CONSTRAINT.get(error_type)
    if mapped is not None:
        constraint: _ConstraintLiteral = mapped
    elif field == "content_hours":
        constraint = "gt_zero"
    else:
        constraint = "invalid_state_code"

    return CourseCompletionValidationErrorResponse(
        message=first["msg"],
        details=ValidationErrorDetails(field=field, constraint=constraint),
    )


def _derive_idempotency_key(req: CourseCompletionRequest) -> uuid.UUID:
    completed_at_utc = req.completed_at.astimezone(timezone.utc).isoformat()
    canonical = f"{req.state_code}:{req.person_id}:{req.course_id}:{completed_at_utc}"
    return uuid.uuid5(uuid.NAMESPACE_URL, canonical)


def create_edovo_api_blueprint() -> Blueprint:
    """Creates the Blueprint for inbound Edovo course-completion webhooks."""
    edovo_api = Blueprint("edovo", __name__)

    @edovo_api.post("/course-completions")
    def handle_course_completion() -> Response:
        body = request.get_data()

        load_secret_and_verify(body, request.headers.get("Authorization", ""))

        try:
            completion_request = CourseCompletionRequest.model_validate_json(body)
        except PydanticValidationError as exc:
            return make_response(
                jsonify(_map_pydantic_error(exc).model_dump()),
                HTTPStatus.BAD_REQUEST,
            )

        try:
            person_id = resolve_person_by_doc_id(
                BigQueryClientImpl(), completion_request.person_id
            )
        except PersonNotFoundError:
            not_found = CourseCompletionPersonNotFoundResponse(
                message=f"No person found for person_id {completion_request.person_id!r}.",
            )
            return make_response(
                jsonify(not_found.model_dump()), HTTPStatus.UNPROCESSABLE_ENTITY
            )

        idempotency_key = _derive_idempotency_key(completion_request)

        prior_hours = query_total_hours(
            current_session, person_id, completion_request.state_code
        )

        try:
            record, is_new = persist_completion(
                current_session, completion_request, person_id, idempotency_key
            )
            completion_id = str(record.id)
        except AlreadyCompletedError:
            return make_response(
                jsonify(CourseCompletionAlreadyCompletedResponse().model_dump()),
                HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        if is_new:
            credits_earned = new_credits_for_completion(
                prior_hours, Decimal(str(completion_request.content_hours))
            )
            logging.info(
                "Edovo: person %s earned %d credit(s) from course %s (%.2f hours, prior total %.2f)",
                person_id,
                credits_earned,
                completion_request.course_id,
                completion_request.content_hours,
                prior_hours,
            )

        current_session.commit()

        if is_new:
            return make_response(
                jsonify(
                    CourseCompletionAcceptedResponse(
                        completion_id=completion_id
                    ).model_dump()
                ),
                HTTPStatus.CREATED,
            )
        return make_response(
            jsonify(
                CourseCompletionDuplicateResponse(
                    completion_id=completion_id
                ).model_dump()
            ),
            HTTPStatus.OK,
        )

    return edovo_api
