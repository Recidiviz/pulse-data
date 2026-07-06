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
is required; the request is authenticated entirely by the shared secret.  HMAC is
the launch mechanism; the spec's preferred Workload Identity Federation (WIF) is
expected to replace it later (TODO(OBT-27565)).

Idempotency: Edovo supplies a client-generated UUID in the ``Idempotency-Key``
header (required).  A repeat of the same key returns the original response with
no side effects; the same person+course pair under a *different* key is rejected
as a double-credit attempt (per the API spec).

Scope: this endpoint validates, authenticates, and durably captures each
completion, with idempotent and no-double-credit dedup enforced via database
constraints.  Earned-time credit calculation (6-hour pooling) and the eOMIS
writeback happen downstream, not in the request path.
"""
import enum
import logging
import uuid
from datetime import datetime, timezone
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
from recidiviz.case_triage.edovo.hmac_verifier import load_secret_and_verify
from recidiviz.case_triage.edovo.persistence import (
    AlreadyCompletedError,
    persist_completion,
)
from recidiviz.case_triage.edovo.person_existence import (
    PersonNotFoundError,
    assert_person_exists,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.sqlalchemy_flask_utils import current_session
from recidiviz.utils.auth.auth0 import AuthorizationError

_IDEMPOTENCY_KEY_HEADER = "Idempotency-Key"


class RequestOutcome(enum.Enum):
    """Terminal outcome recorded in the audit log for every inbound request
    (per the API spec's audit-logging requirement)."""

    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


_ConstraintLiteral = Literal[
    "gt_zero", "required", "timezone_aware", "invalid_state_code", "invalid"
]

_PYDANTIC_TYPE_TO_CONSTRAINT: dict[str, _ConstraintLiteral] = {
    "missing": "required",
    "timezone_aware": "timezone_aware",
}

_FIELD_AND_TYPE_TO_CONSTRAINT: dict[tuple[str, str], _ConstraintLiteral] = {
    ("content_hours", "value_error"): "gt_zero",
    ("state_code", "value_error"): "invalid_state_code",
}


def _log_audit(
    *,
    received_at: datetime,
    idempotency_key: str | None,
    raw_body: bytes,
    outcome: RequestOutcome,
    reason: str | None = None,
) -> None:
    """Emit a single structured audit record for an inbound Edovo request.

    Covers every terminal outcome (accepted / duplicate / rejected + reason) per
    the API spec's audit-logging requirement, capturing the received timestamp,
    the idempotency key, and the full request body. Earned-time credit is
    computed downstream (this endpoint only captures completions), so no credit
    summary is recorded here.

    ``idempotency_key`` is always the raw value received in the
    ``Idempotency-Key`` header (or None if absent), so the audit log records
    exactly what Edovo sent regardless of whether the value parsed as a UUID.
    """
    logging.info(
        "Edovo course-completion request: received_at=[%s] idempotency_key=[%s] "
        "outcome=[%s] reason=[%s] body=[%s]",
        received_at.isoformat(),
        idempotency_key,
        outcome.value,
        reason,
        raw_body.decode("utf-8", errors="replace"),
    )


def _map_pydantic_error(
    exc: PydanticValidationError,
) -> CourseCompletionValidationErrorResponse:
    first = exc.errors()[0]
    field = str(first["loc"][0]) if first["loc"] else "unknown"
    error_type = first["type"]

    constraint: _ConstraintLiteral = (
        _PYDANTIC_TYPE_TO_CONSTRAINT.get(error_type)
        or _FIELD_AND_TYPE_TO_CONSTRAINT.get((field, error_type))
        or "invalid"
    )

    return CourseCompletionValidationErrorResponse(
        message=first["msg"],
        details=ValidationErrorDetails(field=field, constraint=constraint),
    )


def _validation_error_response(
    *, field: str, constraint: _ConstraintLiteral, message: str
) -> CourseCompletionValidationErrorResponse:
    return CourseCompletionValidationErrorResponse(
        message=message,
        details=ValidationErrorDetails(field=field, constraint=constraint),
    )


def create_edovo_api_blueprint() -> Blueprint:
    """Creates the Blueprint for inbound Edovo course-completion webhooks."""
    edovo_api = Blueprint("edovo", __name__)

    @edovo_api.post("/course-completions")
    def handle_course_completion() -> Response:
        received_at = datetime.now(timezone.utc)
        body = request.get_data()
        idempotency_key_header = request.headers.get(_IDEMPOTENCY_KEY_HEADER, "")

        try:
            load_secret_and_verify(
                body, request.headers.get("Authorization", ""), request.path
            )
        except AuthorizationError as error:
            # The body of an unauthenticated request is untrusted, so it is
            # deliberately omitted from this audit record.
            _log_audit(
                received_at=received_at,
                idempotency_key=idempotency_key_header or None,
                raw_body=b"",
                outcome=RequestOutcome.REJECTED,
                reason=f"auth:{error.code}",
            )
            raise

        try:
            idempotency_key = uuid.UUID(idempotency_key_header)
        except ValueError:
            constraint: _ConstraintLiteral = (
                "required" if not idempotency_key_header else "invalid"
            )
            message = (
                f"{_IDEMPOTENCY_KEY_HEADER} header is required."
                if not idempotency_key_header
                else f"{_IDEMPOTENCY_KEY_HEADER} header must be a valid UUID."
            )
            _log_audit(
                received_at=received_at,
                idempotency_key=idempotency_key_header or None,
                raw_body=body,
                outcome=RequestOutcome.REJECTED,
                reason=f"idempotency_key:{constraint}",
            )
            return make_response(
                jsonify(
                    _validation_error_response(
                        field=_IDEMPOTENCY_KEY_HEADER,
                        constraint=constraint,
                        message=message,
                    ).model_dump()
                ),
                HTTPStatus.BAD_REQUEST,
            )

        try:
            completion_request = CourseCompletionRequest.model_validate_json(body)
        except PydanticValidationError as exc:
            validation_response = _map_pydantic_error(exc)
            _log_audit(
                received_at=received_at,
                idempotency_key=idempotency_key_header or None,
                raw_body=body,
                outcome=RequestOutcome.REJECTED,
                reason=f"validation:{validation_response.details.field}",
            )
            return make_response(
                jsonify(validation_response.model_dump()),
                HTTPStatus.BAD_REQUEST,
            )

        try:
            assert_person_exists(
                BigQueryClientImpl(),
                StateCode(completion_request.state_code),
                completion_request.person_external_id,
            )
        except PersonNotFoundError:
            not_found = CourseCompletionPersonNotFoundResponse(
                # Don't echo the submitted person_external_id (a DOC number / PII)
                # back in the response body or audit log; the error_code is enough
                # for Edovo to act on.
                message="No person found for the provided person_external_id.",
            )
            _log_audit(
                received_at=received_at,
                idempotency_key=idempotency_key_header or None,
                raw_body=body,
                outcome=RequestOutcome.REJECTED,
                reason="person_not_found",
            )
            return make_response(
                jsonify(not_found.model_dump()), HTTPStatus.UNPROCESSABLE_ENTITY
            )
        except Exception:
            # Person resolution hit an unexpected error (e.g. BigQuery failure).
            # Audit the rejected outcome before the error handler returns 500.
            _log_audit(
                received_at=received_at,
                idempotency_key=idempotency_key_header or None,
                raw_body=body,
                outcome=RequestOutcome.REJECTED,
                reason="person_resolution_error",
            )
            raise

        try:
            record, is_new = persist_completion(
                current_session,
                completion_request,
                idempotency_key,
                received_at,
            )
            completion_id = str(record.id)
        except AlreadyCompletedError:
            _log_audit(
                received_at=received_at,
                idempotency_key=idempotency_key_header or None,
                raw_body=body,
                outcome=RequestOutcome.REJECTED,
                reason="already_completed",
            )
            return make_response(
                jsonify(CourseCompletionAlreadyCompletedResponse().model_dump()),
                HTTPStatus.UNPROCESSABLE_ENTITY,
            )

        try:
            current_session.commit()
        except Exception:
            # Commit failed (e.g. DB connectivity). Audit before the error
            # handler returns 500 so no terminal outcome goes unlogged.
            _log_audit(
                received_at=received_at,
                idempotency_key=idempotency_key_header or None,
                raw_body=body,
                outcome=RequestOutcome.REJECTED,
                reason="commit_error",
            )
            raise

        _log_audit(
            received_at=received_at,
            idempotency_key=idempotency_key_header or None,
            raw_body=body,
            outcome=RequestOutcome.ACCEPTED if is_new else RequestOutcome.DUPLICATE,
        )

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
