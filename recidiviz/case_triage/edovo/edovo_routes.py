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

Auth: Workload Identity Federation per recidiviz.case_triage.edovo.wif_verifier.
Edovo federates their AWS workload to the ``edovo-wif@`` GCP service account and
calls the endpoint with the GCP access token that federation mints, in the
``Authorization: Bearer`` header; no Auth0 JWT is involved.

Idempotency: Edovo supplies a client-generated UUID in the ``Idempotency-Key``
header (required).  A repeat of the same key returns the original response with
no side effects, and is answered before the body is parsed and before the
identity check below, so a replay cannot be re-judged against either a changed
body or a record that has since changed.  The same
person+course pair under a *different* key is rejected as a double-credit
attempt (per the API spec).  Both duplicate answers name the original
submission, so Edovo can find what we already hold.

Identity: the submitted ``person_external_id`` must resolve to a person we know,
and the submitted name must be that person's name.  An id we hold no record of
is a PERSON_NOT_FOUND; an id we do hold against someone else is a
PERSON_NAME_MISMATCH — the identifier drift Edovo asked to be told about, so
they can reconcile their records against ours.

Scope: this endpoint validates, authenticates, and durably captures each
completion, with idempotent and no-double-credit dedup enforced via database
constraints.  Earned-time credit calculation (6-hour pooling) and the eOMIS
writeback happen downstream, not in the request path.
"""
import enum
import logging
import re
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Literal

from flask import Blueprint, Response, jsonify, make_response, request
from pydantic import ValidationError as PydanticValidationError

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.case_triage.edovo.course_completion_models import (
    FIRST_NAME_FIELD,
    LAST_NAME_FIELD,
    CourseCompletionAcceptedResponse,
    CourseCompletionAlreadyCompletedResponse,
    CourseCompletionDuplicateResponse,
    CourseCompletionForbiddenResponse,
    CourseCompletionPersonNameMismatchResponse,
    CourseCompletionPersonNotFoundResponse,
    CourseCompletionRequest,
    CourseCompletionUnauthenticatedResponse,
    CourseCompletionValidationErrorResponse,
    ValidationErrorDetails,
)
from recidiviz.case_triage.edovo.persistence import (
    AlreadyCompletedError,
    find_completion_by_idempotency_key,
    persist_completion,
)
from recidiviz.case_triage.edovo.person_verification import (
    PersonNameMismatchError,
    PersonNotFoundError,
    verify_person_identity,
)
from recidiviz.case_triage.edovo.wif_verifier import verify_bearer_token
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.sqlalchemy_flask_utils import current_session
from recidiviz.utils.flask_exception import FlaskException

_IDEMPOTENCY_KEY_HEADER = "Idempotency-Key"

_REDACTED_VALUE = "[REDACTED]"
_REDACTED_FIELDS = (FIRST_NAME_FIELD, LAST_NAME_FIELD)
# The value may run to the end of the text unclosed, because a truncated
# payload still carries the part of the name that arrived.
_REDACTED_FIELD_VALUE_REGEX = re.compile(
    r'("(?:'
    + "|".join(re.escape(field) for field in _REDACTED_FIELDS)
    + r')"\s*:\s*)"(?:[^"\\]|\\.)*(?:"|\Z)'
)


def _redacted_body(raw_body: bytes) -> str:
    """Returns |raw_body| as text with the learner's name removed.

    The spec requires the request body in the audit record, and the body carries
    the learner's name. The name is not what the audit is for — reconciling with
    Edovo needs the identifier, the course and the timestamp — so it is dropped
    rather than written to Cloud Logging, consistent with the rest of the
    endpoint keeping names out of responses and error messages.

    The name is cut out of the received text rather than re-serialized from a
    parsed body, so everything else stays byte-for-byte what Edovo sent. That
    matters for a record whose purpose is settling a question about what they
    sent: re-serializing would silently renumber (``1e2`` -> ``100.0``),
    reorder keys, and drop duplicates. It also means a body that never parsed
    is redacted the same way as one that did.
    """
    return _REDACTED_FIELD_VALUE_REGEX.sub(
        rf'\1"{_REDACTED_VALUE}"', raw_body.decode("utf-8", errors="replace")
    )


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
    # A blank value and a JSON null are both reported as the missing field they
    # effectively are; pydantic raises a type error rather than a missing key.
    ("first_name", "value_error"): "required",
    ("last_name", "value_error"): "required",
    ("facility", "value_error"): "required",
    ("first_name", "string_type"): "required",
    ("last_name", "string_type"): "required",
    ("facility", "string_type"): "required",
}

# Stands in for the field name when the body failed to parse at all, so pydantic
# reported no field to attribute the failure to.
_UNPARSED_BODY_FIELD = "unknown"
_UNPARSED_BODY_MESSAGE = "The request body could not be read as a JSON object."

# Phrasing for each constraint, written here rather than taken from pydantic's
# own message, which can quote the submitted value back.
_CONSTRAINT_TO_PHRASE: dict[_ConstraintLiteral, str] = {
    "gt_zero": "must be greater than 0",
    "required": "is required",
    "timezone_aware": "must include a timezone",
    "invalid_state_code": "must be a supported state code",
    "invalid": "is invalid",
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
    the idempotency key, and the request body with the learner's name redacted
    (see ``_redacted_body``). Earned-time credit is computed downstream (this
    endpoint only captures completions), so no credit summary is recorded here.

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
        _redacted_body(raw_body),
    )


def _constraint_violations(
    exc: PydanticValidationError,
) -> list[tuple[str, _ConstraintLiteral]]:
    """Returns one (field, constraint) pair per invalid field, in the order
    pydantic reported them.

    An error carrying no location is the whole body failing to parse; it comes
    back under _UNPARSED_BODY_FIELD. Where pydantic reports a field more than
    once, the first is kept so each field is named once.
    """
    violations: list[tuple[str, _ConstraintLiteral]] = []
    seen_fields: set[str] = set()
    for error in exc.errors():
        field = str(error["loc"][0]) if error["loc"] else _UNPARSED_BODY_FIELD
        if field in seen_fields:
            continue
        seen_fields.add(field)
        error_type = error["type"]
        constraint: _ConstraintLiteral = (
            _PYDANTIC_TYPE_TO_CONSTRAINT.get(error_type)
            or _FIELD_AND_TYPE_TO_CONSTRAINT.get((field, error_type))
            or "invalid"
        )
        violations.append((field, constraint))
    return violations


def _violations_message(violations: list[tuple[str, _ConstraintLiteral]]) -> str:
    """Returns a message naming every invalid field and why it was rejected."""
    return (
        "; ".join(
            _UNPARSED_BODY_MESSAGE.rstrip(".")
            if field == _UNPARSED_BODY_FIELD
            else f"{field} {_CONSTRAINT_TO_PHRASE[constraint]}"
            for field, constraint in violations
        )
        + "."
    )


def _pydantic_error_response(
    violations: list[tuple[str, _ConstraintLiteral]],
) -> CourseCompletionValidationErrorResponse:
    """Returns the spec's 400 body, naming every invalid field in the message.

    The spec fixes ``details`` at a single field and constraint, so it reports
    the first violation; the message carries the rest, so Edovo can fix every
    field in one pass instead of one per request.
    """
    if not violations:
        raise ValueError("Expected at least one constraint violation, found none")
    first_field, first_constraint = violations[0]

    return CourseCompletionValidationErrorResponse(
        message=_violations_message(violations),
        details=ValidationErrorDetails(field=first_field, constraint=first_constraint),
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
            verify_bearer_token(request.headers.get("Authorization", ""))
        except FlaskException as error:
            _log_audit(
                received_at=received_at,
                idempotency_key=idempotency_key_header or None,
                raw_body=b"",
                outcome=RequestOutcome.REJECTED,
                reason=f"auth:{error.code}",
            )
            # Only the two documented auth outcomes get the partner-shaped body.
            # A 5xx is operational, so it re-raises to the shared handler.
            auth_message = f"{error.description} ({error.code})"
            if error.status_code is HTTPStatus.UNAUTHORIZED:
                return make_response(
                    jsonify(
                        CourseCompletionUnauthenticatedResponse(
                            message=auth_message
                        ).model_dump()
                    ),
                    error.status_code,
                )
            if error.status_code is HTTPStatus.FORBIDDEN:
                return make_response(
                    jsonify(
                        CourseCompletionForbiddenResponse(
                            message=auth_message
                        ).model_dump()
                    ),
                    error.status_code,
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
            already_recorded = find_completion_by_idempotency_key(
                current_session, idempotency_key
            )
        except Exception:
            # Audit before the error handler returns 500.
            _log_audit(
                received_at=received_at,
                idempotency_key=idempotency_key_header or None,
                raw_body=body,
                outcome=RequestOutcome.REJECTED,
                reason="idempotency_lookup_error",
            )
            raise
        if already_recorded is not None:
            # A recorded key is answered from what we recorded then, before the
            # body is parsed and before any identity check. Re-judging a replay
            # would let a regressed body, or a change to our own name record,
            # turn a settled completion into a rejection.
            _log_audit(
                received_at=received_at,
                idempotency_key=idempotency_key_header or None,
                raw_body=body,
                outcome=RequestOutcome.DUPLICATE,
            )
            return make_response(
                jsonify(
                    CourseCompletionDuplicateResponse(
                        completion_id=str(already_recorded.id),
                        originally_received_at=already_recorded.received_at,
                    ).model_dump(mode="json")
                ),
                HTTPStatus.OK,
            )

        try:
            completion_request = CourseCompletionRequest.model_validate_json(body)
        except PydanticValidationError as exc:
            violations = _constraint_violations(exc)
            validation_response = _pydantic_error_response(violations)
            _log_audit(
                received_at=received_at,
                idempotency_key=idempotency_key_header or None,
                raw_body=body,
                outcome=RequestOutcome.REJECTED,
                reason=f"validation:{','.join(field for field, _ in violations)}",
            )
            return make_response(
                jsonify(validation_response.model_dump()),
                HTTPStatus.BAD_REQUEST,
            )

        try:
            verify_person_identity(
                bq_client=BigQueryClientImpl(),
                state_code=StateCode(completion_request.state_code),
                person_external_id=completion_request.person_external_id,
                first_name=completion_request.first_name,
                last_name=completion_request.last_name,
            )
        except PersonNameMismatchError as mismatch:
            # We hold this id, but against someone else. Report which fields
            # matched nothing without echoing either name back.
            name_mismatch = CourseCompletionPersonNameMismatchResponse(
                mismatched_fields=mismatch.mismatched_fields
            )
            _log_audit(
                received_at=received_at,
                idempotency_key=idempotency_key_header or None,
                raw_body=body,
                outcome=RequestOutcome.REJECTED,
                reason=f"person_name_mismatch:{','.join(mismatch.mismatched_fields)}",
            )
            return make_response(
                jsonify(name_mismatch.model_dump()), HTTPStatus.UNPROCESSABLE_ENTITY
            )
        except PersonNotFoundError:
            not_found = CourseCompletionPersonNotFoundResponse(
                # The error_code is enough to act on, so the submitted id (PII)
                # is not echoed back.
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
            # Audit before the error handler returns 500.
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
        except AlreadyCompletedError as already_completed:
            _log_audit(
                received_at=received_at,
                idempotency_key=idempotency_key_header or None,
                raw_body=body,
                outcome=RequestOutcome.REJECTED,
                reason="already_completed",
            )
            return make_response(
                jsonify(
                    CourseCompletionAlreadyCompletedResponse(
                        completion_id=str(already_completed.existing.id),
                        originally_received_at=already_completed.existing.received_at,
                    ).model_dump(mode="json")
                ),
                HTTPStatus.UNPROCESSABLE_ENTITY,
            )
        except Exception:
            # Audit before the error handler returns 500.
            _log_audit(
                received_at=received_at,
                idempotency_key=idempotency_key_header or None,
                raw_body=body,
                outcome=RequestOutcome.REJECTED,
                reason="persist_error",
            )
            raise

        try:
            current_session.commit()
        except Exception:
            # Audit before the error handler returns 500.
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
                    completion_id=completion_id,
                    originally_received_at=record.received_at,
                ).model_dump(mode="json")
            ),
            HTTPStatus.OK,
        )

    return edovo_api
