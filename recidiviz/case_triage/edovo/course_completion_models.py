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
"""Pydantic models for the Edovo → Recidiviz course-completion API.

Request shape: POST /edovo/course-completions
Response shapes: accepted (201), duplicate (200), validation error (400),
unauthenticated (401), forbidden (403), person not found (422),
person name mismatch (422), already completed (422).
"""
from decimal import Decimal
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, field_validator

from recidiviz.case_triage.edovo.name_matching import normalized_name
from recidiviz.case_triage.edovo.supported_states import SUPPORTED_STATES
from recidiviz.common.constants.states import StateCode

MismatchedNameField = Literal["first_name", "last_name"]
"""A submitted field that the name check can report as disagreeing with our
records. Only the name fields verify the id, so only they can mismatch."""

FIRST_NAME_FIELD: MismatchedNameField = "first_name"
LAST_NAME_FIELD: MismatchedNameField = "last_name"


class CourseCompletionRequest(BaseModel):
    """Inbound payload sent by Edovo when a learner completes eligible content."""

    model_config = ConfigDict(frozen=True)

    # The DOC-facing id (e.g. a CO ADC number), not the internal person_id.
    person_external_id: str
    state_code: str
    course_id: str
    course_name: str
    content_hours: Decimal
    completed_at: AwareDatetime
    # The name verifies the person_external_id match rather than resolving a
    # person itself; the facility says which system issued the id.
    first_name: str
    last_name: str
    facility: str

    @field_validator("first_name", "last_name")
    @classmethod
    def must_be_a_usable_name(cls, v: str) -> str:
        """Rejects a name the comparison could not use.

        A name with no letters — blank, whitespace, or punctuation like '--' —
        still reaches the name check and still fails it, but it fails as a
        *mismatch*, telling Edovo their record disagrees with ours when in fact
        they sent us nothing usable. That would send them to reconcile a
        discrepancy that does not exist, so it is refused here as the malformed
        request it is.

        Usability is judged with the same normalization the comparison uses, so
        the two cannot disagree about what counts as a name. This mirrors how a
        stored name with no letters is treated: see ``name_matching``.
        """
        if not normalized_name(v):
            raise ValueError("must contain at least one letter")
        return v

    @field_validator("facility")
    @classmethod
    def must_not_be_blank(cls, v: str) -> str:
        """Rejects a facility that is present but carries no value.

        Unlike a name, a facility identifier is never compared to anything we
        hold, and may legitimately be all digits — so it only has to be
        non-empty.
        """
        if not v.strip():
            raise ValueError("must not be blank")
        return v

    @field_validator("state_code")
    @classmethod
    def state_code_must_be_supported(cls, v: str) -> str:
        try:
            state_code = StateCode(v)
        except ValueError as e:
            raise ValueError(f"[{v}] is not a valid state_code") from e
        if state_code not in SUPPORTED_STATES:
            raise ValueError(f"state_code [{v}] is not supported by the Edovo API")
        return v

    @field_validator("content_hours")
    @classmethod
    def content_hours_must_be_positive(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("content_hours must be greater than 0")
        return v

    @property
    def id_type(self) -> str:
        """Returns the external-id type the |person_external_id| is expected to match."""
        return SUPPORTED_STATES[StateCode(self.state_code)]


class CourseCompletionAcceptedResponse(BaseModel):
    """201 Created — the completion was recorded for the first time."""

    model_config = ConfigDict(frozen=True)

    status: Literal["accepted"] = "accepted"
    completion_id: str
    message: str = "Course completion recorded."


class CourseCompletionDuplicateResponse(BaseModel):
    """200 OK — idempotent replay of a previously recorded completion.

    ``originally_received_at`` is when we recorded the first request carrying
    this idempotency key, so Edovo can tell a retry of their own from a replay
    of something sent much earlier (e.g. during a backfill).
    """

    model_config = ConfigDict(frozen=True)

    status: Literal["duplicate"] = "duplicate"
    completion_id: str
    originally_received_at: AwareDatetime
    message: str = "This completion was already recorded."


class ValidationErrorDetails(BaseModel):
    """Field-level detail included in a 400 validation error response.

    The blueprint maps a Pydantic ValidationError into this shape: the first
    error from ``exc.errors()`` supplies ``field`` from its ``loc`` and one of
    the constraint literals below from its ``type``. Where more than one field
    is invalid, the others appear in the response message rather than here.
    """

    model_config = ConfigDict(frozen=True)

    field: str
    constraint: Literal[
        "gt_zero", "required", "timezone_aware", "invalid_state_code", "invalid"
    ]


class CourseCompletionErrorResponse(BaseModel):
    """Base shape shared by every error response: a ``status`` of ``"error"``,
    a machine-readable ``error_code``, and a human-readable ``message``.

    Each concrete subclass pins ``error_code`` to its own literal value.
    """

    model_config = ConfigDict(frozen=True)

    status: Literal["error"] = "error"
    message: str


class CourseCompletionValidationErrorResponse(CourseCompletionErrorResponse):
    """400 Bad Request — a required field was missing or failed validation."""

    error_code: Literal["VALIDATION_ERROR"] = "VALIDATION_ERROR"
    details: ValidationErrorDetails


class CourseCompletionPersonNotFoundResponse(CourseCompletionErrorResponse):
    """422 Unprocessable Content — the person_external_id is not in our system."""

    error_code: Literal["PERSON_NOT_FOUND"] = "PERSON_NOT_FOUND"


class CourseCompletionAlreadyCompletedResponse(CourseCompletionErrorResponse):
    """422 Unprocessable Content — person + course pair already recorded under a different idempotency key.

    Carries the original submission's ``completion_id`` and
    ``originally_received_at`` so Edovo can find what we already hold instead of
    only learning that something exists.
    """

    error_code: Literal["ALREADY_COMPLETED"] = "ALREADY_COMPLETED"
    message: str = "This person has already received credit for this course."
    completion_id: str
    originally_received_at: AwareDatetime


class CourseCompletionPersonNameMismatchResponse(CourseCompletionErrorResponse):
    """422 Unprocessable Content — the external id resolves, but to another name.

    This is the identifier-drift signal Edovo asked for: we hold this id, but
    against a different person than the one they sent. ``mismatched_fields``
    names the submitted fields that matched no record we hold, and echoes
    neither name back.
    """

    error_code: Literal["PERSON_NAME_MISMATCH"] = "PERSON_NAME_MISMATCH"
    message: str = (
        "The provided person_external_id belongs to a person with a different "
        "name in our records."
    )
    mismatched_fields: list[MismatchedNameField]


class CourseCompletionUnauthenticatedResponse(CourseCompletionErrorResponse):
    """401 Unauthorized — the request carried no valid bearer token."""

    error_code: Literal["UNAUTHENTICATED"] = "UNAUTHENTICATED"


class CourseCompletionForbiddenResponse(CourseCompletionErrorResponse):
    """403 Forbidden — the bearer token is valid but is not the expected service account."""

    error_code: Literal["FORBIDDEN"] = "FORBIDDEN"
