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
person not found (422), already completed (422).
"""
from decimal import Decimal
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, field_validator

from recidiviz.case_triage.edovo.supported_states import SUPPORTED_STATES
from recidiviz.common.constants.states import StateCode


class CourseCompletionRequest(BaseModel):
    """Inbound payload sent by Edovo when a learner completes eligible content."""

    model_config = ConfigDict(frozen=True)

    # The state's external, DOC-facing identifier for the learner (e.g. the CO
    # ADC number) — NOT the Recidiviz-internal integer person_id.
    person_external_id: str
    state_code: str
    course_id: str
    course_name: str
    content_hours: Decimal
    completed_at: AwareDatetime

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


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class CourseCompletionAcceptedResponse(BaseModel):
    """201 Created — the completion was recorded for the first time."""

    model_config = ConfigDict(frozen=True)

    status: Literal["accepted"] = "accepted"
    completion_id: str
    message: str = "Course completion recorded."


class CourseCompletionDuplicateResponse(BaseModel):
    """200 OK — idempotent replay of a previously recorded completion."""

    model_config = ConfigDict(frozen=True)

    status: Literal["duplicate"] = "duplicate"
    completion_id: str
    message: str = "This completion was already recorded."


class ValidationErrorDetails(BaseModel):
    """Field-level detail included in a 400 validation error response.

    The blueprint is responsible for mapping a Pydantic ValidationError into
    this shape: pick the first error from ``exc.errors()``, use its ``loc``
    as ``field`` and map its ``type`` to one of the constraint literals below.
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
    """422 Unprocessable Content — person + course pair already recorded under a different idempotency key."""

    error_code: Literal["ALREADY_COMPLETED"] = "ALREADY_COMPLETED"
    message: str = "This person has already received credit for this course."
