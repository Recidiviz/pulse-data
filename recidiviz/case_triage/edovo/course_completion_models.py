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


class CourseCompletionRequest(BaseModel):
    """Inbound payload sent by Edovo when a learner completes eligible content."""

    model_config = ConfigDict(frozen=True)

    person_id: str
    state_code: Literal["US_CO"]
    course_id: str
    course_name: str
    content_hours: Decimal
    completed_at: AwareDatetime

    @field_validator("content_hours")
    @classmethod
    def content_hours_must_be_positive(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("content_hours must be greater than 0")
        return v


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


class CourseCompletionValidationErrorResponse(BaseModel):
    """400 Bad Request — a required field was missing or failed validation."""

    model_config = ConfigDict(frozen=True)

    status: Literal["error"] = "error"
    error_code: Literal["VALIDATION_ERROR"] = "VALIDATION_ERROR"
    message: str
    details: ValidationErrorDetails


class CourseCompletionPersonNotFoundResponse(BaseModel):
    """422 Unprocessable Content — the person_id is not in our system."""

    model_config = ConfigDict(frozen=True)

    status: Literal["error"] = "error"
    error_code: Literal["PERSON_NOT_FOUND"] = "PERSON_NOT_FOUND"
    message: str


class CourseCompletionAlreadyCompletedResponse(BaseModel):
    """422 Unprocessable Content — person + course pair already recorded under a different idempotency key."""

    model_config = ConfigDict(frozen=True)

    status: Literal["error"] = "error"
    error_code: Literal["ALREADY_COMPLETED"] = "ALREADY_COMPLETED"
    message: str = "This person has already received credit for this course."
