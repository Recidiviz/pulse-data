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
"""Unit tests for the Edovo course-completion Pydantic models."""
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase

from pydantic import ValidationError

from recidiviz.case_triage.edovo.course_completion_models import (
    CourseCompletionAcceptedResponse,
    CourseCompletionAlreadyCompletedResponse,
    CourseCompletionDuplicateResponse,
    CourseCompletionPersonNotFoundResponse,
    CourseCompletionRequest,
    CourseCompletionValidationErrorResponse,
    ValidationErrorDetails,
)

VALID_PAYLOAD = {
    "person_id": "012345",
    "state_code": "US_CO",
    "course_id": "foo-bar",
    "course_name": "Course Foo Bar",
    "content_hours": 4.5,
    "completed_at": "2026-04-23T17:42:00Z",
}


class TestCourseCompletionRequest(TestCase):
    def test_valid_payload_round_trips(self) -> None:
        req = CourseCompletionRequest.model_validate(VALID_PAYLOAD)

        self.assertEqual(req.person_id, "012345")
        self.assertEqual(req.state_code, "US_CO")
        self.assertEqual(req.course_id, "foo-bar")
        self.assertEqual(req.course_name, "Course Foo Bar")
        self.assertEqual(req.content_hours, Decimal("4.5"))
        self.assertEqual(
            req.completed_at, datetime(2026, 4, 23, 17, 42, 0, tzinfo=timezone.utc)
        )

        dumped = req.model_dump(mode="json")
        self.assertEqual(dumped["person_id"], "012345")
        self.assertEqual(dumped["state_code"], "US_CO")
        self.assertEqual(dumped["course_id"], "foo-bar")
        self.assertEqual(dumped["course_name"], "Course Foo Bar")
        self.assertEqual(dumped["content_hours"], "4.5")
        self.assertEqual(dumped["completed_at"], "2026-04-23T17:42:00Z")

    # --- content_hours validation ---

    def test_content_hours_zero_is_rejected(self) -> None:
        with self.assertRaises(ValidationError) as cm:
            CourseCompletionRequest.model_validate(
                {**VALID_PAYLOAD, "content_hours": 0}
            )
        errors = cm.exception.errors()
        self.assertEqual(len(errors), 1)
        self.assertIn("content_hours must be greater than 0", errors[0]["msg"])

    def test_content_hours_negative_is_rejected(self) -> None:
        with self.assertRaises(ValidationError) as cm:
            CourseCompletionRequest.model_validate(
                {**VALID_PAYLOAD, "content_hours": -1.5}
            )
        errors = cm.exception.errors()
        self.assertEqual(len(errors), 1)
        self.assertIn("content_hours must be greater than 0", errors[0]["msg"])

    # --- completed_at validation ---

    def test_naive_datetime_is_rejected(self) -> None:
        with self.assertRaises(ValidationError) as cm:
            CourseCompletionRequest.model_validate(
                {**VALID_PAYLOAD, "completed_at": "2026-04-23T17:42:00"}
            )
        errors = cm.exception.errors()
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["loc"], ("completed_at",))

    def test_timezone_aware_datetime_is_accepted(self) -> None:
        req = CourseCompletionRequest.model_validate(
            {**VALID_PAYLOAD, "completed_at": "2026-04-23T17:42:00+05:30"}
        )
        self.assertIsNotNone(req.completed_at.tzinfo)

    # --- state_code validation ---

    def test_invalid_state_code_is_rejected(self) -> None:
        with self.assertRaises(ValidationError) as cm:
            CourseCompletionRequest.model_validate(
                {**VALID_PAYLOAD, "state_code": "INVALID"}
            )
        errors = cm.exception.errors()
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["loc"], ("state_code",))

    def test_unknown_state_code_is_rejected(self) -> None:
        with self.assertRaises(ValidationError) as cm:
            CourseCompletionRequest.model_validate(
                {**VALID_PAYLOAD, "state_code": "US_ZZ"}
            )
        errors = cm.exception.errors()
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["loc"], ("state_code",))

    def test_non_colorado_state_code_is_rejected(self) -> None:
        with self.assertRaises(ValidationError) as cm:
            CourseCompletionRequest.model_validate(
                {**VALID_PAYLOAD, "state_code": "US_PA"}
            )
        errors = cm.exception.errors()
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["loc"], ("state_code",))

    # --- required fields ---

    def test_missing_person_id_raises(self) -> None:
        with self.assertRaises(ValidationError):
            CourseCompletionRequest.model_validate(
                {k: v for k, v in VALID_PAYLOAD.items() if k != "person_id"}
            )

    def test_missing_state_code_raises(self) -> None:
        with self.assertRaises(ValidationError):
            CourseCompletionRequest.model_validate(
                {k: v for k, v in VALID_PAYLOAD.items() if k != "state_code"}
            )

    def test_missing_course_id_raises(self) -> None:
        with self.assertRaises(ValidationError):
            CourseCompletionRequest.model_validate(
                {k: v for k, v in VALID_PAYLOAD.items() if k != "course_id"}
            )

    def test_missing_course_name_raises(self) -> None:
        with self.assertRaises(ValidationError):
            CourseCompletionRequest.model_validate(
                {k: v for k, v in VALID_PAYLOAD.items() if k != "course_name"}
            )

    def test_missing_content_hours_raises(self) -> None:
        with self.assertRaises(ValidationError):
            CourseCompletionRequest.model_validate(
                {k: v for k, v in VALID_PAYLOAD.items() if k != "content_hours"}
            )

    def test_missing_completed_at_raises(self) -> None:
        with self.assertRaises(ValidationError):
            CourseCompletionRequest.model_validate(
                {k: v for k, v in VALID_PAYLOAD.items() if k != "completed_at"}
            )


class TestResponseModels(TestCase):
    def test_accepted_response(self) -> None:
        resp = CourseCompletionAcceptedResponse(completion_id="rec_abc123")
        self.assertEqual(resp.status, "accepted")
        self.assertEqual(resp.completion_id, "rec_abc123")
        self.assertEqual(resp.message, "Course completion recorded.")

    def test_duplicate_response(self) -> None:
        resp = CourseCompletionDuplicateResponse(completion_id="rec_abc123")
        self.assertEqual(resp.status, "duplicate")
        self.assertEqual(resp.completion_id, "rec_abc123")
        self.assertEqual(resp.message, "This completion was already recorded.")

    def test_validation_error_response(self) -> None:
        resp = CourseCompletionValidationErrorResponse(
            message="content_hours must be greater than 0.",
            details=ValidationErrorDetails(field="content_hours", constraint="gt_zero"),
        )
        self.assertEqual(resp.status, "error")
        self.assertEqual(resp.error_code, "VALIDATION_ERROR")
        self.assertEqual(resp.details.field, "content_hours")
        self.assertEqual(resp.details.constraint, "gt_zero")

    def test_validation_error_response_rejects_unknown_constraint(self) -> None:
        with self.assertRaises(ValidationError):
            CourseCompletionValidationErrorResponse(
                message="some error",
                details=ValidationErrorDetails(
                    field="content_hours", constraint="not_a_real_constraint"  # type: ignore[arg-type]
                ),
            )

    def test_person_not_found_response(self) -> None:
        resp = CourseCompletionPersonNotFoundResponse(
            message="No person found with external_id 'X12345' of type 'US_CO_DOC_ID'."
        )
        self.assertEqual(resp.status, "error")
        self.assertEqual(resp.error_code, "PERSON_NOT_FOUND")

    def test_already_completed_response(self) -> None:
        resp = CourseCompletionAlreadyCompletedResponse()
        self.assertEqual(resp.status, "error")
        self.assertEqual(resp.error_code, "ALREADY_COMPLETED")
        self.assertEqual(
            resp.message, "This person has already received credit for this course."
        )
