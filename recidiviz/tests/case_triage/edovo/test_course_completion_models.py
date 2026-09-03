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
    CourseCompletionPersonNameMismatchResponse,
    CourseCompletionPersonNotFoundResponse,
    CourseCompletionRequest,
    CourseCompletionValidationErrorResponse,
    ValidationErrorDetails,
)

_RECEIVED_AT = datetime(2026, 4, 23, 17, 45, 0, tzinfo=timezone.utc)

VALID_PAYLOAD = {
    "person_external_id": "012345",
    "state_code": "US_CO",
    "course_id": "foo-bar",
    "course_name": "Course Foo Bar",
    "content_hours": 4.5,
    "completed_at": "2026-04-23T17:42:00Z",
    "first_name": "Jane",
    "last_name": "Doe",
    "facility": "CDOC-XYZ",
}


class TestCourseCompletionRequest(TestCase):
    """Tests for parsing and validating the inbound CourseCompletionRequest model."""

    def test_valid_payload_round_trips(self) -> None:
        req = CourseCompletionRequest.model_validate(VALID_PAYLOAD)

        self.assertEqual(req.person_external_id, "012345")
        self.assertEqual(req.state_code, "US_CO")
        self.assertEqual(req.id_type, "US_CO_ADCNUMBER")
        self.assertEqual(req.course_id, "foo-bar")
        self.assertEqual(req.course_name, "Course Foo Bar")
        self.assertEqual(req.content_hours, Decimal("4.5"))
        self.assertEqual(
            req.completed_at, datetime(2026, 4, 23, 17, 42, 0, tzinfo=timezone.utc)
        )
        self.assertEqual(req.first_name, "Jane")
        self.assertEqual(req.last_name, "Doe")
        self.assertEqual(req.facility, "CDOC-XYZ")

        dumped = req.model_dump(mode="json")
        self.assertEqual(dumped["person_external_id"], "012345")
        self.assertEqual(dumped["state_code"], "US_CO")
        self.assertEqual(dumped["course_id"], "foo-bar")
        self.assertEqual(dumped["course_name"], "Course Foo Bar")
        self.assertEqual(dumped["content_hours"], "4.5")
        self.assertEqual(dumped["completed_at"], "2026-04-23T17:42:00Z")
        self.assertEqual(dumped["first_name"], "Jane")
        self.assertEqual(dumped["last_name"], "Doe")
        self.assertEqual(dumped["facility"], "CDOC-XYZ")

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

    def test_missing_person_external_id_raises(self) -> None:
        with self.assertRaises(ValidationError):
            CourseCompletionRequest.model_validate(
                {k: v for k, v in VALID_PAYLOAD.items() if k != "person_external_id"}
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

    def test_missing_first_name_raises(self) -> None:
        with self.assertRaises(ValidationError):
            CourseCompletionRequest.model_validate(
                {k: v for k, v in VALID_PAYLOAD.items() if k != "first_name"}
            )

    def test_missing_last_name_raises(self) -> None:
        with self.assertRaises(ValidationError):
            CourseCompletionRequest.model_validate(
                {k: v for k, v in VALID_PAYLOAD.items() if k != "last_name"}
            )

    def test_missing_facility_raises(self) -> None:
        with self.assertRaises(ValidationError):
            CourseCompletionRequest.model_validate(
                {k: v for k, v in VALID_PAYLOAD.items() if k != "facility"}
            )

    def test_null_first_name_raises(self) -> None:
        with self.assertRaises(ValidationError):
            CourseCompletionRequest.model_validate(
                {**VALID_PAYLOAD, "first_name": None}
            )

    def test_null_last_name_raises(self) -> None:
        with self.assertRaises(ValidationError):
            CourseCompletionRequest.model_validate({**VALID_PAYLOAD, "last_name": None})

    def test_null_facility_raises(self) -> None:
        with self.assertRaises(ValidationError):
            CourseCompletionRequest.model_validate({**VALID_PAYLOAD, "facility": None})

    def _assert_rejected_as_blank(self, payload: dict, field: str) -> None:
        """Asserts |payload| fails validation on |field| and nothing else.

        A blank name would otherwise fail the name check as a mismatch,
        reporting identifier drift for a request that simply omitted a value.
        """
        with self.assertRaises(ValidationError) as cm:
            CourseCompletionRequest.model_validate(payload)
        errors = cm.exception.errors()
        self.assertEqual(1, len(errors))
        self.assertEqual((field,), errors[0]["loc"])

    def test_empty_first_name_raises(self) -> None:
        self._assert_rejected_as_blank(
            {**VALID_PAYLOAD, "first_name": ""}, "first_name"
        )

    def test_whitespace_first_name_raises(self) -> None:
        self._assert_rejected_as_blank(
            {**VALID_PAYLOAD, "first_name": "   "}, "first_name"
        )

    def test_empty_last_name_raises(self) -> None:
        self._assert_rejected_as_blank({**VALID_PAYLOAD, "last_name": ""}, "last_name")

    def test_whitespace_last_name_raises(self) -> None:
        self._assert_rejected_as_blank(
            {**VALID_PAYLOAD, "last_name": "   "}, "last_name"
        )

    def test_empty_facility_raises(self) -> None:
        self._assert_rejected_as_blank({**VALID_PAYLOAD, "facility": ""}, "facility")

    def test_whitespace_facility_raises(self) -> None:
        self._assert_rejected_as_blank({**VALID_PAYLOAD, "facility": "   "}, "facility")

    def test_name_is_preserved_verbatim(self) -> None:
        """Normalization is a comparison-time concern; what Edovo sent is what
        we persist."""
        req = CourseCompletionRequest.model_validate(
            {**VALID_PAYLOAD, "first_name": "  Jane ", "last_name": "O'Brien-Doe"}
        )
        self.assertEqual("  Jane ", req.first_name)
        self.assertEqual("O'Brien-Doe", req.last_name)


class TestResponseModels(TestCase):
    """Tests for the course-completion response models and their serialized shapes."""

    def test_accepted_response(self) -> None:
        resp = CourseCompletionAcceptedResponse(completion_id="rec_abc123")
        self.assertEqual(resp.status, "accepted")
        self.assertEqual(resp.completion_id, "rec_abc123")
        self.assertEqual(resp.message, "Course completion recorded.")

    def test_duplicate_response(self) -> None:
        resp = CourseCompletionDuplicateResponse(
            completion_id="rec_abc123", originally_received_at=_RECEIVED_AT
        )
        self.assertEqual(resp.status, "duplicate")
        self.assertEqual(resp.completion_id, "rec_abc123")
        self.assertEqual(resp.originally_received_at, _RECEIVED_AT)
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
        resp = CourseCompletionAlreadyCompletedResponse(
            completion_id="rec_abc123", originally_received_at=_RECEIVED_AT
        )
        self.assertEqual(resp.status, "error")
        self.assertEqual(resp.error_code, "ALREADY_COMPLETED")
        self.assertEqual(resp.completion_id, "rec_abc123")
        self.assertEqual(resp.originally_received_at, _RECEIVED_AT)
        self.assertEqual(
            resp.message, "This person has already received credit for this course."
        )

    def test_person_name_mismatch_response(self) -> None:
        resp = CourseCompletionPersonNameMismatchResponse(
            message=(
                "The provided person_external_id belongs to a person with a "
                "different name in our records."
            ),
            mismatched_fields=["last_name"],
        )
        self.assertEqual(resp.status, "error")
        self.assertEqual(resp.error_code, "PERSON_NAME_MISMATCH")
        self.assertEqual(["last_name"], resp.mismatched_fields)
