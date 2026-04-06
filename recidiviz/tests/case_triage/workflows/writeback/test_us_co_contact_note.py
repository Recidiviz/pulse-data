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
"""Tests for the US_CO contact note writeback stub and discriminated union."""
from datetime import datetime
from typing import Annotated, Union
from unittest import TestCase

from pydantic import Discriminator, TypeAdapter, ValidationError

from recidiviz.case_triage.workflows.writeback.us_co_contact_note import (
    UsCoContactNoteRequestData,
    UsCoContactNoteWritebackExecutor,
)
from recidiviz.case_triage.workflows.writeback.us_tn_contact_note import (
    UsTnContactNoteRequestData,
)

ContactNoteRequest = Annotated[
    Union[UsTnContactNoteRequestData, UsCoContactNoteRequestData],
    Discriminator("state_code"),
]
_contact_note_adapter: TypeAdapter = TypeAdapter(ContactNoteRequest)


class TestUsCoContactNoteRequestData(TestCase):
    """Tests for UsCoContactNoteRequestData pydantic model."""

    def test_valid_request(self) -> None:
        data = UsCoContactNoteRequestData.model_validate(
            {
                "personExternalId": "456",
                "personExternalIdType": "US_CO_OFFENDERID",
                "staffId": "789",
                "staffIdType": "US_CO_DOC_BADGE_NUMBER",
                "contactNoteDateTime": "2026-01-15T10:00:00",
                "noteBody": "Routine check-in with client.",
            }
        )
        self.assertEqual(data.person_external_id, "456")
        self.assertEqual(data.staff_id, "789")
        self.assertEqual(data.note_body, "Routine check-in with client.")

    def test_missing_required_field(self) -> None:
        with self.assertRaises(ValidationError):
            UsCoContactNoteRequestData.model_validate(
                {
                    "personExternalId": "456",
                    "personExternalIdType": "US_CO_OFFENDERID",
                    "staffId": "789",
                    "staffIdType": "US_CO_DOC_BADGE_NUMBER",
                }
            )


class TestUsCoContactNoteWritebackExecutor(TestCase):
    """Tests for UsCoContactNoteWritebackExecutor stub."""

    def test_execute_raises_not_implemented(self) -> None:
        request_data = UsCoContactNoteRequestData(
            person_external_id="456",
            person_external_id_type="US_CO_OFFENDERID",
            staff_id="789",
            staff_id_type="US_CO_DOC_BADGE_NUMBER",
            contact_note_date_time=datetime(2026, 1, 15, 10, 0, 0),
            note_body="some note",
        )
        executor = UsCoContactNoteWritebackExecutor(request_data)
        with self.assertRaises(NotImplementedError):
            executor.execute()

    def test_for_request(self) -> None:
        request_data = UsCoContactNoteRequestData(
            person_external_id="456",
            person_external_id_type="US_CO_OFFENDERID",
            staff_id="789",
            staff_id_type="US_CO_DOC_BADGE_NUMBER",
            contact_note_date_time=datetime(2026, 1, 15, 10, 0, 0),
            note_body="some note",
        )
        executor = UsCoContactNoteWritebackExecutor.for_request(request_data)
        self.assertIsInstance(executor, UsCoContactNoteWritebackExecutor)


class TestContactNoteDiscriminatedUnion(TestCase):
    """Tests for the discriminated union dispatch via state_code."""

    def test_dispatches_to_tn(self) -> None:
        data = _contact_note_adapter.validate_python(
            {
                "stateCode": "US_TN",
                "personExternalId": "123",
                "personExternalIdType": "US_TN_DOC",
                "staffId": "456",
                "staffIdType": "US_TN_STAFF_TOMIS",
                "contactNoteDateTime": "2026-01-15T10:00:00",
                "contactTypeCode": "TEPE",
                "contactNote": {"1": ["line 1"]},
            }
        )
        self.assertIsInstance(data, UsTnContactNoteRequestData)
        self.assertEqual(data.state_code, "US_TN")

    def test_dispatches_to_co(self) -> None:
        data = _contact_note_adapter.validate_python(
            {
                "stateCode": "US_CO",
                "personExternalId": "456",
                "personExternalIdType": "US_CO_OFFENDERID",
                "staffId": "789",
                "staffIdType": "US_CO_DOC_BADGE_NUMBER",
                "contactNoteDateTime": "2026-01-15T10:00:00",
                "noteBody": "some note",
            }
        )
        self.assertIsInstance(data, UsCoContactNoteRequestData)
        self.assertEqual(data.state_code, "US_CO")

    def test_unknown_state_code_raises(self) -> None:
        with self.assertRaises(ValidationError):
            _contact_note_adapter.validate_python(
                {
                    "stateCode": "US_XX",
                    "personExternalId": "123",
                    "staffId": "456",
                    "contactNoteDateTime": "2026-01-15T10:00:00",
                    "noteBody": "some note",
                }
            )

    def test_missing_state_code_raises(self) -> None:
        with self.assertRaises(ValidationError):
            _contact_note_adapter.validate_python(
                {
                    "personExternalId": "123",
                    "staffId": "456",
                    "contactNoteDateTime": "2026-01-15T10:00:00",
                    "noteBody": "some note",
                }
            )
