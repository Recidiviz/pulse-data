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
from unittest import TestCase

from pydantic import ValidationError

from recidiviz.case_triage.workflows.writeback.us_co_contact_note import (
    UsCoContactNoteRequestData,
    UsCoContactNoteWritebackExecutor,
)


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
