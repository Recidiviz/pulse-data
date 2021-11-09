# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Implements tests for the ClientEventPresenter class."""
from datetime import date
from unittest.case import TestCase

from recidiviz.case_triage.client_event.types import ClientEventType
from recidiviz.case_triage.querier.client_event_presenter import ClientEventPresenter
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactMethod,
    StateSupervisionContactType,
)
from recidiviz.persistence.database.schema.case_triage.schema import ETLClientEvent


class TestClientEventPresenter(TestCase):
    """Implements tests for the ClientEventPresenter class."""

    def setUp(self) -> None:
        self.event_date = date(2020, 3, 15)

    def test_assessment_score(self) -> None:
        """Score and calculated difference from previous"""

        event = ETLClientEvent(
            state_code="US_XX",
            person_external_id="abc123",
            event_date=self.event_date,
            event_type=ClientEventType.ASSESSMENT.value,
            event_metadata={
                "assessment_score": 5,
                "previous_assessment_score": 10,
            },
        )

        presenter = ClientEventPresenter(event)
        self.assertEqual(
            presenter.to_json(),
            {
                "eventType": ClientEventType.ASSESSMENT.value,
                "eventDate": "2020-03-15",
                "eventMetadata": {"score": 5, "scoreChange": -5},
            },
        )

        event.event_metadata = {
            "assessment_score": 10,
            "previous_assessment_score": 10,
        }

        self.assertEqual(
            presenter.to_json(),
            {
                "eventType": ClientEventType.ASSESSMENT.value,
                "eventDate": "2020-03-15",
                "eventMetadata": {"score": 10, "scoreChange": 0},
            },
        )

        event.event_metadata["previous_assessment_score"] = 4

        self.assertEqual(
            presenter.to_json(),
            {
                "eventType": ClientEventType.ASSESSMENT.value,
                "eventDate": "2020-03-15",
                "eventMetadata": {"score": 10, "scoreChange": 6},
            },
        )

        event.event_metadata["previous_assessment_score"] = None
        self.assertEqual(
            presenter.to_json(),
            {
                "eventType": ClientEventType.ASSESSMENT.value,
                "eventDate": "2020-03-15",
                "eventMetadata": {"score": 10, "scoreChange": None},
            },
        )

    def test_contact_type(self) -> None:
        """Face-to-face and virtual are expected"""
        event = ETLClientEvent(
            state_code="US_XX",
            person_external_id="abc123",
            event_date=self.event_date,
            event_type=ClientEventType.CONTACT.value,
            event_metadata={
                "contact_type": StateSupervisionContactType.DIRECT.value,
                "contact_method": StateSupervisionContactMethod.VIRTUAL.value,
                "location": StateSupervisionContactLocation.RESIDENCE.value,
            },
        )

        presenter = ClientEventPresenter(event)
        self.assertEqual(
            presenter.to_json(),
            {
                "eventType": ClientEventType.CONTACT.value,
                "eventDate": "2020-03-15",
                "eventMetadata": {
                    "contactType": StateSupervisionContactType.DIRECT.value,
                    "contactMethod": StateSupervisionContactMethod.VIRTUAL.value,
                },
            },
        )

        event.event_metadata[
            "location"
        ] = StateSupervisionContactLocation.SUPERVISION_OFFICE.value

        self.assertEqual(
            presenter.to_json()["eventMetadata"]["contactType"],
            StateSupervisionContactType.DIRECT.value,
        )

        self.assertEqual(
            presenter.to_json()["eventMetadata"]["contactMethod"],
            StateSupervisionContactMethod.VIRTUAL.value,
        )
