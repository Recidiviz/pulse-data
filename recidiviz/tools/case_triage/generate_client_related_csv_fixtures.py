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
"""Generates fixtures for models related to client models, where data across them needs to match.

This is highly specific to the current version of the fixtures. The output destination
is intentional, so we do not offer command-line arguments to write to a different directory.

python -m recidiviz.tools.case_triage.generate_client_related_csv_fixtures"""

import csv
import json
import uuid
from typing import Any, Dict

from recidiviz.case_triage.client_event.types import ClientEventType
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactType,
)
from recidiviz.tools.case_triage.common import csv_row_to_etl_client_json

namespace_uuid = uuid.UUID("797a2210-e739-4477-8f9b-5221dc9683cf")


def _get_event_id(event: Dict[str, Any]) -> uuid.UUID:
    """returns a consistent UUID based on event data to limit diff noise"""
    return uuid.uuid3(namespace_uuid, json.dumps(event))


def generate_client_events_fixture() -> None:
    """Generates events corresponding to contact and assessment data in client fixture
    and writes them to a CSV fixture file."""

    events = []
    with open(
        "./recidiviz/tools/case_triage/fixtures/etl_clients.csv", encoding="utf-8"
    ) as csvfile:
        clients = csv.reader(csvfile)
        for index, row in enumerate(clients):
            client_data = csv_row_to_etl_client_json(row)

            event_base = {
                "exported_at": None,
                "person_external_id": client_data["person_external_id"],
                "state_code": client_data["state_code"],
            }

            if client_data["most_recent_face_to_face_date"] is not None:
                contact_event = {
                    "event_type": ClientEventType.CONTACT.value,
                    "event_date": client_data[
                        "most_recent_face_to_face_date"
                    ].isoformat(),
                    "event_metadata": json.dumps(
                        {
                            "contact_type": StateSupervisionContactType.FACE_TO_FACE.value,
                            "location": StateSupervisionContactLocation.RESIDENCE.value
                            if index % 2
                            else StateSupervisionContactLocation.SUPERVISION_OFFICE.value,
                        }
                    ),
                }
                contact_event.update(event_base)
                contact_event["event_id"] = _get_event_id(contact_event)
                events.append(contact_event)

            if client_data["most_recent_assessment_date"] is not None:
                score_diff = 5 if index % 2 else -2
                assessment_event = {
                    "event_type": ClientEventType.ASSESSMENT.value,
                    "event_date": client_data[
                        "most_recent_assessment_date"
                    ].isoformat(),
                    "event_metadata": json.dumps(
                        {
                            "assessment_score": client_data["assessment_score"],
                            "previous_assessment_score": max(
                                client_data["assessment_score"] + score_diff, 0
                            ),
                        }
                    ),
                }
                assessment_event.update(event_base)
                assessment_event["event_id"] = _get_event_id(assessment_event)
                events.append(assessment_event)

    with open(
        "./recidiviz/tools/case_triage/fixtures/etl_client_events.csv",
        "w",
        encoding="utf-8",
    ) as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "exported_at",
                "event_id",
                "person_external_id",
                "state_code",
                "event_type",
                "event_date",
                "event_metadata",
            ],
        )
        writer.writerows(events)


if __name__ == "__main__":
    generate_client_events_fixture()
