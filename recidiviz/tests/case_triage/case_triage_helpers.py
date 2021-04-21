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
"""Implements helper functions for use in Case Triage tests."""
import json
from datetime import date, datetime
from typing import Optional, Dict

from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.persistence.database.schema.case_triage.schema import (
    ETLClient,
    ETLOfficer,
    ETLOpportunity,
    CaseUpdate,
)


def generate_fake_officer(
    officer_id: str, email: str = "nonexistent_email.com"
) -> ETLOfficer:
    return ETLOfficer(
        external_id=officer_id,
        email_address=email,
        state_code="US_XX",
        given_names="Test",
        surname="Officer",
    )


def generate_fake_client(
    client_id: str,
    supervising_officer_id: str = "id_1",
    last_assessment_date: Optional[date] = None,
    last_face_to_face_date: Optional[date] = None,
) -> ETLClient:
    return ETLClient(
        person_external_id=client_id,
        full_name=json.dumps({"given_name": "TEST NAME"}),
        supervising_officer_external_id=supervising_officer_id,
        supervision_type="PAROLE",
        case_type="GENERAL",
        supervision_level="MEDIUM",
        state_code="US_XX",
        supervision_start_date=date(2018, 1, 1),
        last_known_date_of_employment=date(2018, 2, 1),
        most_recent_assessment_date=last_assessment_date,
        assessment_score=1,
        most_recent_face_to_face_date=last_face_to_face_date,
    )


def generate_fake_opportunity(
    officer_id: str,
    person_external_id: str = "person_id_1",
) -> ETLOpportunity:
    return ETLOpportunity(
        supervising_officer_external_id=officer_id,
        person_external_id=person_external_id,
        state_code="US_XX",
        opportunity_type="OVERDUE_DISCHARGE",
        opportunity_metadata={},
    )


def generate_fake_case_update(
    client: ETLClient,
    officer: ETLOfficer,
    action_type: CaseUpdateActionType,
    action_ts: Optional[datetime] = None,
    comment: Optional[str] = None,
    last_version: Optional[Dict] = None,
) -> CaseUpdate:
    action_ts = datetime.now() if action_ts is None else action_ts
    last_version = {} if last_version is None else last_version

    return CaseUpdate(
        state_code=client.state_code,
        etl_client=client,
        person_external_id=client.person_external_id,
        officer_external_id=str(officer.external_id),
        action_type=action_type.value,
        action_ts=action_ts,
        comment=comment,
        last_version=last_version,
    )
