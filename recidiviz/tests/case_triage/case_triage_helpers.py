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
import hashlib
import json
from base64 import b64encode
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional

from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.client_info.types import PreferredContactMethod
from recidiviz.case_triage.opportunities.models import ComputedOpportunity
from recidiviz.case_triage.opportunities.types import (
    Opportunity,
    OpportunityDeferralType,
    OpportunityType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)
from recidiviz.persistence.database.schema.case_triage.schema import (
    CaseUpdate,
    ClientInfo,
    ETLClient,
    ETLOfficer,
    ETLOpportunity,
    OpportunityDeferral,
)


def hash_email(email: str) -> str:
    return b64encode(hashlib.sha256(email.encode("ascii")).digest()).decode("ascii")


def generate_fake_officer(
    officer_id: str, email: str = "nonexistent_email.com", state_code: str = "US_ID"
) -> ETLOfficer:
    return ETLOfficer(
        external_id=officer_id,
        email_address=email,
        state_code=state_code,
        given_names="Test",
        surname="Officer",
        hashed_email_address=hash_email(email),
    )


def generate_fake_client(
    client_id: str,
    *,
    supervising_officer_id: str = "id_1",
    last_assessment_date: Optional[date] = None,
    last_face_to_face_date: Optional[date] = None,
    last_home_visit_date: Optional[date] = None,
    supervision_level: StateSupervisionLevel = StateSupervisionLevel.MEDIUM,
    assessment_score: int = 1,
) -> ETLClient:
    # these can trigger opportunities if they are overdue or upcoming;
    # default them to today to prevent unexpected side effects
    if last_assessment_date is None:
        last_assessment_date = date.today()
    if last_face_to_face_date is None:
        last_face_to_face_date = date.today()
    next_recommended_assessment_date = last_assessment_date + timedelta(days=365)

    return ETLClient(
        person_external_id=client_id,
        full_name=json.dumps({"given_names": "TEST", "surname": "NAME"}),
        supervising_officer_external_id=supervising_officer_id,
        supervision_type="PAROLE",
        case_type="GENERAL",
        supervision_level=supervision_level.value,
        state_code="US_ID",
        supervision_start_date=date(2018, 1, 1),
        last_known_date_of_employment=date(2018, 2, 1),
        most_recent_assessment_date=last_assessment_date,
        next_recommended_assessment_date=next_recommended_assessment_date,
        assessment_score=assessment_score,
        most_recent_face_to_face_date=last_face_to_face_date,
        most_recent_home_visit_date=last_home_visit_date,
    )


def generate_fake_etl_opportunity(
    *,
    officer_id: str,
    person_external_id: str = "person_id_1",
    opportunity_type: OpportunityType = OpportunityType.OVERDUE_DISCHARGE,
    opportunity_metadata: Optional[Dict[str, Any]] = None,
) -> ETLOpportunity:
    if not opportunity_metadata:
        opportunity_metadata = {}
    return ETLOpportunity(
        supervising_officer_external_id=officer_id,
        person_external_id=person_external_id,
        state_code="US_ID",
        opportunity_type=opportunity_type.value,
        opportunity_metadata=opportunity_metadata,
    )


def generate_fake_computed_opportunity(
    *,
    officer_id: str,
    person_external_id: str = "person_id_1",
    opportunity_type: OpportunityType = OpportunityType.EMPLOYMENT,
    opportunity_metadata: Optional[Dict[str, Any]] = None,
) -> ComputedOpportunity:
    if not opportunity_metadata:
        opportunity_metadata = {}
    return ComputedOpportunity(
        supervising_officer_external_id=officer_id,
        person_external_id=person_external_id,
        state_code="US_ID",
        opportunity_type=opportunity_type.value,
        opportunity_metadata=opportunity_metadata,
    )


def generate_fake_reminder(
    opportunity: Opportunity,
    deferred_until: datetime = None,
) -> OpportunityDeferral:
    if deferred_until is None:
        deferred_until = datetime.now() + timedelta(days=7)

    reminder_args = {
        "state_code": "US_ID",
        "deferral_type": OpportunityDeferralType.REMINDER.value,
        "deferred_until": deferred_until,
        "reminder_was_requested": True,
        "supervising_officer_external_id": opportunity.supervising_officer_external_id,
        "person_external_id": opportunity.person_external_id,
        "opportunity_type": opportunity.opportunity_type,
        "opportunity_metadata": opportunity.opportunity_metadata,
    }

    return OpportunityDeferral(**reminder_args)


def generate_fake_case_update(
    client: ETLClient,
    officer_external_id: str,
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
        officer_external_id=officer_external_id,
        action_type=action_type.value,
        action_ts=action_ts,
        comment=comment,
        last_version=last_version,
    )


def generate_fake_client_info(
    client: ETLClient,
    preferred_name: Optional[str] = None,
    preferred_contact_method: Optional[PreferredContactMethod] = None,
) -> ClientInfo:
    preferred_contact_value = (
        None if not preferred_contact_method else preferred_contact_method.value
    )
    return ClientInfo(
        state_code=client.state_code,
        etl_client=client,
        person_external_id=client.person_external_id,
        preferred_name=preferred_name,
        preferred_contact_method=preferred_contact_value,
    )
