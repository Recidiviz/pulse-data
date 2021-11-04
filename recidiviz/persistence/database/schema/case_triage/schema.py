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
# ============================================================================
# TODO(#7533): Rename CaseTriage schema to UnifiedProducts
"""Define the ORM schema objects that map directly to the database,
for Unified Products related entities.

"""
import uuid
from datetime import date, timedelta
from typing import Any, Dict, Optional

import dateutil.parser
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import text

from recidiviz.persistence.database.base_schema import CaseTriageBase


def _get_json_field_as_date(json_data: Dict[str, Any], field: str) -> Optional[date]:
    if not (date_field := json_data.get(field)):
        return None
    return dateutil.parser.parse(date_field).date()


def _timeshift_date_fields(
    fields: Dict[str, Any], timedelta_shift: timedelta
) -> Dict[str, Any]:
    """Applies a delta to any date values in the fields dict. Primarily useful
    in demo mode for time-shifting fixture files to the present day."""
    results = {}
    for k, v in fields.items():
        if isinstance(v, date):
            results[k] = v + timedelta_shift
        else:
            results[k] = v
    return results


class ETLDerivedEntity:
    """Encodes columns that should be found on all ETL entities."""

    exported_at = Column(DateTime(timezone=True))


class ETLClient(CaseTriageBase, ETLDerivedEntity):
    """Represents a person derived from our ETL pipeline."""

    __tablename__ = "etl_clients"
    __table_args__ = (UniqueConstraint("state_code", "person_external_id"),)
    person_external_id = Column(
        String(255), nullable=False, index=True, primary_key=True
    )
    state_code = Column(String(255), nullable=False, index=True, primary_key=True)
    supervising_officer_external_id = Column(String(255), nullable=False, index=True)

    full_name = Column(String(255))
    gender = Column(String(255))
    current_address = Column(Text)
    birthdate = Column(Date)
    supervision_start_date = Column(Date)
    projected_end_date = Column(Date)
    supervision_type = Column(String(255), nullable=False)
    case_type = Column(String(255), nullable=False)
    supervision_level = Column(String(255), nullable=False)
    employer = Column(String(255))
    employment_start_date = Column(Date)
    last_known_date_of_employment = Column(Date)
    most_recent_assessment_date = Column(Date)
    next_recommended_assessment_date = Column(Date)
    assessment_score = Column(Integer)
    most_recent_face_to_face_date = Column(Date)
    next_recommended_face_to_face_date = Column(Date)
    most_recent_home_visit_date = Column(Date)
    next_recommended_home_visit_date = Column(Date)
    most_recent_treatment_collateral_contact_date = Column(Date)
    next_recommended_treatment_collateral_contact_date = Column(Date)
    days_with_current_po = Column(Integer)
    days_on_current_supervision_level = Column(Integer)
    email_address = Column(Text)
    phone_number = Column(Text)
    most_recent_violation_date = Column(Date)

    case_updates = relationship(
        "CaseUpdate",
        uselist=True,
        foreign_keys=[state_code, person_external_id, supervising_officer_external_id],
        overlaps="client_info,notes,etl_events",
        primaryjoin="and_("
        "   ETLClient.state_code == CaseUpdate.state_code,"
        "   ETLClient.person_external_id == CaseUpdate.person_external_id,"
        "   ETLClient.supervising_officer_external_id == CaseUpdate.officer_external_id,"
        ")",
    )

    client_info = relationship(
        "ClientInfo",
        uselist=False,
        foreign_keys=[state_code, person_external_id],
        overlaps="case_updates,notes,etl_events",
        primaryjoin="and_("
        "   ETLClient.state_code == ClientInfo.state_code,"
        "   ETLClient.person_external_id == ClientInfo.person_external_id,"
        ")",
    )

    etl_officer = relationship(
        "ETLOfficer",
        uselist=False,
        primaryjoin="and_("
        "   foreign(ETLOfficer.state_code) == ETLClient.state_code,"
        "   foreign(ETLOfficer.external_id) == ETLClient.supervising_officer_external_id,"
        ")",
    )

    notes = relationship(
        "OfficerNote",
        uselist=True,
        foreign_keys=[state_code, person_external_id, supervising_officer_external_id],
        overlaps="client_info,case_updates,etl_events",
        primaryjoin="and_("
        "   ETLClient.state_code == OfficerNote.state_code,"
        "   ETLClient.person_external_id == OfficerNote.person_external_id,"
        "   ETLClient.supervising_officer_external_id == OfficerNote.officer_external_id,"
        ")",
    )

    etl_events = relationship(
        "ETLClientEvent",
        uselist=True,
        foreign_keys=[state_code, person_external_id],
        overlaps="client_info,case_updates,notes",
        primaryjoin="and_("
        "   ETLClient.state_code == ETLClientEvent.state_code,"
        "   ETLClient.person_external_id == ETLClientEvent.person_external_id,"
        ")",
        order_by="desc(ETLClientEvent.event_date)",
    )

    @property
    def receiving_ssi_or_disability_income(self) -> bool:
        return (
            self.client_info is not None
            and self.client_info.receiving_ssi_or_disability_income
        )

    @staticmethod
    def from_json(
        json_client: Dict[str, Any], timedelta_shift: Optional[timedelta] = None
    ) -> "ETLClient":
        """Constructs an ETLClient from a corresponding dict (presumed to be from JSON)"""
        client_args = {
            "person_external_id": json_client["person_external_id"],
            "state_code": json_client["state_code"],
            "supervising_officer_external_id": json_client[
                "supervising_officer_external_id"
            ],
            "full_name": json_client.get("full_name"),
            "gender": json_client.get("gender"),
            "current_address": json_client.get("current_address"),
            "birthdate": _get_json_field_as_date(json_client, "birthdate"),
            "supervision_start_date": _get_json_field_as_date(
                json_client, "supervision_start_date"
            ),
            "projected_end_date": _get_json_field_as_date(
                json_client, "projected_end_date"
            ),
            "supervision_type": json_client["supervision_type"],
            "case_type": json_client["case_type"],
            "supervision_level": json_client["supervision_level"],
            "employer": json_client.get("employer"),
            "employment_start_date": _get_json_field_as_date(
                json_client, "employment_start_date"
            ),
            "last_known_date_of_employment": _get_json_field_as_date(
                json_client, "last_known_date_of_employment"
            ),
            "most_recent_assessment_date": _get_json_field_as_date(
                json_client, "most_recent_assessment_date"
            ),
            "next_recommended_assessment_date": _get_json_field_as_date(
                json_client, "next_recommended_assessment_date"
            ),
            "assessment_score": json_client.get("assessment_score"),
            "most_recent_face_to_face_date": _get_json_field_as_date(
                json_client, "most_recent_face_to_face_date"
            ),
            "next_recommended_face_to_face_date": _get_json_field_as_date(
                json_client, "next_recommended_face_to_face_date"
            ),
            "most_recent_home_visit_date": _get_json_field_as_date(
                json_client, "most_recent_home_visit_date"
            ),
            "next_recommended_home_visit_date": _get_json_field_as_date(
                json_client, "next_recommended_home_visit_date"
            ),
            "most_recent_violation_date": _get_json_field_as_date(
                json_client, "most_recent_violation_date"
            ),
            "days_with_current_po": json_client.get("days_with_current_po"),
            "email_address": json_client.get("email_address"),
            "days_on_current_supervision_level": json_client.get(
                "days_on_current_supervision_level"
            ),
            "phone_number": json_client.get("phone_number"),
            "exported_at": json_client.get("exported_at"),
        }

        if timedelta_shift:
            client_args = _timeshift_date_fields(client_args, timedelta_shift)

        return ETLClient(**client_args)


class ETLOfficer(CaseTriageBase, ETLDerivedEntity):
    """Represents an officer derived from our ETL pipeline."""

    __tablename__ = "etl_officers"
    external_id = Column(String(255), nullable=False, index=True, primary_key=True)
    state_code = Column(String(255), nullable=False, index=True, primary_key=True)
    email_address = Column(String(255), nullable=False)
    given_names = Column(String(255), nullable=False)
    surname = Column(String(255), nullable=False)
    hashed_email_address = Column(Text, nullable=True)


class ETLOpportunity(CaseTriageBase, ETLDerivedEntity):
    """Represents an "opportunity" derived from our ETL pipeline."""

    __tablename__ = "etl_opportunities"
    state_code = Column(String(255), nullable=False, index=True, primary_key=True)
    supervising_officer_external_id = Column(
        String(255), nullable=False, index=True, primary_key=True
    )
    person_external_id = Column(
        String(255), nullable=False, index=True, primary_key=True
    )
    opportunity_type = Column(String(255), nullable=False, index=True, primary_key=True)
    opportunity_metadata = Column(JSONB, nullable=False)

    @staticmethod
    def from_json(json_opportunity: Dict[str, Any]) -> "ETLOpportunity":
        return ETLOpportunity(
            person_external_id=json_opportunity["person_external_id"],
            state_code=json_opportunity["state_code"],
            supervising_officer_external_id=json_opportunity[
                "supervising_officer_external_id"
            ],
            opportunity_type=json_opportunity["opportunity_type"],
            opportunity_metadata=json_opportunity["opportunity_metadata"],
            exported_at=json_opportunity.get("exported_at"),
        )


class ETLClientEvent(CaseTriageBase, ETLDerivedEntity):
    """Represents a supervision-related event derived from our ETL pipeline."""

    __tablename__ = "etl_client_events"

    event_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        # need this because events are loaded directly to the DB via ETL without IDs
        server_default=text("gen_random_uuid()"),
    )

    person_external_id = Column(String(255), nullable=False, index=True)
    state_code = Column(String(255), nullable=False, index=True)
    event_type = Column(String(255), nullable=False)
    event_date = Column(Date, nullable=False)
    event_metadata = Column(JSONB, nullable=False)

    @staticmethod
    def from_json(
        json_event: Dict[str, Any], timedelta_shift: Optional[timedelta] = None
    ) -> "ETLClientEvent":
        """Constructs an ETLClientEvent from a corresponding dict (presumed to be from JSON)"""
        event_args = {
            "person_external_id": json_event["person_external_id"],
            "state_code": json_event["state_code"],
            "event_type": json_event["event_type"],
            "event_date": _get_json_field_as_date(json_event, "event_date"),
            "event_metadata": json_event["event_metadata"],
        }
        if timedelta_shift:
            event_args = _timeshift_date_fields(event_args, timedelta_shift)

        return ETLClientEvent(**event_args)


class CaseUpdate(CaseTriageBase):
    """Represents an update to a parole officer's case based on actions that an officer
    indicates they have taken on behalf of a client. We store one active row per
    officer/client/action-type triple.

    Each row represents the most recent set of actions taken by a PO to move the client
    from an "active" to "in-progress" state. It does _not_ store or encode a historical log
    of all actions ever taken.

    We decided to structure it this way because these CaseUpdateActions are meant to provide
    a filter on the accuracy of the data surrounding clients that we receive through our ETL
    pipeline. The ETL-derived data should always be eventually accurate and this is meant to
    help correct that information when our pipeline is behind reality.
    """

    __tablename__ = "case_update_actions"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "person_external_id",
            "officer_external_id",
            "action_type",
            name="unique_person_officer_action_triple",
        ),
    )

    update_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    person_external_id = Column(String(255), nullable=False, index=True)
    officer_external_id = Column(String(255), nullable=False, index=True)
    state_code = Column(String(255), nullable=False, index=True)
    action_type = Column(String(255), nullable=False, index=True)

    action_ts = Column(DateTime, nullable=False, server_default=func.now())
    # Contains a dictionary of metadata of the last known version where this action
    # applied. The specific keys and value types are determined based on `action_type`
    last_version = Column(JSONB, nullable=False)
    comment = Column(Text)

    etl_client = relationship(
        "ETLClient",
        foreign_keys=[state_code, person_external_id, officer_external_id],
        primaryjoin="and_("
        "   ETLClient.state_code == CaseUpdate.state_code,"
        "   ETLClient.person_external_id == CaseUpdate.person_external_id,"
        "   ETLClient.supervising_officer_external_id == CaseUpdate.officer_external_id,"
        ")",
    )


class ClientInfo(CaseTriageBase):
    """We use ClientInfo to encode additional metadata provided by POs about
    their client. This information is only available in the Case Triage tool
    and is not otherwise derivable from our ETL pipeline."""

    __tablename__ = "client_info"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "person_external_id",
            name="unique_person",
        ),
    )

    client_info_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    state_code = Column(String(255), nullable=False, index=True)
    person_external_id = Column(String(255), nullable=False, index=True)

    preferred_name = Column(Text)
    preferred_contact_method = Column(
        Enum("EMAIL", "CALL", "TEXT", name="client_into_preferred_contact_method")
    )
    receiving_ssi_or_disability_income = Column(Boolean, nullable=False, default=False)

    etl_client = relationship(
        "ETLClient",
        foreign_keys=[state_code, person_external_id],
        viewonly=True,
        primaryjoin="and_("
        "   ETLClient.state_code == ClientInfo.state_code,"
        "   ETLClient.person_external_id == ClientInfo.person_external_id,"
        ")",
    )


class OfficerNote(CaseTriageBase):
    """This table contains notes left by officers about people on their caseload."""

    __tablename__ = "officer_notes"

    note_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    state_code = Column(String(255), nullable=False, index=True)
    officer_external_id = Column(String(255), nullable=False, index=True)
    person_external_id = Column(String(255), nullable=False, index=True)

    text = Column(Text, nullable=False)

    created_datetime = Column(DateTime, nullable=False, server_default=func.now())
    updated_datetime = Column(DateTime, nullable=False, server_default=func.now())
    resolved_datetime = Column(DateTime)

    def to_json(self) -> Dict[str, Any]:
        return {
            "noteId": self.note_id,
            "text": self.text,
            "createdDatetime": self.created_datetime,
            "updatedDatetime": self.updated_datetime,
            "resolved": self.resolved_datetime is not None,
        }


class OpportunityDeferral(CaseTriageBase):
    """Represents an "opportunity" that has been deferred by a PO to be acted on at
    a later time. This includes both opportunities that a PO wants to be reminded about
    and ones that they think are not applicable."""

    __tablename__ = "opportunity_deferrals"
    __table_args__ = (
        UniqueConstraint(
            "state_code",
            "person_external_id",
            "supervising_officer_external_id",
            "opportunity_type",
            name="unique_person_officer_opportunity_triple",
        ),
    )

    deferral_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    state_code = Column(String(255), nullable=False, index=True)
    supervising_officer_external_id = Column(String(255), nullable=False, index=True)
    person_external_id = Column(String(255), nullable=False, index=True)
    opportunity_type = Column(String(255), nullable=False, index=True)

    deferral_type = Column(String(255), nullable=False)
    deferred_at = Column(DateTime, nullable=False, server_default=func.now())
    deferred_until = Column(DateTime, nullable=False)
    reminder_was_requested = Column(Boolean, nullable=False)
    opportunity_metadata = Column(JSONB, nullable=False)

    def to_json(self) -> Dict[str, Any]:
        return {
            "deferralId": self.deferral_id,
            "deferralType": self.deferral_type,
            "deferredUntil": self.deferred_until,
        }


class DashboardUserRestrictions(CaseTriageBase):
    """Represents a user's access restrictions for Unified Product dashboards."""

    __tablename__ = "dashboard_user_restrictions"
    __table_args__ = (UniqueConstraint("state_code", "restricted_user_email"),)
    state_code = Column(
        String(255), nullable=False, index=True, comment="Dashboard user's state code"
    )
    restricted_user_email = Column(
        String(255),
        nullable=False,
        index=True,
        primary_key=True,
        comment="Dashboard user's email address",
    )
    allowed_supervision_location_ids = Column(
        String(255),
        nullable=False,
        comment="String array of supervision location IDs the user can access",
    )
    allowed_supervision_location_level = Column(
        String(255),
        nullable=True,
        comment="The supervision location level, i.e. level_1_supervision_location",
    )
    internal_role = Column(
        String(255), nullable=False, comment="Dashboard user's access level role"
    )
    can_access_leadership_dashboard = Column(
        Boolean,
        nullable=False,
        default=False,
        comment="User has permission to access the Leadership Dashboard",
    )
    can_access_case_triage = Column(
        Boolean,
        nullable=False,
        default=False,
        comment="User has permission to access Case Triage",
    )
    routes = Column(
        JSONB,
        nullable=True,
        comment="Page level restrictions for the user",
    )


class OfficerMetadata(CaseTriageBase):
    """This model is used when data is captured as the officer engages with the application."""

    __tablename__ = "officer_metadata"
    state_code = Column(String(255), nullable=False, index=True, primary_key=True)
    officer_external_id = Column(
        String(255), nullable=False, index=True, primary_key=True
    )
    has_seen_onboarding = Column(Boolean, nullable=False, default=False)

    @classmethod
    def from_officer(cls, officer: ETLOfficer) -> "OfficerMetadata":
        return cls(
            state_code=officer.state_code,
            officer_external_id=officer.external_id,
            has_seen_onboarding=False,
        )
