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

"""Define the ORM schema objects that map directly to the database,
for Case Triage related entities.

"""
from typing import Any, Dict

from sqlalchemy import Column, Boolean, Date, DateTime, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from recidiviz.persistence.database.base_schema import CaseTriageBase


class ETLClient(CaseTriageBase):
    """Represents a person derived from our ETL pipeline."""
    __tablename__ = 'etl_clients'
    __table_args__ = (
        UniqueConstraint('state_code', 'person_external_id'),
    )
    person_external_id = Column(String(255), nullable=False, index=True, primary_key=True)
    full_name = Column(String(255))
    supervising_officer_external_id = Column(String(255), nullable=False, index=True)
    current_address = Column(Text)
    birthdate = Column(Date)
    birthdate_inferred_from_age = Column(Boolean)
    supervision_type = Column(String(255), nullable=False)
    case_type = Column(String(255), nullable=False)
    supervision_level = Column(String(255), nullable=False)
    state_code = Column(String(255), nullable=False, index=True, primary_key=True)
    employer = Column(String(255))
    most_recent_assessment_date = Column(Date)
    most_recent_face_to_face_date = Column(Date)

    def to_json(self) -> Dict[str, Any]:
        return {
            'person_external_id': self.person_external_id,
            'full_name': self.full_name,
            'supervising_officer_external_id': self.supervising_officer_external_id,
            'current_address': self.current_address,
            'birthdate': self.birthdate,
            'birthdate_inferred_from_age': self.birthdate_inferred_from_age,
            'supervision_type': self.supervision_type,
            'case_type': self.case_type,
            'supervision_level': self.supervision_level,
            'state_code': self.state_code,
            'employer': self.employer,
            'most_recent_assessment_date': self.most_recent_assessment_date,
            'most_recent_face_to_face_date': self.most_recent_face_to_face_date,
        }


class ETLOfficer(CaseTriageBase):
    """Represents an officer derived from our ETL pipeline."""
    __tablename__ = 'etl_officers'
    external_id = Column(String(255), nullable=False, index=True, primary_key=True)
    state_code = Column(String(255), nullable=False, index=True, primary_key=True)
    email_address = Column(String(255), nullable=False)
    given_names = Column(String(255), nullable=False)
    surname = Column(String(255), nullable=False)


class UserAction(CaseTriageBase):
    """Represents an action that an officer indicates they have taken on behalf of
    a client. We only store one active row per officer/client pair.
    """
    __tablename__ = 'user_actions'

    person_external_id = Column(String(255), nullable=False, index=True, primary_key=True)
    officer_external_id = Column(String(255), nullable=False, index=True, primary_key=True)
    state_code = Column(String(255), nullable=False, index=True, primary_key=True)

    # To start, we're just keeping things in json. We will eventually migrate this to
    # some other format when we know better what we need, but for the moment we will
    # enforce schema decisions and/or migrations largely in code.
    action_metadata = Column(JSONB, nullable=False)
    action_ts = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
