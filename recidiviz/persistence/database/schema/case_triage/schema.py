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
from sqlalchemy import Column, Boolean, Date, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB

from recidiviz.persistence.database.base_schema import CaseTriageBase


class ETLClient(CaseTriageBase):
    """Represents a person derived from our ETL pipeline."""
    __tablename__ = 'etl_clients'
    __table_args__ = (
        UniqueConstraint('state_code', 'person_external_id'),
    )
    person_external_id = Column(String(255), nullable=False, index=True, primary_key=True)
    state_code = Column(String(255), nullable=False, index=True, primary_key=True)
    supervising_officer_external_id = Column(String(255), nullable=False, index=True)

    full_name = Column(String(255))
    gender = Column(String(255))
    current_address = Column(Text)
    birthdate = Column(Date)
    birthdate_inferred_from_age = Column(Boolean)
    supervision_start_date = Column(Date)
    projected_end_date = Column(Date)
    supervision_type = Column(String(255), nullable=False)
    case_type = Column(String(255), nullable=False)
    supervision_level = Column(String(255), nullable=False)
    employer = Column(String(255))
    most_recent_assessment_date = Column(Date)
    assessment_score = Column(Integer)
    most_recent_face_to_face_date = Column(Date)


class ETLOfficer(CaseTriageBase):
    """Represents an officer derived from our ETL pipeline."""
    __tablename__ = 'etl_officers'
    external_id = Column(String(255), nullable=False, index=True, primary_key=True)
    state_code = Column(String(255), nullable=False, index=True, primary_key=True)
    email_address = Column(String(255), nullable=False)
    given_names = Column(String(255), nullable=False)
    surname = Column(String(255), nullable=False)


class CaseUpdate(CaseTriageBase):
    """Represents an update to a parole officer's case based on actions that an officer
    indicates they have taken on behalf of a client. We only store one active row per
    officer/client pair.

    Each row represents the most recent set of actions taken by a PO to move the client
    from an "active" to "in-progress" state. It does _not_ store or encode a historical log
    of all actions ever taken.

    We decided to structure it this way because these CaseUpdates are meant to provide a filter
    on the accuracy of the data surrounding clients that we receive through our ETL pipeline.
    The ETL-derived data should always be eventually accurate and this is meant to help
    correct that information when our pipeline is behind reality.
    """
    __tablename__ = 'case_updates'

    person_external_id = Column(String(255), nullable=False, index=True, primary_key=True)
    officer_external_id = Column(String(255), nullable=False, index=True, primary_key=True)
    state_code = Column(String(255), nullable=False, index=True, primary_key=True)

    # To start, we're just keeping things in json. We will eventually migrate this to
    # some other format when we know better what we need, but for the moment we will
    # enforce schema decisions and/or migrations largely in code.
    update_metadata = Column(JSONB, nullable=False)
