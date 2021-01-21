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
from sqlalchemy import Column, Boolean, Date, String, UniqueConstraint, Text

from recidiviz.persistence.database.base_schema import CaseTriageBase


class Client(CaseTriageBase):
    """Represents a person in the SQL schema"""
    __tablename__ = 'etl_clients'
    __table_args__ = (
        UniqueConstraint('state_code', 'person_external_id'),
    )
    person_external_id = Column(String(255), nullable=False, index=True, primary_key=True)
    full_name = Column(String(255), primary_key=True)
    supervising_officer_external_id = Column(String(255), nullable=False, index=True)
    current_address = Column(Text)
    birthdate = Column(Date)
    birthdate_inferred_from_age = Column(Boolean)
    supervision_type = Column(String(255), nullable=False)
    case_type = Column(String(255), nullable=False)
    supervision_level = Column(String(255), nullable=False)
    state_code = Column(String(255), nullable=False, index=True)
