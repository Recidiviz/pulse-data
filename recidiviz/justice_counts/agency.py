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
"""Interface for working with the Agency model."""

from typing import List

from sqlalchemy.orm import Session

from recidiviz.persistence.database.schema.justice_counts.schema import Agency


class AgencyInterface:
    """Contains methods for setting and getting Agency info."""

    @staticmethod
    def create_agency(session: Session, name: str) -> None:
        session.add(Agency(name=name))

    @staticmethod
    def get_agency_by_name(session: Session, name: str) -> Agency:
        return session.query(Agency).filter(Agency.name == name).one_or_none()

    @staticmethod
    def get_agencies(session: Session) -> List[Agency]:
        return session.query(Agency).all()
