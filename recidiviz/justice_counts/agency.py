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

import logging
from typing import List, Optional

from sqlalchemy.orm import Session

from recidiviz.persistence.database.schema.justice_counts import schema


class AgencyInterface:
    """Contains methods for setting and getting Agency info."""

    @staticmethod
    def create_agency(
        session: Session,
        name: str,
        systems: List[schema.System],
        state_code: str,
        fips_county_code: Optional[str],
    ) -> schema.Agency:
        agency = schema.Agency(
            name=name,
            systems=[system.value for system in systems],
            state_code=state_code,
            fips_county_code=fips_county_code,
        )
        session.add(agency)
        session.commit()
        return agency

    @staticmethod
    def get_agency_by_id(session: Session, agency_id: int) -> schema.Agency:
        return session.query(schema.Agency).filter(schema.Agency.id == agency_id).one()

    @staticmethod
    def get_agencies_by_id(
        session: Session, agency_ids: List[int], raise_on_missing: bool = False
    ) -> List[schema.Agency]:
        agencies = (
            session.query(schema.Agency).filter(schema.Agency.id.in_(agency_ids)).all()
        )
        found_agency_ids = {a.id for a in agencies}
        if len(agency_ids) != len(found_agency_ids):
            missing_agency_ids = set(agency_ids).difference(found_agency_ids)
            msg = f"Could not find the following agencies: {missing_agency_ids}"
            if raise_on_missing:
                raise ValueError(msg)
            logging.warning(msg)

        return agencies

    @staticmethod
    def get_agency_by_name(session: Session, name: str) -> schema.Agency:
        return session.query(schema.Agency).filter(schema.Agency.name == name).one()

    @staticmethod
    def get_agencies_by_name(session: Session, names: List[str]) -> List[schema.Agency]:
        agencies = (
            session.query(schema.Agency).filter(schema.Agency.name.in_(names)).all()
        )
        found_agency_names = {a.name for a in agencies}
        if len(names) != len(found_agency_names):
            missing_agency_names = set(names).difference(found_agency_names)
            raise ValueError(
                f"Could not find the following agencies: {missing_agency_names}"
            )

        return agencies

    @staticmethod
    def get_agencies(session: Session) -> List[schema.Agency]:
        return session.query(schema.Agency).all()
