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
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy.orm import Session, joinedload, selectinload

from recidiviz.justice_counts.report import ReportInterface
from recidiviz.persistence.database.schema.justice_counts import schema


class AgencyInterface:
    """Contains methods for setting and getting Agency info."""

    @staticmethod
    def create_or_update_agency(
        session: Session,
        name: str,
        systems: List[schema.System],
        state_code: str,
        fips_county_code: Optional[str],
        user_account_id: Optional[int] = None,
        is_superagency: Optional[bool] = None,
        super_agency_id: Optional[int] = None,
    ) -> schema.Agency:
        """If there is an existing agency in our DB with this name,
        then update their metadata. Else, create a new agency
        in our DB with this name and metadata.
        """

        existing_agency = AgencyInterface.get_agency_by_name(session=session, name=name)

        if existing_agency is not None:
            existing_agency.systems = [system.value for system in systems]
            existing_agency.state_code = state_code
            if fips_county_code is not None:
                existing_agency.fips_county_code = fips_county_code
            if is_superagency is not None:
                existing_agency.is_superagency = is_superagency
            if super_agency_id is not None:
                existing_agency.super_agency_id = super_agency_id
            session.commit()
            return existing_agency

        agency = schema.Agency(
            name=name,
            systems=[system.value for system in systems],
            state_code=state_code,
            fips_county_code=fips_county_code,
            is_superagency=is_superagency,
            super_agency_id=super_agency_id,
            created_at=datetime.now(tz=timezone.utc),
        )

        session.add(agency)
        session.commit()
        session.refresh(agency)
        ReportInterface.create_reports_for_new_agency(
            session=session, agency_id=agency.id, user_account_id=user_account_id
        )
        return agency

    @staticmethod
    def get_agency_by_id(session: Session, agency_id: int) -> schema.Agency:
        return session.query(schema.Agency).filter(schema.Agency.id == agency_id).one()

    @staticmethod
    def get_agencies_by_id(
        session: Session, agency_ids: List[int], raise_on_missing: bool = False
    ) -> List[schema.Agency]:
        agencies = (
            session.query(schema.Agency)
            .filter(schema.Agency.id.in_(agency_ids))
            # eagerly load the users in this agency
            .options(
                selectinload(schema.Agency.user_account_assocs).joinedload(
                    schema.AgencyUserAccountAssociation.user_account
                )
            )
            # eagerly load the agency settings
            .options(selectinload(schema.Agency.agency_settings))
            .all()
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
    def get_agency_by_name(
        session: Session, name: str, with_settings: bool = False
    ) -> schema.Agency:
        # ilike is case insensitive
        q = session.query(schema.Agency).filter(schema.Agency.name.ilike(name))
        if with_settings:
            q = q.options(joinedload(schema.Agency.agency_settings))
        return q.one_or_none()

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
    def get_agencies(
        session: Session, with_users: bool = False, with_settings: bool = False
    ) -> List[schema.Agency]:
        q = session.query(schema.Agency)
        # eagerly load the users in this agency
        if with_users:
            q = q.options(
                selectinload(schema.Agency.user_account_assocs).joinedload(
                    schema.AgencyUserAccountAssociation.user_account
                )
            )
        if with_settings:
            # eagerly load the agency settings
            q = q.options(selectinload(schema.Agency.agency_settings))
        return q.all()

    @staticmethod
    def get_agency_ids(session: Session) -> List[int]:
        # returns a list of one-tuples
        return [tup[0] for tup in session.query(schema.Agency.id).all()]

    @staticmethod
    def update_agency_systems(
        session: Session, agency_id: int, systems: List[str]
    ) -> None:
        agency = AgencyInterface.get_agency_by_id(session=session, agency_id=agency_id)
        systems_enums = {schema.System[s] for s in systems}
        if (
            schema.System.supervision_subsystems().intersection(systems_enums)
            and schema.System.SUPERVISION not in systems
        ):
            # If the list of systems includes a Supervision subsystem,
            # make sure the agency also belongs to the Supervision system too
            systems_enums.add(schema.System.SUPERVISION)

        agency.systems = [s.value for s in systems_enums]
        session.add(agency)

    @staticmethod
    def update_agency_name(session: Session, agency_id: int, name: str) -> None:
        agency = AgencyInterface.get_agency_by_id(session=session, agency_id=agency_id)
        agency.name = name
        session.add(agency)

    @staticmethod
    def update_is_superagency(
        session: Session, agency_id: int, is_superagency: bool
    ) -> schema.Agency:
        agency = AgencyInterface.get_agency_by_id(session=session, agency_id=agency_id)
        agency.is_superagency = is_superagency
        session.add(agency)
        return agency

    @staticmethod
    def get_child_agencies_for_agency(
        session: Session, agency: schema.Agency
    ) -> List[schema.Agency]:
        if agency.is_superagency is False:
            return []

        return (
            session.query(schema.Agency)
            .filter(schema.Agency.super_agency_id == agency.id)
            .all()
        )

    @staticmethod
    def get_child_agency_ids_for_agency(
        session: Session, agency: schema.Agency
    ) -> List[int]:
        if agency.is_superagency is False:
            return []

        # Extract the agency IDs from the tuples
        child_agency_ids = [
            id[0]
            for id in session.query(schema.Agency.id)
            .filter(schema.Agency.super_agency_id == agency.id)
            .all()
        ]
        return child_agency_ids

    @staticmethod
    def get_child_agencies_by_agency_ids(
        session: Session, agency_ids: List[int]
    ) -> List[schema.Agency]:
        return (
            session.query(schema.Agency)
            .filter(schema.Agency.super_agency_id.in_(agency_ids))
            .all()
        )
