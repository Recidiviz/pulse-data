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

from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload, selectinload

from recidiviz.justice_counts.exceptions import JusticeCountsServerError
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
        super_agency_id: Optional[int],
        agency_id: Optional[int],
        is_dashboard_enabled: Optional[bool],
        is_superagency: Optional[bool] = None,
        with_users: bool = False,
    ) -> schema.Agency:
        """If there is an existing agency, meaning that agency_id is not None,
        the metadata is updated with the fields passed in. If there is no
        existing agency, a new agency is created with the metadata passed in.
        """

        existing_agency = (
            AgencyInterface.get_agency_by_id(
                session=session, agency_id=agency_id, with_users=with_users
            )
            if agency_id is not None
            else AgencyInterface.get_agency_by_name_state_and_systems(
                session=session,
                name=name,
                state_code=state_code,
                systems=[s.value for s in systems],
            )
        )

        # agency_id is not None for update requests, agency_id is None
        # for create requests
        if agency_id is None and existing_agency is not None:
            raise JusticeCountsServerError(
                code="agency_already_exists",
                description=f"Agency with name '{name}' already exists with the state and the systems selected.",
            )

        if existing_agency is not None:
            existing_agency.systems = [system.value for system in systems]
            existing_agency.state_code = state_code
            existing_agency.name = name
            existing_agency.super_agency_id = super_agency_id
            existing_agency.fips_county_code = fips_county_code
            existing_agency.is_dashboard_enabled = is_dashboard_enabled
            existing_agency.is_superagency = is_superagency
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
            is_dashboard_enabled=is_dashboard_enabled,
        )

        session.add(agency)
        session.commit()
        session.refresh(agency)
        ReportInterface.create_reports_for_new_agency(
            session=session,
            agency_id=agency.id,
        )
        return agency

    @staticmethod
    def get_agency_by_id(
        session: Session,
        agency_id: int,
        with_users: bool = False,
        with_settings: bool = False,
    ) -> schema.Agency:
        q = session.query(schema.Agency).filter(schema.Agency.id == agency_id)

        if with_settings is True:
            q = q.options(joinedload(schema.Agency.agency_settings))

        if with_users is True:
            q = q.options(
                selectinload(schema.Agency.user_account_assocs).joinedload(
                    schema.AgencyUserAccountAssociation.user_account
                )
            )  # eagerly load the users in this agency

        return q.one()

    @staticmethod
    def get_agencies_by_id(
        session: Session, agency_ids: List[int], raise_on_missing: bool = False
    ) -> List[schema.Agency]:
        set_agency_ids = set(agency_ids)
        agencies = (
            session.query(schema.Agency)
            .filter(schema.Agency.id.in_(set_agency_ids))
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
            missing_agency_ids = set_agency_ids.difference(found_agency_ids)
            msg = f"Could not find the following agencies: {missing_agency_ids}"
            if raise_on_missing:
                raise ValueError(msg)
            logging.warning(msg)

        return agencies

    @staticmethod
    def get_agency_by_name_state_and_systems(
        session: Session,
        name: str,
        state_code: str,
        systems: List[str],
        with_settings: bool = False,
        with_users: bool = False,
    ) -> schema.Agency:
        """
        Retrieve an agency from the database based on its name, state code, and the systems.

        Args:
            session (Session): The SQLAlchemy session.
            name (str): The name of the agency.
            state_code (str): The state code of the agency.
            systems (List[str]): A list of system names (strings, not schema.System objects).
            with_settings (bool, optional): Whether to include agency settings in the result. Defaults to False.
            with_users (bool, optional): Whether to include user accounts associated with the agency in the result. Defaults to False.

        Returns:
            schema.Agency or None: The agency matching the criteria, or None if not found.
        """

        q = session.query(schema.Agency).filter(
            schema.Agency.name == name,
            func.lower(schema.Agency.state_code) == func.lower(state_code),
            func.array_length(schema.Agency.systems, 1) == len(systems),
            # Checks for agencies that have a 1-dimensional array with the same
            # length of the systems array thats passed in
        )

        for system in systems:
            # This for-loop checks if every element in the systems array that
            # is passed in, is also in DB agency that we are querying for.
            q = q.filter(schema.Agency.systems.any(system))

        if with_settings is True:
            q = q.options(joinedload(schema.Agency.agency_settings))

        if with_users is True:
            q = q.options(
                selectinload(schema.Agency.user_account_assocs).joinedload(
                    schema.AgencyUserAccountAssociation.user_account
                )
            )  # eagerly load the users in this agency
        return q.one_or_none()

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

    @staticmethod
    def does_supervision_agency_report_for_subsystems(agency: schema.Agency) -> bool:
        """This method is used to differentiate between Supervision agencies that do not
        report for any subsystems and supervision agencies that report for subsystems.
        """
        systems_enums = {schema.System[s] for s in agency.systems}

        return (
            len(schema.System.supervision_subsystems().intersection(systems_enums)) > 0
            and schema.System.SUPERVISION in agency.systems
        )

    @staticmethod
    def update_custom_child_agency_name(
        agency: schema.Agency, custom_name: str
    ) -> Optional[schema.Agency]:
        """
        Updates the custom name for a child agency within.

        Parameters:
        ----------
        agency : schema.Agency
            The agency instance to update.

        custom_name : str
            The new custom name to assign to the child agency.

        Returns:
        -------
        Optional[schema.Agency]
            The updated `agency` instance with the new `custom_child_agency_name`,
            or `None` if the `agency` is not a child agency.
        """
        if agency.super_agency_id is None:
            # Agency is not a child agency, do nothing
            return None

        agency.custom_child_agency_name = custom_name
        return agency
