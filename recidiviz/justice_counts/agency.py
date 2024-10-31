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
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import func
from sqlalchemy.orm import Query, Session, joinedload, selectinload

from recidiviz.common.constants.states import StateCode
from recidiviz.justice_counts.control_panel.utils import is_demo_agency
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.metrics.metric_registry import METRICS_BY_SYSTEM
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.utils.constants import (
    METRIC_KEY_TO_V2_DASHBOARD_METRIC_KEY,
)
from recidiviz.persistence.database.schema.justice_counts import schema


class AgencyInterface:
    """Contains methods for setting and getting Agency info."""

    @staticmethod
    def create_or_update_agency(
        *,
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
    def get_agency_dropdown_names(
        session: Session, agency_ids: List[int]
    ) -> Dict[int, str]:
        """Returns all of a user's agencies by agency id and dropdown name - the minimal
        information needed to render the agency dropdown menu."""
        # Speed up the query by only fetching the necessary columns.
        agencies = (
            session.query(
                schema.Agency.id, schema.Agency.name, schema.Agency.state_code
            )
            .filter(schema.Agency.id.in_(agency_ids))
            .all()
        )
        # Convert agency names to dropdown names. An agency dropdown name contains the
        # two-letter state code if the state code does not appear in the agency name.
        agency_id_to_dropdown_name = {
            agency.id: (
                f"{agency.name} ({agency.state_code.split('_')[1].upper()})"
                if agency.state_code
                and agency.state_code.split("_")[1].upper() not in agency.name
                else agency.name
            )
            for agency in agencies
        }
        return agency_id_to_dropdown_name

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
    def get_reporting_agencies_by_id(
        session: Session,
        reporting_agency_ids: List[int],
        raise_on_missing: bool = False,
    ) -> List[schema.Agency]:
        set_reporting_agency_ids = set(reporting_agency_ids)
        reporting_agencies = (
            session.query(schema.Source)
            .filter(schema.Source.id.in_(set_reporting_agency_ids))
            # eagerly load the agency settings
            .options(selectinload(schema.Source.agency_settings))
            .all()
        )
        found_agency_ids = {a.id for a in reporting_agencies}
        if len(reporting_agencies) != len(found_agency_ids):
            missing_agency_ids = set_reporting_agency_ids.difference(found_agency_ids)
            msg = f"Could not find the following agencies: {missing_agency_ids}"
            if raise_on_missing:
                raise ValueError(msg)
            logging.warning(msg)

        return reporting_agencies

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
        session: Session, agency: schema.Agency, with_users: bool = False
    ) -> List[schema.Agency]:

        query = AgencyInterface.get_child_agencies_for_agency_query(
            session=session, agency=agency, with_users=with_users
        )

        if query is None:
            return []

        return query.all()

    @staticmethod
    def get_child_agencies_for_agency_query(
        session: Session, agency: schema.Agency, with_users: bool = False
    ) -> Optional[Query]:
        if agency.is_superagency is False:
            return None

        q = session.query(schema.Agency).filter(
            schema.Agency.super_agency_id == agency.id
        )

        if with_users:
            q = q.options(
                selectinload(schema.Agency.user_account_assocs).joinedload(
                    schema.AgencyUserAccountAssociation.user_account
                )
            )

        return q

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
            and schema.System.SUPERVISION.value in agency.systems
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

    @staticmethod
    def get_agencies_with_enabled_dashboard(session: Session) -> Query:
        """
        Retrieve a list of agencies that have the dashboard enabled.

        This static method queries the database for agencies where the
        'is_dashboard_enabled' attribute is set to True.

        Args:
            session (Session): The SQLAlchemy session to use for the query.

        Returns:
            List[schema.Agency]: A list of agencies with the dashboard enabled.
        """

        return session.query(schema.Agency).filter(
            schema.Agency.is_dashboard_enabled.is_(True)
        )

    @staticmethod
    def get_dashboard_homepage_json(
        agency: schema.Agency,
        fips_code_to_geoid: Dict[str, str],
        county_code_to_county_fips: Dict[str, str],
        county_code_to_county_name: Dict[str, str],
        metric_key_to_metric_interface: Dict[str, MetricInterface],
        metric_key_dim_id_to_available_members: Dict[
            str, Dict[Optional[str], Set[Optional[str]]]
        ],
    ) -> Dict[str, Any]:
        """
        Generate a JSON representation of the dashboard homepage for a given agency.

        This method constructs a dictionary containing key information about the
        specified agency, including its ID, name, available sectors, and geographic
        identifiers based on its state and county codes.

        Args:
            agency (schema.Agency): The agency object containing relevant information
            for the dashboard homepage.

        Returns:
            Dict[str, Any]: A dictionary containing the agency's metadata.
        """
        state_code = StateCode(agency.state_code.upper())
        state = state_code.get_state()
        fips_county_code = (
            county_code_to_county_fips[agency.fips_county_code.upper()]
            if agency.fips_county_code is not None
            else None
        )

        # Get all enabled metrics
        enabled_metrics = {
            metric_key
            for metric_key, metric_interface in metric_key_to_metric_interface.items()
            if metric_interface.is_metric_enabled
        }

        available_sectors = []
        available_metric_keys = []
        # Prepare available disaggregations: remove None keys and convert sets to lists
        available_disaggregations: Dict[str, Dict[str, List[str]]] = defaultdict(dict)

        for system in schema.System.sort(
            systems=[schema.System[system] for system in agency.systems]
        ):
            metrics = METRICS_BY_SYSTEM[system.name]
            available_metric_keys_for_sector = []
            for metric in metrics:
                if metric.key not in enabled_metrics:
                    # If metric is not enabled, don't add it to available metrics
                    continue

                if metric.key not in metric_key_dim_id_to_available_members:
                    # If metric has no non-null published datapoints, don't add it to available metrics
                    continue

                available_metric_keys_for_sector.append(
                    METRIC_KEY_TO_V2_DASHBOARD_METRIC_KEY.get(metric.key, metric.key)
                )

                metric_interface = metric_key_to_metric_interface[metric.key]
                dim_id_to_dimension_to_enabled_status = {
                    dim.dimension_identifier(): dim.dimension_to_enabled_status
                    for dim in metric_interface.aggregated_dimensions
                }
                dim_id_to_available_members = metric_key_dim_id_to_available_members[
                    metric.key
                ]

                for aggregated_dimension in metric.aggregated_dimensions or []:
                    dimension_to_enabled_status = dim_id_to_dimension_to_enabled_status[
                        aggregated_dimension.dimension.dimension_identifier()
                    ]

                    if dimension_to_enabled_status is None:
                        continue

                    available_members = dim_id_to_available_members.get(
                        aggregated_dimension.dimension.dimension_identifier(), set()
                    )
                    available_members_list: List[str] = [
                        member.name
                        for member in aggregated_dimension.dimension  # type: ignore[attr-defined]
                        if member.name in available_members
                        and dimension_to_enabled_status[member] is True
                    ]

                    # Add the sorted members to the available disaggregations
                    if len(available_members_list) > 0:
                        available_disaggregations[
                            METRIC_KEY_TO_V2_DASHBOARD_METRIC_KEY.get(
                                metric.key, metric.key
                            )
                        ][
                            aggregated_dimension.dimension.dimension_identifier()
                        ] = available_members_list
            available_metric_keys += available_metric_keys_for_sector
            if len(available_metric_keys_for_sector) > 0:
                available_sectors.append(system.name)

        return {
            "id": agency.id,
            "name": agency.name,
            "available_sectors": available_sectors,
            "available_metric_keys": available_metric_keys,
            "available_disaggregations": dict(available_disaggregations),
            "is_dashboard_enabled": agency.is_dashboard_enabled is True,
            "is_demo": is_demo_agency(agency.name),
            "state_geoid": fips_code_to_geoid.get(str(state.fips)),
            "county_geoid": (
                fips_code_to_geoid.get(fips_county_code)
                if fips_county_code is not None
                else None
            ),
            "state_name": state.name,
            "county_name": (
                county_code_to_county_name[agency.fips_county_code.upper()]
                if fips_county_code is not None
                else None
            ),
        }
