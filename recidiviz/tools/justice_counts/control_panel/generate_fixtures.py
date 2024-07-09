#!/usr/bin/env bash

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
"""Utilities for generating Justice Counts fixtures."""

import datetime
import itertools
import random
from typing import Iterable, List, cast

from sqlalchemy.orm import Session

from recidiviz.common.constants.justice_counts import ValueType
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    MetricDefinition,
)
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.persistence.database.schema.justice_counts import schema

random.seed(0)

LAW_ENFORCEMENT_AGENCY_ID = 147
DEFENSE_AGENCY_ID = 148
PROSECUTION_AGENCY_ID = 149
COURTS_AND_PRETRIAL_AGENCY_ID = 150
JAILS_AGENCY_ID = 151
PRISONS_AGENCY_ID = 152
SUPERVISION_PAROLE_PROBATION_PRISONS_AGENCY_ID = 153
SUPERVISION_AGENCY_ID = 154


def _create_aggregate_datapoint(
    report: schema.Report, metric_definition: MetricDefinition
) -> schema.Datapoint:
    return schema.Datapoint(
        value=str(random.randint(10000, 100000)),
        metric_definition_key=metric_definition.key,
        value_type=ValueType.NUMBER,
        start_date=report.date_range_start,
        end_date=report.date_range_end,
        dimension_identifier_to_member=None,
        report=report,
        source_id=report.source_id,
        is_report_datapoint=True,
    )


def _create_dimension_datapoints(
    report: schema.Report,
    metric_definition: MetricDefinition,
    dimension: AggregatedDimension,
) -> List[schema.Datapoint]:
    return [
        schema.Datapoint(
            value=random.randint(10000, 100000),
            metric_definition_key=metric_definition.key,
            value_type=ValueType.NUMBER,
            start_date=report.date_range_start,
            end_date=report.date_range_end,
            dimension_identifier_to_member={
                dimension.dimension_identifier(): d.dimension_name
            },
            report=report,
            source_id=report.source_id,
            is_report_datapoint=True,
        )
        for d in cast(Iterable[DimensionBase], dimension.dimension)
    ]


def _get_datapoints_for_report(
    report: schema.Report, system: schema.System, session: Session
) -> List[schema.Datapoint]:

    agency = AgencyInterface.get_agency_by_id(
        session=session, agency_id=report.source_id
    )

    metric_key_to_metric_interface = DatapointInterface.join_report_datapoints_to_metric_interfaces(
        report_datapoints=report.datapoints,
        metric_key_to_metric_interface=MetricSettingInterface.get_metric_key_to_metric_interface(
            session=session,
            agency=agency,
        ),
    )

    metric_definitions = MetricSettingInterface.get_metric_definitions_for_report(
        report_frequency=report.type,
        systems={system},
        starting_month=report.date_range_start.month,
        metric_key_to_metric_interface=metric_key_to_metric_interface,
    )
    return list(
        itertools.chain(
            *[
                [
                    _create_aggregate_datapoint(
                        report=report,
                        metric_definition=metric_definition,
                    )
                ]
                + list(
                    itertools.chain(
                        *[
                            _create_dimension_datapoints(
                                report=report,
                                metric_definition=metric_definition,
                                dimension=dimension,
                            )
                            for dimension in metric_definition.aggregated_dimensions
                            or []
                        ]
                    )
                )
                for metric_definition in metric_definitions
            ]
        )
    )


def generate_fixtures(session: Session) -> List[schema.JusticeCountsBase]:
    """Generate fake data for testing. This data can be loaded into a development
    Postgres database via the `load_fixtures` script, or loaded into a unit test
    that extends from JusticeCountsDatabaseTestCase via self.load_fixtures().
    """

    # `object_groups` should be a *list of list of objects*. Each list (group of objects)
    # will get added to the database, in order, via session.add_all() + session.commit().
    # Objects that depend on another object already being present in the db should
    # be added in a subsequent object group.
    object_groups = []
    users = []

    # First add a group of Users
    num_users = 5
    users = [
        schema.UserAccount(
            id=i,
            auth0_user_id=f"auth0|{i}",
            name=f"User {i}",
            email=f"user{i}@email.com",
        )
        for i in range(1, num_users + 1)  # start user IDs b/c 0 is not a valid user ID
    ]
    session.add_all(users)
    session.execute(
        "SELECT pg_catalog.setval(pg_get_serial_sequence('user_account', 'id'), MAX(id)) FROM user_account;"
    )
    object_groups.append(users)

    # Next add a group of Agencies
    # Use the id that the Agency has on staging, so that users have access
    # on both staging environment and local development
    agency_tuples = [
        (
            LAW_ENFORCEMENT_AGENCY_ID,
            (schema.System.LAW_ENFORCEMENT,),
            "Law Enforcement",
        ),
        (DEFENSE_AGENCY_ID, (schema.System.DEFENSE,), "Defense"),
        (PROSECUTION_AGENCY_ID, (schema.System.PROSECUTION,), "Prosecution"),
        (COURTS_AND_PRETRIAL_AGENCY_ID, (schema.System.COURTS_AND_PRETRIAL,), "Courts"),
        (JAILS_AGENCY_ID, (schema.System.JAILS,), "Jails"),
        (PRISONS_AGENCY_ID, (schema.System.PRISONS,), "Prisons"),
        (
            SUPERVISION_PAROLE_PROBATION_PRISONS_AGENCY_ID,
            (
                schema.System.SUPERVISION,
                schema.System.PAROLE,
                schema.System.PROBATION,
                schema.System.PRISONS,
            ),
            # Add Prisons so that this agency resembles NCDPS
            "Supervision & Prison",
        ),
        (SUPERVISION_AGENCY_ID, (schema.System.SUPERVISION,), "Supervision"),
    ]
    agencies = []
    for agency_id, agency_systems, agency_name in agency_tuples:
        agencies.append(
            schema.Agency(
                id=agency_id,
                name=agency_name,
                systems=[system.value for system in agency_systems],
                state_code="US_NY",
                fips_county_code="us_ny_new_york",
            )
        )
    session.add_all(agencies)
    object_groups.append(agencies)

    # Finally add a group of reports (which depend on agencies already being in the DB)
    reports = []
    for agency_id, agency_systems, agency_name in agency_tuples:
        for year in range(2020, 2023):
            annual_report = ReportInterface.create_report_object(
                agency_id=agency_id,
                user_account_id=random.randrange(num_users),
                year=year,
                month=1,
                frequency="ANNUAL",
                last_modified_at=datetime.datetime(2020, 1, 1),
            )
            annual_report_datapoints = []
            for system in agency_systems:
                annual_report_datapoints += _get_datapoints_for_report(
                    report=annual_report, system=system, session=session
                )
            annual_report.datapoints = annual_report_datapoints
            reports.append(annual_report)

            for month in range(1, 13):
                monthly_report = ReportInterface.create_report_object(
                    agency_id=agency_id,
                    user_account_id=random.randrange(num_users),
                    year=year,
                    month=month,
                    frequency="MONTHLY",
                    last_modified_at=datetime.datetime(2020, 1, 1),
                )
                monthly_report_datapoints = []
                for system in agency_systems:
                    monthly_report_datapoints += _get_datapoints_for_report(
                        report=monthly_report, system=system, session=session
                    )
                monthly_report.datapoints = monthly_report_datapoints
                reports.append(monthly_report)
    session.add_all(reports)
    object_groups.append(reports)
    # Add AgencyUserAccountAssociation for every user in every agency
    assocs = []
    for user in users:
        for agency in agencies:
            assocs.append(
                schema.AgencyUserAccountAssociation(
                    user_account=user,
                    agency=agency,
                    invitation_status=schema.UserAccountInvitationStatus.ACCEPTED,
                    role=random.choice(
                        [
                            schema.UserAccountRole.AGENCY_ADMIN,
                            schema.UserAccountRole.CONTRIBUTOR,
                            schema.UserAccountRole.JUSTICE_COUNTS_ADMIN,
                        ]
                    ),
                )
            )
    session.add_all(assocs)
    object_groups.append(assocs)

    return object_groups
