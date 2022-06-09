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
from typing import Any, Iterable, List, cast

from faker import Faker

from recidiviz.common.constants.justice_counts import ValueType
from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    Context,
    MetricDefinition,
)
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.persistence.database.schema.justice_counts import schema

random.seed(0)
Faker.seed(0)
FAKE = Faker(locale=["en-US"])


def _create_aggregate_datapoint(
    report: schema.Report, metric_definition: MetricDefinition
) -> schema.Datapoint:
    return schema.Datapoint(
        value=random.randint(10000, 100000),
        metric_definition_key=metric_definition.key,
        value_type=ValueType.NUMBER,
        start_date=report.date_range_start,
        end_date=report.date_range_end,
        dimension_identifier_to_member=None,
        report=report,
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
        )
        for d in cast(Iterable[DimensionBase], dimension.dimension)
    ]


def _create_context_datapoint(
    report: schema.Report,
    metric_definition: MetricDefinition,
    context: Context,
) -> schema.Datapoint:
    value: Any
    if context.value_type == ValueType.NUMBER:
        value = random.randint(10000, 100000)
    elif context.value_type == ValueType.MULTIPLE_CHOICE:
        value = random.choice(["True", "False"])
    elif context.value_type == ValueType.TEXT:
        value = FAKE.text(max_nb_chars=50)
    else:
        raise ValueError("Invalid ValueType")
    return schema.Datapoint(
        value=value,
        metric_definition_key=metric_definition.key,
        context_key=context.key.value,
        value_type=context.value_type,
        start_date=report.date_range_start,
        end_date=report.date_range_end,
        dimension_identifier_to_member=None,
        report=report,
    )


def _get_datapoints_for_report(
    report: schema.Report, system: schema.System
) -> List[schema.Datapoint]:
    metric_definitions = ReportInterface.get_metric_definitions_by_report_type(
        report_type=report.type,
        systems={system},
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
                + [
                    _create_context_datapoint(
                        report=report,
                        metric_definition=metric_definition,
                        context=context,
                    )
                    for context in metric_definition.contexts
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


def generate_fixtures() -> List[schema.JusticeCountsBase]:
    """Generate fake data for testing. This data can be loaded into a development
    Postgres database via the `load_fixtures` script, or loaded into a unit test
    that extends from JusticeCountsDatabaseTestCase via self.load_fixtures().
    """

    # `object_groups` should be a *list of list of objects*. Each list (group of objects)
    # will get added to the database, in order, via session.add_all() + session.commit().
    # Objects that depend on another object already being present in the db should
    # be added in a subsequent object group.
    object_groups = []

    # First add a group of Users
    num_users = 5
    object_groups.append(
        [
            schema.UserAccount(
                id=i,
                email_address=f"{FAKE.name().replace(' ','').lower()}@gmail.com",
                name=None,
            )
            for i in range(num_users)
        ]
    )

    # Next add a group of Agencies
    # Use the id that the Agency has on staging, so that users have access
    # on both staging environment and local development
    agency_tuples = [
        (147, schema.System.LAW_ENFORCEMENT, "Law Enforcement"),
        (148, schema.System.DEFENSE, "Defense"),
        (149, schema.System.PROSECUTION, "Prosecution"),
        (150, schema.System.COURTS_AND_PRETRIAL, "Courts"),
        (151, schema.System.JAILS, "Jails"),
        (152, schema.System.PRISONS, "Prisons"),
        (153, schema.System.SUPERVISION, "Supervision"),
    ]
    agencies = []
    for agency_id, agency_system, agency_name in agency_tuples:
        agencies.append(
            schema.Agency(
                id=agency_id,
                name=agency_name,
                system=agency_system,
                state_code="US_NY",
                fips_county_code="us_ny_new_york",
            )
        )
    object_groups.append(agencies)

    # Finally add a group of reports (which depend on agencies already being in the DB)
    reports = []
    for agency_id, agency_system, agency_name in agency_tuples:
        for year in range(2020, 2023):
            annual_report = ReportInterface.create_report_object(
                agency_id=agency_id,
                user_account_id=random.randrange(num_users),
                year=year,
                month=1,
                frequency="ANNUAL",
                last_modified_at=FAKE.date_time_between(
                    start_date=datetime.datetime(2020, 1, 1)
                ),
            )
            annual_report.datapoints = _get_datapoints_for_report(
                report=annual_report,
                system=agency_system,
            )
            reports.append(annual_report)

            for month in range(1, 13):
                monthly_report = ReportInterface.create_report_object(
                    agency_id=agency_id,
                    user_account_id=random.randrange(num_users),
                    year=year,
                    month=month,
                    frequency="MONTHLY",
                    last_modified_at=FAKE.date_time_between(
                        start_date=datetime.datetime(2020, 1, 1)
                    ),
                )
                monthly_report.datapoints = _get_datapoints_for_report(
                    report=monthly_report,
                    system=agency_system,
                )
                reports.append(monthly_report)
    object_groups.append(reports)

    return object_groups
