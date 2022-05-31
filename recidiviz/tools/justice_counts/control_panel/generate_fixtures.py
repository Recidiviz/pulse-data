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

from typing import List

from recidiviz.justice_counts.report import ReportInterface
from recidiviz.persistence.database.schema.justice_counts import schema


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
    object_groups.append(
        [schema.UserAccount(id=0, email_address="john@fake.com", name="John Smith")]
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
    for agency_id, _, agency_name in agency_tuples:
        # For each agency, create two reports, one monthly and one annual.
        monthly_report = ReportInterface.create_report_object(
            agency_id=agency_id,
            user_account_id=0,
            year=2022,
            month=1,
            frequency="MONTHLY",
        )
        annual_report = ReportInterface.create_report_object(
            agency_id=agency_id,
            user_account_id=0,
            year=2022,
            month=1,
            frequency="ANNUAL",
        )
        reports.extend([monthly_report, annual_report])
    object_groups.append(reports)

    return object_groups
