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

    # Should be a list of object groups, each of which will get added,
    # in order, via session.add_all() + session.commit(). Objects that
    # depend on another object already being present in the db should
    # be added in a subsequent object group.
    object_groups = []

    agency_names = ["Agency Alpha", "Department of Beta"]

    agencies = []
    for agency_id, agency_name in enumerate(agency_names):
        agencies.append(
            schema.Agency(
                id=agency_id,
                name=agency_name,
                system=schema.System.LAW_ENFORCEMENT,
                state_code="US_NY",
                fips_county_code="us_ny_new_york",
            )
        )
    object_groups.append(agencies)

    reports = []
    for agency_id, agency_name in enumerate(agency_names):
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
