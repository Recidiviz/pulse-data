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
"""
Tool for loading fixture data into the Justice Counts development database.

This script should be run only after `docker-compose up` has been run.
This will delete everything from the tables and then re-add them from the
fixture files.

Usage against default development database:
SQLALCHEMY_DB_NAME=justice_counts python -m recidiviz.tools.justice_counts.control_panel.load_fixtures

Usage against non-default development database:
SQLALCHEMY_DB_HOST="" SQLALCHEMY_DB_USER="" SQLALCHEMY_DB_PASSWORD="" SQLALCHEMY_DB_NAME="" \
python -m recidiviz.tools.justice_counts.control_panel.load_fixtures
"""
import os

from recidiviz.persistence.database.schema.justice_counts.schema import Report, Source
from recidiviz.tools.utils.fixture_helpers import reset_fixtures


def reset_justice_counts_fixtures() -> None:
    """Deletes all data and then re-imports data from our fixture files."""
    reset_fixtures(
        # TODO(#11588): Add fixture data for all Justice Counts schema tables
        tables=[Source, Report],
        fixture_directory=os.path.join(
            os.path.dirname(__file__),
            "../../../..",
            "recidiviz/tools/justice_counts/control_panel/fixtures/",
        ),
        csv_headers=True,
    )


if __name__ == "__main__":
    reset_justice_counts_fixtures()
