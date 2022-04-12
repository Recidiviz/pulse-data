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
Tool for loading fixture data into our Case Triage development database.

This script should be run only after `docker-compose up` has been run.
This will delete everything from the etl_* tables and then re-add them from the
fixture files.

Usage against default development database:
python -m recidiviz.tools.case_triage.load_fixtures

Usage against non-default development database:
SQLALCHEMY_DB_HOST="" SQLALCHEMY_DB_USER="" SQLALCHEMY_DB_PASSWORD="" SQLALCHEMY_DB_NAME="" \
python -m recidiviz.tools.case_triage.load_fixtures
"""
import os

from recidiviz.case_triage.views.view_config import ETL_TABLES
from recidiviz.tools.utils.fixture_helpers import reset_fixtures


def reset_case_triage_fixtures(in_test: bool = False) -> None:
    """Deletes all ETL data and re-imports data from our fixture files"""

    if not in_test:
        os.environ["SQLALCHEMY_DB_NAME"] = "case_triage"

    reset_fixtures(
        tables=ETL_TABLES,
        fixture_directory=os.path.join(
            os.path.dirname(__file__),
            "../../..",
            "recidiviz/tools/case_triage/fixtures",
        ),
    )


if __name__ == "__main__":
    reset_case_triage_fixtures()
