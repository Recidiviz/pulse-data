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
docker exec pulse-data_control_panel_backend_1 pipenv run python -m recidiviz.tools.justice_counts.control_panel.load_fixtures
"""
import logging
import os

from sqlalchemy.engine import Engine

from recidiviz.persistence.database.schema.justice_counts.schema import (
    Report,
    Source,
    UserAccount,
    agency_user_account_association_table,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.utils.fixture_helpers import reset_fixtures
from recidiviz.utils.environment import in_development


def reset_justice_counts_fixtures(engine: Engine) -> None:
    """Deletes all data and then re-imports data from our fixture files."""
    reset_fixtures(
        engine=engine,
        # TODO(#11588): Add fixture data for all Justice Counts schema tables
        tables=[Source, Report, UserAccount, agency_user_account_association_table],
        fixture_directory=os.path.join(
            os.path.dirname(__file__),
            "../../../..",
            "recidiviz/tools/justice_counts/control_panel/fixtures/",
        ),
        csv_headers=True,
    )


if __name__ == "__main__":
    if not in_development():
        raise RuntimeError(
            "Expected to be called inside a docker container. See usage in docstring"
        )

    logging.basicConfig(level=logging.INFO)
    database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JUSTICE_COUNTS)
    justice_counts_engine = SQLAlchemyEngineManager.init_engine(database_key)

    reset_justice_counts_fixtures(justice_counts_engine)
