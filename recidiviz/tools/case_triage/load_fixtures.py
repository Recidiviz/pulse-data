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
docker exec pulse-data_case_triage_backend_1 pipenv run python -m recidiviz.tools.case_triage.load_fixtures
"""
import logging
import os

from sqlalchemy.engine import Engine

from recidiviz.case_triage.views.view_config import ETL_TABLES
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.utils.fixture_helpers import reset_fixtures
from recidiviz.utils.environment import in_development


def reset_case_triage_fixtures(engine: Engine) -> None:
    """Deletes all ETL data and re-imports data from our fixture files"""
    reset_fixtures(
        engine=engine,
        tables=list(ETL_TABLES.keys()),
        fixture_directory=os.path.join(
            os.path.dirname(__file__),
            "../../..",
            "recidiviz/tools/case_triage/fixtures",
        ),
    )


if __name__ == "__main__":
    if not in_development():
        raise RuntimeError(
            "Expected to be called inside a docker container. See usage in docstring"
        )

    logging.basicConfig(level=logging.INFO)
    database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
    case_triage_engine = SQLAlchemyEngineManager.init_engine(database_key)

    reset_case_triage_fixtures(case_triage_engine)
