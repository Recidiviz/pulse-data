# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tool for loading fixture data into our Operations development database.

This script should be run only after
`docker-compose -f docker-compose.yaml -f docker-compose.admin-panel.yaml up` has been
run.

This will delete everything from the operations tables in the local replica of the
operations DB and then re-add them from the fixture files.

Usage against default development database (docker-compose v1):
docker exec pulse-data_admin_panel_backend_1 pipenv run python -m recidiviz.tools.admin_panel.load_operations_db_fixtures

Usage against default development database (docker-compose v2):
docker exec pulse-data-admin_panel_backend-1 pipenv run python -m recidiviz.tools.admin_panel.load_operations_db_fixtures

You can check your docker-compose version by running `docker-compose --version`.
"""
import logging
import os

from sqlalchemy.engine import Engine

from recidiviz.persistence.database.schema_utils import (
    SchemaType,
    get_operations_table_classes,
)
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.utils.fixture_helpers import reset_fixtures
from recidiviz.utils.environment import in_development


def reset_operations_db_fixtures(engine: Engine) -> None:
    """Deletes all operations DB data and re-imports data from our fixture files"""
    reset_fixtures(
        engine=engine,
        tables=list(get_operations_table_classes()),
        fixture_directory=os.path.join(
            os.path.dirname(__file__),
            "../../..",
            "recidiviz/tools/admin_panel/fixtures/operations_db",
        ),
    )


if __name__ == "__main__":
    if not in_development():
        raise RuntimeError(
            "Expected to be called inside a docker container. See usage in docstring"
        )

    logging.basicConfig(level=logging.INFO)
    database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
    case_triage_engine = SQLAlchemyEngineManager.init_engine(database_key)

    reset_operations_db_fixtures(case_triage_engine)
