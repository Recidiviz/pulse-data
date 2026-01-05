# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
Tool for loading data into the Workflows development database.

This script should be run only after `docker-compose up` has been run.
This will delete everything from the tables and then re-add them from the fixture files.

Usage against default development database (docker-compose v1):
docker exec pulse-data_admin_panel_backend_1 uv run python -m recidiviz.tools.workflows.load_local_db \
    --state_codes US_PA

Usage against default development database (docker-compose v2):
docker exec pulse-data-admin_panel_backend-1 uv run python -m recidiviz.tools.workflows.load_local_db \
    --state_codes US_PA
"""
import argparse
import logging
import os
import sys
from typing import List, Tuple

from sqlalchemy.engine import Engine

from recidiviz.calculator.query.state.views.outliers.workflows_enabled_states import (
    get_workflows_enabled_states,
)
from recidiviz.persistence.database.base_schema import SQLAlchemyModelType
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema.workflows import schema
from recidiviz.persistence.database.schema.workflows.schema import WorkflowsBase
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_all_table_classes_in_schema,
    get_database_entity_by_table_name,
)
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.utils.fixture_helpers import create_dbs, reset_fixtures
from recidiviz.utils.environment import in_development


def reset_workflows_fixtures(
    engine: Engine, tables: List[SQLAlchemyModelType], state: str
) -> None:
    """Deletes all ETL data and re-imports data from our fixture files"""
    logging.info("dropping all tables for %s", state)
    WorkflowsBase.metadata.drop_all(engine)
    for table in tables:
        logging.info("creating table %s.%s", state, table.__tablename__)
        WorkflowsBase.metadata.create_all(engine, tables=[table.__table__])

    reset_fixtures(
        engine=engine,
        tables=tables,
        fixture_directory=os.path.join(
            os.path.dirname(__file__),
            "../../..",
            "recidiviz/tools/workflows/fixtures",
        ),
        csv_headers=True,
    )


def main(
    state_codes: List[str],
    tables: List[SQLAlchemyModelType],
) -> None:
    create_dbs(state_codes, SchemaType.WORKFLOWS)

    for state in state_codes:
        database_manager = StateSegmentedDatabaseManager(
            get_workflows_enabled_states(), SchemaType.WORKFLOWS
        )
        database_key = database_manager.database_key_for_state(state)
        workflows_engine = SQLAlchemyEngineManager.get_engine_for_database(database_key)

        reset_workflows_fixtures(workflows_engine, tables, state)


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--state_codes",
        help="Space-separated state codes to load data for. If unset, will load data for all states.",
        type=str,
        nargs="*",
        choices=get_workflows_enabled_states(),
        default=get_workflows_enabled_states(),
    )

    parser.add_argument(
        "--tables",
        help="Space-separated tables to load data into. If unset, loads data into all tables.",
        type=str,
        nargs="*",
        choices=[
            table.name
            for table in get_all_table_classes_in_schema(SchemaType.WORKFLOWS)
        ],
        default=[
            table.name
            for table in get_all_table_classes_in_schema(SchemaType.WORKFLOWS)
        ],
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    if not in_development():
        raise RuntimeError(
            "Expected to be called inside a docker container. See usage in docstring"
        )

    logging.basicConfig(level=logging.INFO)
    args, _ = parse_arguments(sys.argv)
    table_classes = [
        get_database_entity_by_table_name(schema, table) for table in args.tables
    ]
    main(args.state_codes, table_classes)
