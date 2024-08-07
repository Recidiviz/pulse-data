# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
This script runs alembic migrations in the specified docker database(s).

It is called from docker-compose to run migrations on state segmented databases.
"""
import argparse
import logging
import sys

import alembic.config

from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.server_config import (
    database_keys_for_schema_type,
    state_codes_for_schema_type,
)
from recidiviz.tools.utils.fixture_helpers import create_dbs


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(
        description="Run migrations against PostgresQL database."
    )
    parser.add_argument(
        "--database",
        type=SchemaType,
        choices=list(SchemaType),
        help="Specifies which database to run against.",
        required=True,
    )
    return parser


def main(
    schema_type: SchemaType,
) -> None:
    """
    Invokes the main code path for running migrations in the specified docker database(s).
    """
    # If necessary, create state-segmented DBs, if they don't already exist
    if schema_type.is_multi_db_schema:
        postgres_db_key = SQLAlchemyDatabaseKey(
            schema_type=schema_type, db_name="postgres"
        )
        engine = SQLAlchemyEngineManager.init_engine_for_db_instance(
            database_key=postgres_db_key,
            db_url=SQLAlchemyEngineManager.get_server_postgres_instance_url(
                database_key=postgres_db_key,
                using_unix_sockets=False,
            ),
        )
        state_codes = state_codes_for_schema_type(schema_type)
        create_dbs(state_codes=state_codes, schema_type=schema_type, engine=engine)

    # Run migrations
    for database_key in database_keys_for_schema_type(schema_type):
        SQLAlchemyEngineManager.update_sqlalchemy_env_vars(
            database_key=database_key,
        )
        try:
            config = alembic.config.Config(database_key.alembic_file)
            alembic.command.upgrade(config, "head")
        except Exception as e:
            logging.error("Migrations failed to run: %s", e)
            sys.exit(1)
        finally:
            SQLAlchemyEngineManager.teardown_engine_for_database_key(
                database_key=database_key
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()

    main(
        schema_type=args.database,
    )
