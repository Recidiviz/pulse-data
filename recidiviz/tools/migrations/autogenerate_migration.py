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
Script for auto-generating an alembic migration based on local schema and known migrations.
There are two modes for running this script, either locally or remotely. This script will almost
always be run locally, but in instances where prod/staging are out of sync, it may be necessary
to generate automigrations against them directly.

When run locally (i.e. when --project-id is not set), the script creates a new postgres database,
applies all known migrations, and then auto-generates a new migration based on the known schema.
The local database is cleaned up when everything is finished.

In remote mode (i.e. when a value for --project-id is set), the script points at the pertinent
database associated with the given project and auto-generates a new migration based on the difference
between the existing database and the current schema. These operations proceed in readonly mode.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.migrations.autogenerate_migration --database OPERATIONS --message add_field_foo
"""
import argparse
import logging
import sys

import alembic.config
from alembic.command import revision, upgrade

from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Autogenerate local migration.")
    parser.add_argument(
        "--database",
        type=SchemaType,
        choices=list(SchemaType),
        help="Specifies which database to run against.",
        required=True,
    )
    parser.add_argument(
        "--message",
        type=str,
        help="String message passed to alembic to apply to the revision.",
        required=True,
    )
    return parser


def main(schema_type: SchemaType, message: str) -> None:
    """Runs the script to autogenerate migrations."""
    database_key = SQLAlchemyDatabaseKey.canonical_for_schema(schema_type)
    # TODO(#4619): We should eventually move this from a local postgres instance to running
    # postgres from a docker container.
    if not local_postgres_helpers.can_start_on_disk_postgresql_database():
        logging.error(
            "pg_ctl is not installed, so the script cannot be run locally. "
            "--project-id must be specified to run against staging or production."
        )
        logging.error("Exiting...")
        sys.exit(1)

    logging.info("Starting local postgres database for autogeneration...")
    postgres_launch_result = local_postgres_helpers.start_on_disk_postgresql_database()
    original_env_vars = (
        local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars(
            postgres_launch_result
        )
    )

    try:
        config = alembic.config.Config(database_key.alembic_file)
        upgrade(config, "head")
        revision(config, autogenerate=True, message=message)
    except Exception as e:
        logging.error("Automigration generation failed: %s", e)

    local_postgres_helpers.restore_local_env_vars(original_env_vars)
    logging.info("Stopping local postgres database...")
    local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
        postgres_launch_result
    )


if __name__ == "__main__":
    args = create_parser().parse_args()
    main(args.database, args.message)
