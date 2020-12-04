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

python -m recidiviz.tools.migrations.autogenerate_migration --database JAILS --message add_field_foo
"""
import argparse
import logging
import sys

from alembic.command import revision, upgrade
import alembic.config

from recidiviz.persistence.database.sqlalchemy_engine_manager import SchemaType, SQLAlchemyEngineManager
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Autogenerate local migration.')
    parser.add_argument('--database',
                        type=SchemaType,
                        choices=list(SchemaType),
                        help='Specifies which database to run against.',
                        required=True)
    parser.add_argument('--message',
                        type=str,
                        help='String message passed to alembic to apply to the revision.',
                        required=True)
    parser.add_argument('--project-id',
                        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
                        help='Used to select which GCP project in which to run this script. '
                             'If not set, then this script runs against a locally generated '
                             'database.')
    return parser


def main(database: SchemaType, message: str, use_local_db: bool) -> None:
    if use_local_db:
        # TODO(#4619): We should eventually move this from a local postgres instance to running
        # postgres from a docker container.
        if not local_postgres_helpers.can_start_on_disk_postgresql_database():
            logging.error(
                'pg_ctl is not installed, so the script cannot be run locally. '
                '--project-id must be specified to run against staging or production.')
            logging.error('Exiting...')
            sys.exit(1)
        logging.info('Starting local postgres database for autogeneration...')
        tmp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()
        original_env_vars = local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
    else:
        original_env_vars = SQLAlchemyEngineManager.update_readonly_sqlalchemy_env_vars(database)

    try:
        config = alembic.config.Config(SQLAlchemyEngineManager.get_alembic_file(database))
        if use_local_db:
            upgrade(config, 'head')
        revision(config, autogenerate=True, message=message)
    except Exception as e:
        logging.error('Automigration generation failed: %s', e)

    local_postgres_helpers.restore_local_env_vars(original_env_vars)
    if use_local_db:
        logging.info('Stopping local postgres database...')
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(tmp_db_dir)


if __name__ == '__main__':
    args = create_parser().parse_args()
    if not args.project_id:
        main(args.database, args.message, True)
    else:
        with local_project_id_override(args.project_id):
            main(args.database, args.message, False)
