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
This script spins up a postgres database, runs the existing migrations and then generates
a new migration based on the difference between the schema and the database.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.migrations.autogenerate_migration --database JAILS --message add_field_foo
"""
import argparse
import logging

from alembic.command import revision, upgrade
import alembic.config

from recidiviz.persistence.database.sqlalchemy_engine_manager import SchemaType, SQLAlchemyEngineManager
from recidiviz.tools.postgres import local_postgres_helpers


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
    return parser


def main(database: SchemaType, message: str):
    # TODO(#4619): We should eventually move this from a local postgres instance to running
    # postgres from a docker container.
    tmp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database(create_temporary_db=True)
    overridden_env_vars = local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()

    try:
        config = alembic.config.Config(SQLAlchemyEngineManager.get_alembic_file(database))
        upgrade(config, 'head')
        revision(config, autogenerate=True, message=message)
    except Exception as e:
        logging.error('Automigration generation failed: %s', e)

    local_postgres_helpers.restore_local_sqlalchemy_postgres_env_vars(overridden_env_vars)
    local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(tmp_db_dir)


if __name__ == '__main__':
    args = create_parser().parse_args()
    main(args.database, args.message)
