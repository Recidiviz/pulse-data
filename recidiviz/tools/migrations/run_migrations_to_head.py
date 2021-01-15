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
Important note: This script should be run from `prod-data-client` unless the --dry-run
flag is set. It will not work when run anywhere else (and notably the --dry-run flag will
not work if run on `prod-data-client`).

This script runs alembic migrations against production and staging database instances.
It fetches the appropriate secrets and validates with the user that the intended migrations
are being run before executing them.

Example usage (run from `pipenv shell`):

# Dry run on the jails database:
python -m recidiviz.tools.migrations.run_migrations_to_head \
    --database JAILS \
    --project-id recidiviz-staging \
    --dry-run

# Run against the live jails database:
python -m recidiviz.tools.migrations.run_migrations_to_head \
    --database JAILS \
    --project-id recidiviz-staging \
    --ssl-cert-path ~/dev_data_certs
"""
import argparse
import logging
import sys

import alembic.config

from recidiviz.persistence.database.sqlalchemy_engine_manager import SchemaType, SQLAlchemyEngineManager
from recidiviz.tools.migrations.migration_helpers import confirm_correct_db, confirm_correct_git_branch
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Run migrations against PostgresQL database.')
    parser.add_argument('--database',
                        type=SchemaType,
                        choices=list(SchemaType),
                        help='Specifies which database to run against.',
                        required=True)
    parser.add_argument('--repo-root',
                        type=str,
                        default='./',
                        help='The path to the root pulse-data/ folder. '
                             'This is needed to check the current git branch.')
    parser.add_argument('--dry-run',
                        help='If set, this runs all migrations locally instead of against prod/staging databases.',
                        action='store_true')
    parser.add_argument('--project-id',
                        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
                        help='Used to select which GCP project in which to run this script.',
                        required=True)
    parser.add_argument('--ssl-cert-path',
                        type=str,
                        help='The path to the folder where the certs live. '
                             'This argument is required if running against live databases.')
    return parser


def main(database: SchemaType, repo_root: str, ssl_cert_path: str, dry_run: bool) -> None:
    """
    Invokes the main code path for running migrations.

    This checks for user validations that the database and branches are correct and then runs existing pending
    migrations.
    """
    if dry_run:
        if not local_postgres_helpers.can_start_on_disk_postgresql_database():
            logging.error('pg_ctl is not installed. Cannot perform a dry-run.')
            logging.error('Exiting...')
            sys.exit(1)
        logging.info('Creating a dry-run...\n')
    else:
        if not ssl_cert_path:
            logging.error("SSL certificates are required when running against live databases")
            logging.error('Exiting...')
            sys.exit(1)
        logging.info('Using SSL certificate path: %s', ssl_cert_path)

    is_prod = metadata.project_id() == GCP_PROJECT_PRODUCTION
    if is_prod:
        logging.info('RUNNING AGAINST PRODUCTION\n')

    confirm_correct_db(database)
    confirm_correct_git_branch(repo_root, is_prod=is_prod)

    if dry_run:
        overriden_env_vars = local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
    else:
        overriden_env_vars = SQLAlchemyEngineManager.update_sqlalchemy_env_vars(
            database,
            ssl_cert_path=ssl_cert_path,
            migration_user=True,
        )

    # Run migrations
    try:
        if dry_run:
            logging.info('Starting local postgres database for migrations dry run')
            db_dir = local_postgres_helpers.start_on_disk_postgresql_database()
        config = alembic.config.Config(SQLAlchemyEngineManager.get_alembic_file(database))
        alembic.command.upgrade(config, 'head')
    except Exception as e:
        logging.error('Migrations failed to run: %s', e)
        sys.exit(1)
    finally:
        local_postgres_helpers.restore_local_env_vars(overriden_env_vars)
        if dry_run:
            try:
                logging.info('Stopping local postgres database')
                local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(db_dir)
            except Exception as e2:
                logging.error('Error cleaning up postgres: %s', e2)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        main(args.database, args.repo_root, args.ssl_cert_path, args.dry_run)
