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

python -m recidiviz.tools.migrations.run_migrations --database JAILS --project-id recidiviz-staging --dry-run
"""
import argparse
import logging
import sys

import alembic.config
from pygit2.repository import Repository

from recidiviz.persistence.database.sqlalchemy_engine_manager import SchemaType, SQLAlchemyEngineManager
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
                             'This argument is required if running against production.')
    return parser


def main(database: SchemaType, repo_root: str, ssl_cert_path: str, dry_run: bool):
    """
    Invokes the main code path for running migrations.

    This checks for user validations that the database and branches are correct and then runs existing pending
    migrations.
    """

    requires_ssl = SQLAlchemyEngineManager.database_requires_ssl(metadata.project_id())

    if requires_ssl and not ssl_cert_path:
        logging.error('Specifying an argument to --ssl-cert-path is required for the specified database.')
        logging.error('Exiting...')
        sys.exit(1)

    if dry_run:
        if not local_postgres_helpers.can_start_on_disk_postgresql_database():
            logging.error('pg_ctl is not installed. Cannot perform a dry-run.')
            logging.error('Exiting...')
            sys.exit(1)
        logging.info('Creating a dry-run...\n')

    is_prod = metadata.project_id() == GCP_PROJECT_PRODUCTION
    dbname = SQLAlchemyEngineManager.get_stripped_cloudsql_instance_id(database)
    if is_prod:
        logging.info('RUNNING AGAINST PRODUCTION\n')

    db_check = input(f'Running new migrations on {dbname}. Please type "{dbname}" to confirm. (Anything else exits):\n')
    if db_check != dbname:
        logging.warning('\nConfirmation aborted.')
        sys.exit(1)

    # Get user to validate git branch
    try:
        repo = Repository(repo_root)
    except Exception as e:
        logging.error('improper project root provided: %s', e)
        sys.exit(1)

    current_branch = repo.head.shorthand

    if is_prod and not current_branch.startswith('releases/'):
        logging.error(
            'Migrations run against production must be from a release branch. The current branch is %s.',
            current_branch
        )
        sys.exit(1)

    branch_check = input(
        f'\nThis script will execute all migrations on this branch ({current_branch}) that have not yet been run '
        f'against {dbname}. Please type "{current_branch}" to confirm your branch. (Anything else exits):\n')
    if branch_check != current_branch:
        logging.warning('\nConfirmation aborted.')
        sys.exit(1)

    # Fetch secrets
    if dry_run:
        overriden_env_vars = local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
    else:
        overriden_env_vars = SQLAlchemyEngineManager.update_sqlalchemy_env_vars(database, ssl_cert_path=ssl_cert_path)

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
