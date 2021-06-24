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
Important note: This script should be run from `prod-data-client`. It will not work
when run anywhere else.

This script runs all downgrade migrations for a given state database, so that when
new data is subsequently imported, it will not conflict due to duplicate definitions
of enums for instance.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.migrations.purge_state_db \
    --state-code US_MI \
    --database-version secondary \
    --project-id recidiviz-staging \
    --ssl-cert-path ~/dev_state_data_certs
"""
import argparse
import logging
import sys

import alembic.config

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import (
    SQLAlchemyDatabaseKey,
    SQLAlchemyStateDatabaseVersion,
)
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.migrations.migration_helpers import prompt_for_confirmation
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(
        description="Run a single downgrade migration against the specified PostgresQL database."
    )
    parser.add_argument(
        "--state-code",
        type=StateCode,
        choices=list(StateCode),
        help="Specifies the state where all downgrades will be run.",
        required=True,
    )
    parser.add_argument(
        "--database-version",
        type=SQLAlchemyStateDatabaseVersion,
        choices=list(SQLAlchemyStateDatabaseVersion),
        help="Specifies the database version where all downgrades will be run.",
        required=True,
    )
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument(
        "--ssl-cert-path",
        type=str,
        help="The path to the folder where the certs live.",
        required=True,
    )
    return parser


def main(
    state_code: StateCode,
    database_version: SQLAlchemyStateDatabaseVersion,
    ssl_cert_path: str,
) -> None:
    """
    Invokes the main code path for running a downgrade.

    This checks for user validations that the database and branches are correct and then runs the downgrade
    migration.
    """
    # TODO(#7984): Once we have cut all traffic over to single-database traffic,
    #   delete this branch.
    if database_version == SQLAlchemyStateDatabaseVersion.LEGACY:
        logging.error(
            "Should not invoke purge_state_db script with legacy database version."
        )
        sys.exit(1)

    is_prod = metadata.project_id() == GCP_PROJECT_PRODUCTION
    if is_prod:
        logging.info("RUNNING AGAINST PRODUCTION\n")

    purge_str = (
        "PURGE STATE IN STAGING"
        if metadata.project_id() == GCP_PROJECT_STAGING
        else "PURGE STATE IN PROD"
    )
    prompt_for_confirmation(
        f"This script will run all DOWNGRADE migrations for {state_code=} and {database_version=}.",
        purge_str,
    )

    db_key = SQLAlchemyDatabaseKey.for_state_code(state_code, database_version)

    # Run downgrade
    session = None
    try:
        overriden_env_vars = SQLAlchemyEngineManager.update_sqlalchemy_env_vars(
            database_key=db_key,
            ssl_cert_path=ssl_cert_path,
            migration_user=True,
        )
        config = alembic.config.Config(db_key.alembic_file)
        alembic.command.downgrade(config, "base")

        # We need to manually delete alembic_version because it's leftover after
        # the downgrade migrations
        session = SessionFactory.for_prod_data_client(db_key, ssl_cert_path)
        session.execute("DROP TABLE alembic_version;")
        session.commit()
    except Exception as e:
        logging.error("Downgrade failed to run: %s", e)
        if session is not None:
            session.rollback()
        sys.exit(1)
    finally:
        if session is not None:
            session.close()
        local_postgres_helpers.restore_local_env_vars(overriden_env_vars)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        main(args.state_code, args.database_version, args.ssl_cert_path)
