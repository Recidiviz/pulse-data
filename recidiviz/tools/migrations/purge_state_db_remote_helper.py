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
Important note: THIS SCRIPT SHOULD NOT BE RUN ON ITS OWN. This script will be run from
`prod-data-client` within the context of other scripts. It will not work
when run anywhere else.

This script runs all downgrade migrations for a given state database, so that when
new data is subsequently imported, it will not conflict due to duplicate definitions
of enums for instance.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.migrations.purge_state_db_remote_helper \
    --state-code US_MI \
    --ingest-instance secondary \
    --project-id recidiviz-staging \
    --commit-hash abc1234
    [--purge-schema] \
    [--repo-root ~/pulse-data]
"""

import argparse
import logging
import os
import sys

import alembic.config

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.persistence.database.schema.state.schema import StateAgent, StatePerson
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.migrations.migration_helpers import confirm_correct_git_branch
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(
        description="Helper script for purging all data from a remote PostgresQL database."
    )
    parser.add_argument(
        "--state-code",
        type=StateCode,
        choices=list(StateCode),
        help="Specifies the state where all downgrades will be run.",
        required=True,
    )
    parser.add_argument(
        "--ingest-instance",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
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
        "--purge-schema",
        action="store_true",
        help="When set, runs the downgrade migrations.",
        default=False,
    )
    parser.add_argument(
        "--repo-root",
        type=str,
        default="./",
        help="The path to the root pulse-data/ folder. "
        "This is needed to check the current git branch.",
    )
    parser.add_argument(
        "--commit-hash",
        required=True,
        type=str,
        help="The commit hash that this script should be run from. Script will exit if "
        "it is not run from this commit.",
    )
    return parser


def main(
    *,
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    purge_schema: bool,
    repo_root: str,
    commit_hash: str,
) -> None:
    """
    Invokes the main code path for running a downgrade.

    This checks that the branch is correct and then runs the downgrade migration.
    """
    confirm_correct_git_branch(repo_root, confirm_hash=commit_hash)

    is_prod = metadata.project_id() == GCP_PROJECT_PRODUCTION
    ssl_cert_path = os.path.expanduser(
        f"~/{'prod' if is_prod else 'dev'}_state_data_certs"
    )

    db_key = ingest_instance.database_key_for_state(state_code)
    with SessionFactory.for_prod_data_client(
        db_key, os.path.abspath(ssl_cert_path)
    ) as session:
        tables_to_truncate = [
            StatePerson.__tablename__,
            StateAgent.__tablename__,
        ]
        for table_name in tables_to_truncate:
            command = f"TRUNCATE TABLE {table_name} CASCADE;"
            logging.info('Running query ["%s"]. This may take a while...', command)
            session.execute(command)

        logging.info("Done running truncate commands.")

    if purge_schema:
        with SessionFactory.for_prod_data_client(
            db_key, os.path.abspath(ssl_cert_path)
        ) as purge_session:
            overriden_env_vars = None
            try:
                logging.info("Purging schema...")

                overriden_env_vars = SQLAlchemyEngineManager.update_sqlalchemy_env_vars(
                    database_key=db_key,
                    ssl_cert_path=ssl_cert_path,
                    migration_user=True,
                )
                config = alembic.config.Config(db_key.alembic_file)
                alembic.command.downgrade(config, "base")

                # We need to manually delete alembic_version because it's leftover after
                # the downgrade migrations
                purge_session.execute("DROP TABLE alembic_version;")
            finally:
                if overriden_env_vars:
                    local_postgres_helpers.restore_local_env_vars(overriden_env_vars)

        logging.info("Purge complete.")


if __name__ == "__main__":
    # Route logs to stdout
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)

    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):

        main(
            state_code=args.state_code,
            ingest_instance=args.ingest_instance,
            purge_schema=args.purge_schema,
            repo_root=args.repo_root,
            commit_hash=args.commit_hash,
        )
