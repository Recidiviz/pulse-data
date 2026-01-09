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
This script runs a single downgrade migration against the specified database. Note that
the downgrade alembic migration must exist for it to be run properly, so this script
should be run on a rev where the to-be-rolled-back upgrade migration exists.

Example usage:

uv run python -m recidiviz.tools.migrations.run_downgrade_migration \
    --database STATE \
    --project-id recidiviz-staging \
    --target-revision [revision_id]
"""
import argparse
import logging
import sys

import alembic.config

from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.tools.migrations.migration_helpers import (
    EngineIteratorDelegate,
    confirm_correct_db_instance,
    confirm_correct_git_branch,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(
        description="Run a single downgrade migration against the specified PostgresQL database."
    )
    parser.add_argument(
        "--database",
        type=SchemaType,
        choices=list(SchemaType),
        help="Specifies which database to run against.",
        required=True,
    )
    parser.add_argument(
        "--repo-root",
        type=str,
        default="./",
        help="The path to the root pulse-data/ folder. "
        "This is needed to check the current git branch.",
    )
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument(
        "--target-revision",
        type=str,
        help="The revision id to downgrade to.",
        required=True,
    )
    return parser


def main(schema_type: SchemaType, repo_root: str, target_revision: str) -> None:
    """
    Invokes the main code path for running a downgrade.

    This checks for user validations that the database and branches are correct and then runs the downgrade
    migration.
    """
    prompt_for_confirmation(
        "This script will run a DOWNGRADE migration against a live database.",
        "DOWNGRADE",
    )
    confirm_correct_db_instance(schema_type)
    confirm_correct_git_branch(repo_root)

    with cloudsql_proxy_control.connection(schema_type=schema_type):
        if metadata.running_against(GCP_PROJECT_PRODUCTION):
            logging.info("RUNNING AGAINST PRODUCTION\n")

        for database_key, _ in EngineIteratorDelegate.iterate_and_connect_to_engines(
            schema_type, dry_run=False
        ):
            # Run downgrade
            try:
                config = alembic.config.Config(database_key.alembic_file)
                alembic.command.downgrade(config, target_revision)
            except Exception as e:
                logging.error("Downgrade failed to run: %s", e)
                sys.exit(1)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        main(args.database, args.repo_root, args.target_revision)
