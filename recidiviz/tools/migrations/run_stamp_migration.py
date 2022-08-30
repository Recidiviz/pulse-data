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
This script stamps a migration against the specified database.
Stamping is used to tell Alembic to apply a migration without running any of its operators.

This is particularly useful when we squash our migration history down into a single revision. The newly generated
squashed revision represents the current state of the database. No operators need be run, but Alembic still needs to
mark the revision as having been applied.

Note that the stamped alembic migration must exist for it to be run properly.



Example usage (run from `pipenv shell` on `prod-data-client`):

python -m recidiviz.tools.migrations.run_stamp_migration \
    --database STATE \
    --project-id recidiviz-staging \
    --current-revision [revision_id] \
    --target-revision [revision_id] \
    --ssl-cert-path ~/dev_state_data_certs

Example usage (run from `pipenv shell` locally):

./recidiviz/tools/postgres/cloudsql_proxy_control.sh -c PROJECT_ID:REGION:CLOUD_SQL_INSTANCE_NAME -p 5440

python -m recidiviz.tools.migrations.run_stamp_migration \
    --database STATE \
    --project-id recidiviz-staging \
    --current-revision [revision_id] \
    --target-revision [revision_id] \
    --using-proxy

./recidiviz/tools/postgres/cloudsql_proxy_control.sh -q -p 5440

"""
import argparse
import logging
import sys

import alembic.config
from sqlalchemy.engine import Engine

from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.tools.migrations.migration_helpers import (
    EngineIteratorDelegate,
    confirm_correct_db_instance,
    confirm_correct_git_branch,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(
        description="Stamp a migration against the specified PostgresQL database."
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
        "--current-revision",
        type=str,
        help="""The current revision applied to the database(s)."""
        """This should be the last revision among the old set of migrations that were squashed."""
        """Exits if the current revision is not what is applied""",
        required=True,
    )
    parser.add_argument(
        "--target-revision",
        type=str,
        help="The revision id to stamp",
        required=True,
    )
    parser.add_argument(
        "--ssl-cert-path",
        type=str,
        help="The path to the folder where the certs live.",
    )
    parser.add_argument(
        "--dry-run",
        type=str_to_bool,
        help="Dry run? Migrates a fresh local postgres to HEAD and stamps the target revision",
        default=True,
    )
    parser.add_argument(
        "--using-proxy",
        action="store_true",
        help="If included, SQLAlchemy will be configured to connect to the Cloud SQL Proxy.",
    )
    return parser


def get_latest_applied_alembic_version(engine: Engine) -> str:
    return engine.execute("SELECT version_num FROM alembic_version").fetchone()[0]


def main(
    schema_type: SchemaType,
    repo_root: str,
    current_revision: str,
    target_revision: str,
    ssl_cert_path: str,
    dry_run: bool,
    using_proxy: bool,
) -> None:
    """
    Invokes the main code path for stamping a migration. Stamping is used to tell Alembic to apply a migration without
    running any of its operators.

    This checks for user validations that the database, branches, and applied revisions are correct and
    then runs the stamp migration.
    """
    is_prod = metadata.project_id() == GCP_PROJECT_PRODUCTION
    if is_prod:
        logging.info("RUNNING AGAINST PRODUCTION\n")

    prompt_for_confirmation(
        f"{'[DRY RUN] ' if dry_run else ''}This script will run a STAMP migration.",
        "STAMP",
    )
    confirm_correct_db_instance(schema_type)
    confirm_correct_git_branch(repo_root)

    for database_key, engine in EngineIteratorDelegate.iterate_and_connect_to_engines(
        schema_type,
        using_proxy=using_proxy,
        ssl_cert_path=ssl_cert_path,
        dry_run=dry_run,
    ):
        if dry_run:
            # Apply migrations to a fresh database to create the `alembic_version` table
            logging.info("[DRY RUN] Applying migrations to fresh dry-run DB...")
            logging.getLogger("alembic").setLevel(logging.ERROR)
            config = alembic.config.Config(
                database_key.alembic_file, attributes={"configure_logger": False}
            )
            alembic.command.upgrade(config, "head")

        version = get_latest_applied_alembic_version(engine)

        logging.info("%s is on revision %s", database_key, version)

        if version != current_revision:
            logging.error(
                "Expected %s to be applied; got %s", current_revision, version
            )
            sys.exit()

    for database_key, engine in EngineIteratorDelegate.iterate_and_connect_to_engines(
        schema_type,
        using_proxy=using_proxy,
        ssl_cert_path=ssl_cert_path,
        dry_run=dry_run,
    ):
        logging.info(
            "Stamping %s with revision %s",
            database_key,
            target_revision,
        )
        config = alembic.config.Config(database_key.alembic_file)
        alembic.command.stamp(
            config,
            target_revision,
            # Purge the `alembic_version` table
            # The last applied migration may not exist anymore (i.e. it was squashed)
            purge=True,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        main(
            args.database,
            args.repo_root,
            args.current_revision,
            args.target_revision,
            args.ssl_cert_path,
            args.dry_run,
            args.using_proxy,
        )
