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
from typing import Optional

import alembic.config

from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.tools.migrations.migration_helpers import (
    EngineIteratorDelegate,
    confirm_correct_db_instance,
    confirm_correct_git_branch,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


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
    parser.add_argument(
        "--repo-root",
        type=str,
        default="./",
        help="The path to the root pulse-data/ folder. "
        "This is needed to check the current git branch.",
    )
    parser.add_argument(
        "--dry-run",
        help="If set, this runs all migrations locally instead of against prod/staging databases.",
        action="store_true",
    )
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    # TODO(#14842): Remove this once prod-data-client is deprecated
    parser.add_argument(
        "--ssl-cert-path",
        type=str,
        default=None,
        help="The path to the folder where the certs live. "
        "This argument is required if running against live databases.",
    )
    parser.add_argument(
        "--skip-db-name-check",
        action="store_true",
        help="DO NOT SET unless you know what you're doing. "
        "If set, this skips the check to see whether you're running against the intended database.",
    )
    parser.add_argument(
        "--confirm-hash",
        type=str,
        help="If included, skips the manual git branch confirmation and verifies that the hash is as expected.",
    )
    parser.add_argument(
        "--using-proxy",
        action="store_true",
        help="If included, SQLAlchemy will be configured to connect to the Cloud SQL Proxy.",
    )

    return parser


def main(
    schema_type: SchemaType,
    repo_root: str,
    dry_run: bool,
    skip_db_name_check: bool,
    confirm_hash: Optional[str] = None,
    ssl_cert_path: Optional[str] = None,
    using_proxy: bool = False,
) -> None:
    """
    Invokes the main code path for running migrations.

    This checks for user validations that the database and branches are correct and then runs existing pending
    migrations.
    """
    is_prod = metadata.project_id() == GCP_PROJECT_PRODUCTION

    if is_prod:
        logging.info("RUNNING AGAINST PRODUCTION\n")

    if not skip_db_name_check:
        confirm_correct_db_instance(schema_type)

    confirm_correct_git_branch(repo_root, confirm_hash=confirm_hash)

    # Run migrations
    for database_key, _engine in EngineIteratorDelegate.iterate_and_connect_to_engines(
        schema_type,
        using_proxy=using_proxy,
        ssl_cert_path=ssl_cert_path,
        dry_run=dry_run,
    ):
        try:
            logging.info(
                "*** Starting postgres migrations for schema [%s], db_name [%s] ***",
                database_key.schema_type,
                database_key.db_name,
            )
            config = alembic.config.Config(database_key.alembic_file)
            alembic.command.upgrade(config, "head")
        except Exception as e:
            logging.error("Migrations failed to run: %s", e)
            sys.exit(1)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        main(
            args.database,
            args.repo_root,
            args.dry_run,
            args.skip_db_name_check,
            args.confirm_hash,
            args.ssl_cert_path,
            args.using_proxy,
        )
