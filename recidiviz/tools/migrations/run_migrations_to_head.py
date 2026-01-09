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
This script runs alembic migrations against production and staging database instances.
It fetches the appropriate secrets and validates with the user that the intended migrations
are being run before executing them.

Example usage:

# Dry run on the jails database:
uv run python -m recidiviz.tools.migrations.run_migrations_to_head \
    --database JAILS \
    --project-id recidiviz-staging \
    --dry-run

# Run against the live jails database:
python -m recidiviz.tools.migrations.run_migrations_to_head \
    --database JAILS \
    --project-id recidiviz-staging
"""
import argparse
import logging
import sys
from typing import Optional

import alembic.config

from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.tools.migrations.migration_helpers import (
    EngineIteratorDelegate,
    confirm_correct_db_instance,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils import metadata
from recidiviz.utils.environment import (
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
)
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
        "--dry-run",
        help="If set, this runs all migrations locally instead of against prod/staging databases.",
        action="store_true",
    )
    parser.add_argument(
        "--project-id",
        choices=[
            GCP_PROJECT_STAGING,
            GCP_PROJECT_PRODUCTION,
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument(
        "--skip-db-name-check",
        action="store_true",
        help="DO NOT SET unless you know what you're doing. "
        "If set, this skips the check to see whether you're running against the intended database.",
    )
    parser.add_argument(
        "--no-launch-proxy",
        action="store_true",
        help="If specified, the Cloud SQL Proxy will not be launched. "
        "Inside of Cloud Build, the proxy is already launched when this script is run",
    )
    return parser


def main(
    schema_type: SchemaType,
    dry_run: bool,
    skip_db_name_check: bool,
    secret_prefix_override: Optional[str] = None,
) -> None:
    """
    Invokes the main code path for running migrations.

    This checks for user validations that the database and branches are correct and then runs existing pending
    migrations.
    """
    if metadata.running_against(GCP_PROJECT_PRODUCTION):
        logging.info("RUNNING AGAINST PRODUCTION\n")

    if not skip_db_name_check:
        confirm_correct_db_instance(schema_type)

    # Run migrations
    for database_key, _engine in EngineIteratorDelegate.iterate_and_connect_to_engines(
        schema_type, dry_run=dry_run, secret_prefix_override=secret_prefix_override
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

    # We have two sets of Justice Counts DB instances -- one original set in the
    # Recidiviz GCP projects and one new set in the Justice Counts GCP projects.
    # The secrets for the original instance are prefixed with `justice_counts`,
    # and the secrets for the new instance are prefixed with `justice_counts_v2`.
    # TODO(#23253): Remove when Publisher is migrated to JC GCP project.
    secret_prefix = None
    if args.project_id in [
        GCP_PROJECT_JUSTICE_COUNTS_STAGING,
        GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    ]:
        secret_prefix = JUSTICE_COUNTS_DB_SECRET_PREFIX

    def _run_main(_args: argparse.Namespace) -> None:
        main(
            schema_type=_args.database,
            dry_run=_args.dry_run,
            skip_db_name_check=_args.skip_db_name_check,
            secret_prefix_override=secret_prefix,
        )

    with local_project_id_override(args.project_id):
        if args.no_launch_proxy:
            _run_main(args)
        else:
            with cloudsql_proxy_control.connection(
                schema_type=args.database,
                prompt=not args.skip_db_name_check,
                secret_prefix_override=secret_prefix,
            ):
                _run_main(args)
