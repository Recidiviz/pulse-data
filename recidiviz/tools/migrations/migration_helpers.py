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
Helper functions for confirming user input when running migrations.
"""
import logging
import sys
from typing import Generator, Optional, Tuple

from pygit2.repository import Repository
from sqlalchemy.engine import Engine

from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.server_config import database_keys_for_schema_type
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation


def confirm_correct_db_instance(database: SchemaType) -> None:
    dbname = SQLAlchemyEngineManager.get_stripped_cloudsql_instance_id(database)
    if dbname is None:
        logging.error("Could not find database instance.")
        logging.error("Exiting...")
        sys.exit(1)

    prompt_for_confirmation(f"Running migrations on {dbname}.", dbname)


def confirm_correct_git_branch(
    repo_root: str, confirm_hash: Optional[str] = None
) -> None:
    try:
        repo = Repository(repo_root)
    except Exception as e:
        logging.error("improper project root provided: %s", e)
        sys.exit(1)

    current_branch = repo.head.shorthand

    if confirm_hash is not None:
        if confirm_hash != str(repo.head.target):
            logging.warning(
                "Hash does not match current branch.\nConfirmation aborting."
            )
            sys.exit(1)
        else:
            return

    prompt_for_confirmation(
        f"This script will execute migrations based on the contents of the current branch ({current_branch}).",
        current_branch,
    )


def iterate_and_connect_to_engines(
    schema_type: SchemaType,
    *,
    ssl_cert_path: Optional[str] = None,
    dry_run: bool = True,
) -> Generator[Tuple[SQLAlchemyDatabaseKey, Engine], None, None]:
    """Returns an iterator that contains a `database_key` and corresponding `engine`.
    The engine is torn down after iteration"""
    if dry_run:
        if not local_postgres_helpers.can_start_on_disk_postgresql_database():
            logging.error("pg_ctl is not installed. Cannot perform a dry-run.")
            sys.exit(1)
        logging.info("Creating a dry-run...")
    else:
        if not ssl_cert_path:
            logging.error(
                "SSL certificates are required when running against live databases"
            )
            sys.exit(1)
        logging.info("Using SSL certificate path: %s", ssl_cert_path)

    database_keys = (
        [SQLAlchemyDatabaseKey.canonical_for_schema(schema_type)]
        if dry_run
        else database_keys_for_schema_type(schema_type)
    )

    for database_key in database_keys:
        if dry_run:
            overridden_env_vars = (
                local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
            )
            db_dir = local_postgres_helpers.start_on_disk_postgresql_database()
        else:
            overridden_env_vars = SQLAlchemyEngineManager.update_sqlalchemy_env_vars(
                database_key=database_key,
                ssl_cert_path=ssl_cert_path,
                migration_user=True,
            )

        try:
            if dry_run:
                engine = SQLAlchemyEngineManager.init_engine_for_postgres_instance(
                    database_key=database_key,
                    db_url=local_postgres_helpers.postgres_db_url_from_env_vars(),
                )
            else:
                engine = SQLAlchemyEngineManager.get_engine_for_database_with_ssl_certs(
                    database_key=database_key, ssl_cert_path=ssl_cert_path
                )

            yield database_key, engine
        finally:
            if dry_run:
                SQLAlchemyEngineManager.teardown_engine_for_database_key(
                    database_key=database_key
                )

            local_postgres_helpers.restore_local_env_vars(overridden_env_vars)

            if dry_run:
                try:
                    logging.info("Stopping local postgres database")
                    local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
                        db_dir
                    )
                except Exception as e2:
                    logging.error("Error cleaning up postgres: %s", e2)
