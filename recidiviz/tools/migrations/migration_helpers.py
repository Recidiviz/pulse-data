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
import abc
import logging
import sys
from typing import Generator, List, Optional, Tuple

from pygit2.repository import Repository
from sqlalchemy.engine import Engine

from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.server_config import database_keys_for_schema_type
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult
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


class EngineIteratorDelegate(abc.ABC):
    """Delegate class for iterating / connecting to SQLAlchemy engines"""

    def get_database_keys(self, schema_type: SchemaType) -> List[SQLAlchemyDatabaseKey]:
        return database_keys_for_schema_type(schema_type)

    @abc.abstractmethod
    def setup(self) -> None:
        ...

    @abc.abstractmethod
    def setup_engine(
        self,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str] = None,
    ) -> dict[str, Optional[str]]:
        ...

    @abc.abstractmethod
    def get_engine(
        self,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str] = None,
    ) -> Engine:
        ...

    @abc.abstractmethod
    def teardown_engine(self, database_key: SQLAlchemyDatabaseKey) -> None:
        ...

    @staticmethod
    def iterate_and_connect_to_engines(
        schema_type: SchemaType,
        dry_run: Optional[bool] = False,
        secret_prefix_override: Optional[str] = None,
    ) -> Generator[Tuple[SQLAlchemyDatabaseKey, Engine], None, None]:
        delegate: EngineIteratorDelegate

        if dry_run:
            delegate = DryRunEngineIteratorDelegate()
        else:
            delegate = CloudSQLProxyEngineIteratorDelegate()

        yield from iterate_and_connect_to_engines(
            schema_type=schema_type,
            delegate=delegate,
            secret_prefix_override=secret_prefix_override,
        )


class CloudSQLProxyEngineIteratorDelegate(EngineIteratorDelegate):
    """Engine iterator delegate for connecting to Cloud SQL instances locally using the Cloud SQL Proxy"""

    def setup(self) -> None:
        pass

    def setup_engine(
        self,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str] = None,
    ) -> dict[str, Optional[str]]:
        return SQLAlchemyEngineManager.update_sqlalchemy_env_vars(
            database_key=database_key,
            using_proxy=True,
            secret_prefix_override=secret_prefix_override,
        )

    def get_engine(
        self,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str] = None,
    ) -> Engine:
        return SQLAlchemyEngineManager.get_engine_for_database_with_proxy(
            database_key=database_key, secret_prefix_override=secret_prefix_override
        )

    def teardown_engine(self, database_key: SQLAlchemyDatabaseKey) -> None:
        pass


class DryRunEngineIteratorDelegate(EngineIteratorDelegate):
    """Engine iterator delegate which spins up a local postgres instance and connects engines to it"""

    postgres_launch_result: OnDiskPostgresLaunchResult

    def get_database_keys(self, schema_type: SchemaType) -> List[SQLAlchemyDatabaseKey]:
        return [SQLAlchemyDatabaseKey.canonical_for_schema(schema_type)]

    def setup(self) -> None:
        if not local_postgres_helpers.can_start_on_disk_postgresql_database():
            logging.error("pg_ctl is not installed. Cannot perform a dry-run.")
            sys.exit(1)

        logging.info("Creating a dry-run...")

    def setup_engine(
        self,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str] = None,
    ) -> dict[str, Optional[str]]:
        self.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )
        overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars(
                self.postgres_launch_result
            )
        )
        return overridden_env_vars

    def get_engine(
        self,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str] = None,
    ) -> Engine:
        return SQLAlchemyEngineManager.init_engine_for_postgres_instance(
            database_key=database_key,
            db_url=local_persistence_helpers.postgres_db_url_from_env_vars(),
        )

    def teardown_engine(self, database_key: SQLAlchemyDatabaseKey) -> None:
        if not self.postgres_launch_result:
            raise RuntimeError("teardown_engine called before setup_engine")

        SQLAlchemyEngineManager.teardown_engine_for_database_key(
            database_key=database_key
        )

        try:
            logging.info("Stopping local postgres database")
            local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
                self.postgres_launch_result
            )
        except Exception as e2:
            logging.error("Error cleaning up postgres: %s", e2)


def iterate_and_connect_to_engines(
    schema_type: SchemaType,
    *,
    delegate: EngineIteratorDelegate,
    secret_prefix_override: Optional[str] = None,
) -> Generator[Tuple[SQLAlchemyDatabaseKey, Engine], None, None]:
    """Returns an iterator that contains a `database_key` and corresponding `engine`.
    The engine is torn down after iteration"""
    delegate.setup()

    for database_key in delegate.get_database_keys(schema_type):
        overridden_env_vars = delegate.setup_engine(
            database_key, secret_prefix_override=secret_prefix_override
        )

        try:
            yield database_key, delegate.get_engine(
                database_key=database_key, secret_prefix_override=secret_prefix_override
            )
        finally:
            local_postgres_helpers.restore_local_env_vars(overridden_env_vars)

            delegate.teardown_engine(database_key)
