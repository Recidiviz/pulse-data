# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""This module sets up a local postgres instance using SQLAlchemyDatabaseKey and SQLAlchemyEngineManager
for use in scripts and testing."""

import os
from collections.abc import Generator
from contextlib import contextmanager
from typing import Dict, Optional

from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm.session import _sessions

from recidiviz.persistence.database.async_session_factory import AsyncSessionFactory
from recidiviz.persistence.database.constants import (
    SQLALCHEMY_DB_HOST,
    SQLALCHEMY_DB_NAME,
    SQLALCHEMY_DB_PASSWORD,
    SQLALCHEMY_DB_PORT,
    SQLALCHEMY_DB_USER,
)
from recidiviz.persistence.database.schema.case_triage.schema import CaseTriageBase
from recidiviz.persistence.database.schema.identity.schema import IdentityBase
from recidiviz.persistence.database.schema.insights.schema import InsightsBase
from recidiviz.persistence.database.schema.justice_counts.schema import (
    JusticeCountsBase,
)
from recidiviz.persistence.database.schema.operations.schema import OperationsBase
from recidiviz.persistence.database.schema.pathways.schema import PathwaysBase
from recidiviz.persistence.database.schema.persistence.schema import PersistenceBase
from recidiviz.persistence.database.schema.public_pathways.schema import (
    PublicPathwaysBase,
)
from recidiviz.persistence.database.schema.state.schema import StateBase
from recidiviz.persistence.database.schema.workflows.schema import WorkflowsBase
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_async_engine_manager import (
    SQLAlchemyAsyncEngineManager,
)
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tests.persistence.database.schema_entity_converter.fake_base_schema import (
    FakeBase,
)
from recidiviz.tools.postgres.local_postgres_helpers import (
    TEST_POSTGRES_USER_NAME,
    OnDiskPostgresLaunchResult,
    start_on_disk_postgresql_database,
    start_persistent_on_disk_postgresql_database,
    stop_and_clear_on_disk_postgresql_database,
    stop_on_disk_postgresql_database,
)
from recidiviz.utils import environment


def update_local_sqlalchemy_postgres_env_vars(
    launch_result: OnDiskPostgresLaunchResult,
) -> Dict[str, Optional[str]]:
    """Updates the appropriate env vars for SQLAlchemy to talk to a locally created Postgres instance.

    It returns the old set of env variables that were overridden.
    """
    sqlalchemy_vars = [
        SQLALCHEMY_DB_NAME,
        SQLALCHEMY_DB_HOST,
        SQLALCHEMY_DB_PORT,
        SQLALCHEMY_DB_USER,
        SQLALCHEMY_DB_PASSWORD,
    ]
    original_values = {env_var: os.environ.get(env_var) for env_var in sqlalchemy_vars}

    os.environ[SQLALCHEMY_DB_NAME] = launch_result.database_name
    os.environ[SQLALCHEMY_DB_HOST] = "localhost"
    os.environ[SQLALCHEMY_DB_USER] = TEST_POSTGRES_USER_NAME
    os.environ[SQLALCHEMY_DB_PORT] = str(launch_result.port)
    os.environ[SQLALCHEMY_DB_PASSWORD] = ""

    return original_values


@environment.local_only
def postgres_db_url_from_env_vars() -> URL:
    return URL.create(
        drivername="postgresql",
        username=os.getenv(SQLALCHEMY_DB_USER),
        password=os.getenv(SQLALCHEMY_DB_PASSWORD),
        host=os.getenv(SQLALCHEMY_DB_HOST),
        port=os.getenv(SQLALCHEMY_DB_PORT),
        database=os.getenv(SQLALCHEMY_DB_NAME),
    )


@environment.local_only
def teardown_on_disk_postgresql_database(database_key: SQLAlchemyDatabaseKey) -> None:
    """Clears state in an on-disk postgres database for a given schema, for use once a single test has completed. As an
    optimization, does not actually drop tables, just clears them. As a best practice, you should call
    stop_and_clear_on_disk_postgresql_database() once all tests in a test class are complete to actually drop the
    tables.
    """
    # Ensure all sessions are closed, otherwise the below may hang.
    # Note: close_all_sessions() sometimes raises a RuntimeError about the size of the
    # underlying dictionary changing, despite the IterationGuard used during iteration.
    # It isn't clear why this is happening, but as an attempt to fix, first copy the
    # values to a list and then iterate over that list ourselves.
    for session in list(_sessions.values()):
        session.close()

    for table in reversed(database_key.declarative_meta.metadata.sorted_tables):
        with SessionFactory.using_database(database_key) as session:
            try:
                session.execute(table.delete())
            except ProgrammingError:
                pass

    SQLAlchemyEngineManager.teardown_engine_for_database_key(database_key=database_key)


@environment.local_only
async def async_teardown_on_disk_postgresql_database(
    database_key: SQLAlchemyDatabaseKey,
) -> None:
    """Clears async state in an on-disk postgres database for a given schema, for use once a
    single test has completed. As an optimization, does not actually drop tables, just
    clears them. As a best practice, you should call stop_and_clear_on_disk_postgresql_database()
    once all tests in a test class are complete to actually drop the tables.
    """
    # Ensure all sessions are closed, otherwise the below may hang.
    # Note: close_all_sessions() sometimes raises a RuntimeError about the size of the
    # underlying dictionary changing, despite the IterationGuard used during iteration.
    # It isn't clear why this is happening, but as an attempt to fix, first copy the
    # values to a list and then iterate over that list ourselves.
    for session in list(_sessions.values()):
        session.close()

    for table in reversed(database_key.declarative_meta.metadata.sorted_tables):
        async with AsyncSessionFactory.using_database(database_key) as session:
            try:
                await session.execute(table.delete())
                await session.commit()
            except ProgrammingError:
                pass

    SQLAlchemyAsyncEngineManager.teardown_engine_for_database_key(
        database_key=database_key
    )


@environment.local_only
def use_on_disk_postgresql_database(
    launch_result: OnDiskPostgresLaunchResult,
    database_key: SQLAlchemyDatabaseKey,
    create_tables: Optional[bool] = True,
    engine: Engine | None = None,
) -> Engine:
    """Connects SQLAlchemy to a local test postgres server. Should be called after the test database and user have
    already been initialized.
    This includes:
    1. Create all tables in the newly created Postgres database
    2. Bind the global SessionMaker to the new database engine
    """
    if database_key.declarative_meta not in DECLARATIVE_BASES:
        raise ValueError(f"Unexpected database key: {database_key}.")

    # The default behavior of use_on_disk_postgresql_database initializes an engine
    # using the default on disk postgres database name. This causes issues for state-segmented databases
    # as initializing the engine for distinct database keys ends up connecting to the same database.
    # Users can pass an engine to avoid this behavior
    engine = engine or SQLAlchemyEngineManager.init_engine_for_postgres_instance(
        database_key=database_key,
        db_url=launch_result.url(),
    )

    with SessionFactory.using_database(database_key) as session:
        session.execute(
            f"ALTER DATABASE {launch_result.database_name} SET TIMEZONE TO 'UTC'"
        )
    # ALTER DATABASE ... SET TIMEZONE only applies to connections opened after it
    # commits. Dispose the pool so every later connection reconnects and inherits
    # UTC, rather than reusing a connection that still carries the server's initdb
    # default (the developer's local timezone on a non-UTC machine).
    engine.dispose()

    if create_tables:
        # Auto-generate all tables that exist in our schema in this database
        database_key.declarative_meta.metadata.create_all(engine)

    return engine


@environment.local_only
@contextmanager
def local_postgres(
    *,
    database_key: SQLAlchemyDatabaseKey,
    persistent_data_dir: str | None,
) -> Generator[OnDiskPostgresLaunchResult, None, None]:
    """Spins up an on-disk Postgres bound to |database_key|'s schema for the
    duration of the block, connecting SQLAlchemy to it via
    use_on_disk_postgresql_database, and yields the launch result so callers can
    build their own connection to it.

    When |persistent_data_dir| is None, the database is thrown away on exit: its
    tracking does NOT persist across invocations, so every block starts with no
    prior data.

    When |persistent_data_dir| is a path, the cluster lives in that data
    directory and survives the process, so a later block reconnects to the same
    data. The server is stopped on exit but the data is left in place. Note that
    the schema is created but never migrated: create_all only adds missing
    tables, so after a schema change you must delete the data directory to pick
    it up.
    """
    if persistent_data_dir is not None:
        launch_result = start_persistent_on_disk_postgresql_database(
            persistent_data_dir
        )
    else:
        launch_result = start_on_disk_postgresql_database()
    try:
        # create_all is idempotent, so this is safe whether the cluster is fresh
        # or a persistent cluster already holding prior data. It runs inside the
        # try so a failure here still tears the just-started server down rather
        # than orphaning it.
        use_on_disk_postgresql_database(launch_result, database_key)
        yield launch_result
    finally:
        if persistent_data_dir is not None:
            # Stop the server but preserve the data so the next block can resume.
            # Unregister the engine (without clearing rows, unlike
            # teardown_on_disk_postgresql_database) so re-entering this block in
            # the same process doesn't hit an already-initialized database key.
            stop_on_disk_postgresql_database(launch_result)
            SQLAlchemyEngineManager.teardown_engine_for_database_key(
                database_key=database_key
            )
        else:
            teardown_on_disk_postgresql_database(database_key)
            stop_and_clear_on_disk_postgresql_database(launch_result)


DECLARATIVE_BASES = [
    OperationsBase,
    StateBase,
    JusticeCountsBase,
    FakeBase,
    CaseTriageBase,
    PathwaysBase,
    PersistenceBase,
    PublicPathwaysBase,
    WorkflowsBase,
    InsightsBase,
    IdentityBase,
]
