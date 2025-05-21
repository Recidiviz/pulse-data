# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
from recidiviz.persistence.database.schema.insights.schema import InsightsBase
from recidiviz.persistence.database.schema.justice_counts.schema import (
    JusticeCountsBase,
)
from recidiviz.persistence.database.schema.operations.schema import OperationsBase
from recidiviz.persistence.database.schema.pathways.schema import PathwaysBase
from recidiviz.persistence.database.schema.resource_search.schema import (
    ResourceSearchBase,
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
    get_on_disk_postgres_database_name,
    get_on_disk_postgres_port,
    on_disk_postgres_db_url,
)
from recidiviz.utils import environment


def update_local_sqlalchemy_postgres_env_vars() -> Dict[str, Optional[str]]:
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

    os.environ[SQLALCHEMY_DB_NAME] = get_on_disk_postgres_database_name()
    os.environ[SQLALCHEMY_DB_HOST] = "localhost"
    os.environ[SQLALCHEMY_DB_USER] = TEST_POSTGRES_USER_NAME
    os.environ[SQLALCHEMY_DB_PORT] = str(get_on_disk_postgres_port())
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
        db_url=on_disk_postgres_db_url(),
    )

    with SessionFactory.using_database(database_key) as session:
        session.execute(
            f"ALTER DATABASE {get_on_disk_postgres_database_name()} SET TIMEZONE TO 'UTC'"
        )

    if create_tables:
        # Auto-generate all tables that exist in our schema in this database
        database_key.declarative_meta.metadata.create_all(engine)

    return engine


DECLARATIVE_BASES = [
    OperationsBase,
    StateBase,
    JusticeCountsBase,
    FakeBase,
    CaseTriageBase,
    PathwaysBase,
    WorkflowsBase,
    InsightsBase,
    ResourceSearchBase,
]
