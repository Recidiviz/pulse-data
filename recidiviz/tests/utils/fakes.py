# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Initialize our database schema for in-memory testing via sqlite3."""
import os
import sqlite3
import threading
from typing import Set

from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm.session import close_all_sessions

from recidiviz.persistence.database.base_schema import OperationsBase, StateBase, JailsBase
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_engine_manager import SQLAlchemyEngineManager
from recidiviz.tests.persistence.database.schema_entity_converter.test_base_schema import TestBase
from recidiviz.utils import environment


DECLARATIVE_BASES = [OperationsBase, StateBase, JailsBase, TestBase]
TEST_POSTGRES_DB_NAME = 'recidiviz_test_db'
TEST_POSTGRES_USER_NAME = 'recidiviz_test_usr'

_in_memory_sqlite_connection_thread_ids: Set[int] = set()


def use_in_memory_sqlite_database(declarative_base: DeclarativeMeta) -> None:
    """Creates a new SqlDatabase object used to communicate to a fake in-memory
    sqlite database.

    This includes:
    1. Creates a new in memory sqlite database engine with a shared cache, allowing access from any test thread.
    2. Create all tables in the newly created sqlite database
    3. Bind the global SessionMaker to the new fake database engine

    This will assert if an engine has already been initialized for this schema - you must use
    teardown_in_memory_sqlite_databases() to do post-test cleanup, otherwise subsequent tests will fail. It will also
    assert if you attempt to create connections from multiple threads within the context of a single test - SQLite does
    not handle multi-threading well and will often lock or crash when used in a multi-threading scenario.
    """
    if environment.in_gae():
        raise ValueError('Running test-only code in Google App Engine.')

    def connection_creator() -> sqlite3.Connection:
        global _in_memory_sqlite_connection_thread_ids

        thread_id = threading.get_ident()
        if thread_id in _in_memory_sqlite_connection_thread_ids:
            raise ValueError('Accessing SQLite in-memory database on multiple threads. Either you forgot to call '
                             'teardown_in_memory_sqlite_databases() or you should be using a persistent postgres DB.')

        _in_memory_sqlite_connection_thread_ids.add(thread_id)
        connection = sqlite3.connect('file::memory:', uri=True)

        # Configures SQLite to enforce foreign key constraints
        connection.execute('PRAGMA foreign_keys = ON;')

        return connection

    SQLAlchemyEngineManager.init_engine_for_db_instance(
        db_url='sqlite:///:memory:',
        schema_base=declarative_base,
        creator=connection_creator
    )


def teardown_in_memory_sqlite_databases():
    """Cleans up state after a test started with use_in_memory_sqlite_database() is complete."""
    if environment.in_gae():
        raise ValueError('Running test-only code in Google App Engine.')

    global _in_memory_sqlite_connection_thread_ids
    _in_memory_sqlite_connection_thread_ids.clear()
    SQLAlchemyEngineManager.teardown_engines()


def use_on_disk_sqlite_database(declarative_base: DeclarativeMeta) -> None:
    """Creates a new SqlDatabase object used to communicate to an on-disk
    sqlite database.

    This includes:
    1. Creates a new on-disk sqlite database engine
    2. Create all tables in the newly created sqlite database
    3. Bind the global SessionMaker to the new database engine
    """
    if environment.in_gae():
        raise ValueError('Running test-only code in Google App Engine.')

    SQLAlchemyEngineManager.init_engine_for_db_instance(
        db_url='sqlite:///recidiviz.db',
        schema_base=declarative_base)


def start_on_disk_postgresql_database() -> None:
    """Starts and initializes a local postgres database for use in tests. Should be called in the setUpClass function so
    this only runs once per test class."""
    if environment.in_gae():
        raise ValueError('Running test-only code in Google App Engine.')

    if not environment.in_travis():
        # If this test is running locally and not in Travis, we must start the local postgres server. In Travis, the
        # postgres server has already been initialized via config in .travis.yaml.
        os.system('pg_ctl -D /usr/local/var/postgres start &> /dev/null')

        # Create a user with the same name as the user initialized in .travis.yaml (no password set)
        os.system(f'createuser {TEST_POSTGRES_USER_NAME} &> /dev/null')

        # Create a database with the same name as the database initialized in .travis.yaml
        os.system(f'createdb -O {TEST_POSTGRES_USER_NAME} {TEST_POSTGRES_DB_NAME} &> /dev/null')


def stop_and_clear_on_disk_postgresql_database() -> None:
    """Drops all tables in the local postgres database and stops the postgres server. Should be called in the
    tearDownClass function so this only runs once per test class."""
    if environment.in_gae():
        raise ValueError('Running test-only code in Google App Engine.')

    # Ensure all sessions are closed, otherwise `drop_all` below may hang.
    close_all_sessions()

    for declarative_base in DECLARATIVE_BASES:
        use_on_disk_postgresql_database(declarative_base)
        declarative_base.metadata.drop_all(SQLAlchemyEngineManager.get_engine_for_schema_base(declarative_base))
        SQLAlchemyEngineManager.teardown_engine_for_schema(declarative_base)

    if not environment.in_travis():
        # If we are running locally, we must stop the local postgres server
        os.system('pg_ctl -D /usr/local/var/postgres stop &> /dev/null')


def use_on_disk_postgresql_database(declarative_base: DeclarativeMeta) -> None:
    """Connects SQLAlchemy to a local test postgres server. Should be called after the test database and user have
    already been initialized. In Travis, these are set up via commands in the .travis.yaml, locally, you must have
    first called start_on_disk_postgresql_database().

    This includes:
    1. Create all tables in the newly created sqlite database
    2. Bind the global SessionMaker to the new database engine
    """
    if environment.in_gae():
        raise ValueError('Running test-only code in Google App Engine.')

    if declarative_base not in DECLARATIVE_BASES:
        raise ValueError(f"Unexpected declarative base: {declarative_base}.")

    SQLAlchemyEngineManager.init_engine_for_postgres_instance(
        db_url=f'postgresql://{TEST_POSTGRES_USER_NAME}:@localhost:5432/{TEST_POSTGRES_DB_NAME}',
        schema_base=declarative_base)


def teardown_on_disk_postgresql_database(declarative_base: DeclarativeMeta) -> None:
    """Clears state in an on-disk postgres database for a given schema, for use once a single test has completed. As an
    optimization, does not actually drop tables, just clears them. As a best practice, you should call
    stop_and_clear_on_disk_postgresql_database() once all tests in a test class are complete to actually drop the
    tables.
    """
    if environment.in_gae():
        raise ValueError('Running test-only code in Google App Engine.')

    session = SessionFactory.for_schema_base(declarative_base)
    try:
        for table in reversed(declarative_base.metadata.sorted_tables):
            session.execute(table.delete())
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

    SQLAlchemyEngineManager.teardown_engine_for_schema(declarative_base)
