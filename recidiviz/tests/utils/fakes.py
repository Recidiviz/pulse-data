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
import pwd
import sqlite3
import subprocess
import threading
from typing import Callable, Optional, Set

from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm.session import close_all_sessions

from recidiviz.persistence.database.base_schema import OperationsBase, StateBase, JailsBase
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_engine_manager import SQLAlchemyEngineManager
from recidiviz.tests.persistence.database.schema_entity_converter.test_base_schema import TestBase
from recidiviz.utils import environment


DECLARATIVE_BASES = [OperationsBase, StateBase, JailsBase, TestBase]
LINUX_TEST_DB_OWNER_NAME = 'recidiviz_test_db_owner'
TEST_POSTGRES_DB_NAME = 'recidiviz_test_db'
TEST_POSTGRES_USER_NAME = 'recidiviz_test_usr'

_in_memory_sqlite_connection_thread_ids: Set[int] = set()


def use_in_memory_sqlite_database(declarative_base: DeclarativeMeta) -> None:
    """Creates a new SqlDatabase object used to communicate to a fake in-memory
    sqlite database.

    This includes:
    1. Creates a new in memory sqlite database engine
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


def _get_run_as_user_fn(password_record: pwd.struct_passwd) -> Callable[[], None]:
    """Returns a function that modifes the current OS user and group to those given.

    To be used in preexec_fn when creating new subprocesses."""

    def set_ids() -> None:
        # Must set group id first. If user id is set first, then that user won't have permission to modify the group.
        os.setgid(password_record.pw_gid)
        os.setuid(password_record.pw_uid)

    return set_ids


def _is_root_user() -> bool:
    """Returns True if we are currently running as root, otherwise False."""
    return os.getuid() == 0


def _run_command(command: str, assert_success: bool = True, as_user: Optional[pwd.struct_passwd] = None) -> str:
    """Runs the given command.

    Runs the command as a different OS user if `as_user` is not None. If the command succeeds, returns any output from
    stdout. If the command fails and `assert_success` is set, raises an error.
    """
    # pylint: disable=subprocess-popen-preexec-fn
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                            preexec_fn=_get_run_as_user_fn(as_user) if as_user else None)
    try:
        out, err = proc.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
        raise RuntimeError(f'Command timed out: `{command}`\n{err}\n{out}')

    if assert_success and proc.returncode != 0:
        raise RuntimeError(f'Command failed: `{command}`\n{err}\n{out}')
    return out


def start_on_disk_postgresql_database() -> None:
    """Starts and initializes a local postgres database for use in tests. Should be called in the setUpClass function so
    this only runs once per test class."""
    if environment.in_gae():
        raise ValueError('Running test-only code in Google App Engine.')

    # Create the directory to use for the postgres database, if it does not already exist.
    os.makedirs('/usr/local/var/postgres', exist_ok=True)

    # The database can't be owned by root so create a separate OS user to own the database if we are currently root.
    password_record = None
    if _is_root_user():
        # `useradd` is a Linux command so this raises an error on MacOS. We expect tests running on MacOs will never be
        # run as root anyway so this is okay. This will also fail if the user already exists, so we ignore failure.
        _run_command(f'useradd {LINUX_TEST_DB_OWNER_NAME}', assert_success=False)
        # Get the password record for the new user, fails if the user does not exist.
        password_record = pwd.getpwnam(LINUX_TEST_DB_OWNER_NAME)

        # Ensure the necessary directories are owned by the postgres user.
        os.chown('/usr/local/var/postgres', uid=password_record.pw_uid, gid=password_record.pw_gid)
        os.chown('/var/run/postgresql', uid=password_record.pw_uid, gid=password_record.pw_gid)

    # Start the local postgres server.
    # initdb will fail if the database system already exists, ignore that failure and continue.
    # Write logs to file so that pg_ctl closes its stdout file descriptor when it moves to the background, otherwise
    # the subprocess will hang.
    _run_command('pg_ctl -D /usr/local/var/postgres -l /tmp/postgres initdb', as_user=password_record,
                 assert_success=False)
    _run_command('pg_ctl -D /usr/local/var/postgres -l /tmp/postgres start', as_user=password_record)

    # Create a user and database within postgres.
    # These will fail if they already exist, ignore that failure and continue.
    _run_command(f'createuser {TEST_POSTGRES_USER_NAME}', as_user=password_record, assert_success=False)
    _run_command(f'createdb -O {TEST_POSTGRES_USER_NAME} {TEST_POSTGRES_DB_NAME}', as_user=password_record,
                 assert_success=False)


def stop_and_clear_on_disk_postgresql_database() -> None:
    """Drops all tables in the local postgres database and stops the postgres server. Should be called in the
    tearDownClass function so this only runs once per test class."""
    if environment.in_gae():
        raise ValueError('Running test-only code in Google App Engine.')

    _clear_on_disk_postgresql_database()
    _stop_on_disk_postgresql_database()

def _stop_on_disk_postgresql_database():
    # If the current user is root then the database is owned by a separate OS test user. Run as them to stop the server.
    password_record = pwd.getpwnam(LINUX_TEST_DB_OWNER_NAME) if _is_root_user() else None
    _run_command('pg_ctl -D /usr/local/var/postgres -l /tmp/poostgres stop', as_user=password_record)

def _clear_on_disk_postgresql_database():
    # Ensure all sessions are closed, otherwise `drop_all` below may hang.
    close_all_sessions()

    for declarative_base in DECLARATIVE_BASES:
        use_on_disk_postgresql_database(declarative_base)
        declarative_base.metadata.drop_all(SQLAlchemyEngineManager.get_engine_for_schema_base(declarative_base))
        SQLAlchemyEngineManager.teardown_engine_for_schema(declarative_base)


def use_on_disk_postgresql_database(declarative_base: DeclarativeMeta) -> None:
    """Connects SQLAlchemy to a local test postgres server. Should be called after the test database and user have
    already been initialized.

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
