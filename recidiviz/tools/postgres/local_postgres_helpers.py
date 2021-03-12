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
"""This module generates a local postgres instance for use in scripts in testing."""
import os
import pwd
import shutil
import subprocess
import tempfile
from typing import Callable, Optional, Dict

from sqlalchemy.orm.session import close_all_sessions

from recidiviz.persistence.database import (
    SQLALCHEMY_DB_NAME,
    SQLALCHEMY_DB_HOST,
    SQLALCHEMY_USE_SSL,
    SQLALCHEMY_DB_USER,
    SQLALCHEMY_DB_PASSWORD,
)
from recidiviz.persistence.database.base_schema import (
    OperationsBase,
    StateBase,
    JailsBase,
    JusticeCountsBase,
    CaseTriageBase,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tests.persistence.database.schema_entity_converter.test_base_schema import (
    TestBase,
)
from recidiviz.utils import environment

DECLARATIVE_BASES = [
    OperationsBase,
    StateBase,
    JailsBase,
    JusticeCountsBase,
    TestBase,
    CaseTriageBase,
]
LINUX_TEST_DB_OWNER_NAME = "recidiviz_test_db_owner"
TEST_POSTGRES_DB_NAME = "recidiviz_test_db"
TEST_POSTGRES_USER_NAME = "recidiviz_test_usr"
DEFAULT_POSTGRES_DATA_DIRECTORY = "/usr/local/var/postgres"


def update_local_sqlalchemy_postgres_env_vars() -> Dict[str, Optional[str]]:
    """Updates the appropriate env vars for SQLAlchemy to talk to a locally created Postgres instance.

    It returns the old set of env variables that were overridden.
    """
    sqlalchemy_vars = [
        SQLALCHEMY_DB_NAME,
        SQLALCHEMY_DB_HOST,
        SQLALCHEMY_USE_SSL,
        SQLALCHEMY_DB_USER,
        SQLALCHEMY_DB_PASSWORD,
    ]
    original_values = {env_var: os.environ.get(env_var) for env_var in sqlalchemy_vars}

    os.environ[SQLALCHEMY_DB_NAME] = TEST_POSTGRES_DB_NAME
    os.environ[SQLALCHEMY_DB_HOST] = "localhost"
    os.environ[SQLALCHEMY_USE_SSL] = "0"
    os.environ[SQLALCHEMY_DB_USER] = TEST_POSTGRES_USER_NAME
    os.environ[SQLALCHEMY_DB_PASSWORD] = ""

    return original_values


def restore_local_env_vars(overridden_env_vars: Dict[str, Optional[str]]) -> None:
    for var, value in overridden_env_vars.items():
        if value is None:
            del os.environ[var]
        else:
            os.environ[var] = value


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


def _run_command(
    command: str,
    assert_success: bool = True,
    as_user: Optional[pwd.struct_passwd] = None,
) -> str:
    """Runs the given command.

    Runs the command as a different OS user if `as_user` is not None. If the command succeeds, returns any output from
    stdout. If the command fails and `assert_success` is set, raises an error.
    """
    # pylint: disable=subprocess-popen-preexec-fn
    proc = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        preexec_fn=_get_run_as_user_fn(as_user) if as_user else None,
    )
    try:
        out, err = proc.communicate(timeout=15)
    except subprocess.TimeoutExpired as e:
        proc.kill()
        out, err = proc.communicate()
        raise RuntimeError(f"Command timed out: `{command}`\n{err}\n{out}") from e

    if assert_success and proc.returncode != 0:
        raise RuntimeError(f"Command failed: `{command}`\n{err}\n{out}")
    return out


@environment.local_only
def can_start_on_disk_postgresql_database() -> bool:
    try:
        _run_command("which pg_ctl")
    except RuntimeError:
        return False
    return True


@environment.local_only
def start_on_disk_postgresql_database() -> str:
    """Starts and initializes a local postgres database for use in tests.
    Clears all postgres instances in the tmp folder. Should be called in the setUpClass function so
    this only runs once per test class.

    Returns the directory where the database data lives.
    """
    # Clears the tmp directory of all postgres directories
    _clear_all_on_disk_postgresql_databases()

    # Create the directory to use for the postgres database, if it does not already exist.
    temp_db_data_dir = tempfile.mkdtemp(prefix="postgres")
    os.makedirs(temp_db_data_dir, exist_ok=True)

    # The database can't be owned by root so create a separate OS user to own the database if we are currently root.
    password_record = None
    if _is_root_user():
        # `useradd` is a Linux command so this raises an error on MacOS. We expect tests running on MacOs will never be
        # run as root anyway so this is okay. This will also fail if the user already exists, so we ignore failure.
        _run_command(f"useradd {LINUX_TEST_DB_OWNER_NAME}", assert_success=False)
        # Get the password record for the new user, fails if the user does not exist.
        password_record = pwd.getpwnam(LINUX_TEST_DB_OWNER_NAME)

        # Ensure the necessary directories are owned by the postgres user.
        os.chown(
            temp_db_data_dir, uid=password_record.pw_uid, gid=password_record.pw_gid
        )
        os.chown(
            "/var/run/postgresql",
            uid=password_record.pw_uid,
            gid=password_record.pw_gid,
        )

    # Start the local postgres server.
    # Write logs to file so that pg_ctl closes its stdout file descriptor when it moves to the background, otherwise
    # the subprocess will hang.
    _run_command(
        f"pg_ctl -D {temp_db_data_dir} -l /tmp/postgres initdb", as_user=password_record
    )

    if os.environ.get("CI") == "true":
        # We need to set the host to 0.0.0.0 as CircleCI/GitHub Actions don't let us bind to 127.0.0.1 as the default.
        _run_command(
            f'pg_ctl -D {temp_db_data_dir} -l /tmp/postgres start -o "-h 0.0.0.0"',
            as_user=password_record,
        )
    else:
        _run_command(
            f"pg_ctl -D {temp_db_data_dir} -l /tmp/postgres start",
            as_user=password_record,
        )

    # Create a user and database within postgres.
    # These will fail if they already exist, ignore that failure and continue.
    _run_command(
        f"createuser {TEST_POSTGRES_USER_NAME}",
        as_user=password_record,
        assert_success=False,
    )
    _run_command(
        f"createdb -O {TEST_POSTGRES_USER_NAME} {TEST_POSTGRES_DB_NAME}",
        as_user=password_record,
        assert_success=False,
    )
    return temp_db_data_dir


def _clear_all_on_disk_postgresql_databases() -> None:
    tmp_dir = tempfile.gettempdir()
    tmp_directory_dirs = [
        name
        for name in os.listdir(tmp_dir)
        if os.path.isdir(os.path.join(tmp_dir, name))
    ]
    postgres_dirs = [
        os.path.join(tmp_dir, name)
        for name in tmp_directory_dirs
        if name.startswith("postgres")
    ]
    for postgres_dir in postgres_dirs:
        stop_and_clear_on_disk_postgresql_database(postgres_dir, assert_success=False)


@environment.local_only
def stop_and_clear_on_disk_postgresql_database(
    temp_db_data_dir: str, assert_success: bool = True
) -> None:
    """Stops the postgres server and performs rm -rf of the PG data directory.
    Should be called in the tearDownClass function so this only runs once per test class.
    """
    _stop_on_disk_postgresql_database(
        temp_db_data_dir=temp_db_data_dir, assert_success=assert_success
    )
    shutil.rmtree(temp_db_data_dir)


def _stop_on_disk_postgresql_database(
    temp_db_data_dir: str, assert_success: bool = True
) -> None:
    # If the current user is root then the database is owned by a separate OS test user. Run as them to stop the server.
    password_record = (
        pwd.getpwnam(LINUX_TEST_DB_OWNER_NAME) if _is_root_user() else None
    )
    _run_command(
        f"pg_ctl -D {temp_db_data_dir} -l /tmp/postgres stop",
        as_user=password_record,
        assert_success=assert_success,
    )


@environment.local_only
def use_on_disk_postgresql_database(database_key: SQLAlchemyDatabaseKey) -> None:
    """Connects SQLAlchemy to a local test postgres server. Should be called after the test database and user have
    already been initialized.

    This includes:
    1. Create all tables in the newly created sqlite database
    2. Bind the global SessionMaker to the new database engine
    """
    if database_key.declarative_meta not in DECLARATIVE_BASES:
        raise ValueError(f"Unexpected database key: {database_key}.")

    engine = SQLAlchemyEngineManager.init_engine_for_postgres_instance(
        database_key=database_key,
        db_url=on_disk_postgres_db_url(),
    )
    # Auto-generate all tables that exist in our schema in this database
    database_key.declarative_meta.metadata.create_all(engine)


@environment.local_only
def on_disk_postgres_db_url() -> str:
    return f"postgresql://{TEST_POSTGRES_USER_NAME}:@localhost:5432/{TEST_POSTGRES_DB_NAME}"


@environment.local_only
def postgres_db_url_from_env_vars() -> str:
    return (
        f"postgresql://{os.getenv(SQLALCHEMY_DB_USER)}:{os.getenv(SQLALCHEMY_DB_PASSWORD)}@"
        + f"{os.getenv(SQLALCHEMY_DB_HOST)}:5432/{os.getenv(SQLALCHEMY_DB_NAME)}"
    )


@environment.local_only
def teardown_on_disk_postgresql_database(database_key: SQLAlchemyDatabaseKey) -> None:
    """Clears state in an on-disk postgres database for a given schema, for use once a single test has completed. As an
    optimization, does not actually drop tables, just clears them. As a best practice, you should call
    stop_and_clear_on_disk_postgresql_database() once all tests in a test class are complete to actually drop the
    tables.
    """
    # Ensure all sessions are closed, otherwise the below may hang.
    close_all_sessions()

    session = SessionFactory.for_database(database_key)
    try:
        for table in reversed(database_key.declarative_meta.metadata.sorted_tables):
            session.execute(table.delete())
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

    SQLAlchemyEngineManager.teardown_engine_for_database_key(database_key=database_key)
