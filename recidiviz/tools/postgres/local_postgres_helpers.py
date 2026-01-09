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
"""This module generates a local postgres instance for use in scripts and testing.
It is purposely separated from SQLAlchemyEngine / DatabaseKey dependencies for use in
both the Airflow and Recidiviz virtual environments."""
import logging
import os
import pwd
import shutil
import socket
import subprocess
import tempfile
import time
from typing import Dict, Optional

import attr
from sqlalchemy.engine import URL

from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.test_setup_utils import get_pytest_worker_id
from recidiviz.tools.utils.script_helpers import (
    retry_on_exceptions_with_backoff,
    run_command,
)
from recidiviz.utils import environment
from recidiviz.utils.environment import in_ci

LINUX_TEST_DB_OWNER_NAME = "recidiviz_test_db_owner"
TEST_POSTGRES_DB_NAME = "recidiviz_test_db"
TEST_POSTGRES_USER_NAME = "recidiviz_test_usr"
DEFAULT_POSTGRES_DATA_DIRECTORY = "/usr/local/var/postgres"
REGISTERED_ON_DISK_POSTGRES_INSTANCES: dict[str, "OnDiskPostgresLaunchResult"] = {}


def restore_local_env_vars(overridden_env_vars: Dict[str, Optional[str]]) -> None:
    for var, value in overridden_env_vars.items():
        if value is None:
            del os.environ[var]
        else:
            os.environ[var] = value


def _is_root_user() -> bool:
    """Returns True if we are currently running as root, otherwise False."""
    return os.getuid() == 0


def _kill_process_on_port(port: int) -> None:
    # Find the process using the port and kill it
    try:
        out = subprocess.check_output(
            f"lsof -t -i:{port}", shell=True
        )  # nosec B607 B603 B602 B404 B608
        pids = out.decode().strip().split("\n")
        for pid in pids:
            os.kill(int(pid), 9)
    except subprocess.CalledProcessError:
        pass  # No process is using the port


def _start_postgresql_server(
    db_data_dir: str,
    db_log_path: str,
    port: int,
    password_record: pwd.struct_passwd | None = None,
) -> None:
    try:
        assert_postgres_port_is_free(port)
    except PostgresPortStillInUseError:
        _kill_process_on_port(port)

    try:
        if in_ci():
            # We need to set the host to 0.0.0.0 as CircleCI/GitHub Actions don't let us bind to 127.0.0.1 as the default.
            run_command(
                f'pg_ctl -D {db_data_dir} -l {db_log_path} start -o "-h 0.0.0.0 -p {port}"',
                as_user=password_record,
            )
        else:
            run_command(
                f'pg_ctl -D {db_data_dir} -l {db_log_path} start -o "-p {port}"',
                as_user=password_record,
            )
    except RuntimeError as e:
        with open(db_log_path, "r", encoding="utf-8") as log:
            logging.error("Log below:")
            logging.error(log.read())
        raise e


def pick_random_port() -> int:
    """Returns a random port that is not currently in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@environment.local_only
def can_start_on_disk_postgresql_database() -> bool:
    try:
        run_command("which pg_ctl")
    except RuntimeError:
        return False
    return True


def get_on_disk_postgres_log_dir_prefix() -> str:
    return f"postgres_log_{get_pytest_worker_id()}_"


@attr.s(auto_attribs=True, frozen=True)
class OnDiskPostgresLaunchResult:
    temp_db_data_dir: str
    database_name: str
    port: int

    def url(self, database_name: str | None = None) -> URL:
        return URL.create(
            drivername="postgresql",
            username=TEST_POSTGRES_USER_NAME,
            host="localhost",
            port=self.port,
            database=database_name or self.database_name,
        )


@environment.local_only
def start_on_disk_postgresql_database(
    additional_databases_to_create: list[SQLAlchemyDatabaseKey] | None = None,
) -> OnDiskPostgresLaunchResult:
    """Starts and initializes a local postgres database for use in tests.
    Clears all postgres instances in the tmp folder. Should be called in the setUpClass function so
    this only runs once per test class.
    Returns the directory where the database data lives.
    """

    if additional_databases_to_create is None:
        additional_databases_to_create = []

    # Clears the tmp directory of all postgres directories
    _clear_all_on_disk_postgresql_databases()

    # Create the directory to use for the postgres database, if it does not already exist.
    temp_db_data_dir = tempfile.mkdtemp(prefix=get_on_disk_postgres_temp_dir_prefix())
    os.makedirs(temp_db_data_dir, exist_ok=True)

    _temp_db_log_file, temp_db_log_path = tempfile.mkstemp(
        prefix=get_on_disk_postgres_log_dir_prefix()
    )

    # The database can't be owned by root so create a separate OS user to own the database if we are currently root.
    password_record = None
    if _is_root_user():
        # `useradd` is a Linux command so this raises an error on MacOS. We expect tests running on MacOs will never be
        # run as root anyway so this is okay. This will also fail if the user already exists, so we ignore failure.
        run_command(f"useradd {LINUX_TEST_DB_OWNER_NAME}", assert_success=False)
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
    run_command(
        f"pg_ctl -D {temp_db_data_dir} -l {temp_db_log_path} initdb",
        as_user=password_record,
    )

    on_disk_postgres_port: int = 0

    def _start_postgresql_server_with_random_port() -> None:
        nonlocal on_disk_postgres_port
        on_disk_postgres_port = pick_random_port()
        return _start_postgresql_server(
            temp_db_data_dir,
            temp_db_log_path,
            on_disk_postgres_port,
            password_record,
        )

    retry_on_exceptions_with_backoff(
        _start_postgresql_server_with_random_port,
        errors_to_retry=(RuntimeError, PostgresPortStillInUseError),
        max_tries=5,
        min_backoff_secs=1,
        max_backoff_secs=3,
    )

    # Create a user and database within postgres.
    # These will fail if they already exist, ignore that failure and continue.
    # TODO(#7018): Right now we just enforce that this is a superuser in test, but we
    # should actually make sure that the set of permissions line up with what we have
    # in production.
    run_command(
        f"createuser -p {on_disk_postgres_port} --superuser {TEST_POSTGRES_USER_NAME}",
        as_user=password_record,
        assert_success=False,
    )
    default_database_name = f"{TEST_POSTGRES_DB_NAME}{get_pytest_worker_id()}"
    databases_to_create = [
        default_database_name,
        *[database.db_name for database in additional_databases_to_create],
    ]
    for database in databases_to_create:
        run_command(
            f"createdb -p {on_disk_postgres_port} -O {TEST_POSTGRES_USER_NAME} {database}",
            as_user=password_record,
        )

    launch_result = OnDiskPostgresLaunchResult(
        database_name=default_database_name,
        temp_db_data_dir=temp_db_data_dir,
        port=on_disk_postgres_port,
    )
    REGISTERED_ON_DISK_POSTGRES_INSTANCES[temp_db_data_dir] = launch_result

    print(
        f"Created database `{launch_result.database_name}` on postgres instance bound to port {launch_result.port}"
    )
    print(f"To query data, connect with `psql {launch_result.url()}`")

    return launch_result


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
        if name.startswith(get_on_disk_postgres_temp_dir_prefix())
    ]
    for postgres_dir in postgres_dirs:
        # If the Postgres instance was registered in this process, stop it, otherwise just remove the directory
        if postgres_dir in REGISTERED_ON_DISK_POSTGRES_INSTANCES:
            launch_result = REGISTERED_ON_DISK_POSTGRES_INSTANCES.pop(postgres_dir)
            stop_and_clear_on_disk_postgresql_database(
                launch_result,
                assert_success=False,
            )
        else:
            shutil.rmtree(postgres_dir)


@environment.local_only
def stop_and_clear_on_disk_postgresql_database(
    launch_result: OnDiskPostgresLaunchResult,
    assert_success: bool = True,
) -> None:
    """Stops the postgres server and performs rm -rf of the PG data directory.
    Should be called in the tearDownClass function so this only runs once per test class.
    """
    _stop_on_disk_postgresql_database(
        launch_result=launch_result, assert_success=assert_success
    )
    shutil.rmtree(launch_result.temp_db_data_dir)


def is_port_in_use(port: int) -> int | None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


class PostgresPortStillInUseError(TimeoutError):
    pass


def assert_postgres_port_is_free(port: int) -> None:
    # Raise an error and print logs if the server did not stop
    interval = 0.1
    attempts = 0
    max_tries = 30

    while attempts < max_tries and is_port_in_use(port):
        time.sleep(interval)
        attempts += 1

    if attempts == max_tries:
        print_all_on_disk_postgresql_logs()
        raise PostgresPortStillInUseError(
            "Failed to stop postgres within the timeout period"
        )


def _stop_on_disk_postgresql_database(
    launch_result: OnDiskPostgresLaunchResult, assert_success: bool = True
) -> None:
    # If the current user is root then the database is owned by a separate OS test user. Run as them to stop the server.
    password_record = (
        pwd.getpwnam(LINUX_TEST_DB_OWNER_NAME) if _is_root_user() else None
    )

    run_command(
        f"pg_ctl -D {launch_result.temp_db_data_dir} -o '-p {launch_result.port}' stop -m immediate",
        as_user=password_record,
        assert_success=assert_success,
    )

    if assert_success:
        assert_postgres_port_is_free(launch_result.port)


def get_on_disk_postgres_temp_dir_prefix() -> str:
    return f"postgres{get_pytest_worker_id()}_"


def print_all_on_disk_postgresql_logs() -> None:
    temp_dir = tempfile.gettempdir()
    for entry in os.scandir(temp_dir):
        if not entry.is_file():
            continue
        prefix = get_on_disk_postgres_log_dir_prefix()
        filename = entry.name
        if filename.startswith(prefix):
            with open(entry.path, "r", encoding="utf-8") as f:
                contents = f.read()
                print(f"Contents of {entry.path}:\n{contents}")


@environment.local_only
def async_on_disk_postgres_db_url(
    port: int,
    database: str,
    username: str = TEST_POSTGRES_USER_NAME,
    password: Optional[str] = None,
) -> URL:
    return URL.create(
        drivername="postgresql+asyncpg",
        username=username,
        host="localhost",
        port=port,
        database=database,
        password=password,
    )
