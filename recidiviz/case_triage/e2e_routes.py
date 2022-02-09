# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""This file defines routes useful for the Case Triage end to end test suite """
import os
import subprocess
from http import HTTPStatus
from typing import Tuple

import psycopg2
from flask import Blueprint
from flask_sqlalchemy_session import current_session
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from recidiviz.persistence.database.constants import (
    SQLALCHEMY_DB_HOST,
    SQLALCHEMY_DB_NAME,
    SQLALCHEMY_DB_PASSWORD,
    SQLALCHEMY_DB_USER,
)
from recidiviz.utils.environment import local_only

e2e_blueprint = Blueprint("e2e", __name__)

# Name of the test database that is used in our `docker-compose.case-triage.test.yaml`
TEST_CASE_TRIAGE_DATABASE = "case_triage_test"

# Specifies the list of tables which should be cleared upon test completion.
# Generally, if an user taking an action results in a row being added to the table, the table name should be
# specified here
TABLES_TO_TRUNCATE = [
    "case_update_actions",
    "opportunity_deferrals",
]


def get_non_transactional_connection(
    user: str, password: str, host: str
) -> "psycopg2.cursor":
    connection = psycopg2.connect(
        dbname="postgres", host=host, user=user, password=password
    )
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    return connection.cursor()


@e2e_blueprint.route("/setup_suite", methods=["GET"])
@local_only
def _setup_suite() -> Tuple[str, int]:
    """Creates the test database if it does not exist;
    Runs migrations against test database;
    Loads fixtures
    """
    user = os.environ[SQLALCHEMY_DB_USER]
    password = os.environ[SQLALCHEMY_DB_PASSWORD]
    host = os.environ[SQLALCHEMY_DB_HOST]
    database = os.environ[SQLALCHEMY_DB_NAME]

    if database != TEST_CASE_TRIAGE_DATABASE:
        raise ValueError(
            f"Must be run against the TEST_CASE_TRIAGE_DATABASE {TEST_CASE_TRIAGE_DATABASE}"
        )

    # Database creation queries cannot be run inside transactions
    # Acquire a connection using psycopg2, which will not automatically wrap our queries in a transaction
    cursor = get_non_transactional_connection(user, password, host)
    cursor.execute(f"SELECT datname FROM pg_database WHERE datname='{database}'")
    exists = cursor.fetchall()

    if not exists:
        cursor.execute(f"CREATE DATABASE {database}")
        cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE case_triage_test TO {user}")

        subprocess.check_output(
            [
                "pipenv",
                "run",
                "alembic",
                "-c",
                "recidiviz/persistence/database/migrations/case_triage_alembic.ini",
                "upgrade",
                "head",
            ],
            env=os.environ.copy(),
        )

        subprocess.check_output(
            [
                "pipenv",
                "run",
                "python",
                "-m",
                "recidiviz.tools.case_triage.load_fixtures",
            ],
            env=os.environ.copy(),
        )

    return "OK - setup suite", HTTPStatus.OK


@e2e_blueprint.route("/teardown_scenario", methods=["GET"])
@local_only
def _teardown() -> Tuple[str, int]:
    """Truncates tables that are modified by our E2E tests"""
    database = os.environ[SQLALCHEMY_DB_NAME]

    if database != TEST_CASE_TRIAGE_DATABASE:
        raise ValueError("Must be run against the test database")

    for table in TABLES_TO_TRUNCATE:
        current_session.execute(f"TRUNCATE TABLE {table}")

    current_session.commit()

    return "OK - teardown", HTTPStatus.OK
