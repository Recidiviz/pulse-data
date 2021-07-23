#!/usr/bin/env bash

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
"""
Tool for loading fixture data into our Case Triage instance

This script should be run only after `docker-compose -f docker-compose.case-triage.yaml up`
has been run. This will delete everything from the etl_* tables and then re-add them from the
fixture files.

Usage against default database:
python -m recidiviz.tools.case_triage.load_fixtures

Usage against non-default database:
SQLALCHEMY_DB_HOST="" SQLALCHEMY_DB_USER="" SQLALCHEMY_DB_PASSWORD="" SQLALCHEMY_DB_NAME="" \
python -m recidiviz.tools.case_triage.load_fixtures
"""
import os

import psycopg2

from recidiviz.persistence.database.constants import (
    SQLALCHEMY_DB_HOST,
    SQLALCHEMY_DB_NAME,
    SQLALCHEMY_DB_PASSWORD,
    SQLALCHEMY_DB_USER,
)
from recidiviz.persistence.database.schema.case_triage.schema import ETL_TABLES


def reset_case_triage_fixtures() -> None:
    user = os.getenv(SQLALCHEMY_DB_USER, "postgres")
    password = os.getenv(SQLALCHEMY_DB_PASSWORD, "example")
    host = os.getenv(SQLALCHEMY_DB_HOST, "localhost")
    database = os.getenv(SQLALCHEMY_DB_NAME, "postgres")

    connection = psycopg2.connect(
        dbname=database, host=host, user=user, password=password
    )

    with connection.cursor() as cursor:

        def _import_csv(path: str, table: str) -> None:
            with open(path, "r") as csv:
                cursor.copy_expert(
                    f"COPY {table} FROM STDIN WITH DELIMITER ',' CSV",
                    csv,
                )

        for table in ETL_TABLES:
            table_name = table.__table__.name
            cursor.execute(f"DELETE FROM {table_name}")
            _import_csv(
                f"recidiviz/tools/case_triage/fixtures/{table_name}.csv",
                table_name,
            )

        cursor.execute("commit")


if __name__ == "__main__":
    reset_case_triage_fixtures()
