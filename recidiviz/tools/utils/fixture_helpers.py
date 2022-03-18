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
Helper functions for loading fixture data into Postgres instances
"""
import os

import psycopg2
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.persistence.database.constants import (
    SQLALCHEMY_DB_HOST,
    SQLALCHEMY_DB_NAME,
    SQLALCHEMY_DB_PASSWORD,
    SQLALCHEMY_DB_PORT,
    SQLALCHEMY_DB_USER,
)


def reset_fixtures(tables: DeclarativeMeta, fixture_directory: str) -> None:
    """
    Deletes all existing data in `tables` and re-imports data from CSV files
    in `fixture_directory`.
    """

    user = os.getenv(SQLALCHEMY_DB_USER, "postgres")
    password = os.getenv(SQLALCHEMY_DB_PASSWORD, "example")
    host = os.getenv(SQLALCHEMY_DB_HOST, "localhost")
    port = os.getenv(SQLALCHEMY_DB_PORT, "5432")
    database = os.getenv(SQLALCHEMY_DB_NAME, "postgres")

    connection = psycopg2.connect(
        dbname=database, host=host, port=port, user=user, password=password
    )

    with connection.cursor() as cursor:

        def _import_csv(path: str, table: str) -> None:
            with open(path, "r", encoding="utf-8") as csv:
                cursor.copy_expert(
                    f"COPY {table} FROM STDIN WITH DELIMITER ',' CSV",
                    csv,
                )

        for table in tables:
            table_name = table.__table__.name
            cursor.execute(f"DELETE FROM {table_name}")
            _import_csv(
                os.path.realpath(
                    os.path.join(
                        fixture_directory,
                        f"{table_name}.csv",
                    )
                ),
                table_name,
            )

        cursor.execute("commit")
