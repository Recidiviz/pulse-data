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
import logging
import os
from types import ModuleType
from typing import List, Optional

from sqlalchemy import Table, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)

logger = logging.getLogger(__name__)


def create_dbs(
    state_codes: List[str], schema_type: SchemaType, engine: Optional[Engine] = None
) -> None:
    if not engine:
        # Connect to the "postgres" database so we can operate on all the states.
        pg_db_key = SQLAlchemyDatabaseKey(schema_type, db_name="postgres")
        engine = SQLAlchemyEngineManager.init_engine(pg_db_key)

    with engine.connect() as connection:
        for state_code in state_codes:
            logger.info("creating database %s if it doesn't exist", state_code.lower())
            try:
                # The easiest way to create it if it doesn't exist is to just try to create it, and
                # ignore the error we get if it already exists.
                connection.execution_options(isolation_level="AUTOCOMMIT").execute(
                    text(f"CREATE DATABASE {state_code.lower()}")
                )
            except ProgrammingError as e:
                if e.orig.pgcode == "42P04":  # ignore duplicate_database error
                    logger.info(
                        "found existing DB, feel free to ignore 'already exists' errors"
                    )
                else:
                    raise e


def reset_fixtures(
    engine: Engine,
    tables: List[DeclarativeMeta],
    fixture_directory: str,
    csv_headers: Optional[bool] = False,
) -> None:
    """
    Deletes all existing data in `tables` and re-imports data from CSV files
    in `fixture_directory`. If `csv_headers=True`, the fixture files are
    assumed to have a header row that names the columns.
    """
    logger.info("Resetting fixtures for database %s", engine.url)
    connection = engine.raw_connection()
    try:
        cursor = connection.cursor()

        def _import_csv(path: str, table: str) -> None:
            logger.info("Importing `%s` to `%s` table", path, table)
            column_names = ""
            header = ""
            if csv_headers:
                # Read the column names from the first line of the csv
                with open(path, "r", encoding="utf-8") as f:
                    column_names = f"({f.readline().rstrip()})"
                header = " HEADER"

            with open(path, "r", encoding="utf-8") as csv:
                cursor.copy_expert(
                    f"COPY {table} {column_names} FROM STDIN WITH DELIMITER ',' CSV{header}",
                    csv,
                )

        # Clear all tables in reverse order to avoid "foreign key is still referenced" errors
        for table in reversed(tables):
            table_name = _get_table_name(module=table)
            logger.info("Removing data from `%s` table", table_name)
            cursor.execute(f"DELETE FROM {table_name}")

        for table in tables:
            table_name = _get_table_name(module=table)
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
        cursor.close()
    finally:
        connection.close()


def _get_table_name(module: ModuleType) -> str:
    if isinstance(module, Table):
        # Handle association tables properly
        return module.name
    return module.__table__.name
