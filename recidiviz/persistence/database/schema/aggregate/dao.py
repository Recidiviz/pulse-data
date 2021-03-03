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

"""Data Access Object (DAO) with logic for accessing aggregate information from
a SQL Database."""

import logging

import pandas as pd
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.persistence.database.base_schema import JailsBase


def write_df(table: DeclarativeMeta, df: pd.DataFrame) -> None:
    """
    Writes the |df| to the |table|.

    The column headers on |df| must match the column names in |table|. All rows
    in |df| will be appended to |table|. If a row in |df| already exists in
    |table|, then that row will be skipped.
    """
    try:
        df.to_sql(
            table.__tablename__,
            SQLAlchemyEngineManager.get_engine_for_schema_base(JailsBase),
            if_exists="append",
            index=False,
        )
    except IntegrityError:
        _write_df_only_successful_rows(table, df)


def _write_df_only_successful_rows(table: DeclarativeMeta, df: pd.DataFrame) -> None:
    """If the dataframe can't be written all at once (eg. some rows already
    exist in the database) then we write only the rows that we can."""
    for i in range(len(df)):
        row = df.iloc[i : i + 1]
        try:
            row.to_sql(
                table.__tablename__,
                SQLAlchemyEngineManager.get_engine_for_schema_base(JailsBase),
                if_exists="append",
                index=False,
            )
        except IntegrityError:
            # Skip rows that can't be written
            logging.info("Skipping write_df to %s table: %s.", table, row)
