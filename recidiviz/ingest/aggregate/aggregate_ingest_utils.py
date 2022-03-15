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
"""Common utility functions used across aggregated_ingest."""
import calendar
import datetime
import re
from typing import Dict, Iterable, Optional, Pattern, Set

import pandas as pd
from more_itertools import one

from recidiviz.ingest.aggregate.errors import DataFrameCastError


def collapse_header(columns: pd.MultiIndex) -> pd.MultiIndex:
    """Strip empty information from a multi-index Tabula header."""
    pruned_headers = [
        filter(_should_keep_word_in_tabula_header, column_header)
        for column_header in columns.values
    ]
    return [" ".join(word).strip() for word in pruned_headers]


def _should_keep_word_in_tabula_header(word: str) -> bool:
    """
    Tabula may parse headers as a pd.MultiIndex which contains multiple rows of
    header text. Rows that are blank contain the word 'Unnamed' and should be
    ignored.
    """
    return "Unnamed" not in word


def rename_columns_and_select(
    df: pd.DataFrame, rename_dict: Dict[str, str], *, use_regex: bool = False
) -> pd.DataFrame:
    """Selects only the DataFrame columns listed in |rename_dict| and performs a
    rename operation as described in |rename_dict|.

    |rename_dict| keys may be the exact |df| column_name or a unique regex.
    """
    if use_regex:
        rename_dict = _create_rename_dict_from_regex(df, rename_dict)

    df = df.rename(columns=rename_dict)
    return df[list(rename_dict.values())]


def _create_rename_dict_from_regex(
    df: pd.DataFrame, regex_rename_dict: Dict[str, str]
) -> Dict[str, str]:
    """Converts a Dict[regex, new_column_name] to a Dict[existing_column_name,
    new_column_name]."""
    rename_dict = {}
    for regex in regex_rename_dict:
        actual_column_name = _get_match(df.columns, re.compile(regex))
        rename_dict[actual_column_name] = regex_rename_dict[regex]

    return rename_dict


def _get_match(iterable: Iterable[str], regex: Pattern) -> str:
    return one([item for item in iterable if regex.match(item)])


def cast_columns_to_int(
    df: pd.DataFrame,
    *,
    ignore_columns: Optional[Set[str]] = None,
    nullable_int_columns: Optional[Set[str]] = None,
) -> pd.DataFrame:
    """Casts every column in |df| to an int, unless otherwise specified.

    If a column is listed in |ignore_columns| then it will be left as is (likely
    as an object or string).

    If a column is listed in |nullable_int_columns| then it will be cast to a
    float.

    Note: If a column contains ints and NaN, then the column must be cast using
    nullable_int_columns. This is because np.NaN is a float.
    """
    ignore_columns = ignore_columns or set()
    nullable_int_columns = nullable_int_columns or set()

    _validate_column_names(df, ignore_columns | nullable_int_columns)

    for column_name in df.columns:
        if column_name in ignore_columns:
            continue
        if column_name in nullable_int_columns:
            # Since NaN is a float, we must cast the whole column to floats
            df[column_name] = df[column_name].astype(float)
        else:
            df[column_name] = df[column_name].astype(float)
            df[column_name] = df[column_name].astype(int)

    return df


def _validate_column_names(df: pd.DataFrame, column_names: Iterable[str]) -> None:
    """Verify that all column_names exist as columns in |df|."""
    for column_name in column_names:
        if column_name not in df.columns:
            raise DataFrameCastError(f"Invalid column_name when casting: {column_name}")


def on_last_day_of_month(date: datetime.date) -> datetime.date:
    return date.replace(day=_last_day_of_month(date.year, date.month))


def last_date_of_month(year: int, month: int) -> datetime.date:
    day = _last_day_of_month(year, month)
    return datetime.date(year=year, month=month, day=day)


def _last_day_of_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def subtract_month(date: datetime.date) -> datetime.date:
    if date.month == 1:
        year = date.year - 1
        month = 12
    else:
        year = date.year
        month = date.month - 1
    day = min(date.day, _last_day_of_month(year, month))
    return datetime.date(year=year, month=month, day=day)
