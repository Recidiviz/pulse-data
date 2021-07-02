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
# ============================================================================
"""
Parses PDF and excel files obtained directly from the Indiana state government.
"""

import locale
from datetime import date
from typing import Dict, Union

import pandas as pd
import tabula
import us
from sqlalchemy.orm import DeclarativeMeta

from recidiviz.common import fips
from recidiviz.common.constants.aggregate import enum_canonical_strings as enum_strings
from recidiviz.persistence.database.schema.aggregate.schema import InCountyAggregate


def parse(filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    df = _parse(filename)
    df = fips.add_column_to_df(df, df["county"], us.states.IN)  # type: ignore

    df["aggregation_window"] = enum_strings.daily_granularity
    df["report_frequency"] = enum_strings.quarterly_granularity

    return {InCountyAggregate: df}


def _parse(filename: str) -> pd.DataFrame:
    locale.setlocale(locale.LC_ALL, "en_US.UTF-8")

    if filename.endswith(".pdf"):
        [df] = tabula.read_pdf(filename, pages="all", multiple_tables=False)
    else:
        df = pd.read_excel(filename)

    df = df[df["County Name"].notna()]
    df["report_date"] = df["Week"].apply(_to_date)
    df["county"] = df["County Name"]
    df["total_jail_population"] = df["Jail Population"].apply(_to_int)
    df["jail_capacity"] = df["Jail Capacity"].apply(_to_int)
    df = df[["report_date", "county", "total_jail_population", "jail_capacity"]]
    df = df.reset_index(drop=True)
    return df


def _to_int(x: Union[int, float, str]) -> int:
    if isinstance(x, str):
        return locale.atoi(x)
    return int(x)


def _to_date(year_and_week: Union[int, float]) -> date:
    year, week = 2000 + int(year_and_week) // 100, int(year_and_week) % 100
    return date.fromisocalendar(year, week, 1)  # Pick the Monday of the given week
