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
"""Parse the NY Aggregated Statistics PDF."""
import datetime
import itertools
import locale
from typing import Dict, Generator

import dateutil.parser
import numpy
import numpy as np
import pandas as pd
import tabula
import us
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common import fips
from recidiviz.common.constants.aggregate import enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate import aggregate_ingest_utils
from recidiviz.persistence.database.schema.aggregate.schema import NyFacilityAggregate


def parse(filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    _setup()

    table = _parse_table(filename)

    # Fuzzy match each facility_name to a county fips
    county_names = table.facility_name.map(_pretend_facility_is_county)
    table = fips.add_column_to_df(table, county_names, us.states.NY)

    table["aggregation_window"] = enum_strings.monthly_granularity
    table["report_frequency"] = enum_strings.monthly_granularity

    return {NyFacilityAggregate: table}


def _parse_table(filename: str) -> pd.DataFrame:
    """Parses all tables in the GA PDF."""
    all_dfs = tabula.read_pdf(
        filename,
        pages="all",
        multiple_tables=True,
        lattice=True,
        pandas_options={"header": 0},
    )
    report_date = _parse_report_date(all_dfs[0].columns[-2])
    # Trim unnecessary tables
    if report_date.year >= 2021:
        all_dfs = all_dfs[3:]
    else:
        all_dfs = all_dfs[3:-1]

    dfs_split_by_page = [_split_page(df_for_page) for df_for_page in all_dfs]
    all_split_dfs = list(itertools.chain.from_iterable(dfs_split_by_page))

    results = [_format_df(df) for df in all_split_dfs]
    return pd.concat(results, ignore_index=True)


def _split_page(df: pd.DataFrame) -> Generator[pd.DataFrame, None, None]:
    """Create a new DataFrame for each facility listed on a page."""
    df = df.dropna(how="all")

    # bottom_df is parsed offset by one column and needs to be shifted
    last_column = df[df.columns[-1]]
    top_df = df[last_column.notnull()]
    bottom_df = df[last_column.isnull()].shift(1, axis="columns")

    # Recombine top_df and bottom_df since it's not the correct table division
    aligned_df = pd.concat([top_df, bottom_df], ignore_index=True)

    # New table starts when a new facility is listed
    table_starts = np.where(aligned_df["FACILITY"].notnull())[0]
    table_starts_and_end = numpy.append(table_starts, len(aligned_df))

    for start, end in aggregate_ingest_utils.pairwise(table_starts_and_end):
        yield aligned_df[start:end]


def _format_df(df: pd.DataFrame) -> pd.DataFrame:
    """Format the DataFrame to match the schema."""
    result = _transpose_df(df)

    result = aggregate_ingest_utils.rename_columns_and_select(
        result,
        {
            "report_date": "report_date",
            "Census": "census",
            "In House": "in_house",
            "Boarded In": "boarded_in",
            "Boarded Out": "boarded_out",
            "- Sentenced": "sentenced",
            "- Civil": "civil",
            "- Federal": "federal",
            "- Technical Parole Violators": "technical_parole_violators",
            "- State Readies": "state_readies",
            "- Other Unsentenced **": "other_unsentenced",
        },
    )

    result["report_date"] = result["report_date"].apply(_parse_report_date)

    for column_name in set(result.columns) - {"report_date"}:
        result[column_name] = result[column_name].apply(
            lambda d: int(d)
            if isinstance(d, (int, float))
            else 0
            if "(" in d
            else locale.atoi(d)
        )

    result["facility_name"] = df["FACILITY"].iloc[0]

    return result


def _transpose_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Since the pdf contains the labels witch match the schema in a single column,
    we set that column as the DataFrame index and then transpose the DataFrame.
    This makes the DataFrame column headers match the schema fields.
    """
    # Remove facility column & percent difference over time column
    df = df[df.columns[1:-1]]

    # Column 0 contains the column header text
    df = df.set_index(df.columns[0])

    # Transpose to swap the index and column headers
    df = df.transpose()

    # Since report_date was the old column header, instead make it a new column
    df = df.rename_axis("report_date").reset_index()

    return df


def _parse_report_date(report_date: str) -> datetime.date:
    """Parse the |report_date| as a date, with day as the last day of the month.

    Example: "12/2018" -> 12/31/2018
    """
    parsed_date = dateutil.parser.parse(report_date).date()
    return aggregate_ingest_utils.on_last_day_of_month(parsed_date)


def _pretend_facility_is_county(facility_name: str):
    """Format facility_name like a county_name to match each to a fips."""
    return facility_name.split(" ")[0] + " " + facility_name.split(" ")[1]


def _setup() -> None:
    # This allows us to call `locale.atoi` when converting str -> int
    locale.setlocale(locale.LC_ALL, "en_US.UTF-8")
