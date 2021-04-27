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
"""Parse the FL Aggregated Statistics PDF."""
import datetime
import locale
from typing import Dict, Optional

import pandas as pd
import tabula
import us
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common.constants.aggregate import enum_canonical_strings as enum_strings
from recidiviz.common import str_field_utils
from recidiviz.common import fips
from recidiviz.ingest.aggregate import aggregate_ingest_utils
from recidiviz.ingest.aggregate.errors import AggregateDateParsingError
from recidiviz.persistence.database.schema.aggregate.schema import (
    FlCountyAggregate,
    FlFacilityAggregate,
)


def parse(location: str, filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    _setup()

    fl_county_table = _parse_county_table(location, filename)
    fl_county_table = fips.add_column_to_df(
        fl_county_table, fl_county_table.county_name, us.states.FL
    )

    # TODO(#689): Also set the facility_fips
    fl_facility_table = _parse_facility_table(location, filename)
    names = fl_facility_table.facility_name.apply(_pretend_facility_is_county)
    fl_facility_table = fips.add_column_to_df(fl_facility_table, names, us.states.FL)

    result = {
        FlCountyAggregate: fl_county_table,
        FlFacilityAggregate: fl_facility_table,
    }

    date_scraped = _parse_date(filename)
    for table in result.values():
        table["report_date"] = date_scraped
        table["aggregation_window"] = enum_strings.monthly_granularity
        table["report_frequency"] = enum_strings.monthly_granularity

    return result


def _parse_county_table(_: str, filename: str) -> pd.DataFrame:
    """Parses the FL County - Table 1 in the PDF."""
    [result] = tabula.read_pdf(
        filename,
        pages=[3, 4],
        multiple_tables=False,
        pandas_options={"skipfooter": 1, "engine": "python"},
    )

    result.columns = [c.replace("\r", " ") for c in result.columns]
    result = aggregate_ingest_utils.rename_columns_and_select(
        result,
        {
            "Florida County": "county_name",
            "County Population": "county_population",
            "Average Daily Population (ADP)": "average_daily_population",
            "*Date Reported": "date_reported",
        },
    )

    # Drop rows from header on second table (page 4)
    result = result[~result["county_name"].isin(("Florida", "County"))]

    for column_name in {"county_population", "average_daily_population"}:
        result[column_name] = result[column_name].apply(locale.atoi)

    # Sometimes extra notes are indicated in the date reported field.
    result["date_reported"] = result["date_reported"].str.replace(r"^\*\*$", "")

    result["date_reported"] = pd.to_datetime(result["date_reported"])

    return result


def _parse_facility_table(_: str, filename: str) -> pd.DataFrame:
    """Parse the FL County Pretrial Inmate Report - Table 2 in the PDF."""
    # Set column names directly since the pdf format makes them hard to parse
    column_names = [
        "Detention Facility Name",
        "Average Daily Population",
        "Number Felony Pretrial",
        "Number Misdemeanor Pretrial",
        "Total Percent Pretrial",
    ]
    [result] = tabula.read_pdf(
        filename,
        pages=[5, 6],
        multiple_tables=False,
        pandas_options={
            "usecols": range(1, 6),
            "names": column_names,
            "skiprows": [0],
            "skipfooter": 2,
            "engine": "python",
        },
    )

    result = aggregate_ingest_utils.rename_columns_and_select(
        result,
        {
            "Detention Facility Name": "facility_name",
            "Average Daily Population": "average_daily_population",
            "Number Felony Pretrial": "number_felony_pretrial",
            "Number Misdemeanor Pretrial": "number_misdemeanor_pretrial",
        },
    )
    result = result.replace("Detention\rFacility\rName", None).dropna(how="all")

    result["average_daily_population"] = (
        result["average_daily_population"].apply(_use_stale_adp).apply(_to_int)
    )
    for column_name in {"number_felony_pretrial", "number_misdemeanor_pretrial"}:
        result[column_name] = result[column_name].apply(_to_int)

    return result


def _parse_date(filename: str) -> datetime.date:
    end = filename.index(".pdf")
    start = end - 7

    try:
        d = str_field_utils.parse_date(filename[start:end])
        if d:
            return aggregate_ingest_utils.on_last_day_of_month(d)
    except Exception:
        pass

    # alternate filename format.
    try:
        d = str_field_utils.parse_date(filename.split()[0][-7:])
        if d:
            return aggregate_ingest_utils.on_last_day_of_month(d)
    except Exception:
        pass

    raise AggregateDateParsingError(f"Could not extract date from filename: {filename}")


def _use_stale_adp(adp_str: str) -> str:
    """Use adp values listed in Table 2 that are listed as stale."""
    # Stale values are marked with an '*', so strip all '*'
    return adp_str.rstrip("*") if isinstance(adp_str, str) else adp_str


def _to_int(int_str: str) -> Optional[int]:
    if not isinstance(int_str, str):
        return int_str
    try:
        return locale.atoi(int_str)
    except ValueError:
        # Values containing a '-' have no reported data, so just return None
        return None


def _pretend_facility_is_county(facility_name: str):
    """Format facility_name like a county_name to match each to a fips."""
    words_before_county_name = ["County", "Conte", "CSO", "Central"]
    for delimiter in words_before_county_name:
        facility_name = facility_name.split(delimiter)[0]

    return facility_name


def _setup() -> None:
    # This allows us to call `locale.atoi` when converting str -> int
    locale.setlocale(locale.LC_ALL, "en_US.UTF-8")
