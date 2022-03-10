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
"""Parse CA Aggregated Statistics."""
import datetime
from typing import Dict

import pandas as pd
import us
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common import fips
from recidiviz.common.constants.aggregate import enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate import aggregate_ingest_utils
from recidiviz.persistence.database.schema.aggregate.schema import CaFacilityAggregate


def parse(filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    table = _parse_table(filename)

    county_names = table.jurisdiction_name.map(_pretend_jurisdiction_is_county)
    table = fips.add_column_to_df(table, county_names, us.states.CA)

    table["aggregation_window"] = enum_strings.monthly_granularity
    table["report_frequency"] = enum_strings.monthly_granularity

    return {CaFacilityAggregate: table}


def _parse_table(filename: str) -> pd.DataFrame:
    """Parses the CA aggregate report."""

    # Although the file is downloaded with the '.xls' extension, the contents of
    # the file are in the shape of an HTML file.
    df = pd.read_html(filename, header=0)[0]
    df = df.fillna(0)

    df["report_date"] = df[["Year", "Month"]].apply(_last_date_of_month, axis="columns")

    df = aggregate_ingest_utils.rename_columns_and_select(
        df,
        {
            "Jurisdiction": "jurisdiction_name",
            "Facility": "facility_name",
            "Total facility ADP": "average_daily_population",
            "Unsentenced males": "unsentenced_male_adp",
            "Unsentenced females": "unsentenced_female_adp",
            "Sentenced males": "sentenced_male_adp",
            "Sentenced females": "sentenced_female_adp",
            "report_date": "report_date",
        },
    )

    string_columns = {"jurisdiction_name", "facility_name", "report_date"}
    df = df.replace("u", 0)
    df = df.replace("d", 0)
    df = aggregate_ingest_utils.cast_columns_to_int(df, ignore_columns=string_columns)

    return df


def _last_date_of_month(series: pd.Series) -> datetime.date:
    year = series[0]
    month = series[1]
    return aggregate_ingest_utils.last_date_of_month(year=year, month=month)


def _pretend_jurisdiction_is_county(jurisdiction: str) -> str:
    """Format jurisdiction_name like a county_name to match each to a fips."""
    jurisdiction = jurisdiction.lower()
    jurisdiction = jurisdiction.replace(" corrections dept.", "")
    jurisdiction = jurisdiction.replace(" police dept.", "")
    jurisdiction = jurisdiction.replace(" probation dept.", "")
    jurisdiction = jurisdiction.replace(" sheriff's dept.", "")
    jurisdiction = jurisdiction.replace(" sheriffs office", "")
    jurisdiction = jurisdiction.replace(" work furlough", "")

    if jurisdiction == "oakland":
        return "alameda"
    if jurisdiction == "scapular house":
        return "los angeles"

    return jurisdiction
