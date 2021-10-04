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
"""Download and parse csvs for West Virginia jail reports.
"""
from datetime import date, datetime
from typing import Dict

import numpy as np
import pandas as pd
import us
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common import fips
from recidiviz.common.constants.aggregate import enum_canonical_strings as enum_strings
from recidiviz.common.constants.aggregate.enum_canonical_strings import (
    community_corrections_wv_facility_type,
    jail_wv_facility_type,
    juvenile_center_wv_facility_type,
    prison_wv_facility_type,
)
from recidiviz.persistence.database.schema.aggregate.schema import WvFacilityAggregate

_FACILITY_TYPES = {
    "Jails": jail_wv_facility_type,
    "Jails Central": jail_wv_facility_type,
    "Regional Jails": jail_wv_facility_type,
    "Regional jails": jail_wv_facility_type,
    "Prisons": prison_wv_facility_type,
    "Correctional Centers (prisons)": prison_wv_facility_type,
    "Community Corrections": community_corrections_wv_facility_type,
    "Community Corrections (work-release)": community_corrections_wv_facility_type,
    "Juvenile Centers": juvenile_center_wv_facility_type,
    "Juvenile Services": juvenile_center_wv_facility_type,
}


def parse(filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    df = _parse_table(filename)

    df = fips.add_column_to_df(df, df["county"], us.states.WV)  # type: ignore
    df["aggregation_window"] = enum_strings.daily_granularity
    df["report_frequency"] = enum_strings.daily_granularity

    return {WvFacilityAggregate: df}


def _parse_table(filename: str) -> pd.DataFrame:
    """Parse the TSV, remove extra rows and columns, and tag each row with facility type."""
    report_date = datetime.strptime(
        filename.lower().split("covid19_dcr_")[-1][:10], "%Y_%m-%d"
    ).date()
    if date(2020, 5, 20) <= report_date <= date(2020, 10, 29):
        header = 2
    else:
        header = 1
    df = pd.read_csv(filename, sep="\t", encoding="latin1", header=header)
    df.columns = ["Total" if c == "Pop." else c.strip() for c in df.columns]
    df.rename(
        columns={
            df.columns[0]: "facility_name",
            "County": "county",
            "Total": "total_jail_population",
        },
        inplace=True,
    )
    df.replace(r"^\s+$", np.nan, regex=True, inplace=True)
    df = add_facility_type(df)

    # Only keep rows with a "County" value, ignoring headers, notes, and totals.
    df = df.loc[
        df["county"].notna() & ~df["county"].astype("str").str.startswith("*"),
        ["facility_name", "county", "total_jail_population", "facility_type"],
    ]
    df["facility_name"] = df["facility_name"].fillna("").str.strip().str.rstrip("*")
    df["county"] = df["county"].str.strip()
    df = df[~df["facility_name"].str.startswith("Deaths:")]
    df = df[~df["facility_name"].str.startswith("About this report:")]

    # The CSV sometimes doesn't list all values for the Braxton County Central Jail.
    if 1 in df.index and df.loc[1, "county"] == "Braxton" and df.loc[1].isna().any():
        df.loc[1, "facility_name"] = "Central"
        df.loc[1, "facility_type"] = "JAIL"

    df["report_date"] = report_date
    return df.reset_index(drop=True)


def add_facility_type(df: pd.DataFrame) -> pd.DataFrame:
    """Facility types are listed as sub-headers. This loops through each row, keeping track of the latest encountered
    facility type and adding it to each row."""
    df = df.copy()
    current_facility_type = jail_wv_facility_type
    for i, facility in df["facility_name"].iteritems():
        if pd.isna(facility):
            continue
        new_facility_type = _FACILITY_TYPES.get(facility.strip().strip("*"))
        if new_facility_type:
            current_facility_type = new_facility_type
        elif current_facility_type:
            df.loc[i, "facility_type"] = current_facility_type
    return df
