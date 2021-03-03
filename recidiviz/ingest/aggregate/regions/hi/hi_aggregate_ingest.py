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
"""Parse the HI Aggregated Statistics PDF."""
import datetime
import re
from typing import Dict, Iterable, List

import more_itertools
import pandas as pd
import tabula
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common.constants.aggregate import enum_canonical_strings as enum_strings
from recidiviz.common import str_field_utils
from recidiviz.common.errors import FipsMergingError
from recidiviz.ingest.aggregate.errors import (
    AggregateDateParsingError,
    AggregateIngestError,
)
from recidiviz.persistence.database.schema.aggregate.schema import HiFacilityAggregate

_COLUMN_NAMES = [
    "facility_name",
    "design_bed_capacity",
    "operation_bed_capacity",
    "total_population",
    "male_population",
    "female_population",
    "sentenced_felony_male_population",
    "sentenced_felony_female_population",
    "sentenced_felony_probation_male_population",
    "sentenced_felony_probation_female_population",
    "sentenced_misdemeanor_male_population",
    "sentenced_misdemeanor_female_population",
    "pretrial_felony_male_population",
    "pretrial_felony_female_population",
    "pretrial_misdemeanor_male_population",
    "pretrial_misdemeanor_female_population",
    "held_for_other_jurisdiction_male_population",
    "held_for_other_jurisdiction_female_population",
    "parole_violation_male_population",
    "parole_violation_female_population",
    "probation_violation_male_population",
    "probation_violation_female_population",
]

_FACILITY_ACRONYM_TO_NAME = {
    "HCCC": "Hawaii Community Correctional Center",
    "SNF": "Halawa Correctional Facility - Special Needs",
    "HMSF": "Halawa Correctional Facility - Medium Security",
    "KCCC": "Kauai Community Correctional Center",
    "KCF": " Kulani Correctional Facility",
    "MCCC": "Maui Community Correctional Center",
    "OCCC": "Oahu Community Correctional Center",
    "WCCC": "Women’s Community Correctional Center",
    "WCF": "Waiawa Correctional Facility",
    "RED ROCK CC, AZ": "Red Rock Correctional Center, AZ",
    "SAGUARO CC, AZ": "Saguaro Correctional Center, AZ",
    "FEDERAL DET. CTR.": "Federal Detention Center, Honolulu",
    "FEDERAL DET. CTR. 1": "Federal Detention Center, Honolulu",
}

_FACILITY_ACRONYM_TO_FIPS = {
    "HCCC": "15001",  # Hawaii
    "SNF": "15003",  # Honolulu
    "HMSF": "15003",  # Honolulu
    "KCCC": "15007",  # Kauai
    "KCF": "15001",  # Hawaii
    "MCCC": "15009",  # Maui
    "OCCC": "15003",  # Honolulu
    "WCCC": "15003",  # Honolulu
    "WCF": "15003",  # Honolulu
    "RED ROCK CC, AZ": "04021",  # Pinal
    "SAGUARO CC, AZ": "04021",  # Pinal
    "FEDERAL DET. CTR.": "15003",  # Honolulu
    "FEDERAL DET. CTR. 1": "15003",  # Honolulu
}

DATE_PARSE_ANCHOR_FILENAME = "pop-reports-eom-"


def parse(location: str, filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    table = _parse_table(location, filename)

    table["report_date"] = parse_date(filename)
    table["aggregation_window"] = enum_strings.daily_granularity
    table["report_frequency"] = enum_strings.monthly_granularity

    return {HiFacilityAggregate: table}


def parse_date(filename: str) -> datetime.date:
    # Hawaii report pdfs have names that start with `Pop-Reports-EOM-`, followed
    # by a 10-character date and possibly another number (version?). For example
    # `Pop-Reports-EOM-2019-03-21.pdf` and `Pop-Reports-EOM-2018-03-31-1.pdf`.
    regex = r".*?Pop-Reports-EOM-([\d-]{10})"
    match = re.search(regex, filename, re.IGNORECASE)
    if match:
        date_str = match.group(1)
        parsed_date = str_field_utils.parse_date(date_str)
        if parsed_date:
            return parsed_date
    raise AggregateDateParsingError("Could not extract date")


def _parse_table(_: str, filename: str) -> pd.DataFrame:
    """Parse the Head Count Endings and Contracted Facilities Tables."""
    all_dfs = tabula.read_pdf(
        filename,
        multiple_tables=True,
        lattice=True,
        pandas_options={
            "header": [0, 1],
        },
    )

    head_count_ending_df = _df_matching_substring(
        all_dfs, {"total", "head count ending"}
    )
    head_count_ending_df = _format_head_count_ending(head_count_ending_df)

    facilities_df = _df_matching_substring(all_dfs, {"contracted facilities"})
    facilities_df = _format_contracted_facilities(facilities_df)

    result = head_count_ending_df.append(facilities_df, ignore_index=True)

    result["fips"] = result.facility_name.map(_facility_acronym_to_fips)
    result["facility_name"] = result.facility_name.map(_facility_acronym_to_name)

    # Rows that may be NaN need to be cast as a float, otherwise use int
    string_columns = {"facility_name", "fips"}
    nullable_columns = {"design_bed_capacity", "operation_bed_capacity"}
    int_columns = set(result.columns) - string_columns - nullable_columns

    for column_name in int_columns:
        result[column_name] = result[column_name].astype(int)
    for column_name in nullable_columns:
        result[column_name] = result[column_name].astype(float)

    return result


def _df_matching_substring(
    dfs: List[pd.DataFrame], strings: Iterable[str]
) -> pd.DataFrame:
    """Get the one df containing all the matching substrings."""
    matches: List[pd.DataFrame] = []
    for df in dfs:
        if all(_df_contains_substring(df, string) for string in strings):
            matches.append(df)

    return more_itertools.one(matches)


def _df_contains_substring(df: pd.DataFrame, substring: str) -> bool:
    """Returns True if any entity in |df| contains the substring."""
    df_contains_str_mask = df.applymap(
        lambda element: substring.lower() in str(element).lower()
    )
    return df_contains_str_mask.any().any()


def _format_head_count_ending(df: pd.DataFrame) -> pd.DataFrame:
    # Throw away the incorrectly parsed header rows
    df = df.iloc[3:].reset_index(drop=True)

    # Last row contains the totals
    df = df.iloc[:-1].reset_index(drop=True)

    # The pdf leaves an empty cell when nobody exists for that section
    df = df.fillna(0)

    # Since we can't parse the column_headers, just set them ourselves
    df.columns = _COLUMN_NAMES

    return df


def _format_contracted_facilities(df: pd.DataFrame) -> pd.DataFrame:
    # Throw away the incorrectly parsed header rows
    df = df.iloc[3:].reset_index(drop=True)

    # Last row contains the totals
    df = df.iloc[:-1].reset_index(drop=True)

    # The pdf leaves an empty cell when nobody exists for that section
    df = df.fillna(0)

    # Design/Operational Bed Capacity is never set for Contracted Facilities
    df.insert(1, "design_bed_capacity", None)
    df.insert(2, "operation_bed_capacity", None)

    # The last column is sometimes empty
    if len(df.columns) == len(_COLUMN_NAMES) + 1:
        df = df.drop(df.columns[-1], axis="columns")

    # Since we can't parse the column_headers, just set them ourselves
    df.columns = _COLUMN_NAMES

    return df


def _facility_acronym_to_name(facility_acronym: str) -> str:
    if facility_acronym not in _FACILITY_ACRONYM_TO_FIPS:
        raise AggregateIngestError(
            "Failed to match facility acronym '{}' to facility_name".format(
                facility_acronym
            )
        )

    return _FACILITY_ACRONYM_TO_NAME[facility_acronym]


def _facility_acronym_to_fips(facility_acronym: str) -> str:
    if facility_acronym not in _FACILITY_ACRONYM_TO_FIPS:
        raise FipsMergingError(
            "Failed to match facility acronym '{}' to fips".format(facility_acronym)
        )

    return _FACILITY_ACRONYM_TO_FIPS[facility_acronym]
