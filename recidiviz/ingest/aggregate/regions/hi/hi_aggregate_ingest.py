# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
from typing import Dict

import pandas as pd
import tabula
from sqlalchemy.ext.declarative import DeclarativeMeta

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.persistence.database.schema import HiFacilityAggregate

_COLUMN_NAMES = [
    'facility_name',
    'design_bed_capacity',
    'operation_bed_capacity',
    'total_population',
    'male_population',
    'female_population',
    'sentenced_felony_male_population',
    'sentenced_felony_female_population',
    'sentenced_felony_probation_male_population',
    'sentenced_felony_probation_female_population',
    'sentenced_misdemeanor_male_population',
    'sentenced_misdemeanor_female_population',
    'sentenced_pretrial_felony_male_population',
    'sentenced_pretrial_felony_female_population',
    'sentenced_pretrial_misdemeanor_male_population',
    'sentenced_pretrial_misdemeanor_female_population',
    'held_for_other_jurisdiction_male_population',
    'held_for_other_jurisdiction_female_population',
    'parole_violation_male_population',
    'parole_violation_female_population',
    'probation_violation_male_population',
    'probation_violation_female_population'
]


def parse(filename: str, date_scraped: datetime.date) \
        -> Dict[DeclarativeMeta, pd.DataFrame]:
    table = _parse_table(filename)

    # TODO(#698): Set county fips based on the county_name
    table['fips'] = None
    table['report_date'] = date_scraped
    table['report_granularity'] = enum_strings.monthly_granularity

    return {
        HiFacilityAggregate: table
    }


def _parse_table(filename: str) -> pd.DataFrame:
    """Parse the Head Count Endings and Contracted Facilities Tables."""
    all_dfs = tabula.read_pdf(
        filename,
        multiple_tables=True,
        lattice=True,
        pandas_options={
            'header': [0, 1],
        })

    head_count_ending_df = all_dfs[1]
    head_count_ending_df = _format_head_count_ending(head_count_ending_df)

    contracted_facilities_df = all_dfs[3]
    contracted_facilities_df = _format_contracted_facilities(
        contracted_facilities_df)

    result = head_count_ending_df.append(contracted_facilities_df,
                                         ignore_index=True)

    # Rows that may be NaN need to be cast as a float, otherwise use int
    string_columns = {'facility_name'}
    nullable_columns = {'design_bed_capacity', 'operation_bed_capacity'}
    int_columns = set(result.columns) - string_columns - nullable_columns

    for column_name in int_columns:
        result[column_name] = result[column_name].astype(int)
    for column_name in nullable_columns:
        result[column_name] = result[column_name].astype(float)

    return result


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

    # The last column is parsed but is always empty
    df = df.drop(df.columns[-1], axis='columns')

    # Design/Operational Bed Capacity is never set for Contracted Facilities
    df.insert(1, 'design_bed_capacity', None)
    df.insert(2, 'operation_bed_capacity', None)

    # Since we can't parse the column_headers, just set them ourselves
    df.columns = _COLUMN_NAMES

    return df
