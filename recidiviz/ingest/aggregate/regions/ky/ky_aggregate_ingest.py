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
"""Parse the KY Aggregated Statistics PDF."""
import datetime
from typing import Dict, List

import dateparser
import numpy as np
import pandas as pd
import tabula
import us
from sqlalchemy.ext.declarative import DeclarativeMeta

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate import aggregate_ingest_utils, fips
from recidiviz.persistence.database.schema import KyFacilityAggregate


def parse(filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    table = _parse_table(filename)

    # Fuzzy match each facility_name to a county fips
    county_names = table.facility_name.map(_pretend_facility_is_county)
    table = fips.add_column_to_df(table, county_names, us.states.KY)

    table['report_date'] = _parse_date(filename)
    table['report_granularity'] = enum_strings.monthly_granularity

    return {
        KyFacilityAggregate: table
    }


def _parse_table(filename: str) -> pd.DataFrame:
    """Parses the table in the KY PDF."""
    whole_df = tabula.read_pdf(
        filename,
        pages='all',
        lattice=True,
        pandas_options={
            'skipfooter': 5  # Last rows are totals
        })

    whole_df = _shift_headers(whole_df)
    whole_df.columns = whole_df.columns.str.replace('\n', ' ')

    # Each block of county data starts with a filled in 'Total Jail Beds'
    start_of_county_indices = np.where(whole_df['Total Jail Beds'].notnull())[0]
    dfs_split_by_county = _split_df(whole_df, start_of_county_indices)

    dfs_grouped_by_gender = []
    for df in dfs_split_by_county:
        df['Gender'] = None
        df = _collapse_by_gender_rows(df, 'Male')
        df = _collapse_by_gender_rows(df, 'Female')

        # The first row contains header data for both Male and Female
        df['County'] = df['County'][0]
        df['total_jail_beds'] = df['Total Jail Beds'][0]
        df['reported_population'] = \
            df['Reported Population (Total and Male/Female)'][0]
        df = df[1:]

        dfs_grouped_by_gender.append(df)

    df_by_gender = pd.concat(dfs_grouped_by_gender)

    # Split into male_df and female_df to independently set column headers
    male_df = df_by_gender.loc[df_by_gender['Gender'] == 'Male']
    female_df = df_by_gender.loc[df_by_gender['Gender'] == 'Female']

    # Since both male_df and female_df contain shared data, pick arbitrarily
    shared_df = aggregate_ingest_utils.rename_columns_and_select(female_df, {
        'County': 'facility_name',
        'total_jail_beds': 'total_jail_beds',
        'reported_population': 'reported_population',
    })

    male_df = aggregate_ingest_utils.rename_columns_and_select(male_df, {
        'County': 'facility_name',
        # Since we've grouped by Male, this Reported Population is only Male
        'Reported Population (Total and Male/Female)': 'male_population',
        'Class D Inmates': 'class_d_male_population',
        'Community Custody Inmates': 'community_custody_male_population',
        'Alternative Sentence': 'alternative_sentence_male_population',
        'Controlled Intake': 'controlled_intake_male_population',
        'Parole Violators': 'parole_violators_male_population',
        'Federal Inmates': 'federal_male_population',
    })

    female_df = aggregate_ingest_utils.rename_columns_and_select(female_df, {
        'County': 'facility_name',
        # Since we've grouped by Female, this Reported Population is only Female
        'Reported Population (Total and Male/Female)': 'female_population',
        'Class D Inmates': 'class_d_female_population',
        'Community Custody Inmates': 'community_custody_female_population',
        'Alternative Sentence': 'alternative_sentence_female_population',
        'Controlled Intake': 'controlled_intake_female_population',
        'Parole Violators': 'parole_violators_female_population',
        'Federal Inmates': 'federal_female_population',
    })

    result = shared_df.join(male_df.set_index('facility_name'),
                            on='facility_name')
    result = result.join(female_df.set_index('facility_name'),
                         on='facility_name')

    for column_name in set(result.columns) - {'facility_name'}:
        result[column_name] = result[column_name].astype(int)

    return result.reset_index(drop=True)


def _shift_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Shift the parsed headers one column to the left."""
    columns = df.columns[1:]  # Ignore the first header
    df = df.drop(df.columns[-1], axis=1)  # Drop the last column
    df.columns = columns  # Re-Assign the correct column headers
    return df


def _split_df(df: pd.DataFrame, indices: List[int]) -> List[pd.DataFrame]:
    """
    Split the fully parsed DataFrame into a new DataFrame based on |indices|.
    """
    end = None

    dfs = []
    for start, end in aggregate_ingest_utils.pairwise(indices):
        dfs.append(df[start:end].reset_index(drop=True))

    if end:
        dfs.append(df[end:].reset_index(drop=True))

    return dfs


def _collapse_by_gender_rows(df: pd.DataFrame, gender: str) -> pd.DataFrame:
    """
    Collapse all rows with |gender_str| in the 'County' column and group them
    by the 'Gender' column. This has the effect of combining both Secure and
    Non-Secure groups.
    """
    matching_rows = df['County'].str.contains(gender)

    # To get counts from the PDF, sum secure/non-secure. For example:
    # male_population = male_population (secure) + male_population (unsecure)
    collapsed_row = df.loc[matching_rows].sum(axis='rows')

    collapsed_row['Gender'] = gender

    df = df[~matching_rows]
    df = df.append(collapsed_row, ignore_index=True)

    return df.reset_index(drop=True)


def _parse_date(filename: str) -> datetime.date:
    """
    Parse the report_date from the filename since the PDF contents can't
    easily be parsed for the date.
    """
    filename = filename.split('/')[-1]
    date_string = filename.strip('.pdf')
    return dateparser.parse(date_string).date()


def _pretend_facility_is_county(facility_name: str):
    """Format facility_name like a county_name to match each to a fips."""
    if facility_name == 'Three Forks (Lee)':
        return 'lee county'

    return facility_name.split(' ')[0]
