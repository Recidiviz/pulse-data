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
"""Parse the KY Aggregated Statistics PDF."""
import datetime
from typing import Dict, List

import numpy as np
import pandas as pd
import tabula
import us
from more_itertools.more import one
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common.constants.aggregate import (
    enum_canonical_strings as enum_strings
)
from recidiviz.common import str_field_utils
from recidiviz.common import fips
from recidiviz.ingest.aggregate import aggregate_ingest_utils
from recidiviz.ingest.aggregate.errors import AggregateDateParsingError
from recidiviz.persistence.database.schema.aggregate.schema import \
    KyFacilityAggregate


def parse(location: str, filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    table = _parse_table(location, filename)

    # Fuzzy match each facility_name to a county fips
    county_names = table.facility_name.map(_pretend_facility_is_county)
    table = fips.add_column_to_df(table, county_names, us.states.KY)

    table['report_date'] = parse_date(filename)
    table['aggregation_window'] = enum_strings.daily_granularity
    table['report_frequency'] = enum_strings.weekly_granularity

    return {
        KyFacilityAggregate: table
    }


def _parse_table(_, filename: str) -> pd.DataFrame:
    """Parses the table in the KY PDF."""
    whole_df = one(tabula.read_pdf(
        filename,
        pages='all',
        multiple_tables=False,
        lattice=True
    ))

    if filename.endswith('04-16-20.pdf'):
        whole_df[323:331] = whole_df[323:331].shift(-1, axis='columns')
    elif filename.endswith('07-09-20.pdf'):
        whole_df.loc[432] = whole_df.iloc[432].shift(-1)
        whole_df.loc[434:436] = whole_df.loc[434:436].shift(-1, axis='columns')
        whole_df.loc[438] = whole_df.iloc[438].shift(-1)
        whole_df.loc[440] = whole_df.iloc[440].shift(-1)
        whole_df.loc[442:445] = whole_df.loc[442:445].shift(-1, axis='columns')
        whole_df.loc[447:462] = whole_df.loc[447:462].shift(-1, axis='columns')
        whole_df.loc[464:] = whole_df.loc[464:].shift(-1, axis='columns')
        whole_df.loc[451, 'County'] = 86
        whole_df.loc[456, 'County'] = 264
        whole_df.loc[461, 'County'] = 52
        whole_df.loc[464, 'County'] = 161
        whole_df.loc[469, 'County'] = 70
        whole_df.loc[472, 'County'] = 204
        whole_df.loc[477, 'County'] = 182
        whole_df.loc[482, 'County'] = 137
        whole_df.loc[487, 'County'] = 45
        whole_df.loc[492, 'County'] = 410
        whole_df.loc[497, 'County'] = 152
        whole_df.loc[500, 'County'] = 95
        whole_df.loc[505, 'County'] = 85
        whole_df.loc[508, 'County'] = 194
        whole_df.loc[513, 'County'] = 72
        whole_df.loc[516, 'County'] = 134
        whole_df.loc[521, 'County'] = 50
        whole_df.loc[524, 'County'] = 63
        whole_df.loc[529, 'County'] = 32

    # Remove totals separate from parsing since it's a variable length
    totals_start_index = np.where(whole_df['Date'].str.contains('Totals'))[0][0]
    whole_df = whole_df[:totals_start_index]

    # Some rows are parsed including the date, which shift them 1 too far right
    shifted_rows = whole_df['County'].astype(str).str.contains('Secure')
    whole_df[shifted_rows] = whole_df[shifted_rows].shift(-1, axis='columns')

    whole_df = whole_df[whole_df['County'].astype(str) != 'County']

    whole_df.reset_index(drop=True)

    whole_df = _shift_headers(whole_df)
    whole_df.columns = whole_df.columns.str.replace('\n', ' ')
    whole_df.columns = whole_df.columns.str.replace('\r', ' ')

    # Column names can change over time : (
    column_name_map = {
        'CC Eligible Inmates': 'Community Custody Inmates',
    }
    whole_df.columns = [column_name_map[c] if c in column_name_map else c
                        for c in whole_df.columns]

    # Each block of county data starts with a filled in 'Total Jail Beds'
    start_of_county_indices = np.where(whole_df['Total Jail Beds'].notnull())[0]
    dfs_split_by_county = _split_df(whole_df, start_of_county_indices)

    dfs_grouped_by_gender = []
    for df in dfs_split_by_county:
        # This is a typo in several reports
        if '12/' in df['Federal Inmates'].values:
            df['Federal Inmates'] = df['Federal Inmates'].replace({'12/': '12'})
        if 'yo' in df['Federal Inmates'].values:
            df['Federal Inmates'] = df['Federal Inmates'].replace({'yo': '0'})
        if 'pe' in df['Federal Inmates'].values:
            df['Federal Inmates'] = df['Federal Inmates'].replace({'pe': '0'})

        # Cast everything to int before summing below
        df = df.fillna(0)
        df = aggregate_ingest_utils.cast_columns_to_int(
            df, ignore_columns={'County', 'Facility Security', 'Inmate Cusody'})

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
    male_df = df_by_gender[df_by_gender['Gender'] == 'Male']
    female_df = df_by_gender[df_by_gender['Gender'] == 'Female']

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

    if filename.endswith('04-16-20.pdf'):
        result.loc[result['facility_name'] == 'Lincoln', 'total_jail_beds'] = 72

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
    matching_rows = df['County'].str.contains(gender).fillna(False)

    # To get counts from the PDF, sum secure/non-secure. For example:
    # male_population = male_population (secure) + male_population (unsecure)
    collapsed_row = df[matching_rows].sum(axis='rows')

    collapsed_row['Gender'] = gender

    df = df[~matching_rows]
    df = df.append(collapsed_row, ignore_index=True)

    return df.reset_index(drop=True)


def parse_date(filename: str) -> datetime.date:
    """
    Parse the report_date from the filename since the PDF contents can't
    easily be parsed for the date.
    """
    date_str = filename.replace(' revised', ''). \
                   replace(' new', '').replace('.pdf', '')[-8:]
    parsed_date = str_field_utils.parse_date(date_str)
    if parsed_date:
        return parsed_date
    raise AggregateDateParsingError("Could not extract date")


def _pretend_facility_is_county(facility_name: str):
    """Format facility_name like a county_name to match each to a fips."""
    if facility_name == 'Three Forks (Lee)':
        return 'lee county'

    return facility_name.split(' ')[0]
