# Recidiviz - a platform for tracking granular recidivism metrics in real time
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
"""Ingest TN aggregate jail data.
"""
import datetime
import os
from typing import Dict

import pandas as pd
import tabula
import us
from sqlalchemy.ext.declarative import DeclarativeMeta

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.common import date, fips
from recidiviz.ingest.aggregate import aggregate_ingest_utils
from recidiviz.persistence.database.schema import TnFacilityAggregate, \
    TnFacilityFemaleAggregate


_MANUAL_FACILITY_TO_COUNTY_MAP = {
    'Johnson City (F)': 'Washington',
    'Kingsport City': 'Sullivan',
}


def parse(filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    # There are two types of reports, total jail population and female
    # jail population. The reports are very similar, but need to be
    # handled slightly differently.
    is_female = 'Female' in filename

    table = _parse_table(filename, is_female)

    names = table.facility_name.apply(_pretend_facility_is_county)
    table = fips.add_column_to_df(table, names, us.states.TN)

    table['report_date'] = _parse_date(filename)
    table['report_granularity'] = enum_strings.monthly_granularity

    return {
        TnFacilityFemaleAggregate: table
    } if is_female else {
        TnFacilityAggregate: table
    }


def _parse_table(filename: str, is_female: bool) -> pd.DataFrame:
    table = tabula.read_pdf(filename, pages=[2, 3, 4], multiple_tables=True)

    formatted_dfs = [_format_table(df, is_female) for df in table]

    table = pd.concat(formatted_dfs, ignore_index=True)

    # Discard 'TOTAL' row.
    table = table.iloc[:-1]

    table = aggregate_ingest_utils.cast_columns_to_int(
        table, ignore_columns={'facility_name'})

    return table


def _parse_date(filename: str) -> datetime.date:
    base_filename = os.path.basename(filename).replace('Female', '')
    end = base_filename.index('.pdf')
    start = 4
    d = date.parse_date(base_filename[start:end])
    return aggregate_ingest_utils.on_last_day_of_month(d)


def _format_table(df: pd.DataFrame, is_female: bool) -> pd.DataFrame:
    """Format the dataframe that comes from one page of the PDF."""

    # The first four rows are parsed containing the column names.
    df.columns = df.iloc[:4].apply(lambda rows: ' '.join(rows.dropna()).strip())
    df = df.iloc[4:]

    # Some columns get smashed together on parse due to column grouping.
    smashed_columns_placeholder_name = 'smashed'
    if is_female:
        smashed_cols = [
            'local_felons_population',
            'other_convicted_felons_population',
            'federal_and_other_population',
            'convicted_misdemeanor_population',
            'pretrial_felony_population',
        ]

        rename = {
            r'FACILITY': 'facility_name',
            r'TDOC Backup.*': 'tdoc_backup_population',
            r'FEMALE POPULATION': smashed_columns_placeholder_name,
            r'Pre- trial.*': 'pretrial_misdemeanor_population',
            r'Female Jail Pop\.': 'female_jail_population',
            r'Female Beds\*\*': 'female_beds',
        }

    else:
        smashed_cols = [
            'other_convicted_felons_population',
            'federal_and_other_population',
        ]

        rename = {
            r'FACILITY': 'facility_name',
            r'TDOC Backup.*': 'tdoc_backup_population',
            r'Local': 'local_felons_population',
            r'Other .* Conv.*': smashed_columns_placeholder_name,
            r'Conv\. Misd\.': 'convicted_misdemeanor_population',
            r'Pre- trial Felony': 'pretrial_felony_population',
            r'Pre- trial Misd\.': 'pretrial_misdemeanor_population',
            r'Total Jail Pop\.': 'total_jail_population',
            r'Total Beds\*\*': 'total_beds',
        }

    df = aggregate_ingest_utils.rename_columns_and_select(
        df, rename, use_regex=True)

    # When the notes column has more than one line of text, tabula
    # parses a row of null.
    df = df.dropna(how='all')

    # Extract individual values from the smashed columns.
    for ind, col_name in enumerate(smashed_cols):
        df[col_name] = df[smashed_columns_placeholder_name].map(
            lambda element, ind=ind: element.split()[ind])

    df = df.drop(smashed_columns_placeholder_name, axis=1)

    return df


def _pretend_facility_is_county(facility_name: str) -> str:
    """Format facility_name like a county_name to match each to a fips."""
    if facility_name in _MANUAL_FACILITY_TO_COUNTY_MAP:
        return _MANUAL_FACILITY_TO_COUNTY_MAP[facility_name]

    words_after_county_name = [
        '-',
        'Annex',
        'Co. Det. Center',
        'Det. Center',
        'Det, Center',
        'Extension',
        'Jail',
        'SCCC',
        'Work Center',
        'Workhouse',
    ]
    for delimiter in words_after_county_name:
        facility_name = facility_name.split(delimiter)[0]

    return facility_name
