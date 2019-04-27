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
"""Ingest TN aggregate jail data.
"""
import datetime
from typing import Dict

import numpy as np
import pandas as pd
import tabula
import us
from sqlalchemy.ext.declarative import DeclarativeMeta

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.common import str_field_utils
from recidiviz.common import fips
from recidiviz.ingest.aggregate import aggregate_ingest_utils
from recidiviz.persistence.database.schema import TnFacilityAggregate, \
    TnFacilityFemaleAggregate


_JAIL_REPORT_COLUMN_NAMES = [
    'facility_name',
    'tdoc_backup_population',
    'local_felons_population',
    'other_convicted_felons_population',
    'federal_and_other_population',
    'convicted_misdemeanor_population',
    'pretrial_felony_population',
    'pretrial_misdemeanor_population',
    'total_jail_population',
    'total_beds',
]


_FEMALE_JAIL_REPORT_COLUMN_NAMES = [
    'facility_name',
    'tdoc_backup_population',
    'local_felons_population',
    'other_convicted_felons_population',
    'federal_and_other_population',
    'convicted_misdemeanor_population',
    'pretrial_felony_population',
    'pretrial_misdemeanor_population',
    'female_jail_population',
    'total_beds',
    'percent_total_capacity',
    'female_beds',
]


_KEEP_FEMALE_JAIL_REPORT_COLUMN_NAMES = [
    'facility_name',
    'tdoc_backup_population',
    'local_felons_population',
    'other_convicted_felons_population',
    'federal_and_other_population',
    'convicted_misdemeanor_population',
    'pretrial_felony_population',
    'pretrial_misdemeanor_population',
    'female_jail_population',
    'female_beds',
]


_MANUAL_FACILITY_TO_COUNTY_MAP = {
    'Johnson City': 'Washington',
    'Johnson City (F)': 'Washington',
    'Kingsport': 'Sullivan',
    'Kingsport City': 'Sullivan',
}


def parse(filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    # There are two types of reports, total jail population and female
    # jail population. The reports are very similar, but need to be
    # handled slightly differently.
    is_female = 'female' in filename

    table = _parse_table(filename, is_female)

    names = table.facility_name.apply(_pretend_facility_is_county)
    table = fips.add_column_to_df(table, names, us.states.TN)

    table['report_date'] = _parse_date(filename)
    table['aggregation_window'] = enum_strings.daily_granularity
    table['report_frequency'] = enum_strings.monthly_granularity

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
    # Slashes are converted to underscores in the GCS bucket. This
    # assumes there are no underscores in the URL basename.
    base_filename = filename.split('_')[-1].replace('female', '')
    end = base_filename.index('.pdf')
    start = 4
    d = str_field_utils.parse_date(base_filename[start:end])
    return aggregate_ingest_utils.on_last_day_of_month(d)


def _expand_columns_with_spaces_to_new_columns(
        df: pd.DataFrame) -> pd.DataFrame:
    """Varying numbers of columns are parsed into a single column based
    on headers that change over time. To account for this, create a
    new dataframe with columns that are created by splitting the
    contents of the smashed together columns, when we find that situation.
    """
    expanded_df = pd.DataFrame(index=df.index)
    for col_ind in range(len(df.columns)):
        col = df.iloc[:, col_ind]
        if col.isnull().all():
            continue

        # Just copy over the first column and columns with no spaces,
        # which haven't been smashed together, presumably.
        if col_ind == 0 or not col.str.contains(' ').any():
            expanded_df = expanded_df.join(col)
        else:
            # Extract all the smashed together columns into their own
            # columns.

            def grab_one_smashed_col(smashed, col_ind):
                if pd.isnull(smashed) or len(smashed.split()) <= col_ind:
                    return np.nan
                return smashed.split()[col_ind]

            cur_smashed_col = 0
            while True:
                smashed_col = col.apply(
                    lambda smashed, col_ind=cur_smashed_col:
                    grab_one_smashed_col(smashed, col_ind))
                if (smashed_col.isnull()).all():
                    break

                smashed_col.name = col.name + '_{}'.format(cur_smashed_col)
                expanded_df = expanded_df.join(smashed_col)
                cur_smashed_col += 1

    return expanded_df


def _format_table(df: pd.DataFrame, is_female: bool) -> pd.DataFrame:
    """Format the dataframe that comes from one page of the PDF."""

    # The first four rows are parsed containing the column names.
    df.columns = df.iloc[:4].apply(lambda rows: ' '.join(rows.dropna()).strip())
    df = df.iloc[4:]

    df = _expand_columns_with_spaces_to_new_columns(df)

    # Discard extra columns and rename the columns based on the table.
    if is_female:
        df = df.iloc[:, 0:len(_FEMALE_JAIL_REPORT_COLUMN_NAMES)]
        df.columns = _FEMALE_JAIL_REPORT_COLUMN_NAMES
        df = df[[col for col in df.columns if col in
                 _KEEP_FEMALE_JAIL_REPORT_COLUMN_NAMES]]

        # Until 2013, the female reports didn't have beds, so percent
        # capacity gets misinterpreted as female beds.
        keep_cols = [col for col in df.columns
                     if not df[col].apply(lambda val: isinstance(val, str) and
                                          val.endswith('%')).any()]
        df = df[keep_cols]
    else:
        df = df.iloc[:, 0:len(_JAIL_REPORT_COLUMN_NAMES)]
        df.columns = _JAIL_REPORT_COLUMN_NAMES

    # When the notes column has more than one line of text, tabula
    # parses a row of null.
    df = df.dropna(how='all')

    # Sometimes there are missing values, the best we can do is make them zeros?
    # TODO The real trouble here is that column shifts might happen if
    # missing values occur in smashed columns.
    df = df.fillna(0)

    df = df.replace('`', 0)
    df = df.replace('.', 0)
    df = df.replace('N/A', 0)

    return df


def _pretend_facility_is_county(facility_name: str) -> str:
    """Format facility_name like a county_name to match each to a fips."""
    if facility_name in _MANUAL_FACILITY_TO_COUNTY_MAP:
        return _MANUAL_FACILITY_TO_COUNTY_MAP[facility_name]

    words_after_county_name = [
        '-',
        'Annex',
        'Co. Det. Center',
        'CJC',
        'CWC (CDC',
        'CDC (F)',
        'CDC (M)',
        'Det. Center',
        'Det, Center',
        'Extension',
        'Extention',
        'Jail',
        'SCCC',
        '(Temporarily closed)',
        'Work Center',
        'Workhouse',
    ]
    for delimiter in words_after_county_name:
        facility_name = facility_name.split(delimiter)[0]

    return facility_name
