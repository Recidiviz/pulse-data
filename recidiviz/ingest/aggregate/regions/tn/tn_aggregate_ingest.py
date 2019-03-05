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
import os
from typing import Dict

import datetime
import dateparser
import pandas as pd
from sqlalchemy.ext.declarative import DeclarativeMeta
import tabula
import us

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate import aggregate_ingest_utils
from recidiviz.common import fips
from recidiviz.persistence.database.schema import TnFacilityAggregate


_MANUAL_FACILITY_TO_COUNTY_MAP = {
    'Johnson City (F)': 'Washington',
    'Kingsport City': 'Sullivan',
}


def parse(filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    table = _parse_table(filename)

    names = table.facility_name.apply(_pretend_facility_is_county)
    table = fips.add_column_to_df(table, names, us.states.TN)

    table['report_date'] = _parse_date(filename)
    table['report_granularity'] = enum_strings.monthly_granularity

    return {
        TnFacilityAggregate: table
    }


def _parse_table(filename: str) -> pd.DataFrame:
    table = tabula.read_pdf(filename, pages=[2, 3, 4], multiple_tables=True)

    formatted_dfs = [_format_table(df) for df in table]

    table = pd.concat(formatted_dfs, ignore_index=True)

    # Discard 'TOTAL' row.
    table = table.iloc[:-1]

    table = aggregate_ingest_utils.cast_columns_to_int(
        table, ignore_columns={'facility_name'})

    return table


def _parse_date(filename: str) -> datetime.date:
    base_filename = os.path.basename(filename)
    end = base_filename.index('.pdf')
    start = 4
    d = dateparser.parse(base_filename[start:end]).date()
    return aggregate_ingest_utils.on_last_day_of_month(d)


def _format_table(df: pd.DataFrame) -> pd.DataFrame:
    """Format the dataframe that comes from one page of the PDF."""

    # The first four rows are parsed containing the column names.
    df.columns = df.iloc[:4].apply(lambda rows: ' '.join(rows.dropna()).strip())
    df = df.iloc[4:]

    rename = {
        r'FACILITY': 'facility_name',
        r'TDOC Backup.*': 'tdoc_backup_population',
        r'Local': 'local_felons_population',
        r'Other .* Conv.*': 'other_convicted_felons_population',
        r'Conv\. Misd\.': 'convicted_misdemeanor_population',
        r'Pre- trial Felony': 'pretrial_felony_population',
        r'Pre- trial Misd\.': 'pretrial_misdemeanor_population',
        r'Total Jail Pop\.': 'total_jail_population',
        r'Total Beds\*\*': 'total_beds',
    }

    df = aggregate_ingest_utils.rename_columns_and_select(
        df, rename, use_regex=True)

    # Some rows are two rows tall because of notes. These are parsed
    # into two rows, one of which is all null except some text in the
    # notes, which we ignore anyhow.
    df = df.dropna(how='all')

    # Tablua smashes two columns together when parsing.
    df['federal_and_other_population'] = df[
        'other_convicted_felons_population'].map(
            lambda element: element.split()[1])
    df['other_convicted_felons_population'] = df[
        'other_convicted_felons_population'].map(
            lambda element: element.split()[0])

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
