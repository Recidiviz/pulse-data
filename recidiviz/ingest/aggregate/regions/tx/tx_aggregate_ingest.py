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
"""Parse the TX Aggregated Statistics PDF."""
import datetime
from collections import OrderedDict
from typing import Dict

import pandas as pd
import us
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common.constants.aggregate import (
    enum_canonical_strings as enum_strings
)
from recidiviz.common import str_field_utils
from recidiviz.common import fips
from recidiviz.common.read_pdf import read_pdf
from recidiviz.ingest.aggregate import aggregate_ingest_utils
from recidiviz.ingest.aggregate.errors import AggregateDateParsingError
from recidiviz.persistence.database.schema.aggregate.schema import \
    TxCountyAggregate

DATE_PARSE_ANCHOR_FILENAME = 'abbreviated pop rpt'


def parse(location: str, filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    report_date = _parse_date(filename)
    table = _parse_table(location, filename, report_date)

    county_names = table.facility_name.map(_pretend_facility_is_county)
    table = fips.add_column_to_df(table, county_names, us.states.TX)

    table['report_date'] = report_date
    table['aggregation_window'] = enum_strings.daily_granularity
    table['report_frequency'] = enum_strings.monthly_granularity

    return {
        TxCountyAggregate: table
    }


def _parse_date(filename: str) -> datetime.date:
    # If this doesn't work, try scraping it from the url name
    filename_date = filename.lower()
    if DATE_PARSE_ANCHOR_FILENAME in filename_date:
        # The names can be a few formats, the most robust way is to take
        # all of the text after the anchor.
        # (eg. report Jan 2017.pdf)
        start = filename_date.index(DATE_PARSE_ANCHOR_FILENAME) \
                + len(DATE_PARSE_ANCHOR_FILENAME)
        date_str = filename_date[start:].strip('.pdf')
        parsed_date = str_field_utils.parse_date(date_str)
        if parsed_date:
            return parsed_date.replace(day=1)

    try:
        return datetime.datetime.strptime(
            filename.split('/')[-1],
            "_wp-content_uploads_%Y_%m_abbrerptcurrent.pdf")
    except ValueError as e:
        raise AggregateDateParsingError("Could not extract date") from e


def _parse_table(location: str, filename: str,
                 report_date: datetime.date) -> pd.DataFrame:
    """Parses the TX County Table in the PDF."""
    num_pages = 9
    columns_to_schema = _get_column_names(report_date)

    pages = []
    for page_num in range(1, num_pages + 1):
        # Each page has 1 or more tables on it with the last table being the
        # one with the data on it.  The headers are poorly read by tabula and
        # some years have different responses to this call so we generally
        # just get all of the tables and consider only the one with numbers on
        # it.  That lets us clean it up by dropping nonsense columns and rows,
        # and then assigning our own columns names to them.
        df = read_pdf(
            location,
            filename,
            multiple_tables=True,
            pages=page_num,
        )
        df = df[-1]
        df = df.dropna(axis='columns', thresh=5)
        # We want to remove all of the rows and columns that have no data.
        numeric_elements = df.apply(pd.to_numeric, errors='coerce').notnull()
        rows_containing_data = numeric_elements.any(axis='columns')
        df = df.loc[rows_containing_data]
        # Next we finally break up some of the columns that were incorrectly
        # concatenated.
        for column in df.columns[1:]:
            # By this point we should only have numeric data in the rows,
            # if this happens it means some columns were concatenated and they
            # must be split.  If the columns are concatenated, we need only
            # check one of the rows for a space because they are all
            # concatenated.
            if ' ' in df[column].iloc[0]:
                index_to_insert = df.columns.get_loc(column)
                df_temp = pd.DataFrame(
                    df.pop(column).str.split(n=1, expand=True))
                df.insert(index_to_insert, str(column) + '_a', df_temp[0])
                df.insert(index_to_insert + 1, str(column) + '_b', df_temp[1])
        pages.append(df)

    # Drop last rows since it's the 'Totals' section
    pages[-1] = pages[-1].drop(pages[-1].tail(1).index)

    # Build result for all the pages.  We rename the columns before calling
    # concat because the column names might all be different.  Renaming them
    # allows concat to pass happily.
    columns_to_drop = ['percent_capacity', 'total_local']
    for i, page in enumerate(pages):
        page.columns = columns_to_schema.keys()
        page = aggregate_ingest_utils.rename_columns_and_select(
            page, columns_to_schema)
        # We don't care about % of capacity and total_local so we drop these
        # columns.
        page = page.drop(columns_to_drop, axis='columns')
        pages[i] = page

    result = pd.concat(pages, ignore_index=True)

    for column_name in set(result.columns) - {'facility_name'}:
        result[column_name] = result[column_name].astype(int)

    return result


def _get_column_names(report_date):
    """Returns the correct column names based on the report date."""
    columns_to_schema = OrderedDict([
        ('County', 'facility_name'),
        ('Pretrial Felons', 'pretrial_felons'),
        ('Conv. Felons', 'convicted_felons'),
        ('Conv. Felons Sentenced to County Jail time',
         'convicted_felons_sentenced_to_county_jail'),
        ('Parole Violators', 'parole_violators'),
        ('Parole Violators with a New Charge',
         'parole_violators_with_new_charge'),
        ('Pretrial Misd.', 'pretrial_misdemeanor'),
        ('Conv. Misd.', 'convicted_misdemeanor'),
        ('Bench Warrants', 'bench_warrants'),
        ('Federal', 'federal'),
        ('Pretrial SJF', 'pretrial_sjf'),
        ('Conv. Sentenced to Co. Jail Time',
         'convicted_sjf_sentenced_to_county_jail'),
        ('Conv. SJF Sentenced to State Jail',
         'convicted_sjf_sentenced_to_state_jail'),
        ('Total Others', 'total_other'),
        ('Total Local', 'total_local'),
        ('Total Contract', 'total_contract'),
        ('Total Population', 'total_population'),
        ('Total Capacity', 'total_capacity'),
        ('% of Capacity', 'percent_capacity'),
        ('Available Beds', 'available_beds'),
    ])
    if report_date.year < 1996:
        del columns_to_schema['Conv. Felons Sentenced to County Jail time']
        del columns_to_schema['Pretrial SJF']
        del columns_to_schema['Conv. Sentenced to Co. Jail Time']
        del columns_to_schema['Conv. SJF Sentenced to State Jail']
    elif report_date.year == 1996:
        del columns_to_schema['Pretrial SJF']
        del columns_to_schema['Conv. Sentenced to Co. Jail Time']
        del columns_to_schema['Conv. SJF Sentenced to State Jail']

    return columns_to_schema


def _pretend_facility_is_county(facility_name: str) -> str:
    """Format facility_name like a county_name to match each to a fips."""
    if facility_name.lower().startswith('crystl'):
        return 'Zavala'
    if facility_name.lower().startswith('odessa'):
        return 'Ector'
    if facility_name.lower().startswith('littlefield'):
        return 'Lamb'
    if facility_name.lower().startswith('reeves'):
        return 'Reeves'
    return facility_name.replace('(P)', '')
