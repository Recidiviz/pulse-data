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
"""Parse PA Aggregated Statistics."""
import datetime
from typing import Dict

import pandas as pd
import us
from numpy import NaN
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common.constants.aggregate import (
    enum_canonical_strings as enum_strings
)
from recidiviz.ingest.aggregate import aggregate_ingest_utils
from recidiviz.common import fips
from recidiviz.persistence.database.schema.aggregate.schema import \
    PaFacilityPopAggregate, PaCountyPreSentencedAggregate


def parse(_, filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    table_1 = _parse_tab_1(filename)
    table_2 = _parse_tab_2(filename)

    return {
        PaFacilityPopAggregate: table_1,
        PaCountyPreSentencedAggregate: table_2
    }


def _parse_tab_1(filename: str) -> pd.DataFrame:
    """Parses the first tab in the PA aggregate report."""
    column_names = {
        r'County Name': 'facility_name',
        r'Bed Capacity': 'bed_capacity',
        r'.*Community Corrections Beds.*':
            'work_release_community_corrections_beds',
        r'.*In-House Daily Pop.*': 'in_house_adp',
        r'.*Housed Elsewhere Daily Pop.*': 'housed_elsewhere_adp',
        r'.*In-House Work Release.*': 'work_release_adp',
        r'Admissions': 'admissions',
        r'Discharge': 'discharge'
    }

    # Parse everything directly to allow us to correctly map "N/A" and "N/R"
    keep_default_na = False
    df = pd.read_excel(filename, sheet_name=0, header=1, keep_default_na=keep_default_na, engine='openpyxl')

    # Drop "F/T" and "P/T" line
    df = df[1:]

    # Drop Totals footer
    df = df[:-10]

    df.columns = df.columns.map(lambda name: name.rstrip(' '))
    df = aggregate_ingest_utils.rename_columns_and_select(
        df, column_names, use_regex=True)

    # Some cells have extra '*'
    df = df.applymap(lambda e: str(e).rstrip(' *'))

    df = df.apply(_to_numeric)

    df['report_date'] = _report_date_tab_1(filename)
    df = fips.add_column_to_df(df, df['facility_name'], us.states.PA)
    df['aggregation_window'] = enum_strings.yearly_granularity
    df['report_frequency'] = enum_strings.yearly_granularity

    return df.reset_index(drop=True)


def _report_date_tab_1(filename):
    df = pd.read_excel(filename, sheet_name=0, header=None, engine='openpyxl')

    # The first cell contains the date
    year = int(df[0][0].replace(' Statistics', ''))

    return datetime.date(year=year, month=1, day=1)


def _parse_tab_2(filename: str):
    df = pd.read_excel(filename, sheet_name=1, header=1, engine='openpyxl')

    # Drop Totals footer
    df = df[:-5]

    # Set index/columns with correct names
    df = df.rename({df.columns[0]: 'county_name'}, axis='columns')
    df = df.set_index('county_name')
    df = df.rename_axis('report_date', axis='columns')

    # Collapse each column into a new row
    df = df.unstack()
    df = df.rename('pre_sentenced_population')
    df = df.reset_index()

    df['county_name'] = df['county_name'].str.rstrip(' ')
    df['report_date'] = df['report_date'].dt.date
    df['pre_sentenced_population'] = _to_numeric(df['pre_sentenced_population'])

    df = fips.add_column_to_df(df, df['county_name'], us.states.PA)
    df['aggregation_window'] = enum_strings.daily_granularity
    df['report_frequency'] = enum_strings.quarterly_granularity

    return df


def _to_numeric(column):
    # Skip columns that shouldn't be numeric
    if column.name == 'facility_name':
        return column

    # "N/A" means a value could never be set, so set it explicitly to 0
    column = column.map(lambda cell: 0 if cell == 'N/A' else cell)

    # "N/R" means "Not Reported", so write null to the database with NaN
    column = column.map(lambda cell: NaN if cell == 'N/R' else cell)

    return pd.to_numeric(column)
