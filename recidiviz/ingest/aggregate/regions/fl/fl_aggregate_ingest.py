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
"""Parse the FL Aggregated Statistics PDF."""
import datetime
import locale
from typing import Dict

import pandas as pd
import tabula
from sqlalchemy.ext.declarative import DeclarativeMeta

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate import aggregate_ingest_utils
from recidiviz.persistence.database.schema import FlCountyAggregate


def parse(filename: str, date_scraped: datetime.datetime) \
        -> Dict[DeclarativeMeta, pd.DataFrame]:
    _setup()

    fl_county_table = _parse_table_1(filename)

    # TODO(#698): Set county fips based on the county_name
    fl_county_table['fips'] = None
    fl_county_table['report_date'] = date_scraped
    fl_county_table['report_granularity'] = enum_strings.monthly_granularity

    return {
        FlCountyAggregate: fl_county_table
    }


def _parse_table_1(filename: str) -> pd.DataFrame:
    """Parses the FL County - Table 1 in the PDF."""
    part1 = tabula.read_pdf(
        filename,
        pages=[3],
        pandas_options={
            'header': [0, 1],
        })
    part2 = tabula.read_pdf(
        filename,
        pages=[4],
        pandas_options={
            'header': [0, 1],
            'skipfooter': 1  # The last row is the total
        })
    result = part1.append(part2, ignore_index=True)

    result.columns = aggregate_ingest_utils.collapse_header(result.columns)
    result = aggregate_ingest_utils.rename_columns_and_select(result, {
        'Florida County': 'county_name',
        'County Population': 'county_population',
        'Average Daily Population (ADP)': 'average_daily_population',
        '*Date Reported': 'date_reported'
    })

    for column_name in {'county_population', 'average_daily_population'}:
        result[column_name] = result[column_name].apply(locale.atoi)
    result['date_reported'] = pd.to_datetime(result['date_reported'])

    return result


def _setup() -> None:
    # This allows us to call `locale.atoi` when converting str -> int
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
