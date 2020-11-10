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
"""Parse the GA Aggregated Statistics PDF."""
import datetime
from typing import Dict, List

import pandas as pd
import tabula
import us
from PyPDF2 import PdfFileReader
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common import fips, str_field_utils
from recidiviz.common.constants.aggregate import (
    enum_canonical_strings as enum_strings
)
from recidiviz.ingest.aggregate import aggregate_ingest_utils
from recidiviz.ingest.aggregate.errors import AggregateDateParsingError
from recidiviz.persistence.database.schema.aggregate.schema import \
    GaCountyAggregate

DATE_PARSE_ANCHOR = 'DATA SUMMARY'


def parse(location: str, filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    table = _parse_table(location, filename)

    # Fuzzy match each facility_name to a county fips
    county_names = table.county_name.map(_sanitize_county_name)
    table = fips.add_column_to_df(table, county_names, us.states.GA)

    table['report_date'] = _parse_date(filename)
    table['aggregation_window'] = enum_strings.daily_granularity
    table['report_frequency'] = enum_strings.monthly_granularity

    return {
        GaCountyAggregate: table
    }


def _parse_date(filename: str) -> datetime.date:
    with open(filename, 'rb') as f:
        try:
            pdf = PdfFileReader(f)
            page = pdf.getPage(0)
            text = page.extractText()
            lines = text.split('\n')
        except Exception as e:
            raise AggregateDateParsingError(str(e)) from e
        for index, line in enumerate(lines):
            if DATE_PARSE_ANCHOR in line:
                # The date is on the next line if anchor is present on the line
                parsed_date = str_field_utils.parse_date(lines[index + 1])
                if parsed_date:
                    return parsed_date
        raise AggregateDateParsingError("Could not extract date")


def _parse_table(_: str, filename: str) -> pd.DataFrame:
    """Parses the last table in the GA PDF."""

    # Set column names since the pdf makes them hard to parse directly
    column_names = [
        'Index',
        'Jurisdiction',
        'Total Number of Inmates In Jail',
        'Jail Capacity',
        'Inmates as % of Capacity',
        'Number of Inmates Sentenced to State [Number]',
        'Number of Inmates Sentenced to State [% of Total]',
        'Number of Inmates Awaiting Trial in Jail [Number]',
        'Number of Inmates Awaiting Trial in Jail [% of Total]',
        'Number of Inmates Serving County Sentence [Number]',
        'Number of Inmates Serving County Sentence [% of Total]',
        'Number of Other Inmates [Number]',
        'Number of Other Inmates [% of Total]'
    ]

    # Tables at the end of the doc contain all data we want to parse
    pages = [8, 9, 10, 11]

    # Use lattice parsing since default parsing fails to parse columns on
    # the right half of the page
    use_lattice = True

    result = tabula.read_pdf(
        filename,
        pages=pages,
        lattice=use_lattice,
        pandas_options={
            'names': column_names,
            'skiprows': _header_on_each_page(),
            'skipfooter': 1,  # The last row is the grand totals
            'engine': 'python'  # Only python engine supports 'skipfooter'
        })

    result = aggregate_ingest_utils.rename_columns_and_select(result, {
        'Jurisdiction': 'county_name',
        'Total Number of Inmates In Jail': 'total_number_of_inmates_in_jail',
        'Jail Capacity': 'jail_capacity',
        'Number of Inmates Sentenced to State [Number]':
            'number_of_inmates_sentenced_to_state',
        'Number of Inmates Awaiting Trial in Jail [Number]':
            'number_of_inmates_awaiting_trial',
        'Number of Inmates Serving County Sentence [Number]':
            'number_of_inmates_serving_county_sentence',
        'Number of Other Inmates [Number]':
            'number_of_other_inmates'
    })

    # Tabula may parse extra empty rows
    result = result.dropna()

    aggregate_ingest_utils.cast_columns_to_int(
        result, ignore_columns={'county_name'})

    return result


def _sanitize_county_name(county_name: str) -> str:
    return county_name.replace('NO JAIL', '').rstrip(' ')


def _header_on_each_page() -> List[int]:
    """Every 43rd row is the header on a new page. This is better passed to
    pandas as a lambda x: x % 43 == 0, but since pandas options are sent over
    HTTP we use a list instead."""
    return [x * 43 for x in range(10000)]
