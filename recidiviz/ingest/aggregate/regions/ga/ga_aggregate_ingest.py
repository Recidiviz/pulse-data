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
"""Parse the GA Aggregated Statistics PDF."""
from typing import Dict
import datetime
import dateparser

import pandas as pd
from PyPDF2 import PdfFileReader
import tabula
from sqlalchemy.ext.declarative import DeclarativeMeta

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate import aggregate_ingest_utils
from recidiviz.ingest.aggregate.errors import AggregateDateParsingError
from recidiviz.persistence.database.schema import GaCountyAggregate

DATE_PARSE_ANCHOR = 'DATA SUMMARY'


def parse(filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    ga_county_table = _parse_table(filename)

    # TODO(#698): Set county fips based on the county_name
    ga_county_table['fips'] = None
    ga_county_table['report_date'] = _parse_date(filename)
    ga_county_table['report_granularity'] = enum_strings.monthly_granularity

    return {
        GaCountyAggregate: ga_county_table
    }


def _parse_date(filename: str) -> datetime.date:
    with open(filename, 'rb') as f:
        try:
            pdf = PdfFileReader(f)
            page = pdf.getPage(0)
            text = page.extractText()
            lines = text.split('\n')
        except Exception as e:
            raise AggregateDateParsingError(str(e))
        for index, line in enumerate(lines):
            if DATE_PARSE_ANCHOR in line:
                # The date is on the next line if anchor is present on the line
                return dateparser.parse(lines[index+1]).date()
        raise AggregateDateParsingError('Could not extract date')


def _parse_table(filename: str) -> pd.DataFrame:
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
            'skiprows': _every_43,  # Skip header rows at the top of each page
            'skipfooter': 1  # The last row is the grand totals
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

    return result


def _every_43(index: int) -> bool:
    return index % 43 == 0
