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
"""Parse the TX Aggregated Statistics PDF."""
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
from recidiviz.persistence.database.schema import TxCountyAggregate

DATE_PARSE_ANCHOR = 'Abbreviated Population Report for'


def parse(filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    table = _parse_table(filename)

    # TODO(#698): Set county fips based on the county_name
    table['fips'] = None
    table['report_date'] = _parse_date(filename)
    table['report_granularity'] = enum_strings.monthly_granularity

    return {
        TxCountyAggregate: table
    }


def _parse_date(filename: str) -> datetime.date:
    with open(filename, 'rb') as f:
        try:
            pdf = PdfFileReader(f)
            page = pdf.getPage(0)
            text = page.extractText()
            lines = text.split('\n')
        except Exception as e:
            AggregateDateParsingError(str(e))
        for index, line in enumerate(lines):
            if DATE_PARSE_ANCHOR in line:
                # The date is on the next line if anchor is present on the line
                return dateparser.parse(lines[index+1]).date()
        raise AggregateDateParsingError('Could not extract date')


def _parse_table(filename: str) -> pd.DataFrame:
    """Parses the TX County Table in the PDF."""
    num_pages = 9

    pages = []
    for page in range(1, num_pages):
        page = tabula.read_pdf(
            filename,
            pages=page,
            pandas_options={
                'header': [1, 2, 3, 4, 5],
            }
        )
        pages.append(page)

    _, _, last_page = tabula.read_pdf(
        filename,
        multiple_tables=True,
        pages=num_pages,
        pandas_options={
            'header': [1, 2, 3, 4, 5],
        }
    )

    # Drop empty column created by the 'Totals' section
    last_page = last_page.drop(columns=1)

    # Drop last rows since it's the 'Totals' section
    last_page = last_page.drop(last_page.tail(1).index)

    # Build result for all the pages (excluding the last_page)
    result = pd.concat(pages, ignore_index=True)
    result.columns = aggregate_ingest_utils.collapse_header(result.columns)

    # Add the last_page to the result
    last_page.columns = result.columns
    result = result.append(last_page, ignore_index=True)

    result = aggregate_ingest_utils.rename_columns_and_select(result, {
        'County': 'county_name',
        'Pretrial Felons': 'pretrial_felons',
        'Conv. Felons': 'convicted_felons',
        'Sentenced to County Jail time':
            'convicted_felons_sentenced_to_county_jail',
        'Parole Violators': 'parole_violators',
        'Violators with a New Charge': 'parole_violators_with_new_charge',
        'Pretrial Misd.': 'pretrial_misdemeanor',
        'Conv. Misd.': 'convicted_misdemeanor',
        'Bench Warrants': 'bench_warrants',
        'Federal': 'federal',
        'Pretrial SJF': 'pretrial_sjf',
        'Sentenced to Co. Jail Time': 'convicted_sjf_sentenced_to_county_jail',
        'SJF Sentenced to State Jail': 'convicted_sjf_sentenced_to_state_jail',
        'Total Others': 'total_other',
        'Total Contract': 'total_contract',
        'Total Population': 'total_population',
        'Total Capacity': 'total_capacity',
        'Available Beds': 'available_beds'
    })

    for column_name in set(result.columns) - {'county_name'}:
        result[column_name] = result[column_name].astype(int)

    return result
