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
import locale
from typing import Dict

import pandas as pd
import tabula

from recidiviz.ingest.aggregate import aggregate_ingest_utils


def parse(filename: str) -> Dict[str, pd.DataFrame]:
    _setup()

    return {
        'fl_county_adp': _parse_table_1(filename)
    }


def _parse_table_1(filename: str) -> pd.DataFrame:
    """Parses the FL County ADP - Table 1 in the PDF."""
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
    result = result.rename(
        columns={
            'Florida County': 'County',
            'Average Daily Population (ADP)': 'Average Daily Population',
            '*Date Reported': 'Date Reported'
        })

    result = result[['County', 'County Population', 'Average Daily Population',
                     'Date Reported']]

    for column_name in {'County Population', 'Average Daily Population'}:
        result[column_name] = result[column_name].apply(locale.atoi)
    result['Date Reported'] = pd.to_datetime(result['Date Reported'])

    return result


def _setup() -> None:
    # This allows us to call `locale.atoi` when converting str -> int
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
