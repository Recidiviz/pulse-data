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
from typing import Dict, Optional

import pandas as pd
import tabula
from sqlalchemy.ext.declarative import DeclarativeMeta

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate import aggregate_ingest_utils
from recidiviz.persistence.database.schema import FlCountyAggregate, \
    FlFacilityAggregate


def parse(filename: str, date_scraped: datetime.datetime) \
        -> Dict[DeclarativeMeta, pd.DataFrame]:
    _setup()

    # TODO(#698): Set county fips based on the county_name
    fl_county_table = _parse_county_table(filename)
    fl_county_table['fips'] = None

    # TODO(#698, #689): Set facility fips based on facility_name
    fl_facility_table = _parse_facility_table(filename)
    fl_facility_table['fips'] = None

    result = {
        FlCountyAggregate: fl_county_table,
        FlFacilityAggregate: fl_facility_table
    }

    for table in result.values():
        table['report_date'] = date_scraped
        table['report_granularity'] = enum_strings.monthly_granularity

    return result


def _parse_county_table(filename: str) -> pd.DataFrame:
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


def _parse_facility_table(filename: str) -> pd.DataFrame:
    """Parse the FL County Pretrial Inmate Report - Table 2 in the PDF."""
    # Set column names directly since the pdf format makes them hard to parse
    column_names = [
        'Detention Facility Name',
        'Average Daily Population',
        'Number Felony Pretrial',
        'Number Misdemeanor Pretrial',
        'Total Percent Pretrial']

    part1 = tabula.read_pdf(
        filename,
        pages=[5],
        pandas_options={
            'skiprows': [0, 1, 2],
            'names': column_names,
        })
    part2 = tabula.read_pdf(
        filename,
        pages=[6],
        pandas_options={
            'skiprows': [0, 1, 2],
            'usecols': [0, 2, 3, 4, 5],  # Column 1 contains no data
            'names': column_names,
            'skipfooter': 2  # The last 2 rows are the totals
        })
    result = part1.append(part2, ignore_index=True)

    result = aggregate_ingest_utils.rename_columns_and_select(result, {
        'Detention Facility Name': 'facility_name',
        'Average Daily Population': 'average_daily_population',
        'Number Felony Pretrial': 'number_felony_pretrial',
        'Number Misdemeanor Pretrial': 'number_misdemeanor_pretrial'
    })

    result['average_daily_population'] = result[
        'average_daily_population'].apply(_use_stale_adp).apply(_to_int)
    for column_name in {'number_felony_pretrial',
                        'number_misdemeanor_pretrial'}:
        result[column_name] = result[column_name].apply(_to_int)

    return result


def _use_stale_adp(adp_str: str) -> str:
    """Use adp values listed in Table 2 that are listed as stale."""
    # Stale values are marked with an '*', so strip all '*'
    return adp_str.rstrip('*')


def _to_int(int_str: str) -> Optional[int]:
    try:
        return locale.atoi(int_str)
    except ValueError:
        # Values containing a '-' have no reported data, so just return None
        return None


def _setup() -> None:
    # This allows us to call `locale.atoi` when converting str -> int
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
