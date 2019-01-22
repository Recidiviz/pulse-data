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
"""Tests for fl_aggregate_ingest.py."""
import datetime
from unittest import TestCase

import pandas as pd
from pandas.util.testing import assert_frame_equal
from more_itertools import one
from sqlalchemy import func

from recidiviz import Session
import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate.regions.fl import fl_aggregate_ingest
from recidiviz.persistence.database import database
from recidiviz.persistence.database.schema import FlCountyAggregate
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes

DATE_SCRAPED = datetime.datetime(year=2019, month=1, day=1)

# Cache the parsed pdf between tests since it's expensive to compute
PARSED_PDF = fl_aggregate_ingest.parse(
    fixtures.as_filepath('jails-2018-01.pdf'), DATE_SCRAPED)


class TestFlAggregateIngest(TestCase):
    """Test that fl_aggregate_ingest correctly parses the FL PDF."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def testParseCountyAdp_parsesHeadAndTail(self):
        result = PARSED_PDF[FlCountyAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'county_name': ['Alachua', 'Baker', 'Bay', 'Bradford', 'Brevard'],
            'county_population': [257062, 26965, 176016, 27440, 568919],
            'average_daily_population': [799, 478, 1015, 141, 1547],
            'date_reported': [pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT],
            'fips': 5 * [None],
            'report_date': 5 * [DATE_SCRAPED],
            'report_granularity': 5 * [enum_strings.monthly_granularity]
        })
        assert_frame_equal(result.head(), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'county_name': [
                'Union', 'Volusia', 'Wakulla', 'Walton', 'Washington'
            ],
            'county_population': [15887, 517411, 31599, 62943, 24888],
            'average_daily_population': [38, 1413, 174, 293, 110],
            'date_reported': [pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT],
            'fips': 5 * [None],
            'report_date': 5 * [DATE_SCRAPED],
            'report_granularity': 5 * [enum_strings.monthly_granularity]
        }, index=range(62, 67))
        assert_frame_equal(result.tail(), expected_tail)

    def testParseCountyAdp_parsesDateReported(self):
        result = PARSED_PDF[FlCountyAggregate]

        # Specifically verify Row 43 since it has 'Date Reported' set
        expected_row_43 = pd.DataFrame({
            'county_name': ['Monroe'],
            'county_population': [76047],
            'average_daily_population': [545],
            'date_reported': [datetime.datetime(day=1, month=9, year=2017)],
            'fips': [None],
            'report_date': [DATE_SCRAPED],
            'report_granularity': [enum_strings.monthly_granularity]
        }, index=[43])

        result_row_43 = result.iloc[43:44]
        assert_frame_equal(result_row_43, expected_row_43)

    def testWrite_CorrectlyReadsHernandoCounty(self):
        # Act
        for table, df in PARSED_PDF.items():
            database.write_df(table, df)

        # Assert
        query = Session() \
            .query(FlCountyAggregate) \
            .filter(FlCountyAggregate.county_name == 'Hernando')

        hernando_row = one(query.all())

        self.assertEqual(hernando_row.county_name, 'Hernando')
        self.assertEqual(hernando_row.county_population, 179503)
        self.assertEqual(hernando_row.average_daily_population, 632)
        self.assertEqual(hernando_row.date_reported,
                         datetime.datetime(year=2017, month=9, day=1))

    def testWrite_CalculatesSum(self):
        # Act
        for table, df in PARSED_PDF.items():
            database.write_df(table, df)

        # Assert
        query = Session().query(func.sum(FlCountyAggregate.county_population))
        result = one(one(query.all()))

        expected_sum_county_populations = 20148654
        self.assertEqual(result, expected_sum_county_populations)
