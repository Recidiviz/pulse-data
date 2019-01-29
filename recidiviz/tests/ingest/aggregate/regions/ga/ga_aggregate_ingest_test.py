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
"""Tests for ga_aggregate_ingest.py."""
import datetime
from unittest import TestCase

import pandas as pd
from pandas.util.testing import assert_frame_equal
from more_itertools import one
from sqlalchemy import func

from recidiviz import Session
import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate.regions.ga import ga_aggregate_ingest
from recidiviz.persistence.database import database
from recidiviz.persistence.database.schema import GaCountyAggregate
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes

DATE_SCRAPED = datetime.date(year=2018, month=6, day=7)

# Cache the parsed pdf between tests since it's expensive to compute
PARSED_PDF = ga_aggregate_ingest.parse(
    fixtures.as_filepath('jailreport_june18.pdf'))


class TestGaAggregateIngest(TestCase):
    """Test that ga_aggregate_ingest correctly parses the GA PDF."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def testParse_ParsesHeadAndTail(self):
        result = PARSED_PDF[GaCountyAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'county_name': ['APPLING', 'ATKINSON', 'BACON'],
            'total_number_of_inmates_in_jail': [83, 15, 53],
            'jail_capacity': [84, 18, 76],
            'number_of_inmates_sentenced_to_state': [10, 0, 0],
            'number_of_inmates_awaiting_trial': [49, 15, 0],
            'number_of_inmates_serving_county_sentence': [7, 0, 42],
            'number_of_other_inmates': [17, 0, 11],
            'fips': 3 * [None],
            'report_date': 3 * [DATE_SCRAPED],
            'report_granularity': 3 * [enum_strings.monthly_granularity]
        })
        assert_frame_equal(result.head(n=3), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'county_name': ['WILKES', 'WILKINSON', 'WORTH'],
            'total_number_of_inmates_in_jail': [33, 37, 48],
            'jail_capacity': [80, 44, 46],
            'number_of_inmates_sentenced_to_state': [1, 3, 1],
            'number_of_inmates_awaiting_trial': [23, 30, 43],
            'number_of_inmates_serving_county_sentence': [3, 0, 4],
            'number_of_other_inmates': [6, 4, 0],
            'fips': 3 * [None],
            'report_date': 3 * [DATE_SCRAPED],
            'report_granularity': 3 * [enum_strings.monthly_granularity]
        }, index=range(156, 159))
        assert_frame_equal(result.tail(n=3), expected_tail)

    def testWrite_CalculatesSum(self):
        # Act
        for table, df in PARSED_PDF.items():
            database.write_df(table, df)

        # Assert
        query = Session().query(
            func.sum(GaCountyAggregate.total_number_of_inmates_in_jail))
        result = one(one(query.all()))

        expected_sum_county_populations = 37697
        self.assertEqual(result, expected_sum_county_populations)
