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
"""Tests for ca_aggregate_ingest.py."""
import datetime
from unittest import TestCase

import pandas as pd
from pandas.util.testing import assert_frame_equal
from more_itertools import one
from sqlalchemy import func

from recidiviz import Session
import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate.regions.ca import ca_aggregate_ingest
from recidiviz.persistence.database import database
from recidiviz.persistence.database.schema import CaFacilityAggregate
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes

# Cache the parsed result between tests since it's expensive to compute
PARSED_RESULT = ca_aggregate_ingest.parse(
    fixtures.as_filepath('QueryResult.xls'))


class TestCaAggregateIngest(TestCase):
    """Test that ca_aggregate_ingest correctly parses the CA report file."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def testParse_ParsesHeadAndTail(self):
        result = PARSED_RESULT[CaFacilityAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'jurisdiction_name': ["Alameda Sheriff's Dept.",
                                  "Alameda Sheriff's Dept."],
            'facility_name': ['Glen Dyer Jail', 'Santa Rita Jail'],
            'average_daily_population': [379, 2043],
            'unsentenced_male_adp': [356, 1513],
            'unsentenced_female_adp': [0, 192],
            'sentenced_male_adp': [23, 301],
            'sentenced_female_adp': [0, 37],
            'report_date': 2 * [datetime.date(2017, 1, 31)],
            'fips': ['06001', '06001'],
            'aggregation_window': 2 * [enum_strings.monthly_granularity],
            'report_frequency': 2 * [enum_strings.monthly_granularity]
        })
        assert_frame_equal(result.head(n=2), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'jurisdiction_name': ["Yuba Sheriff's Dept.",
                                  "Yuba Sheriff's Dept."],
            'facility_name': ['Yuba County Jail', 'Yuba County Jail'],
            'average_daily_population': [373, 366],
            'unsentenced_male_adp': [307, 292],
            'unsentenced_female_adp': [26, 30],
            'sentenced_male_adp': [29, 34],
            'sentenced_female_adp': [11, 10],
            'report_date': [datetime.date(2017, 11, 30),
                            datetime.date(2017, 12, 31)],
            'fips': ['06115', '06115'],
            'aggregation_window': 2 * [enum_strings.monthly_granularity],
            'report_frequency': 2 * [enum_strings.monthly_granularity]
        }, index=range(1435, 1437))
        assert_frame_equal(result.tail(n=2), expected_tail)

    def testWrite_CalculatesSum(self):
        # Act
        for table, df in PARSED_RESULT.items():
            database.write_df(table, df)

        # Assert
        query = Session().query(
            func.sum(CaFacilityAggregate.average_daily_population))
        result = one(one(query.all()))

        expected_sum_adp = 900124
        self.assertEqual(result, expected_sum_adp)
