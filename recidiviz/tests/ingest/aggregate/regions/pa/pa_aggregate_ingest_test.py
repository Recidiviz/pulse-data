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
"""Tests for pa_aggregate_ingest.py."""
import datetime
from unittest import TestCase

import pandas as pd
from pandas.util.testing import assert_frame_equal
from more_itertools import one
from numpy import NaN
from sqlalchemy import func

from recidiviz.common.constants.aggregate import (
    enum_canonical_strings as enum_strings
)
from recidiviz import Session
from recidiviz.ingest.aggregate.regions.pa import pa_aggregate_ingest
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.persistence.database.schema.aggregate.schema import \
    PaFacilityPopAggregate, PaCountyPreSentencedAggregate
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes

REPORT_DATE = datetime.date(year=2017, month=1, day=1)

# Cache the parsed result between tests since it's expensive to compute
PARSED_RESULT = pa_aggregate_ingest.parse(fixtures.as_filepath(
    '2018 County Statistics _ General Information - 2017 Data.xlsx'))


class TestPaAggregateIngest(TestCase):
    """Test that pa_aggregate_ingest correctly parses the CA report file."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def testParse_Table1_ParsesHeadAndTail(self):
        result = PARSED_RESULT[PaFacilityPopAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'facility_name': ['Adams', 'Allegheny'],
            'bed_capacity': [285, 3129],
            'work_release_community_corrections_beds': [NaN, 0.],
            'in_house_adp': [196., 2352.],
            'housed_elsewhere_adp': [7., 0.],
            'work_release_adp': [113., 0.],
            'admissions': [2048, 14810],
            'discharge': [2068, 14642],
            'report_date': 2 * [REPORT_DATE],
            'fips': ['42001', '42003'],
            'aggregation_window': 2 * [enum_strings.yearly_granularity],
            'report_frequency': 2 * [enum_strings.yearly_granularity]
        })
        assert_frame_equal(result.head(n=2), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'facility_name': ['Wyoming', 'York'],
            'bed_capacity': [78, 2368],
            'work_release_community_corrections_beds': [0., 308.],
            'in_house_adp': [75., 2243.],
            'housed_elsewhere_adp': [10., 0.],
            'work_release_adp': [0., 147.],
            'admissions': [506, 13900],
            'discharge': [513, 14132],
            'report_date': 2 * [REPORT_DATE],
            'fips': ['42131', '42133'],
            'aggregation_window': 2 * [enum_strings.yearly_granularity],
            'report_frequency': 2 * [enum_strings.yearly_granularity]
        }, index=range(65, 67))
        assert_frame_equal(result.tail(n=2), expected_tail)

    def testParse_Table2_ParsesHeadAndTail(self):
        result = PARSED_RESULT[PaCountyPreSentencedAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'report_date': [datetime.date(year=2017, month=1, day=31),
                            datetime.date(year=2017, month=4, day=30)],
            'county_name': ['Adams', 'Adams'],
            'pre_sentenced_population': [111., 127.],
            'fips': ['42001', '42001'],
            'aggregation_window': 2 * [enum_strings.daily_granularity],
            'report_frequency':
                2 * [enum_strings.quarterly_granularity]
        })
        assert_frame_equal(result.head(n=2), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'report_date': [datetime.date(year=2017, month=8, day=31),
                            datetime.date(year=2017, month=12, day=31)],
            'county_name': ['York', 'York'],
            'pre_sentenced_population': [715., 687.],
            'fips': ['42133', '42133'],
            'aggregation_window': 2 * [enum_strings.daily_granularity],
            'report_frequency':
                2 * [enum_strings.quarterly_granularity]
        }, index=range(246, 248))
        assert_frame_equal(result.tail(n=2), expected_tail)

    def testWrite_Table1_CalculatesSums(self):
        # Act
        for table, df in PARSED_RESULT.items():
            dao.write_df(table, df)

        # Assert
        query = Session().query(
            func.sum(PaFacilityPopAggregate.housed_elsewhere_adp))
        result = one(one(query.all()))

        # Note: This report contains fractional averages
        expected_housed_elsewhere_adp = 1564.0257
        self.assertEqual(result, expected_housed_elsewhere_adp)

    def testWrite_Table2_CalculateSum(self):
        # Act
        for table, df in PARSED_RESULT.items():
            dao.write_df(table, df)

        # Assert
        query = Session().query(
            func.sum(PaCountyPreSentencedAggregate.pre_sentenced_population))
        result = one(one(query.all()))

        expected_pretrial_population = 82521
        self.assertEqual(result, expected_pretrial_population)
