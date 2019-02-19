# Recidiviz - a platform for tracking granular recidivism metrics in real time
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

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz import Session
from recidiviz.ingest.aggregate.regions.pa import pa_aggregate_ingest
from recidiviz.persistence.database import database
from recidiviz.persistence.database.schema import PaFacilityPopAggregate
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

    def testParse_ParsesHeadAndTail(self):
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
            'fips': [42001, 42003],
            'report_granularity': 2 * [enum_strings.yearly_granularity]
        })
        assert_frame_equal(result.head(n=2), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'facility_name': ["Wyoming", "York"],
            'bed_capacity': [78, 2368],
            'work_release_community_corrections_beds': [0., 308.],
            'in_house_adp': [75., 2243.],
            'housed_elsewhere_adp': [10., 0.],
            'work_release_adp': [0., 147.],
            'admissions': [506, 13900],
            'discharge': [513, 14132],
            'report_date': 2 * [REPORT_DATE],
            'fips': [42131, 42133],
            'report_granularity': 2 * [enum_strings.yearly_granularity]
        }, index=range(65, 67))
        assert_frame_equal(result.tail(n=2), expected_tail)

    def testWrite_CalculatesSum(self):
        # Act
        for table, df in PARSED_RESULT.items():
            database.write_df(table, df)

        # Assert
        query = Session().query(
            func.sum(PaFacilityPopAggregate.housed_elsewhere_adp))
        result = one(one(query.all()))

        # Note: This report contains fractional averages
        expected_housed_elsewhere_adp = 1564.0257
        self.assertEqual(result, expected_housed_elsewhere_adp)
