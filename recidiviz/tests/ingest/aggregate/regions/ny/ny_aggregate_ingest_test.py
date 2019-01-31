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
"""Tests for ny_aggregate_ingest.py."""
import datetime
from unittest import TestCase

import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest
from more_itertools import one
from sqlalchemy import func

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz import Session
from recidiviz.ingest.aggregate.regions.ny import ny_aggregate_ingest
from recidiviz.persistence.database import database
from recidiviz.persistence.database.schema import NyFacilityAggregate
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes

# Cache the parsed pdf between tests since it's expensive to compute
@pytest.fixture(scope="class")
def parsed_pdf(request):
    request.cls.parsed_pdf = ny_aggregate_ingest.parse(
        fixtures.as_filepath('jail_population.pdf'))


@pytest.mark.usefixtures("parsed_pdf")
class TestNyAggregateIngest(TestCase):
    """Test that ny_aggregate_ingest correctly parses the NY PDF."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def testParse_ParsesHeadAndTail(self):
        result = self.parsed_pdf[NyFacilityAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'report_date': [
                datetime.date(year=2017, month=12, day=31),
                datetime.date(year=2018, month=1, day=31),
                datetime.date(year=2018, month=2, day=28)
            ],
            'census': [574, 552, 540],
            'in_house': [610, 589, 586],
            'boarded_in': [37, 39, 47],
            'boarded_out': [1, 1, 1],
            'sentenced': [146, 139, 155],
            'civil': [0, 0, 0],
            'federal': [41, 36, 28],
            'technical_parole_violators': [41, 35, 32],
            'state_readies': [18, 22, 16],
            'other_unsentenced': [365, 357, 355],
            'facility_name': [
                'Albany County Jail',
                'Albany County Jail',
                'Albany County Jail'
            ],
            'fips': 3 * [None],
            'report_granularity': 3 * [enum_strings.monthly_granularity]
        })
        assert_frame_equal(result.head(n=3), expected_head, check_names=False)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'report_date': [
                datetime.date(year=2018, month=10, day=31),
                datetime.date(year=2018, month=11, day=30),
                datetime.date(year=2018, month=12, day=31)
            ],
            'census': [39, 42, 40],
            'in_house': [43, 45, 40],
            'boarded_in': [4, 2, 1],
            'boarded_out': [0, 0, 0],
            'sentenced': [15, 13, 11],
            'civil': [0, 0, 0],
            'federal': [5, 6, 6],
            'technical_parole_violators': [2, 6, 3],
            'state_readies': [2, 2, 1],
            'other_unsentenced': [18, 18, 19],
            'facility_name': [
                'Yates County Jail',
                'Yates County Jail',
                'Yates County Jail'
            ],
            'fips': 3 * [None],
            'report_granularity': 3 * [enum_strings.monthly_granularity]
        }, index=range(816, 819))
        assert_frame_equal(result.tail(n=3), expected_tail, check_names=False)

    def testWrite_CalculatesSum(self):
        # Act
        for table, df in self.parsed_pdf.items():
            database.write_df(table, df)

        # Assert
        query = Session().query(
            func.sum(NyFacilityAggregate.in_house))
        result = one(one(query.all()))

        expected_sum_in_house = 189012
        self.assertEqual(result, expected_sum_in_house)
