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
"""Tests for tx_aggregate_ingest.py."""
import datetime
from unittest import TestCase

import pandas as pd
from pandas.util.testing import assert_frame_equal
from more_itertools import one
from sqlalchemy import func

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz import Session
from recidiviz.ingest.aggregate.regions.tx import tx_aggregate_ingest
from recidiviz.persistence.database import database
from recidiviz.persistence.database.schema import TxCountyAggregate
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes

DATE_SCRAPED = datetime.date(year=2019, month=1, day=1)

# Cache the parsed pdf between tests since it's expensive to compute
PARSED_PDF = tx_aggregate_ingest.parse(
    fixtures.as_filepath('Abbreviated Pop Rpt Dec 2017.pdf'), DATE_SCRAPED)


class TestTxAggregateIngest(TestCase):
    """Test that tx_aggregate_ingest correctly parses the TX PDF."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def testParse_ParsesHeadAndTail(self):
        result = PARSED_PDF[TxCountyAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'county_name': ['Anderson', 'Andrews', 'Angelina'],
            'pretrial_felons': [90, 20, 105],
            'convicted_felons': [2, 11, 26],
            'convicted_felons_sentenced_to_county_jail': [2, 0, 1],
            'parole_violators': [4, 2, 9],
            'parole_violators_with_new_charge': [14, 6, 13],
            'pretrial_misdemeanor': [18, 5, 10],
            'convicted_misdemeanor': [2, 0, 12],
            'bench_warrants': [1, 1, 6],
            'federal': [0, 0, 0],
            'pretrial_sjf': [33, 3, 28],
            'convicted_sjf_sentenced_to_county_jail': [2, 0, 0],
            'convicted_sjf_sentenced_to_state_jail': [0, 2, 2],
            'total_other': [0, 0, 0],
            'total_contract': [0, 0, 0],
            'total_population': [168, 26, 212],
            'total_capacity': [300, 50, 279],
            'available_beds': [102, 19, 39],
            'fips': 3 * [None],
            'report_date': 3 * [DATE_SCRAPED],
            'report_granularity': 3 * [enum_strings.monthly_granularity]
        })
        assert_frame_equal(result.head(n=3), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'county_name': ['Zapata', 'Zavala', 'Zavala (P)'],
            'pretrial_felons': [9, 16, 0],
            'convicted_felons': [3, 5, 0],
            'convicted_felons_sentenced_to_county_jail': [0, 0, 0],
            'parole_violators': [2, 3, 0],
            'parole_violators_with_new_charge': [0, 0, 0],
            'pretrial_misdemeanor': [3, 4, 0],
            'convicted_misdemeanor': [0, 0, 0],
            'bench_warrants': [0, 0, 0],
            'federal': [111, 9, 0],
            'pretrial_sjf': [4, 0, 0],
            'convicted_sjf_sentenced_to_county_jail': [0, 0, 0],
            'convicted_sjf_sentenced_to_state_jail': [0, 0, 0],
            'total_other': [0, 1, 0],
            'total_contract': [129, 16, 0],
            'total_population': [150, 42, 0],
            'total_capacity': [240, 66, 515],
            'available_beds': [66, 17, 464],
            'fips': 3 * [None],
            'report_date': 3 * [DATE_SCRAPED],
            'report_granularity': 3 * [enum_strings.monthly_granularity]
        }, index=range(264, 267))
        assert_frame_equal(result.tail(n=3), expected_tail)

    def testWrite_CalculatesSum(self):
        # Act
        for table, df in PARSED_PDF.items():
            database.write_df(table, df)

        # Assert
        query = Session().query(func.sum(TxCountyAggregate.available_beds))
        result = one(one(query.all()))

        expected_sum_available_beds = 20315
        self.assertEqual(result, expected_sum_available_beds)
