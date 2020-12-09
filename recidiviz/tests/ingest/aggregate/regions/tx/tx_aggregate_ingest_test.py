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
"""Tests for tx_aggregate_ingest.py."""
import datetime
from unittest import TestCase

import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest
from more_itertools import one
from sqlalchemy import func

from recidiviz.common.constants.aggregate import (
    enum_canonical_strings as enum_strings
)
from recidiviz.ingest.aggregate.regions.tx import tx_aggregate_ingest
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.base_schema import \
    JailsBase
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.persistence.database.schema.aggregate.schema import \
    TxCountyAggregate
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes

DATE_SCRAPED_AFTER_1996 = datetime.date(year=2017, month=12, day=1)
DATE_SCRAPED_1996 = datetime.date(year=1996, month=6, day=1)
DATE_SCRAPED_BEFORE_1996 = datetime.date(year=1994, month=3, day=1)
DATE_SCRAPED_CONCAT = datetime.date(year=2003, month=10, day=1)
# Cache the parsed pdf between tests since it's expensive to compute
@pytest.fixture(scope="class")
def parsed_pdf_after_1996(request):
    request.cls.parsed_pdf_after_1996 = tx_aggregate_ingest.parse(
        '', fixtures.as_filepath('Abbreviated Pop Rpt Dec 2017.pdf'))

# Cache the parsed pdf between tests since it's expensive to compute
@pytest.fixture(scope="class")
def parsed_pdf_1996(request):
    request.cls.parsed_pdf_1996 = tx_aggregate_ingest.parse(
        '', fixtures.as_filepath('texas_url_abbreviated pop rpt June 1996.pdf'))

# Cache the parsed pdf between tests since it's expensive to compute
@pytest.fixture(scope="class")
def parsed_pdf_before_1996(request):
    request.cls.parsed_pdf_before_1996 = tx_aggregate_ingest.parse(
        '', fixtures.as_filepath('abbreviated pop rpt march 1994.pdf'))

@pytest.fixture(scope="class")
def parsed_pdf_concat(request):
    request.cls.parsed_pdf_concat = tx_aggregate_ingest.parse(
        '', fixtures.as_filepath(
            'docs_abbreviatedpopreports_abbreviated pop rpt oct 2003.pdf'))


@pytest.mark.usefixtures("parsed_pdf_after_1996")
@pytest.mark.usefixtures("parsed_pdf_before_1996")
@pytest.mark.usefixtures("parsed_pdf_1996")
@pytest.mark.usefixtures("parsed_pdf_concat")
@pytest.mark.skip("TODO(#4865): This test fails on master for some, possibly due to underlying Java issues related to "
                  "Apache Beam and Tabula.")
class TestTxAggregateIngest(TestCase):
    """Test that tx_aggregate_ingest correctly parses the TX PDF."""

    def setUp(self) -> None:
        fakes.use_in_memory_sqlite_database(JailsBase)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def testParse_ParsesHeadAndTail_After_1996(self):
        result = self.parsed_pdf_after_1996[TxCountyAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'facility_name': ['Anderson', 'Andrews', 'Angelina'],
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
            'fips': ['48001', '48003', '48005'],
            'report_date': 3 * [DATE_SCRAPED_AFTER_1996],
            'aggregation_window': 3 * [enum_strings.daily_granularity],
            'report_frequency': 3 * [enum_strings.monthly_granularity]
        })
        assert_frame_equal(result.head(n=3), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'facility_name': ['Zapata', 'Zavala', 'Zavala (P)'],
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
            'fips': ['48505', '48507', '48507'],
            'report_date': 3 * [DATE_SCRAPED_AFTER_1996],
            'aggregation_window': 3 * [enum_strings.daily_granularity],
            'report_frequency': 3 * [enum_strings.monthly_granularity]
        }, index=range(264, 267))
        assert_frame_equal(result.tail(n=3), expected_tail)

    def testWrite_CalculatesSum_After_1996(self):
        for table, df in self.parsed_pdf_after_1996.items():
            dao.write_df(table, df)

        # Assert
        query = SessionFactory.for_schema_base(JailsBase).query(
            func.sum(TxCountyAggregate.available_beds))
        result = one(one(query.all()))

        expected_sum_available_beds = 20315
        self.assertEqual(result, expected_sum_available_beds)

    def testParse_ParsesHeadAndTail_Concat(self):
        result = self.parsed_pdf_concat[TxCountyAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'facility_name': ['Anderson', 'Andrews'],
            'pretrial_felons': [77, 15],
            'convicted_felons': [12, 4],
            'convicted_felons_sentenced_to_county_jail': [0, 2],
            'parole_violators': [9, 0],
            'parole_violators_with_new_charge': [13, 0],
            'pretrial_misdemeanor': [13, 2],
            'convicted_misdemeanor': [7, 4],
            'bench_warrants': [0, 0],
            'federal': [0, 0],
            'pretrial_sjf': [0, 0],
            'convicted_sjf_sentenced_to_county_jail': [0, 1],
            'convicted_sjf_sentenced_to_state_jail': [1, 0],
            'total_other': [0, 1],
            'total_contract': [0, 0],
            'total_population': [132, 29],
            'total_capacity': [129, 50],
            'available_beds': [0, 16],
            'fips': ['48001', '48003'],
            'report_date': 2 * [DATE_SCRAPED_CONCAT],
            'aggregation_window': 2 * [enum_strings.daily_granularity],
            'report_frequency': 2 * [enum_strings.monthly_granularity]
        })
        assert_frame_equal(result.head(n=2), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'facility_name': ['Zapata', 'Zavala'],
            'pretrial_felons': [10, 12],
            'convicted_felons': [3, 3],
            'convicted_felons_sentenced_to_county_jail': [0, 0],
            'parole_violators': [1, 0],
            'parole_violators_with_new_charge': [1, 1],
            'pretrial_misdemeanor': [14, 1],
            'convicted_misdemeanor': [1, 0],
            'bench_warrants': [0, 0],
            'federal': [166, 44],
            'pretrial_sjf': [4, 0],
            'convicted_sjf_sentenced_to_county_jail': [0, 0],
            'convicted_sjf_sentenced_to_state_jail': [1, 1],
            'total_other': [0, 1],
            'total_contract': [166, 44],
            'total_population': [201, 63],
            'total_capacity': [240, 66],
            'available_beds': [15, 0],
            'fips': ['48505', '48507'],
            'report_date': 2 * [DATE_SCRAPED_CONCAT],
            'aggregation_window': 2 * [enum_strings.daily_granularity],
            'report_frequency': 2 * [enum_strings.monthly_granularity]
        }, index=range(262, 264))
        assert_frame_equal(result.tail(n=2), expected_tail)

    def testWrite_CalculatesSum_Concat(self):
        for table, df in self.parsed_pdf_concat.items():
            dao.write_df(table, df)

        # Assert
        query = SessionFactory.for_schema_base(JailsBase).query(
            func.sum(TxCountyAggregate.available_beds))
        result = one(one(query.all()))

        expected_sum_available_beds = 7044
        self.assertEqual(result, expected_sum_available_beds)

    def testParse_ParsesHeadAndTail_1996(self):
        result = self.parsed_pdf_1996[TxCountyAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'facility_name': ['ANDERSON', 'ANDREWS'],
            'pretrial_felons': [32, 10],
            'convicted_felons': [4, 0],
            'convicted_felons_sentenced_to_county_jail': [2, 1],
            'parole_violators': [8, 2],
            'parole_violators_with_new_charge': [2, 1],
            'pretrial_misdemeanor': [10, 3],
            'convicted_misdemeanor': [3, 1],
            'bench_warrants': [5, 0],
            'federal': [0, 0],
            'total_other': [0, 1],
            'total_contract': [0, 0],
            'total_population': [67, 22],
            'total_capacity': [129, 50],
            'available_beds': [49, 23],
            'fips': ['48001', '48003'],
            'report_date': 2 * [DATE_SCRAPED_1996],
            'aggregation_window': 2 * [enum_strings.daily_granularity],
            'report_frequency': 2 * [enum_strings.monthly_granularity]
        })
        assert_frame_equal(result.head(n=2), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'facility_name': ['CRYSTL CTY (P', 'ZAVALA'],
            'pretrial_felons': [0, 16],
            'convicted_felons': [0, 7],
            'convicted_felons_sentenced_to_county_jail': [0, 0],
            'parole_violators': [0, 0],
            'parole_violators_with_new_charge': [0, 0],
            'pretrial_misdemeanor': [0, 3],
            'convicted_misdemeanor': [0, 0],
            'bench_warrants': [0, 1],
            'federal': [0, 0],
            'total_other': [432, 0],
            'total_contract': [432, 0],
            'total_population': [432, 27],
            'total_capacity': [467, 66],
            'available_beds': [0, 32],
            'fips': ['48507', '48507'],
            'report_date': 2 * [DATE_SCRAPED_1996],
            'aggregation_window': 2 * [enum_strings.daily_granularity],
            'report_frequency': 2 * [enum_strings.monthly_granularity]
        }, index=range(259, 261))
        assert_frame_equal(result.tail(n=2), expected_tail)

    def testWrite_CalculatesSum_1996(self):
        for table, df in self.parsed_pdf_1996.items():
            dao.write_df(table, df)

        # Assert
        query = SessionFactory.for_schema_base(JailsBase).query(
            func.sum(TxCountyAggregate.pretrial_felons))
        result = one(one(query.all()))

        expected_pretrial_felons = 14140
        self.assertEqual(result, expected_pretrial_felons)

    def testParse_ParsesHeadAndTail_before_1996(self):
        result = self.parsed_pdf_before_1996[TxCountyAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'facility_name': ['ANDERSON', 'ANDREWS'],
            'pretrial_felons': [43, 14],
            'convicted_felons': [81, 15],
            'parole_violators': [3, 0],
            'parole_violators_with_new_charge': [6, 1],
            'pretrial_misdemeanor': [4, 6],
            'convicted_misdemeanor': [5, 3],
            'bench_warrants': [0, 0],
            'federal': [0, 0],
            'total_other': [0, 0],
            'total_contract': [0, 4],
            'total_population': [142, 43],
            'total_capacity': [129, 50],
            'available_beds': [0, 0],
            'fips': ['48001', '48003'],
            'report_date': 2 * [DATE_SCRAPED_BEFORE_1996],
            'aggregation_window': 2 * [enum_strings.daily_granularity],
            'report_frequency': 2 * [enum_strings.monthly_granularity]
        })
        assert_frame_equal(result.head(n=2), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'facility_name': ['ZAPATA', 'ZAVALA'],
            'pretrial_felons': [4, 10],
            'convicted_felons': [0, 1],
            'parole_violators': [0, 1],
            'parole_violators_with_new_charge': [0, 0],
            'pretrial_misdemeanor': [0, 0],
            'convicted_misdemeanor': [0, 0],
            'bench_warrants': [0, 0],
            'federal': [0, 10],
            'total_other': [0, 1],
            'total_contract': [0, 39],
            'total_population': [0, 52],
            'total_capacity': [12, 67],
            'available_beds': [11, 0],
            'fips': ['48505', '48507'],
            'report_date': 2 * [DATE_SCRAPED_BEFORE_1996],
            'aggregation_window': 2 * [enum_strings.daily_granularity],
            'report_frequency': 2 * [enum_strings.monthly_granularity]
        }, index=range(257, 259))
        assert_frame_equal(result.tail(n=2), expected_tail)

    def testWrite_CalculatesSum_before_1996(self):

        for table, df in self.parsed_pdf_before_1996.items():
            dao.write_df(table, df)

        # Assert
        query = SessionFactory.for_schema_base(JailsBase).query(
            func.sum(TxCountyAggregate.pretrial_felons))
        result = one(one(query.all()))

        expected_pretrial_felons = 14727
        self.assertEqual(result, expected_pretrial_felons)
