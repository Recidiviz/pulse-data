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
"""Tests for ga_aggregate_ingest.py."""
import datetime
from unittest import TestCase

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from more_itertools import one
from sqlalchemy import func

from recidiviz.common.constants.aggregate import (
    enum_canonical_strings as enum_strings
)
from recidiviz.ingest.aggregate.regions.ga import ga_aggregate_ingest
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.base_schema import \
    JailsBase
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.persistence.database.schema.aggregate.schema import \
    GaCountyAggregate
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes

DATE_SCRAPED = datetime.date(year=2018, month=6, day=7)
DATE_SCRAPED_PDF_EXTRA_ROWS = datetime.date(year=2016, month=7, day=7)


# Cache the parsed pdf between tests since it's expensive to compute
@pytest.fixture(scope="class")
def parsed_pdf(request):
    request.cls.parsed_pdf = ga_aggregate_ingest.parse(
        '', fixtures.as_filepath('jailreport_june18.pdf'))
    request.cls.parsed_pdf_with_extra_rows = ga_aggregate_ingest.parse(
        '', fixtures.as_filepath('jul16_jail_report.pdf'))


@pytest.mark.usefixtures("parsed_pdf")
class TestGaAggregateIngest(TestCase):
    """Test that ga_aggregate_ingest correctly parses the GA PDF."""

    def setUp(self) -> None:
        fakes.use_in_memory_sqlite_database(JailsBase)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def testParse_ParsesHeadAndTail(self):
        result = self.parsed_pdf[GaCountyAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'county_name': ['APPLING', 'ATKINSON', 'BACON'],
            'total_number_of_inmates_in_jail': [83, 15, 53],
            'jail_capacity': [84, 18, 76],
            'number_of_inmates_sentenced_to_state': [10, 0, 0],
            'number_of_inmates_awaiting_trial': [49, 15, 0],
            'number_of_inmates_serving_county_sentence': [7, 0, 42],
            'number_of_other_inmates': [17, 0, 11],
            'fips': ['13001', '13003', '13005'],
            'report_date': 3 * [DATE_SCRAPED],
            'aggregation_window': 3 * [enum_strings.daily_granularity],
            'report_frequency': 3 * [enum_strings.monthly_granularity]
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
            'fips': ['13317', '13319', '13321'],
            'report_date': 3 * [DATE_SCRAPED],
            'aggregation_window': 3 * [enum_strings.daily_granularity],
            'report_frequency': 3 * [enum_strings.monthly_granularity]
        }, index=range(156, 159))
        assert_frame_equal(result.tail(n=3), expected_tail)

    def testParseReportWithExtraRows_ParsesHeadAndTail(self):
        result = self.parsed_pdf_with_extra_rows[GaCountyAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'county_name': ['APPLING', 'ATKINSON', 'BACON'],
            'total_number_of_inmates_in_jail': [69, 15, 49],
            'jail_capacity': [84, 18, 68],
            'number_of_inmates_sentenced_to_state': [14, 2, 0],
            'number_of_inmates_awaiting_trial': [38, 12, 39],
            'number_of_inmates_serving_county_sentence': [4, 1, 9],
            'number_of_other_inmates': [13, 0, 1],
            'fips': ['13001', '13003', '13005'],
            'report_date': 3 * [DATE_SCRAPED_PDF_EXTRA_ROWS],
            'aggregation_window': 3 * [enum_strings.daily_granularity],
            'report_frequency': 3 * [enum_strings.monthly_granularity]
        })
        assert_frame_equal(result.head(n=3), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'county_name': ['WILKES', 'WILKINSON', 'WORTH'],
            'total_number_of_inmates_in_jail': [35, 39, 46],
            'jail_capacity': [80, 44, 46],
            'number_of_inmates_sentenced_to_state': [1, 7, 2],
            'number_of_inmates_awaiting_trial': [22, 0, 42],
            'number_of_inmates_serving_county_sentence': [7, 0, 2],
            'number_of_other_inmates': [5, 32, 0],
            'fips': ['13317', '13319', '13321'],
            'report_date': 3 * [DATE_SCRAPED_PDF_EXTRA_ROWS],
            'aggregation_window': 3 * [enum_strings.daily_granularity],
            'report_frequency': 3 * [enum_strings.monthly_granularity]
        }, index=range(156, 159))
        assert_frame_equal(result.tail(n=3), expected_tail)

    def testWrite_CalculatesSum(self):
        # Act
        for table, df in self.parsed_pdf.items():
            dao.write_df(table, df)

        # Assert
        query = SessionFactory.for_schema_base(JailsBase).query(
            func.sum(GaCountyAggregate.total_number_of_inmates_in_jail))
        result = one(one(query.all()))

        expected_sum_county_populations = 37697
        self.assertEqual(result, expected_sum_county_populations)
