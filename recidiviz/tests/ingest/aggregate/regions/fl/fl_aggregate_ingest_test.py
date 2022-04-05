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
"""Tests for fl_aggregate_ingest.py."""
import datetime
from unittest import TestCase

import numpy as np
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest
from more_itertools import one
from sqlalchemy import func

from recidiviz.common.constants.aggregate import (
    enum_canonical_strings as enum_strings
)
from recidiviz import Session
from recidiviz.ingest.aggregate.regions.fl import fl_aggregate_ingest
from recidiviz.persistence.database.jails_base_schema import \
    JailsBase
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.persistence.database.schema.aggregate.schema import \
    FlCountyAggregate, FlFacilityAggregate
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes

DATE_SCRAPED = datetime.date(year=2018, month=1, day=31)

# Cache the parsed pdf between tests since it's expensive to compute
@pytest.fixture(scope="class")
def parsed_pdf(request):
    request.cls.parsed_pdf = fl_aggregate_ingest.parse(
        '', fixtures.as_filepath('jails-2018-01.pdf'))

@pytest.mark.usefixtures("parsed_pdf")
class TestFlAggregateIngest(TestCase):
    """Test that fl_aggregate_ingest correctly parses the FL PDF."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database(JailsBase)

    # ==================== TABLE 1 TESTS ====================

    def testParseCountyAdp_parsesHeadAndTail(self):
        result = self.parsed_pdf[FlCountyAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'county_name': ['Alachua', 'Baker', 'Bay', 'Bradford', 'Brevard'],
            'county_population': [257062, 26965, 176016, 27440, 568919],
            'average_daily_population': [799, 478, 1015, 141, 1547],
            'date_reported': [pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT],
            'fips': ['12001', '12003', '12005', '12007', '12009'],
            'report_date': 5 * [DATE_SCRAPED],
            'aggregation_window': 5 * [enum_strings.monthly_granularity],
            'report_frequency': 5 * [enum_strings.monthly_granularity]
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
            'fips': ['12125', '12127', '12129', '12131', '12133'],
            'report_date': 5 * [DATE_SCRAPED],
            'aggregation_window': 5 * [enum_strings.monthly_granularity],
            'report_frequency': 5 * [enum_strings.monthly_granularity]
        }, index=range(62, 67))
        assert_frame_equal(result.tail(), expected_tail)

    def testParseCountyAdp_parsesDateReported(self):
        result = self.parsed_pdf[FlCountyAggregate]

        # Specifically verify Row 43 since it has 'Date Reported' set
        expected_row_43 = pd.DataFrame({
            'county_name': ['Monroe'],
            'county_population': [76047],
            'average_daily_population': [545],
            'date_reported': [datetime.datetime(day=1, month=9, year=2017)],
            'fips': ['12087'],
            'report_date': [DATE_SCRAPED],
            'aggregation_window': [enum_strings.monthly_granularity],
            'report_frequency': [enum_strings.monthly_granularity]
        }, index=[43])

        result_row_43 = result.iloc[43:44]
        assert_frame_equal(result_row_43, expected_row_43)

    def testWrite_CorrectlyReadsHernandoCounty(self):
        # Act
        for table, df in self.parsed_pdf.items():
            dao.write_df(table, df)

        # Assert
        query = Session() \
            .query(FlCountyAggregate) \
            .filter(FlCountyAggregate.county_name == 'Hernando')

        hernando_row = one(query.all())

        self.assertEqual(hernando_row.county_name, 'Hernando')
        self.assertEqual(hernando_row.county_population, 179503)
        self.assertEqual(hernando_row.average_daily_population, 632)
        self.assertEqual(hernando_row.date_reported,
                         datetime.date(year=2017, month=9, day=1))

    def testWrite_CalculatesCountyPopulationSum(self):
        # Act
        for table, df in self.parsed_pdf.items():
            dao.write_df(table, df)

        # Assert
        query = Session().query(func.sum(FlCountyAggregate.county_population))
        result = one(one(query.all()))

        expected_sum_county_populations = 20148654
        self.assertEqual(result, expected_sum_county_populations)

    # ==================== TABLE 2 TESTS ====================

    def testParseFacilityAggregates_parsesHeadAndTail(self):
        result = self.parsed_pdf[FlFacilityAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'facility_name': [
                'Alachua CSO, Department of the Jail',
                'Alachua County Work Release',
                'Baker County Detention Center',
                'Bay County Jail and Annex',
                'Bradford County Jail'],
            'average_daily_population': [749., 50, 478, 1015, 141],
            'number_felony_pretrial': [463., 4, 81, 307, 50],
            'number_misdemeanor_pretrial': [45., 1, 15, 287, 9],
            'fips': ['12001', '12001', '12003', '12005', '12007'],
            'report_date': 5 * [DATE_SCRAPED],
            'aggregation_window': 5 * [enum_strings.monthly_granularity],
            'report_frequency': 5 * [enum_strings.monthly_granularity]
        })
        assert_frame_equal(result.head(), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'facility_name': [
                'Volusia County Corr. Facility',
                'Volusia County/Branch Jail',
                'Wakulla County Jail',
                'Walton County Jail',
                'Washington County Jail'],
            'average_daily_population': [516., 897, 174, 293, 110],
            'number_felony_pretrial': [203., 349, 67, 135, 54],
            'number_misdemeanor_pretrial': [42., 44, 8, 38, 7],
            'fips': ['12127', '12127', '12129', '12131', '12133'],
            'report_date': 5 * [DATE_SCRAPED],
            'aggregation_window': 5 * [enum_strings.monthly_granularity],
            'report_frequency': 5 * [enum_strings.monthly_granularity]
        }, index=range(82, 87))
        assert_frame_equal(result.tail(), expected_tail)

    def testParseFacilityAdp_parsesMissingDataAsNone(self):
        result = self.parsed_pdf[FlFacilityAggregate]

        # Specifically verify Row 43 since it has 'Date Reported' set
        expected_row_40 = pd.DataFrame({
            'facility_name': ['Lake County Jail'],
            'average_daily_population': [835.],
            'number_felony_pretrial': np.array([None]).astype(float),
            'number_misdemeanor_pretrial': np.array([None]).astype(float),
            'fips': ['12069'],
            'report_date': [DATE_SCRAPED],
            'aggregation_window': [enum_strings.monthly_granularity],
            'report_frequency': [enum_strings.monthly_granularity]
        }, index=[40])

        result_row_40 = result.iloc[40:41]
        assert_frame_equal(result_row_40, expected_row_40)

    def testWrite_CalculatesFacilityAdpSum(self):
        # Act
        for table, df in self.parsed_pdf.items():
            dao.write_df(table, df)

        # Assert
        query = Session().query(
            func.sum(FlFacilityAggregate.average_daily_population))
        result = one(one(query.all()))

        expected_sum_facility_adp = 52388
        self.assertEqual(result, expected_sum_facility_adp)
