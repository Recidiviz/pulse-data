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
"""Tests for hi_aggregate_ingest.py."""
import datetime
from unittest import TestCase

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from more_itertools import one
from numpy import NaN
from sqlalchemy import func

from recidiviz.common.constants.aggregate import (
    enum_canonical_strings as enum_strings
)
from recidiviz.ingest.aggregate.regions.hi import hi_aggregate_ingest
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.base_schema import \
    JailsBase
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.persistence.database.schema.aggregate.schema import \
    HiFacilityAggregate
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes

DATE_SCRAPED = datetime.date(year=2018, month=11, day=30)
DATE_SCRAPED_2 = datetime.date(year=2017, month=9, day=30)


# Cache the parsed pdf between tests since it's expensive to compute
@pytest.fixture(scope="class")
def parsed_pdf(request):
    request.cls.parsed_pdf = hi_aggregate_ingest.parse(
        '', fixtures.as_filepath('Pop-Reports-EOM-2018-11-30.pdf'))
    request.cls.parsed_pdf_2 = hi_aggregate_ingest.parse(
        '', fixtures.as_filepath('pop-reports-eom-2017-09-30-17.pdf'))


@pytest.mark.usefixtures("parsed_pdf")
class TestHiAggregateIngest(TestCase):
    """Test that hi_aggregate_ingest correctly parses the HI PDF."""

    def setUp(self) -> None:
        fakes.use_in_memory_sqlite_database(JailsBase)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def testParse_ParsesHeadAndTail(self):
        result = self.parsed_pdf[HiFacilityAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'facility_name': ['Hawaii Community Correctional Center',
                              'Halawa Correctional Facility - Special Needs'],
            'design_bed_capacity': [206., 90.],
            'operation_bed_capacity': [226., 132.],
            'total_population': [387, 125],
            'male_population': [317, 125],
            'female_population': [70, 0],
            'sentenced_felony_male_population': [52, 125],
            'sentenced_felony_female_population': [4, 0],
            'sentenced_felony_probation_male_population': [59, 0],
            'sentenced_felony_probation_female_population': [6, 0],
            'sentenced_misdemeanor_male_population': [24, 0],
            'sentenced_misdemeanor_female_population': [5, 0],
            'pretrial_felony_male_population': [99, 0],
            'pretrial_felony_female_population': [30, 0],
            'pretrial_misdemeanor_male_population': [26, 0],
            'pretrial_misdemeanor_female_population': [9, 0],
            'held_for_other_jurisdiction_male_population': [3, 0],
            'held_for_other_jurisdiction_female_population': [0, 0],
            'parole_violation_male_population': [12, 0],
            'parole_violation_female_population': [4, 0],
            'probation_violation_male_population': [42, 0],
            'probation_violation_female_population': [12, 0],
            'fips': ['15001', '15003'],
            'report_date': 2 * [DATE_SCRAPED],
            'aggregation_window': 2 * [enum_strings.daily_granularity],
            'report_frequency': 2 * [enum_strings.monthly_granularity]
        })
        assert_frame_equal(result.head(n=2), expected_head, check_names=False)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'facility_name': ['Saguaro Correctional Center, AZ',
                              'Federal Detention Center, Honolulu'],
            'design_bed_capacity': [NaN, NaN],
            'operation_bed_capacity': [NaN, NaN],
            'total_population': [1459, 159],
            'male_population': [1459, 147],
            'female_population': [0, 12],
            'sentenced_felony_male_population': [1374, 2],
            'sentenced_felony_female_population': [0, 0],
            'sentenced_felony_probation_male_population': [0, 10],
            'sentenced_felony_probation_female_population': [0, 1],
            'sentenced_misdemeanor_male_population': [0, 7],
            'sentenced_misdemeanor_female_population': [0, 2],
            'pretrial_felony_male_population': [0, 9],
            'pretrial_felony_female_population': [0, 1],
            'pretrial_misdemeanor_male_population': [0, 4],
            'pretrial_misdemeanor_female_population': [0, 0],
            'held_for_other_jurisdiction_male_population': [0, 1],
            'held_for_other_jurisdiction_female_population': [0, 0],
            'parole_violation_male_population': [85, 9],
            'parole_violation_female_population': [0, 0],
            'probation_violation_male_population': [0, 105],
            'probation_violation_female_population': [0, 8],
            'fips': ['04021', '15003'],
            'report_date': 2 * [DATE_SCRAPED],
            'aggregation_window': 2 * [enum_strings.daily_granularity],
            'report_frequency': 2 * [enum_strings.monthly_granularity]
        }, index=range(10, 12))
        assert_frame_equal(result.tail(n=2), expected_tail, check_names=False)

    def testParseNewTableOrder_ParsesHeadAndTail(self):
        result = self.parsed_pdf_2[HiFacilityAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'facility_name': ['Hawaii Community Correctional Center',
                              'Halawa Correctional Facility - Special Needs'],
            'design_bed_capacity': [206., 90.],
            'operation_bed_capacity': [226., 132.],
            'total_population': [379, 113],
            'male_population': [310, 113],
            'female_population': [69, 0],
            'sentenced_felony_male_population': [32, 113],
            'sentenced_felony_female_population': [5, 0],
            'sentenced_felony_probation_male_population': [52, 0],
            'sentenced_felony_probation_female_population': [14, 0],
            'sentenced_misdemeanor_male_population': [19, 0],
            'sentenced_misdemeanor_female_population': [6, 0],
            'pretrial_felony_male_population': [107, 0],
            'pretrial_felony_female_population': [16, 0],
            'pretrial_misdemeanor_male_population': [30, 0],
            'pretrial_misdemeanor_female_population': [11, 0],
            'held_for_other_jurisdiction_male_population': [4, 0],
            'held_for_other_jurisdiction_female_population': [0, 0],
            'parole_violation_male_population': [4, 0],
            'parole_violation_female_population': [1, 0],
            'probation_violation_male_population': [62, 0],
            'probation_violation_female_population': [16, 0],
            'fips': ['15001', '15003'],
            'report_date': 2 * [DATE_SCRAPED_2],
            'aggregation_window': 2 * [enum_strings.daily_granularity],
            'report_frequency': 2 * [enum_strings.monthly_granularity]
        })
        assert_frame_equal(result.head(n=2), expected_head, check_names=False)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'facility_name': ['Saguaro Correctional Center, AZ',
                              'Federal Detention Center, Honolulu'],
            'design_bed_capacity': [NaN, NaN],
            'operation_bed_capacity': [NaN, NaN],
            'total_population': [1618, 114],
            'male_population': [1618, 104],
            'female_population': [0, 10],
            'sentenced_felony_male_population': [1528, 2],
            'sentenced_felony_female_population': [0, 0],
            'sentenced_felony_probation_male_population': [0, 6],
            'sentenced_felony_probation_female_population': [0, 4],
            'sentenced_misdemeanor_male_population': [0, 5],
            'sentenced_misdemeanor_female_population': [0, 0],
            'pretrial_felony_male_population': [0, 5],
            'pretrial_felony_female_population': [0, 2],
            'pretrial_misdemeanor_male_population': [0, 3],
            'pretrial_misdemeanor_female_population': [0, 0],
            'held_for_other_jurisdiction_male_population': [0, 1],
            'held_for_other_jurisdiction_female_population': [0, 0],
            'parole_violation_male_population': [90, 10],
            'parole_violation_female_population': [0, 0],
            'probation_violation_male_population': [0, 72],
            'probation_violation_female_population': [0, 4],
            'fips': ['04021', '15003'],
            'report_date': 2 * [DATE_SCRAPED_2],
            'aggregation_window': 2 * [enum_strings.daily_granularity],
            'report_frequency': 2 * [enum_strings.monthly_granularity]
        }, index=range(10, 12))
        assert_frame_equal(result.tail(n=2), expected_tail, check_names=False)

    def testWrite_CalculatesSum(self):
        # Act
        for table, df in self.parsed_pdf.items():
            dao.write_df(table, df)

        # Assert
        query = SessionFactory.for_schema_base(JailsBase).query(
            func.sum(HiFacilityAggregate.total_population))
        result = one(one(query.all()))

        expected_sum_total_population = 5241
        self.assertEqual(result, expected_sum_total_population)
