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
"""Tests for hi_aggregate_ingest.py."""
import datetime
from unittest import TestCase

import pandas as pd
from pandas.util.testing import assert_frame_equal
from more_itertools import one
from numpy import NaN
from sqlalchemy import func

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz import Session
from recidiviz.ingest.aggregate.regions.hi import hi_aggregate_ingest
from recidiviz.persistence.database import database
from recidiviz.persistence.database.schema import HiFacilityAggregate
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes

DATE_SCRAPED = datetime.date(year=2019, month=1, day=1)

# Cache the parsed pdf between tests since it's expensive to compute
PARSED_PDF = hi_aggregate_ingest.parse(
    fixtures.as_filepath('Pop-Reports-EOM-2018-11-30.pdf'), DATE_SCRAPED)


class TestHiAggregateIngest(TestCase):
    """Test that hi_aggregate_ingest correctly parses the HI PDF."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def testParse_ParsesHeadAndTail(self):
        result = PARSED_PDF[HiFacilityAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'facility_name': ['HCCC', 'SNF'],
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
            'sentenced_pretrial_felony_male_population': [99, 0],
            'sentenced_pretrial_felony_female_population': [30, 0],
            'sentenced_pretrial_misdemeanor_male_population': [26, 0],
            'sentenced_pretrial_misdemeanor_female_population': [9, 0],
            'held_for_other_jurisdiction_male_population': [3, 0],
            'held_for_other_jurisdiction_female_population': [0, 0],
            'parole_violation_male_population': [12, 0],
            'parole_violation_female_population': [4, 0],
            'probation_violation_male_population': [42, 0],
            'probation_violation_female_population': [12, 0],
            'fips': 2 * [None],
            'report_date': 2 * [DATE_SCRAPED],
            'report_granularity': 2 * [enum_strings.monthly_granularity]
        })
        assert_frame_equal(result.head(n=2), expected_head, check_names=False)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'facility_name': ['SAGUARO CC, AZ', 'FEDERAL DET. CTR.'],
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
            'sentenced_pretrial_felony_male_population': [0, 9],
            'sentenced_pretrial_felony_female_population': [0, 1],
            'sentenced_pretrial_misdemeanor_male_population': [0, 4],
            'sentenced_pretrial_misdemeanor_female_population': [0, 0],
            'held_for_other_jurisdiction_male_population': [0, 1],
            'held_for_other_jurisdiction_female_population': [0, 0],
            'parole_violation_male_population': [85, 9],
            'parole_violation_female_population': [0, 0],
            'probation_violation_male_population': [0, 105],
            'probation_violation_female_population': [0, 8],
            'fips': 2 * [None],
            'report_date': 2 * [DATE_SCRAPED],
            'report_granularity': 2 * [enum_strings.monthly_granularity]
        }, index=range(10, 12))
        assert_frame_equal(result.tail(n=2), expected_tail, check_names=False)

    def testWrite_CalculatesSum(self):
        # Act
        for table, df in PARSED_PDF.items():
            database.write_df(table, df)

        # Assert
        query = Session().query(
            func.sum(HiFacilityAggregate.total_population))
        result = one(one(query.all()))

        expected_sum_total_population = 5241
        self.assertEqual(result, expected_sum_total_population)
