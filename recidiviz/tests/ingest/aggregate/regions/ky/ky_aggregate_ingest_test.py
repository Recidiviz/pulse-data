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
"""Tests for ky_aggregate_ingest.py."""
import datetime
from unittest import TestCase

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from more_itertools import one
from sqlalchemy import func

from recidiviz.common.constants.aggregate import enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate.regions.ky import ky_aggregate_ingest
from recidiviz.persistence.database.base_schema import JailsBase
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.persistence.database.schema.aggregate.schema import KyFacilityAggregate
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes

DATE_SCRAPED = datetime.date(year=2018, month=12, day=20)
DATE_SCRAPED_2 = datetime.date(year=2018, month=8, day=23)
DATE_SCRAPED_3 = datetime.date(year=2019, month=8, day=22)

# Cache the parsed pdf between tests since it's expensive to compute
@pytest.fixture(scope="class")
def parsed_pdf(request):
    request.cls.parsed_pdf = ky_aggregate_ingest.parse(
        "", fixtures.as_filepath("12-20-18.pdf")
    )
    request.cls.parsed_pdf_2 = ky_aggregate_ingest.parse(
        "", fixtures.as_filepath("08-23-18.pdf")
    )
    request.cls.parsed_pdf_3 = ky_aggregate_ingest.parse(
        "", fixtures.as_filepath("08-22-19.pdf")
    )


@pytest.mark.usefixtures("parsed_pdf")
class TestKyAggregateIngest(TestCase):
    """Test that ky_aggregate_ingest correctly parses the KY PDF."""

    def setUp(self) -> None:
        fakes.use_in_memory_sqlite_database(JailsBase)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def testParse_ParsesHeadAndTail(self):
        result = self.parsed_pdf[KyFacilityAggregate]

        # Assert Head
        expected_head = pd.DataFrame(
            {
                "facility_name": ["Adair", "Adair RCC"],
                "total_jail_beds": [51, 31],
                "reported_population": [116, 20],
                "male_population": [91, 20],
                "class_d_male_population": [11, 19],
                "community_custody_male_population": [0, 1],
                "alternative_sentence_male_population": [2, 0],
                "controlled_intake_male_population": [10, 0],
                "parole_violators_male_population": [6, 0],
                "federal_male_population": [0, 0],
                "female_population": [25, 0],
                "class_d_female_population": [0, 0],
                "community_custody_female_population": [0, 0],
                "alternative_sentence_female_population": [0, 0],
                "controlled_intake_female_population": [1, 0],
                "parole_violators_female_population": [0, 0],
                "federal_female_population": [0, 0],
                "fips": ["21001", "21001"],
                "report_date": 2 * [DATE_SCRAPED],
                "aggregation_window": 2 * [enum_strings.daily_granularity],
                "report_frequency": 2 * [enum_strings.weekly_granularity],
            }
        )
        assert_frame_equal(result.head(n=2), expected_head, check_names=False)

        # Assert Tail
        expected_tail = pd.DataFrame(
            {
                "facility_name": ["Woodford", "Woodford JRC"],
                "total_jail_beds": [63, 32],
                "reported_population": [90, 32],
                "male_population": [79, 32],
                "class_d_male_population": [1, 13],
                "community_custody_male_population": [0, 19],
                "alternative_sentence_male_population": [0, 0],
                "controlled_intake_male_population": [5, 0],
                "parole_violators_male_population": [0, 0],
                "federal_male_population": [46, 0],
                "female_population": [11, 0],
                "class_d_female_population": [1, 0],
                "community_custody_female_population": [1, 0],
                "alternative_sentence_female_population": [0, 0],
                "controlled_intake_female_population": [3, 0],
                "parole_violators_female_population": [1, 0],
                "federal_female_population": [2, 0],
                "fips": ["21239", "21239"],
                "report_date": 2 * [DATE_SCRAPED],
                "aggregation_window": 2 * [enum_strings.daily_granularity],
                "report_frequency": 2 * [enum_strings.weekly_granularity],
            },
            index=range(122, 124),
        )
        assert_frame_equal(result.tail(n=2), expected_tail, check_names=False)

    def testParse2_ParsesHeadAndTail(self):
        result = self.parsed_pdf_2[KyFacilityAggregate]

        # Assert Head
        expected_head = pd.DataFrame(
            {
                "facility_name": ["Adair", "Adair RCC"],
                "total_jail_beds": [51, 31],
                "reported_population": [132, 31],
                "male_population": [104, 31],
                "class_d_male_population": [6, 28],
                "community_custody_male_population": [0, 3],
                "alternative_sentence_male_population": [3, 0],
                "controlled_intake_male_population": [17, 0],
                "parole_violators_male_population": [6, 0],
                "federal_male_population": [0, 0],
                "female_population": [28, 0],
                "class_d_female_population": [1, 0],
                "community_custody_female_population": [0, 0],
                "alternative_sentence_female_population": [0, 0],
                "controlled_intake_female_population": [2, 0],
                "parole_violators_female_population": [2, 0],
                "federal_female_population": [0, 0],
                "fips": ["21001", "21001"],
                "report_date": 2 * [DATE_SCRAPED_2],
                "aggregation_window": 2 * [enum_strings.daily_granularity],
                "report_frequency": 2 * [enum_strings.weekly_granularity],
            }
        )
        assert_frame_equal(result.head(n=2), expected_head, check_names=False)

        # Assert Tail
        expected_tail = pd.DataFrame(
            {
                "facility_name": ["Woodford", "Woodford JRC"],
                "total_jail_beds": [63, 32],
                "reported_population": [103, 37],
                "male_population": [88, 37],
                "class_d_male_population": [1, 22],
                "community_custody_male_population": [0, 15],
                "alternative_sentence_male_population": [0, 0],
                "controlled_intake_male_population": [4, 0],
                "parole_violators_male_population": [3, 0],
                "federal_male_population": [43, 0],
                "female_population": [15, 0],
                "class_d_female_population": [3, 0],
                "community_custody_female_population": [0, 0],
                "alternative_sentence_female_population": [0, 0],
                "controlled_intake_female_population": [1, 0],
                "parole_violators_female_population": [0, 0],
                "federal_female_population": [2, 0],
                "fips": ["21239", "21239"],
                "report_date": 2 * [DATE_SCRAPED_2],
                "aggregation_window": 2 * [enum_strings.daily_granularity],
                "report_frequency": 2 * [enum_strings.weekly_granularity],
            },
            index=range(122, 124),
        )
        assert_frame_equal(result.tail(n=2), expected_tail, check_names=False)

    def testParse3_ParsesHeadAndTail(self):
        result = self.parsed_pdf_3[KyFacilityAggregate]

        # Assert Head
        expected_head = pd.DataFrame(
            {
                "facility_name": ["Adair", "Adair RCC"],
                "total_jail_beds": [51, 31],
                "reported_population": [113, 31],
                "male_population": [91, 31],
                "class_d_male_population": [8, 29],
                "community_custody_male_population": [0, 2],
                "alternative_sentence_male_population": [7, 0],
                "controlled_intake_male_population": [6, 0],
                "parole_violators_male_population": [6, 0],
                "federal_male_population": [0, 0],
                "female_population": [22, 0],
                "class_d_female_population": [0, 0],
                "community_custody_female_population": [0, 0],
                "alternative_sentence_female_population": [0, 0],
                "controlled_intake_female_population": [3, 0],
                "parole_violators_female_population": [0, 0],
                "federal_female_population": [0, 0],
                "fips": ["21001", "21001"],
                "report_date": 2 * [DATE_SCRAPED_3],
                "aggregation_window": 2 * [enum_strings.daily_granularity],
                "report_frequency": 2 * [enum_strings.weekly_granularity],
            }
        )
        assert_frame_equal(result.head(n=2), expected_head, check_names=False)

        # Assert Tail
        expected_tail = pd.DataFrame(
            {
                "facility_name": ["Woodford", "Woodford JRC"],
                "total_jail_beds": [63, 32],
                "reported_population": [107, 36],
                "male_population": [86, 36],
                "class_d_male_population": [5, 13],
                "community_custody_male_population": [0, 23],
                "alternative_sentence_male_population": [0, 0],
                "controlled_intake_male_population": [3, 0],
                "parole_violators_male_population": [2, 0],
                "federal_male_population": [47, 0],
                "female_population": [21, 0],
                "class_d_female_population": [2, 0],
                "community_custody_female_population": [2, 0],
                "alternative_sentence_female_population": [0, 0],
                "controlled_intake_female_population": [0, 0],
                "parole_violators_female_population": [0, 0],
                "federal_female_population": [5, 0],
                "fips": ["21239", "21239"],
                "report_date": 2 * [DATE_SCRAPED_3],
                "aggregation_window": 2 * [enum_strings.daily_granularity],
                "report_frequency": 2 * [enum_strings.weekly_granularity],
            },
            index=range(122, 124),
        )
        assert_frame_equal(result.tail(n=2), expected_tail, check_names=False)

    def testWrite_CalculatesSum(self):
        # Act
        for table, df in self.parsed_pdf.items():
            dao.write_df(table, df)

        # Assert
        query = SessionFactory.for_schema_base(JailsBase).query(
            func.sum(KyFacilityAggregate.reported_population)
        )
        result = one(one(query.all()))

        expected_total_reported_population = 24174
        self.assertEqual(result, expected_total_reported_population)
