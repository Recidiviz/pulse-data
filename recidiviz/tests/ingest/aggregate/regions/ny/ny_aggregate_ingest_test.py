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
"""Tests for ny_aggregate_ingest.py."""
import datetime
from typing import Dict
from unittest import TestCase

import pandas as pd
from more_itertools import one
from pandas.testing import assert_frame_equal
from sqlalchemy import func
from sqlalchemy.orm import DeclarativeMeta

from recidiviz.common.constants.aggregate import enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate.regions.ny import ny_aggregate_ingest
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.persistence.database.schema.aggregate.schema import NyFacilityAggregate
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes


class TestNyAggregateIngest(TestCase):
    """Test that ny_aggregate_ingest correctly parses the NY PDF."""

    parsed_pdf: Dict[DeclarativeMeta, pd.DataFrame] = {}
    parsed_pdf_3_pages: Dict[DeclarativeMeta, pd.DataFrame] = {}

    @classmethod
    def setUpClass(cls) -> None:
        # Cache the parsed pdf between tests since it's expensive to compute
        cls.parsed_pdf = ny_aggregate_ingest.parse(
            "", fixtures.as_filepath("jail_population.pdf")
        )
        cls.parsed_pdf_3_pages = ny_aggregate_ingest.parse(
            "", fixtures.as_filepath("jail_population_2019.pdf")
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def testParse_ParsesHeadAndTail(self) -> None:
        if not self.parsed_pdf:
            raise ValueError("Unexpectedly empty parsed_pdf")

        result = self.parsed_pdf[NyFacilityAggregate]

        # Assert Head
        expected_head = pd.DataFrame(
            {
                "report_date": [
                    datetime.date(year=2017, month=12, day=31),
                    datetime.date(year=2018, month=1, day=31),
                    datetime.date(year=2018, month=2, day=28),
                ],
                "census": [574, 552, 540],
                "in_house": [610, 589, 586],
                "boarded_in": [37, 39, 47],
                "boarded_out": [1, 1, 1],
                "sentenced": [146, 139, 155],
                "civil": [0, 0, 0],
                "federal": [41, 36, 28],
                "technical_parole_violators": [41, 35, 32],
                "state_readies": [18, 22, 16],
                "other_unsentenced": [365, 357, 355],
                "facility_name": [
                    "Albany County Jail",
                    "Albany County Jail",
                    "Albany County Jail",
                ],
                "fips": ["36001", "36001", "36001"],
                "aggregation_window": 3 * [enum_strings.monthly_granularity],
                "report_frequency": 3 * [enum_strings.monthly_granularity],
            }
        )
        assert_frame_equal(result.head(n=3), expected_head, check_names=False)

        # Assert Tail
        expected_tail = pd.DataFrame(
            {
                "report_date": [
                    datetime.date(year=2018, month=10, day=31),
                    datetime.date(year=2018, month=11, day=30),
                    datetime.date(year=2018, month=12, day=31),
                ],
                "census": [39, 42, 40],
                "in_house": [43, 45, 40],
                "boarded_in": [4, 2, 1],
                "boarded_out": [0, 0, 0],
                "sentenced": [15, 13, 11],
                "civil": [0, 0, 0],
                "federal": [5, 6, 6],
                "technical_parole_violators": [2, 6, 3],
                "state_readies": [2, 2, 1],
                "other_unsentenced": [18, 18, 19],
                "facility_name": [
                    "Yates County Jail",
                    "Yates County Jail",
                    "Yates County Jail",
                ],
                "fips": ["36123", "36123", "36123"],
                "aggregation_window": 3 * [enum_strings.monthly_granularity],
                "report_frequency": 3 * [enum_strings.monthly_granularity],
            },
            index=range(816, 819),
        )
        assert_frame_equal(result.tail(n=3), expected_tail, check_names=False)

    def testParseThreeTablesPerPage_ParsesHeadAndTail(self) -> None:
        if not self.parsed_pdf_3_pages:
            raise ValueError("Unexpectedly empty parsed_pdf_3_pages")

        result = self.parsed_pdf_3_pages[NyFacilityAggregate]

        # Assert Head
        expected_head = pd.DataFrame(
            {
                "report_date": [
                    datetime.date(year=2018, month=1, day=31),
                    datetime.date(year=2018, month=2, day=28),
                    datetime.date(year=2018, month=3, day=31),
                ],
                "census": [552, 540, 520],
                "in_house": [589, 586, 565],
                "boarded_in": [39, 47, 46],
                "boarded_out": [1, 1, 1],
                "sentenced": [139, 155, 137],
                "civil": [0, 0, 0],
                "federal": [36, 28, 27],
                "technical_parole_violators": [35, 32, 36],
                "state_readies": [22, 16, 18],
                "other_unsentenced": [357, 355, 347],
                "facility_name": [
                    "Albany County Jail",
                    "Albany County Jail",
                    "Albany County Jail",
                ],
                "fips": ["36001", "36001", "36001"],
                "aggregation_window": 3 * [enum_strings.monthly_granularity],
                "report_frequency": 3 * [enum_strings.monthly_granularity],
            }
        )
        assert_frame_equal(result.head(n=3), expected_head, check_names=False)

        # Assert Tail
        expected_tail = pd.DataFrame(
            {
                "report_date": [
                    datetime.date(year=2018, month=11, day=30),
                    datetime.date(year=2018, month=12, day=31),
                    datetime.date(year=2019, month=1, day=31),
                ],
                "census": [42, 40, 39],
                "in_house": [45, 40, 39],
                "boarded_in": [2, 1, 1],
                "boarded_out": [0, 0, 0],
                "sentenced": [13, 11, 11],
                "civil": [0, 0, 0],
                "federal": [6, 6, 5],
                "technical_parole_violators": [6, 3, 2],
                "state_readies": [2, 1, 0],
                "other_unsentenced": [18, 19, 20],
                "facility_name": [
                    "Yates County Jail",
                    "Yates County Jail",
                    "Yates County Jail",
                ],
                "fips": ["36123", "36123", "36123"],
                "aggregation_window": 3 * [enum_strings.monthly_granularity],
                "report_frequency": 3 * [enum_strings.monthly_granularity],
            },
            index=range(816, 819),
        )
        assert_frame_equal(result.tail(n=3), expected_tail, check_names=False)

    def testWrite_CalculatesSum(self) -> None:
        if not self.parsed_pdf:
            raise ValueError("Unexpectedly empty parsed_pdf")

        # Act
        for table, df in self.parsed_pdf.items():
            dao.write_df(table, df)

        # Assert
        query = SessionFactory.for_database(self.database_key).query(
            func.sum(NyFacilityAggregate.in_house)
        )
        result = one(one(query.all()))

        expected_sum_in_house = 189012
        self.assertEqual(result, expected_sum_in_house)
