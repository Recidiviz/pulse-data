# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for in_aggregate_ingest.py."""
import datetime
from typing import Dict
from unittest import TestCase

import pandas as pd
from more_itertools import one
from pandas.testing import assert_frame_equal
from sqlalchemy import func
from sqlalchemy.orm import DeclarativeMeta

from recidiviz.ingest.aggregate.regions.indiana import in_aggregate_ingest
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.persistence.database.schema.aggregate.schema import InCountyAggregate
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes


class TestInAggregateIngest(TestCase):
    """Test that co_aggregate_ingest correctly parses the CO PDF."""

    parsed_excel: Dict[DeclarativeMeta, pd.DataFrame] = {}
    parsed_pdf: Dict[DeclarativeMeta, pd.DataFrame] = {}

    @classmethod
    def setUpClass(cls) -> None:
        # Cache the parsed csv between tests since it's expensive to compute
        cls.parsed_excel = in_aggregate_ingest.parse(
            fixtures.as_filepath("JAIL CAPACITY % WEEK 10 2021.xlsx")
        )
        cls.parsed_pdf = in_aggregate_ingest.parse(
            fixtures.as_filepath("JAIL CAPACITY % WEEK 14 2021.pdf")
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def testParse_ParsesExcel(self) -> None:
        result = self.parsed_excel[InCountyAggregate]

        expected_head = pd.DataFrame(
            {
                "report_date": 4 * [datetime.date(2021, 3, 8)],
                "county": ["Adams", "Allen", "Bartholomew", "Benton"],
                "total_jail_population": [102, 791, 193, 13],
                "jail_capacity": [225, 741, 366, 54],
                "fips": ["18001", "18003", "18005", "18007"],
                "aggregation_window": 4 * ["DAILY"],
                "report_frequency": 4 * ["QUARTERLY"],
            }
        )
        expected_tail = pd.DataFrame(
            {
                "report_date": 4 * [datetime.date(2021, 3, 8)],
                "county": ["Wayne", "Wells", "White", "Whitley"],
                "total_jail_population": [281, 78, 63, 104],
                "jail_capacity": [416, 94, 164, 104],
                "fips": ["18177", "18179", "18181", "18183"],
                "aggregation_window": 4 * ["DAILY"],
                "report_frequency": 4 * ["QUARTERLY"],
            },
            index=range(88, 92),
        )

        assert_frame_equal(result.head(4), expected_head)
        assert_frame_equal(result.tail(4), expected_tail)

    def testParse_ParsesPdf(self) -> None:
        result = self.parsed_pdf[InCountyAggregate]

        expected_head = pd.DataFrame(
            {
                "report_date": 4 * [datetime.date(2021, 4, 5)],
                "county": ["Adams", "Allen", "Bartholomew", "Benton"],
                "total_jail_population": [95, 808, 195, 14],
                "jail_capacity": [225, 741, 366, 54],
                "fips": ["18001", "18003", "18005", "18007"],
                "aggregation_window": 4 * ["DAILY"],
                "report_frequency": 4 * ["QUARTERLY"],
            }
        )
        expected_tail = pd.DataFrame(
            {
                "report_date": 4 * [datetime.date(2021, 4, 5)],
                "county": ["Wayne", "Wells", "White", "Whitley"],
                "total_jail_population": [283, 78, 65, 104],
                "jail_capacity": [416, 94, 164, 104],
                "fips": ["18177", "18179", "18181", "18183"],
                "aggregation_window": 4 * ["DAILY"],
                "report_frequency": 4 * ["QUARTERLY"],
            },
            index=range(88, 92),
        )

        assert_frame_equal(result.head(4), expected_head)
        assert_frame_equal(result.tail(4), expected_tail)

    def testWrite_CalculatesSum(self) -> None:
        # Act
        for table, df in self.parsed_excel.items():
            dao.write_df(table, df)

        # Assert
        query = SessionFactory.for_database(self.database_key).query(
            func.sum(InCountyAggregate.total_jail_population)
        )
        result = one(one(query.all()))

        # This is the expected sum, even though the excel file has a different sum.
        expected_sum = 17164
        self.assertEqual(result, expected_sum)
