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
"""Tests for ca_aggregate_ingest.py."""
import datetime
from typing import Dict
from unittest import TestCase

import pandas as pd
from more_itertools import one
from pandas.testing import assert_frame_equal
from sqlalchemy import func
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common.constants.aggregate import enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate.regions.ca import ca_aggregate_ingest
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.persistence.database.schema.aggregate.schema import CaFacilityAggregate
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes

# Cache the parsed result between tests since it's expensive to compute

_PARSED_RESULT: Dict[DeclarativeMeta, pd.DataFrame] = {}


def _parsed_result() -> Dict[DeclarativeMeta, pd.DataFrame]:
    global _PARSED_RESULT

    if not _PARSED_RESULT:
        _PARSED_RESULT = ca_aggregate_ingest.parse(
            fixtures.as_filepath("QueryResult.xls")
        )

    return _PARSED_RESULT


class TestCaAggregateIngest(TestCase):
    """Test that ca_aggregate_ingest correctly parses the CA report file."""

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def testParse_ParsesHeadAndTail(self) -> None:
        result = _parsed_result()[CaFacilityAggregate]

        # Assert Head
        expected_head = pd.DataFrame(
            {
                "jurisdiction_name": [
                    "Alameda Sheriff's Dept.",
                    "Alameda Sheriff's Dept.",
                ],
                "facility_name": ["Glen Dyer Jail", "Santa Rita Jail"],
                "average_daily_population": [379, 2043],
                "unsentenced_male_adp": [356, 1513],
                "unsentenced_female_adp": [0, 192],
                "sentenced_male_adp": [23, 301],
                "sentenced_female_adp": [0, 37],
                "report_date": 2 * [datetime.date(2017, 1, 31)],
                "fips": ["06001", "06001"],
                "aggregation_window": 2 * [enum_strings.monthly_granularity],
                "report_frequency": 2 * [enum_strings.monthly_granularity],
            }
        )
        assert_frame_equal(result.head(n=2), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame(
            {
                "jurisdiction_name": ["Yuba Sheriff's Dept.", "Yuba Sheriff's Dept."],
                "facility_name": ["Yuba County Jail", "Yuba County Jail"],
                "average_daily_population": [373, 366],
                "unsentenced_male_adp": [307, 292],
                "unsentenced_female_adp": [26, 30],
                "sentenced_male_adp": [29, 34],
                "sentenced_female_adp": [11, 10],
                "report_date": [
                    datetime.date(2017, 11, 30),
                    datetime.date(2017, 12, 31),
                ],
                "fips": ["06115", "06115"],
                "aggregation_window": 2 * [enum_strings.monthly_granularity],
                "report_frequency": 2 * [enum_strings.monthly_granularity],
            },
            index=range(1435, 1437),
        )
        assert_frame_equal(result.tail(n=2), expected_tail)

    def testParse_ParsesHeadAndTail2(self) -> None:
        result = ca_aggregate_ingest.parse(
            fixtures.as_filepath("california_california2018")
        )[CaFacilityAggregate]

        # Assert Head
        expected_head = pd.DataFrame(
            {
                "jurisdiction_name": [
                    "Alameda Sheriff's Dept.",
                    "Alameda Sheriff's Dept.",
                ],
                "facility_name": ["Glen Dyer Jail", "Santa Rita Jail"],
                "average_daily_population": [396, 2185],
                "unsentenced_male_adp": [370, 1589],
                "unsentenced_female_adp": [0, 177],
                "sentenced_male_adp": [26, 370],
                "sentenced_female_adp": [0, 49],
                "report_date": 2 * [datetime.date(2018, 1, 31)],
                "fips": ["06001", "06001"],
                "aggregation_window": 2 * [enum_strings.monthly_granularity],
                "report_frequency": 2 * [enum_strings.monthly_granularity],
            }
        )
        assert_frame_equal(result.head(n=2), expected_head)

        # Assert Tail
        expected_tail = pd.DataFrame(
            {
                "jurisdiction_name": ["Yuba Sheriff's Dept.", "Yuba Sheriff's Dept."],
                "facility_name": ["Yuba County Jail", "Yuba County Jail"],
                "average_daily_population": [342, 339],
                "unsentenced_male_adp": [246, 261],
                "unsentenced_female_adp": [32, 28],
                "sentenced_male_adp": [59, 44],
                "sentenced_female_adp": [5, 6],
                "report_date": [
                    datetime.date(2018, 9, 30),
                    datetime.date(2018, 12, 31),
                ],
                "fips": ["06115", "06115"],
                "aggregation_window": 2 * [enum_strings.monthly_granularity],
                "report_frequency": 2 * [enum_strings.monthly_granularity],
            },
            index=range(1404, 1406),
        )
        assert_frame_equal(result.tail(n=2), expected_tail)

    def testWrite_CalculatesSum(self) -> None:
        # Act
        parsed_result = _parsed_result()
        for table, df in parsed_result.items():
            dao.write_df(table, df)

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            query = session.query(
                func.sum(CaFacilityAggregate.average_daily_population)
            )
            result = one(one(query.all()))

        expected_sum_adp = 900124
        self.assertEqual(result, expected_sum_adp)
