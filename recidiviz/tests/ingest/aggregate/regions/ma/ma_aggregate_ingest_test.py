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
"""Tests for ma_aggregate_ingest.py.

Author: Albert Sun"""
from typing import Dict
from unittest import TestCase

import numpy as np
import pandas as pd
from more_itertools import one
from pandas.testing import assert_frame_equal
from sqlalchemy import func
from sqlalchemy.orm import DeclarativeMeta

from recidiviz.common.constants.aggregate import enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate.regions.ma import ma_aggregate_ingest
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.persistence.database.schema.aggregate.schema import MaFacilityAggregate
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes


class TestMaAggregateIngest(TestCase):
    """Test that ma_aggregate_ingest correctly parses the MA PDF."""

    parsed_csv: Dict[DeclarativeMeta, pd.DataFrame] = {}

    @classmethod
    def setUpClass(cls) -> None:
        # Cache the parsed csv between tests since it's expensive to compute
        cls.parsed_csv = ma_aggregate_ingest.parse(
            fixtures.as_filepath("Weekly_DOC_Count_12252017.pdf")
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def testParse_ParsesHeadAndTail(self) -> None:
        result = self.parsed_csv[MaFacilityAggregate]

        # Assert Head
        expected_head = pd.DataFrame(
            {
                "county": ["BARNSTABLE", "BERKSHIRE"],
                "hoc_county_female": [12.0, 0.0],
                "jail_county_female": [15.0, 0.0],
                "hoc_federal_female": [0.0, 0.0],
                "jail_federal_female": [0.0, 0.0],
                "jail_jail_other_female": [2.0, 0.0],
                "hoc_state_female": [0.0, 0.0],
                "hoc_total_female": [12.0, 0.0],
                "jail_total_female": [46.0, 0.0],
                "hoc_county_male": [157.0, 93.0],
                "jail_county_male": [130.0, 86.0],
                "hoc_federal_male": [0.0, 0.0],
                "jail_federal_male": [0.0, 0.0],
                "jail_jail_other_male": [1.0, 0.0],
                "hoc_state_male": [0.0, 5.0],
                "hoc_total_male": [157.0, 98.0],
                "jail_total_male": [419.0, 270.0],
                "hoc_county_total": [169.0, 93.0],
                "jail_county_total": [145.0, 86.0],
                "hoc_federal_total": [0.0, 0.0],
                "jail_federal_total": [0.0, 0.0],
                "jail_jail_other_total": [3.0, 0.0],
                "hoc_state_total": [0.0, 5.0],
                "hoc_total_total": [169.0, 98.0],
                "jail_total_total": [465.0, 270.0],
                "facility_name": ["BARNSTABLE", "BERKSHIRE"],
                "fips": ["25001", "25003"],
                "report_date": [
                    pd.Timestamp(year=2017, month=12, day=25),
                    pd.Timestamp(year=2017, month=12, day=25),
                ],
                "aggregation_window": [
                    enum_strings.daily_granularity,
                    enum_strings.daily_granularity,
                ],
                "report_frequency": [
                    enum_strings.weekly_granularity,
                    enum_strings.weekly_granularity,
                ],
            }
        )
        assert_frame_equal(result.head(n=2), expected_head, check_names=False)

        # Assert Tail
        expected_tail = pd.DataFrame(
            {
                "county": ["SUFFOLK", "WORCESTER"],
                "hoc_county_female": [111.0, 0.0],
                "jail_county_female": [np.nan, 0.0],
                "hoc_federal_female": [0.0, 0.0],
                "jail_federal_female": [np.nan, 0.0],
                "jail_jail_other_female": [np.nan, 0.0],
                "hoc_state_female": [2.0, 0.0],
                "hoc_total_female": [226.0, 0.0],
                "jail_total_female": [np.nan, 0.0],
                "hoc_county_male": [889.0, 416.0],
                "jail_county_male": [np.nan, 497.0],
                "hoc_federal_male": [0.0, 0.0],
                "jail_federal_male": [np.nan, 0.0],
                "jail_jail_other_male": [np.nan, 0.0],
                "hoc_state_male": [1.0, 0.0],
                "hoc_total_male": [1780.0, 416.0],
                "jail_total_male": [np.nan, 1410.0],
                "hoc_county_total": [1000.0, 416.0],
                "jail_county_total": [np.nan, 497.0],
                "hoc_federal_total": [0.0, 0.0],
                "jail_federal_total": [np.nan, 0.0],
                "jail_jail_other_total": [np.nan, 0.0],
                "hoc_state_total": [3.0, 0.0],
                "hoc_total_total": [2006.0, 416.0],
                "jail_total_total": [np.nan, 1410.0],
                "facility_name": ["SUFFOLK SOUTH BAY", "WORCESTER"],
                "fips": ["25025", "25027"],
                "report_date": [
                    pd.Timestamp(year=2017, month=12, day=25),
                    pd.Timestamp(year=2017, month=12, day=25),
                ],
                "aggregation_window": [
                    enum_strings.daily_granularity,
                    enum_strings.daily_granularity,
                ],
                "report_frequency": [
                    enum_strings.weekly_granularity,
                    enum_strings.weekly_granularity,
                ],
            },
            index=range(20, 22),
        )
        assert_frame_equal(result.tail(n=2), expected_tail, check_names=False)

    def testWrite_CalculatesSum(self) -> None:
        # Act
        for table, df in self.parsed_csv.items():
            dao.write_df(table, df)

        # Assert
        query = SessionFactory.for_database(self.database_key).query(
            func.sum(MaFacilityAggregate.jail_total_male)
        )
        result = one(one(query.all()))

        expected_sum_male = 12366
        self.assertEqual(result, expected_sum_male)
