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
"""Tests for wv_aggregate_ingest.py."""
import datetime
from typing import Dict
from unittest import TestCase

import pandas as pd
from more_itertools import one
from pandas.testing import assert_frame_equal
from sqlalchemy import func
from sqlalchemy.orm import DeclarativeMeta

from recidiviz.ingest.aggregate.regions.wv import wv_aggregate_ingest
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.persistence.database.schema.aggregate.schema import WvFacilityAggregate
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes


class TestWvAggregateIngest(TestCase):
    """Test that wv_aggregate_ingest correctly parses the WV CSV."""

    parsed_csv: Dict[DeclarativeMeta, pd.DataFrame] = {}

    @classmethod
    def setUpClass(cls) -> None:
        # Cache the parsed csv between tests since it's expensive to compute
        cls.parsed_csv = wv_aggregate_ingest.parse(
            fixtures.as_filepath("covid19_dcr_2020_04-27-2pm.txt")
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def testParse(self) -> None:
        result = self.parsed_csv[WvFacilityAggregate]

        expected_result = pd.DataFrame(
            {
                "facility_name": [
                    "Eastern",
                    "Martinsburg",
                    "Vicki V. Douglas",
                    "Donald R. Kuhn",
                    "Central",
                    "Western",
                    "Robert L. Shell",
                    "North Central",
                    "Mount Olive",
                    "Gene Spadaro",
                    "Potomac Highlands",
                    'J.M. "Chick" Buckbee',
                    "Salem",
                    "South Central",
                    "Charleston",
                    'James H. "Tiger" Morton',
                    "Southwestern",
                    "Northern",
                    "Northern",
                    "Lakin",
                    "McDowell (contract)",
                    "Sam Perdue",
                    "Ohio County",
                    "Saint Marys",
                    "Denmar",
                    "Southern",
                    "Beckley",
                    "Tygart Valley",
                    "Huttonsville",
                    "Pruntytown",
                    "Kenneth \x93Honey\x94 Rubenstein",
                    "Parkersburg",
                    "Lorrie Yeager Jr.",
                ],
                "county": [
                    "Berkeley",
                    "Berkeley",
                    "Berkeley",
                    "Boone",
                    "Braxton",
                    "Cabell",
                    "Cabell",
                    "Doddridge",
                    "Fayette",
                    "Fayette",
                    "Hampshire",
                    "Hampshire",
                    "Harrison",
                    "Kanawha",
                    "Kanawha",
                    "Kanawha",
                    "Logan",
                    "Marshall",
                    "Marshall",
                    "Mason",
                    "McDowell",
                    "Mercer",
                    "Ohio",
                    "Pleasants",
                    "Pocahontas",
                    "Raleigh",
                    "Raleigh",
                    "Randolph",
                    "Randolph",
                    "Taylor",
                    "Tucker",
                    "Wood",
                    "Wood",
                ],
                "total_jail_population": [
                    16.0,
                    1.0,
                    0.0,
                    0.0,
                    0.0,
                    12.0,
                    0.0,
                    6.0,
                    8.0,
                    1.0,
                    1.0,
                    0.0,
                    5.0,
                    10.0,
                    0.0,
                    0.0,
                    2.0,
                    8.0,
                    1.0,
                    1.0,
                    1.0,
                    0.0,
                    0.0,
                    4.0,
                    0.0,
                    1.0,
                    0.0,
                    1.0,
                    1.0,
                    7.0,
                    0.0,
                    1.0,
                    0.0,
                ],
                "facility_type": [
                    "JAIL",
                    "PRISON",
                    "JUVENILE CENTER",
                    "JUVENILE CENTER",
                    "JAIL",
                    "JAIL",
                    "JUVENILE CENTER",
                    "JAIL",
                    "PRISON",
                    "JUVENILE CENTER",
                    "JAIL",
                    "JUVENILE CENTER",
                    "PRISON",
                    "JAIL",
                    "COMMUNITY CORRECTIONS",
                    "JUVENILE CENTER",
                    "JAIL",
                    "JAIL",
                    "PRISON",
                    "PRISON",
                    "PRISON",
                    "JUVENILE CENTER",
                    "PRISON",
                    "PRISON",
                    "PRISON",
                    "JAIL",
                    "COMMUNITY CORRECTIONS",
                    "JAIL",
                    "PRISON",
                    "PRISON",
                    "JUVENILE CENTER",
                    "COMMUNITY CORRECTIONS",
                    "JUVENILE CENTER",
                ],
                "report_date": 33 * [datetime.date(2020, 4, 27)],
                "fips": [
                    "54003",
                    "54003",
                    "54003",
                    "54005",
                    "54007",
                    "54011",
                    "54011",
                    "54017",
                    "54019",
                    "54019",
                    "54027",
                    "54027",
                    "54033",
                    "54039",
                    "54039",
                    "54039",
                    "54045",
                    "54051",
                    "54051",
                    "54053",
                    "54047",
                    "54055",
                    "54069",
                    "54073",
                    "54075",
                    "54081",
                    "54081",
                    "54083",
                    "54083",
                    "54091",
                    "54093",
                    "54107",
                    "54107",
                ],
                "aggregation_window": 33 * ["DAILY"],
                "report_frequency": 33 * ["DAILY"],
            }
        )
        assert_frame_equal(result, expected_result)

    def testWrite_CalculatesSum(self) -> None:
        # Act
        for table, df in self.parsed_csv.items():
            dao.write_df(table, df)

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            query = session.query(func.sum(WvFacilityAggregate.total_jail_population))
            result = one(one(query.all()))

        expected_sum = 88
        self.assertEqual(result, expected_sum)
