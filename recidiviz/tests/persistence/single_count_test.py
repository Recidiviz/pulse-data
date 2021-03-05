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
"""Tests for single_count.py."""
import datetime
from unittest import TestCase

from mock import patch
from more_itertools import one

from recidiviz.common.constants.entity_enum import EnumParsingError
from recidiviz.common.constants.person_characteristics import Ethnicity, Gender, Race
from recidiviz.ingest.models.single_count import SingleCount
from recidiviz.persistence.database.schema.aggregate.schema import SingleCountAggregate
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.single_count import store_single_count
from recidiviz.tests.utils import fakes

_JID = "01001001"
_COUNT = 311
_TODAY = datetime.date(2000, 1, 1)


class MockDate(datetime.date):
    @classmethod
    def today(cls):
        return cls(2000, 1, 1)


@patch.dict("os.environ", {"PERSIST_LOCALLY": "true"})
class TestSingleCountPersist(TestCase):
    """Test that store_single_count correctly persists a count."""

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    @patch("recidiviz.ingest.models.single_count.datetime.date", MockDate)
    def testWrite_SingleCount(self):
        store_single_count(SingleCount(count=_COUNT), "01001001")

        query = SessionFactory.for_database(self.database_key).query(
            SingleCountAggregate
        )
        result = one(query.all())

        self.assertEqual(result.jid, _JID)
        self.assertEqual(result.count, _COUNT)
        self.assertEqual(result.date, _TODAY)

    @patch("recidiviz.ingest.models.single_count.datetime.date", MockDate)
    def testWrite_SingleStrCount(self):
        store_single_count(SingleCount(count=str(_COUNT)), "01001001")

        query = SessionFactory.for_database(self.database_key).query(
            SingleCountAggregate
        )
        result = one(query.all())

        self.assertEqual(result.jid, _JID)
        self.assertEqual(result.count, _COUNT)
        self.assertEqual(result.date, _TODAY)

    def testWrite_SingleCountWithDate(self):
        store_single_count(SingleCount(count=_COUNT, date=_TODAY), "01001001")

        query = SessionFactory.for_database(self.database_key).query(
            SingleCountAggregate
        )
        result = one(query.all())

        self.assertEqual(result.jid, _JID)
        self.assertEqual(result.count, _COUNT)
        self.assertEqual(result.date, _TODAY)

    def testWrite_SingleCountWithEthnicity(self):
        store_single_count(
            SingleCount(count=_COUNT, ethnicity=Ethnicity.HISPANIC), "01001001"
        )

        query = SessionFactory.for_database(self.database_key).query(
            SingleCountAggregate
        )
        result = one(query.all())

        self.assertEqual(result.jid, _JID)
        self.assertEqual(result.count, _COUNT)
        self.assertEqual(Ethnicity(result.ethnicity), Ethnicity.HISPANIC)

    def testWrite_SingleCountWithGender(self):
        store_single_count(SingleCount(count=_COUNT, gender=Gender.FEMALE), "01001001")

        query = SessionFactory.for_database(self.database_key).query(
            SingleCountAggregate
        )
        result = one(query.all())

        self.assertEqual(result.jid, _JID)
        self.assertEqual(result.count, _COUNT)
        self.assertEqual(Gender(result.gender), Gender.FEMALE)

    def testWrite_SingleCountWithRace(self):
        store_single_count(SingleCount(count=_COUNT, race=Race.ASIAN), "01001001")

        query = SessionFactory.for_database(self.database_key).query(
            SingleCountAggregate
        )
        result = one(query.all())

        self.assertEqual(result.jid, _JID)
        self.assertEqual(result.count, _COUNT)
        self.assertEqual(Race(result.race), Race.ASIAN)

    def testWrite_SingleCountBadDataFails(self):
        def test_db_empty():
            query = SessionFactory.for_database(self.database_key).query(
                SingleCountAggregate
            )
            self.assertEqual(query.all(), [])

        with self.assertRaises(ValueError):
            store_single_count(SingleCount(count=311), "1001001")
        test_db_empty()

        with self.assertRaises(EnumParsingError):
            store_single_count(
                SingleCount(count=311, ethnicity="Not an Ethnicity"), "01001001"
            )
        test_db_empty()

        with self.assertRaises(EnumParsingError):
            store_single_count(
                SingleCount(count=311, gender="Not a Gender"), "01001001"
            )
        test_db_empty()

        with self.assertRaises(EnumParsingError):
            store_single_count(SingleCount(count=311, race="Not a Race"), "01001001")
        test_db_empty()

        with self.assertRaises(ValueError):
            store_single_count(SingleCount(count=311, date="Not a date"), "01001001")
        test_db_empty()
