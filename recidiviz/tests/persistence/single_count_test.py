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

from more_itertools import one

from recidiviz.common.constants.entity_enum import EnumParsingError
from recidiviz.common.constants.person_characteristics import Ethnicity, \
    Gender, Race
from recidiviz.ingest.models.single_count import SingleCount
from recidiviz.persistence.database.base_schema import \
    JailsBase
from recidiviz.persistence.database.schema.aggregate.schema import \
    SingleCountAggregate
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.single_count import store_single_count
from recidiviz.tests.utils import fakes

_JID = '01001001'
_COUNT = 311


class TestSingleCountPersist(TestCase):
    """Test that store_single_count correctly persists a count."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database(JailsBase)

    def testWrite_SingleCount(self):
        store_single_count(
            SingleCount(
                jid='01001001',
                count=_COUNT,
            )
        )

        query = SessionFactory.for_schema_base(JailsBase).query(
            SingleCountAggregate)
        result = one(query.all())

        self.assertEqual(result.jid, _JID)
        self.assertEqual(result.count, _COUNT)
        self.assertEqual(result.date, datetime.date.today())

    def testWrite_SingleStrCount(self):
        store_single_count(
            SingleCount(
                jid='01001001',
                count=str(_COUNT),
            )
        )

        query = SessionFactory.for_schema_base(JailsBase).query(
            SingleCountAggregate)
        result = one(query.all())

        self.assertEqual(result.jid, _JID)
        self.assertEqual(result.count, _COUNT)
        self.assertEqual(result.date, datetime.date.today())

    def testWrite_SingleCountWithDate(self):
        store_single_count(
            SingleCount(
                jid='01001001',
                count=_COUNT,
                date=datetime.date.today()
            )
        )

        query = SessionFactory.for_schema_base(JailsBase).query(
            SingleCountAggregate)
        result = one(query.all())

        self.assertEqual(result.jid, _JID)
        self.assertEqual(result.count, _COUNT)
        self.assertEqual(result.date, datetime.date.today())

    def testWrite_SingleCountWithEthnicity(self):
        store_single_count(
            SingleCount(
                jid='01001001',
                count=_COUNT,
                ethnicity=Ethnicity.HISPANIC,
            )
        )

        query = SessionFactory.for_schema_base(JailsBase).query(
            SingleCountAggregate)
        result = one(query.all())

        self.assertEqual(result.jid, _JID)
        self.assertEqual(result.count, _COUNT)
        self.assertEqual(Ethnicity(result.ethnicity), Ethnicity.HISPANIC)

    def testWrite_SingleCountWithGender(self):
        store_single_count(
            SingleCount(
                jid='01001001',
                count=_COUNT,
                gender=Gender.FEMALE,
            )
        )

        query = SessionFactory.for_schema_base(JailsBase).query(
            SingleCountAggregate)
        result = one(query.all())

        self.assertEqual(result.jid, _JID)
        self.assertEqual(result.count, _COUNT)
        self.assertEqual(Gender(result.gender), Gender.FEMALE)

    def testWrite_SingleCountWithRace(self):
        store_single_count(
            SingleCount(
                jid='01001001',
                count=_COUNT,
                race=Race.ASIAN,
            )
        )

        query = SessionFactory.for_schema_base(JailsBase).query(
            SingleCountAggregate)
        result = one(query.all())

        self.assertEqual(result.jid, _JID)
        self.assertEqual(result.count, _COUNT)
        self.assertEqual(Race(result.race), Race.ASIAN)

    def testWrite_SingleCountBadDataFails(self):
        def test_db_empty():
            query = SessionFactory.for_schema_base(JailsBase).query(
                SingleCountAggregate)
            self.assertEqual(query.all(), [])

        with self.assertRaises(ValueError):
            store_single_count(
                SingleCount(
                    jid='1001001',
                    count=311,
                )
            )
        test_db_empty()

        with self.assertRaises(EnumParsingError):
            store_single_count(
                SingleCount(
                    jid='01001001',
                    count=311,
                    ethnicity='Not an Ethnicity'
                )
            )
        test_db_empty()

        with self.assertRaises(EnumParsingError):
            store_single_count(
                SingleCount(
                    jid='01001001',
                    count=311,
                    gender='Not a Gender'
                )
            )
        test_db_empty()

        with self.assertRaises(EnumParsingError):
            store_single_count(
                SingleCount(
                    jid='01001001',
                    count=311,
                    race='Not a Race'
                )
            )
        test_db_empty()

        with self.assertRaises(ValueError):
            store_single_count(
                SingleCount(
                    jid='01001001',
                    count=311,
                    date='Not a date'
                )
            )
        test_db_empty()
