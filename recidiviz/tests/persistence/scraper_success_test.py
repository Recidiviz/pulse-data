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
"""Tests for scaper_success.py."""
import datetime
from unittest import TestCase

from mock import patch
from more_itertools import one

from recidiviz.ingest.models.scraper_success import (
    ScraperSuccess as ScraperSuccessModel,
)
from recidiviz.persistence.database.schema.county.schema import (
    ScraperSuccess as ScraperSuccessTable,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.scraper_success import store_scraper_success
from recidiviz.tests.utils import fakes

_JID = "01001001"
_TODAY = datetime.date(2000, 1, 1)


class MockDate(datetime.date):
    @classmethod
    def today(cls):
        return cls(2000, 1, 1)


class TestScraperSuccessPersist(TestCase):
    """Test that store_scraper_success correctly persists."""

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    @patch("recidiviz.ingest.models.single_count.datetime.date", MockDate)
    def testWrite_SingleCount(self):
        store_scraper_success(ScraperSuccessModel(), "01001001")

        query = SessionFactory.for_database(self.database_key).query(
            ScraperSuccessTable
        )
        result = one(query.all())

        self.assertEqual(result.jid, _JID)
        self.assertEqual(result.date, _TODAY)

    def testWrite_ScraperSuccessWithDate(self):
        store_scraper_success(ScraperSuccessModel(date=_TODAY), "01001001")

        query = SessionFactory.for_database(self.database_key).query(
            ScraperSuccessTable
        )
        result = one(query.all())

        self.assertEqual(result.jid, _JID)
        self.assertEqual(result.date, _TODAY)
