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
from urllib.parse import urlencode

from flask import Flask
from mock import Mock, patch
from more_itertools import one

from recidiviz.common import str_field_utils
from recidiviz.common.constants.person_characteristics import Ethnicity, Gender, Race
from recidiviz.ingest.aggregate.single_count import store_single_count_blueprint
from recidiviz.persistence.database.schema.aggregate.schema import SingleCountAggregate
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.utils import fakes

APP_ID = "recidiviz-scraper-aggregate-report-test"

app = Flask(__name__)
app.register_blueprint(store_single_count_blueprint)
app.config["TESTING"] = True
TEST_ENV = "recidiviz-test"


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", Mock(return_value="123456789"))
@patch.dict("os.environ", {"PERSIST_LOCALLY": "true"})
class TestSingleCountIngest(TestCase):
    """Test that store_single_count correctly stores a count."""

    def setUp(self) -> None:
        self.client = app.test_client()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        fakes.use_in_memory_sqlite_database(self.database_key)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def testWrite_SingleCountToday(self) -> None:
        params = {
            "jid": "01001001",
            "count": 311,
        }

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get(
            f"/single_count?{urlencode(params)}", headers=headers
        )
        self.assertEqual(response.status_code, 200)

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            query = session.query(SingleCountAggregate)
            result = one(query.all())

        self.assertEqual(result.count, params["count"])
        self.assertEqual(result.date, datetime.date.today())

    def testWrite_SingleCountGenderToday(self) -> None:
        params = {
            "jid": "01001001",
            "count": 311,
            "gender": Gender.MALE.value,
        }

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get(
            f"/single_count?{urlencode(params)}", headers=headers
        )
        self.assertEqual(response.status_code, 200)

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            query = session.query(SingleCountAggregate)
            result = one(query.all())

        self.assertEqual(result.count, params["count"])
        self.assertEqual(result.date, datetime.date.today())
        self.assertEqual(result.gender, params["gender"])

    def testWrite_SingleCountEthnicityToday(self) -> None:
        params = {
            "jid": "01001001",
            "count": 311,
            "ethnicity": Ethnicity.HISPANIC.value,
        }

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get(
            f"/single_count?{urlencode(params)}", headers=headers
        )
        self.assertEqual(response.status_code, 200)

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            query = session.query(SingleCountAggregate)
            result = one(query.all())

        self.assertEqual(result.count, params["count"])
        self.assertEqual(result.date, datetime.date.today())
        self.assertEqual(result.ethnicity, params["ethnicity"])

    def testWrite_SingleCountRaceToday(self) -> None:
        params = {
            "jid": "01001001",
            "count": 311,
            "race": Race.ASIAN.value,
        }

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get(
            f"/single_count?{urlencode(params)}", headers=headers
        )
        self.assertEqual(response.status_code, 200)

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            query = session.query(SingleCountAggregate)
            result = one(query.all())

        self.assertEqual(result.count, params["count"])
        self.assertEqual(result.date, datetime.date.today())
        self.assertEqual(result.race, params["race"])

    def testWrite_SingleCountWithDate(self) -> None:
        params = {
            "jid": "01001001",
            "count": 311,
            "date": "2019-01-01",
        }

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get(
            f"/single_count?{urlencode(params)}", headers=headers
        )
        self.assertEqual(response.status_code, 200)

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            query = session.query(SingleCountAggregate)
            result = one(query.all())

        self.assertEqual(result.count, params["count"])

        date_str = params["date"]
        if not isinstance(date_str, str):
            raise ValueError(f"Unexpected type for date_str: [{type(date_str)}]")
        self.assertEqual(result.date, str_field_utils.parse_date(date_str))

    def testWrite_SingleCountWithDateAndAllDemographics(self) -> None:
        params = {
            "jid": "01001001",
            "ethnicity": Ethnicity.HISPANIC.value,
            "gender": Gender.FEMALE.value,
            "race": Race.BLACK.value,
            "count": 311,
            "date": "2019-01-01",
        }

        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get(
            f"/single_count?{urlencode(params)}", headers=headers
        )
        self.assertEqual(response.status_code, 200)

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            query = session.query(SingleCountAggregate)
            result = one(query.all())

        self.assertEqual(result.count, params["count"])
        date_str = params["date"]
        if not isinstance(date_str, str):
            raise ValueError(f"Unexpected type for date_str: [{type(date_str)}]")
        self.assertEqual(result.date, str_field_utils.parse_date(date_str))
        self.assertEqual(result.ethnicity, params["ethnicity"])
        self.assertEqual(result.gender, params["gender"])
        self.assertEqual(result.race, params["race"])
