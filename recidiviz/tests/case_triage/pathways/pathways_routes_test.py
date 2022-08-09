# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""This class implements tests for Pathways api routes."""
import os
from http import HTTPStatus
from typing import Callable, Dict, Optional
from unittest import mock
from unittest.case import TestCase
from unittest.mock import MagicMock

import pytest
from fakeredis import FakeRedis
from flask import Flask
from flask.testing import FlaskClient

from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.pathways.dimensions.time_period import TimePeriod
from recidiviz.case_triage.pathways.pathways_authorization import (
    on_successful_authorization,
)
from recidiviz.case_triage.pathways.pathways_routes import create_pathways_api_blueprint
from recidiviz.persistence.database.schema.pathways.schema import (
    LibertyToPrisonTransitions,
    PrisonToSupervisionTransitions,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.case_triage.pathways.metrics.base_metrics_test import (
    load_metrics_fixture,
)
from recidiviz.tools.postgres import local_postgres_helpers


class PathwaysBlueprintTestCase(TestCase):
    """Base class for pathways flask tests"""

    mock_authorization_handler: MagicMock
    test_app: Flask
    test_client: FlaskClient

    def setUp(self) -> None:
        self.mock_authorization_handler = MagicMock()

        self.redis_patcher = mock.patch(
            "recidiviz.case_triage.pathways.metric_cache.get_pathways_metric_redis",
            return_value=FakeRedis(),
        )
        self.redis_patcher.start()

        self.auth_patcher = mock.patch(
            f"{create_pathways_api_blueprint.__module__}.build_authorization_handler",
            return_value=self.mock_authorization_handler,
        )

        self.auth_patcher.start()

        self.test_app = Flask(__name__)
        register_error_handlers(self.test_app)
        self.test_app.register_blueprint(
            create_pathways_api_blueprint(), url_prefix="/pathways"
        )
        self.test_client = self.test_app.test_client()

    def tearDown(self) -> None:
        self.auth_patcher.stop()
        self.redis_patcher.stop()


@pytest.mark.uses_db
class TestPathwaysMetrics(PathwaysBlueprintTestCase):
    """Implements tests for the pathways routes."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        super().setUp()

        self.mock_authorization_handler.side_effect = (
            lambda: on_successful_authorization(
                {
                    f"{os.environ['AUTH0_CLAIM_NAMESPACE']}/app_metadata": {
                        "state_code": "recidiviz"
                    }
                }
            )
        )

        self.old_auth_claim_namespace = os.environ.get("AUTH0_CLAIM_NAMESPACE", None)
        os.environ["AUTH0_CLAIM_NAMESPACE"] = "https://recidiviz-test"

        self.database_key = SQLAlchemyDatabaseKey(SchemaType.PATHWAYS, db_name="us_tn")
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        self.count_by_dimension_metric_path = (
            "/pathways/US_TN/LibertyToPrisonTransitionsCount"
        )
        self.person_level_metric_path = (
            "/pathways/US_TN/PrisonToSupervisionTransitionsPersonLevel"
        )

        with SessionFactory.using_database(self.database_key) as session:
            for metric in load_metrics_fixture(LibertyToPrisonTransitions):
                session.add(LibertyToPrisonTransitions(**metric))
            for metric in load_metrics_fixture(PrisonToSupervisionTransitions):
                session.add(PrisonToSupervisionTransitions(**metric))

    def tearDown(self) -> None:
        super().tearDown()

        if self.old_auth_claim_namespace:
            os.environ["AUTH0_CLAIM_NAMESPACE"] = self.old_auth_claim_namespace
        else:
            os.unsetenv("AUTH0_CLAIM_NAMESPACE")
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_metrics_invalid_params(self) -> None:
        # Requesting fake metric
        response = self.test_client.get(
            "/pathways/US_TN/FakeMetric", headers={"Origin": "http://localhost:3000"}
        )
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.get_json(),
            (response.get_json() or {})
            | {"description": "FakeMetric is not enabled for US_TN"},
        )

        # Requesting real metric without group
        response = self.test_client.get(
            self.count_by_dimension_metric_path,
            headers={"Origin": "http://localhost:3000"},
        )
        self.assertEqual(
            HTTPStatus.BAD_REQUEST, response.status_code, response.get_json()
        )
        self.assertEqual(
            response.get_json(),
            (response.get_json() or {})
            | {"description": {"group": ["Missing data for required field."]}},
        )

        # Requesting real metric with fake grouping column
        response = self.test_client.get(
            self.count_by_dimension_metric_path,
            headers={"Origin": "http://localhost:3000"},
            query_string={"group": "fake"},
        )
        self.assertEqual(
            HTTPStatus.BAD_REQUEST, response.status_code, response.get_json()
        )
        self.assertEqual(
            response.get_json(),
            (response.get_json() or {})
            | {"description": {"group": ["Invalid enum value fake"]}},
        )

        # Requesting person-level metric with group
        response = self.test_client.get(
            self.person_level_metric_path,
            headers={"Origin": "http://localhost:3000"},
            query_string={"group": Dimension.AGE_GROUP.value},
        )
        self.assertEqual(
            HTTPStatus.BAD_REQUEST, response.status_code, response.get_json()
        )
        self.assertEqual(
            response.get_json(),
            (response.get_json() or {})
            | {"description": {"group": ["Unknown field."]}},
        )

    def test_metrics_base(self) -> None:
        response = self.test_client.get(
            self.count_by_dimension_metric_path,
            headers={"Origin": "http://localhost:3000"},
            query_string={"group": Dimension.AGE_GROUP.value},
        )
        self.assertEqual(HTTPStatus.OK, response.status_code, response.get_json())
        self.assertEqual(
            response.get_json(),
            [
                {"count": 3, "ageGroup": "20-25"},
                {"count": 1, "ageGroup": "30-34"},
                {"count": 4, "ageGroup": "60+"},
            ],
        )

        response = self.test_client.get(
            self.count_by_dimension_metric_path,
            headers={"Origin": "http://localhost:3000"},
            query_string={
                "group": Dimension.RACE.value,
                f"filters[{Dimension.RACE.value}]": "BLACK",
            },
        )
        self.assertEqual(response.status_code, HTTPStatus.OK, response.get_json())
        self.assertEqual(
            [
                {"count": 2, "race": "BLACK"},
            ],
            response.get_json(),
        )

    def test_person_level_metrics(self) -> None:
        self.maxDiff = None
        response = self.test_client.get(
            self.person_level_metric_path,
            headers={"Origin": "http://localhost:3000"},
        )
        self.assertEqual(HTTPStatus.OK, response.status_code, response.get_json())
        self.assertEqual(
            response.get_json(),
            [
                {
                    "ageGroup": "20-25",
                    "age": "22, 23",
                    "gender": "MALE",
                    "race": "WHITE",
                    "facility": "ABC, DEF",
                    "fullName": "TEST, PERSON",
                    "timePeriod": "months_0_6",
                    "stateId": "0001",
                },
                {
                    "ageGroup": "60+",
                    "age": "62",
                    "gender": "FEMALE",
                    "race": "BLACK",
                    "facility": "ABC",
                    "fullName": "FAKE, USER",
                    "timePeriod": "months_0_6",
                    "stateId": "0003",
                },
                {
                    "ageGroup": "60+",
                    "age": "64",
                    "gender": "MALE",
                    "race": "ASIAN",
                    "facility": "ABC",
                    "fullName": "EXAMPLE, INDIVIDUAL",
                    "timePeriod": "months_0_6",
                    "stateId": "0005",
                },
                {
                    "ageGroup": "60+",
                    "age": "63",
                    "gender": "MALE",
                    "race": "BLACK",
                    "facility": "DEF",
                    "fullName": "FAKE2, USER2",
                    "timePeriod": "months_0_6",
                    "stateId": "0004",
                },
                {
                    "ageGroup": "60+",
                    "age": "61",
                    "gender": "MALE",
                    "race": "WHITE",
                    "facility": "DEF",
                    "fullName": "TEST, PERSON2",
                    "timePeriod": "months_0_6",
                    "stateId": "0002",
                },
                {
                    "ageGroup": "60+",
                    "age": "65",
                    "gender": "MALE",
                    "race": "WHITE",
                    "facility": "GHI",
                    "fullName": "EXAMPLE, TIME",
                    "timePeriod": "months_25_60",
                    "stateId": "0006",
                },
            ],
        )

        response = self.test_client.get(
            self.person_level_metric_path,
            headers={"Origin": "http://localhost:3000"},
            query_string={
                f"filters[{Dimension.FACILITY.value}]": "DEF",
            },
        )
        self.assertEqual(response.status_code, HTTPStatus.OK, response.get_json())
        self.assertEqual(
            [
                {
                    "ageGroup": "20-25",
                    "age": "23",
                    "gender": "MALE",
                    "race": "WHITE",
                    "facility": "DEF",
                    "fullName": "TEST, PERSON",
                    "timePeriod": "months_0_6",
                    "stateId": "0001",
                },
                {
                    "ageGroup": "60+",
                    "age": "63",
                    "gender": "MALE",
                    "race": "BLACK",
                    "facility": "DEF",
                    "fullName": "FAKE2, USER2",
                    "timePeriod": "months_0_6",
                    "stateId": "0004",
                },
                {
                    "ageGroup": "60+",
                    "age": "61",
                    "gender": "MALE",
                    "race": "WHITE",
                    "facility": "DEF",
                    "fullName": "TEST, PERSON2",
                    "timePeriod": "months_0_6",
                    "stateId": "0002",
                },
            ],
            response.get_json(),
        )

    def test_multiple_filters(self) -> None:
        # Filters for the same attribute are combined using boolean OR
        # Filters across attributes are combined using AND
        response = self.test_client.get(
            self.count_by_dimension_metric_path,
            headers={"Origin": "http://localhost:3000"},
            query_string=f"group={Dimension.RACE.value}"
            f"&filters[{Dimension.RACE.value}]=BLACK"
            f"&filters[{Dimension.RACE.value}]=WHITE"
            f"&filters[{Dimension.GENDER.value}]=MALE",
        )

        self.assertEqual(response.status_code, HTTPStatus.OK, response.get_json())
        self.assertEqual(
            [
                {"count": 1, "race": "BLACK"},
                {"count": 4, "race": "WHITE"},
            ],
            response.get_json(),
        )

    def test_metrics_time_period(self) -> None:
        response = self.test_client.get(
            self.count_by_dimension_metric_path,
            headers={"Origin": "http://localhost:3000"},
            query_string={
                "group": Dimension.RACE.value,
                "time_period": TimePeriod.MONTHS_0_6.value,
            },
        )
        self.assertEqual(HTTPStatus.OK, response.status_code, response.get_json())
        self.assertEqual(
            [
                {"count": 2, "race": "ASIAN"},
                {"count": 2, "race": "BLACK"},
                {"count": 3, "race": "WHITE"},
            ],
            response.get_json(),
        )

        response = self.test_client.get(
            self.count_by_dimension_metric_path,
            headers={"Origin": "http://localhost:3000"},
            query_string={
                "group": Dimension.RACE.value,
                "filters[time_period]": TimePeriod.MONTHS_25_60.value,
            },
        )
        self.assertEqual(HTTPStatus.OK, response.status_code, response.get_json())
        self.assertEqual(
            [
                {"count": 2, "race": "ASIAN"},
                {"count": 2, "race": "BLACK"},
                {"count": 4, "race": "WHITE"},
            ],
            response.get_json(),
        )


def make_cors_test(
    request_origin: str,
    request_method: str = "OPTIONS",
    expected_headers: Optional[Dict[str, str]] = None,
    expected_status: int = 200,
) -> Callable:
    def inner(self: PathwaysBlueprintTestCase) -> None:
        response = self.test_client.open(
            method=request_method,
            path="/pathways/US_TN/LibertyToPrisonTransitionsCount",
            headers={"Origin": request_origin},
        )

        self.assertEqual(expected_status, response.status_code, response.get_json())

        for header, value in (expected_headers or {}).items():
            self.assertEqual(value, response.headers[header])

    return inner


class TestPathwaysCORS(PathwaysBlueprintTestCase):
    """Tests various CORS scenarios"""

    test_localhost_is_allowed = make_cors_test(
        request_origin="http://localhost:3000",
        expected_headers={
            "Access-Control-Allow-Origin": "http://localhost:3000",
            "Access-Control-Allow-Headers": "authorization, sentry-trace",
            "Access-Control-Max-Age": "7200",
            "Vary": "Origin",
        },
    )

    test_staging_is_allowed = make_cors_test(
        request_origin="https://dashboard-staging.recidiviz.org",
        expected_headers={
            "Access-Control-Allow-Origin": "https://dashboard-staging.recidiviz.org",
            "Access-Control-Allow-Headers": "authorization, sentry-trace",
            "Vary": "Origin",
        },
    )

    test_prod_is_allowed = make_cors_test(
        request_origin="https://dashboard.recidiviz.org",
        expected_headers={
            "Access-Control-Allow-Origin": "https://dashboard.recidiviz.org",
            "Access-Control-Allow-Headers": "authorization, sentry-trace",
            "Vary": "Origin",
        },
    )

    test_preview_apps_are_allowed = make_cors_test(
        request_origin="https://recidiviz-dashboard-stag-e1108--preview-999a999.web.app",
        expected_headers={
            "Access-Control-Allow-Origin": "https://recidiviz-dashboard-stag-e1108--preview-999a999.web.app",
            "Access-Control-Allow-Headers": "authorization, sentry-trace",
            "Vary": "Origin",
        },
    )
    test_cors_headers_sent_on_all_responses = make_cors_test(
        request_origin="https://recidiviz-dashboard-stag-e1108--preview-999a999.web.app",
        request_method="GET",
        expected_headers={
            "Access-Control-Allow-Origin": "https://recidiviz-dashboard-stag-e1108--preview-999a999.web.app",
            "Access-Control-Allow-Headers": "authorization, sentry-trace",
            "Vary": "Origin",
        },
        expected_status=HTTPStatus.BAD_REQUEST,
    )

    test_spoof_preview_is_disallowed = make_cors_test(
        request_origin="https://recidiviz-dashboard-stag-e1108--officer-7tjx0jmi.fake.web.app",
        expected_status=HTTPStatus.FORBIDDEN,
    )

    test_spoof_preview_2_is_disallowed = make_cors_test(
        request_origin="https://preview.hacked.com/web.app",
        expected_status=HTTPStatus.FORBIDDEN,
    )

    test_wrong_localhost_port_is_disallowed = make_cors_test(
        request_origin="http://localhost:9999",
        expected_status=HTTPStatus.FORBIDDEN,
    )


class TestAuthorizationIntegration(PathwaysBlueprintTestCase):
    """Tests that routes require authorization"""

    def test_all_non_options_request_require_authorization(self) -> None:
        self.test_client.get(
            "/pathways/US_TN/LibertyToPrisonTransitionsCount",
            query_string={"group": Dimension.RACE.value},
        )

        self.mock_authorization_handler.assert_called()

    def test_options_routes(self) -> None:
        self.test_client.options("/pathways/US_TN/LibertyToPrisonTransitionsCount")

        self.mock_authorization_handler.assert_not_called()
