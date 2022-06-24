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
"""Implements tests for the Justice Counts Control Panel backend API."""
import datetime
from http import HTTPStatus

import pytest
from flask import g, session
from freezegun import freeze_time
from sqlalchemy.engine import Engine

from recidiviz.common.constants.justice_counts import ContextKey
from recidiviz.justice_counts.control_panel.config import Config
from recidiviz.justice_counts.control_panel.constants import ControlPanelPermission
from recidiviz.justice_counts.control_panel.server import create_app
from recidiviz.justice_counts.control_panel.user_context import UserContext
from recidiviz.justice_counts.dimensions.law_enforcement import (
    CallType,
    SheriffBudgetType,
)
from recidiviz.justice_counts.metrics import law_enforcement
from recidiviz.justice_counts.metrics.metric_definition import CallsRespondedOptions
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts.schema import (
    Report,
    ReportingFrequency,
    ReportStatus,
    Source,
    UserAccount,
)
from recidiviz.tests.auth.utils import get_test_auth0_config
from recidiviz.tests.justice_counts.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils.auth.auth0 import passthrough_authorization_decorator
from recidiviz.utils.types import assert_type


@pytest.mark.uses_db
class TestJusticeCountsControlPanelAPI(JusticeCountsDatabaseTestCase):
    """Implements tests for the Justice Counts Control Panel backend API."""

    def setUp(self) -> None:
        test_config = Config(
            DB_URL=local_postgres_helpers.on_disk_postgres_db_url(),
            WTF_CSRF_ENABLED=False,
            AUTH_DECORATOR=passthrough_authorization_decorator(),
            AUTH0_CONFIGURATION=get_test_auth0_config(),
        )
        self.app = create_app(config=test_config)
        self.client = self.app.test_client()
        self.app.secret_key = "NOT A SECRET"
        # `flask_sqlalchemy_session` sets the `scoped_session` attribute on the app,
        # even though this is not specified in the types for `app`.
        self.session = self.app.scoped_session  # type: ignore[attr-defined]
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        super().setUp()

    def get_engine(self) -> Engine:
        return self.session.get_bind()

    def test_logout(self) -> None:
        with self.app.test_client() as client:
            with client.session_transaction() as sess:
                sess["session_data"] = {"foo": "bar"}

            response = client.post("/auth/logout")
            self.assertEqual(response.status_code, 200)
            with self.app.test_request_context():
                self.assertEqual(0, len(session.keys()))

    def test_auth0_config(self) -> None:
        response = self.client.get("/auth0_public_config.js")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.data,
            b"window.AUTH0_CONFIG = {'audience': 'http://localhost', 'clientId': 'test_client_id', 'domain': 'auth0.localhost'};",
        )

    def test_get_all_reports(self) -> None:
        user_A = self.test_schema_objects.test_user_A
        report = self.test_schema_objects.test_report_monthly
        self.session.add_all([report, user_A])
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user_A.auth0_user_id, agency_ids=[report.source_id]
            )
            response = self.client.get(f"/api/reports?agency_id={report.source_id}")

        self.assertEqual(response.status_code, 200)
        response_list = assert_type(response.json, list)
        self.assertEqual(len(response_list), 1)
        response_json = assert_type(response_list[0], dict)
        self.assertEqual(response_json["editors"], [])
        self.assertEqual(response_json["frequency"], ReportingFrequency.MONTHLY.value)
        self.assertEqual(response_json["last_modified_at"], None)
        self.assertEqual(response_json["month"], 6)
        self.assertEqual(response_json["status"], ReportStatus.NOT_STARTED.value)
        self.assertEqual(response_json["year"], 2022)

    def test_get_report_metrics(self) -> None:
        user_A = self.test_schema_objects.test_user_A
        report = self.test_schema_objects.test_report_monthly
        self.session.add_all([report, user_A])
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user_A.auth0_user_id, agency_ids=[report.source_id]
            )
            response = self.client.get(f"/api/reports/{report.id}")

        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertEqual(response_json["editors"], [])
        self.assertEqual(response_json["frequency"], ReportingFrequency.MONTHLY.value)
        self.assertEqual(response_json["last_modified_at"], None)
        self.assertEqual(response_json["month"], 6)
        self.assertEqual(response_json["status"], ReportStatus.NOT_STARTED.value)
        self.assertEqual(response_json["year"], 2022)
        metrics = assert_type(response_json["metrics"], list)
        self.assertEqual(len(metrics), 3)
        self.assertEqual(metrics[0]["key"], law_enforcement.calls_for_service.key)
        self.assertEqual(metrics[1]["key"], law_enforcement.reported_crime.key)
        self.assertEqual(metrics[2]["key"], law_enforcement.total_arrests.key)

    def test_create_report_invalid_permissions(self) -> None:
        user = self.test_schema_objects.test_user_A
        agency = self.test_schema_objects.test_agency_A
        self.session.add_all([agency, user])
        self.session.commit()
        month = 3
        year = 2022
        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user.auth0_user_id)
            response = self.client.post(
                "/api/reports",
                json={
                    "agency_id": agency.id,
                    "month": month,
                    "year": year,
                    "frequency": ReportingFrequency.MONTHLY.value,
                },
            )
        self.assertEqual(response.status_code, 401)

    def test_create_report(self) -> None:
        user = self.test_schema_objects.test_user_A
        agency = self.test_schema_objects.test_agency_A
        self.session.add_all([agency, user])
        self.session.commit()
        month = 3
        year = 2022
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user.auth0_user_id,
                permissions=[ControlPanelPermission.RECIDIVIZ_ADMIN.value],
            )

            response = self.client.post(
                "/api/reports",
                json={
                    "agency_id": agency.id,
                    "month": month,
                    "year": year,
                    "frequency": ReportingFrequency.MONTHLY.value,
                },
            )
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertEqual(response_json["editors"], ["Jane Doe"])
        self.assertEqual(response_json["frequency"], ReportingFrequency.MONTHLY.value)
        self.assertIsNotNone(response_json["last_modified_at"])
        self.assertEqual(response_json["month"], 3)
        self.assertEqual(response_json["status"], ReportStatus.NOT_STARTED.value)
        self.assertEqual(response_json["year"], 2022)

    def test_cannot_get_another_users_reports(self) -> None:
        user_B = self.test_schema_objects.test_user_B
        report = self.test_schema_objects.test_report_monthly
        self.session.add_all([report, user_B])
        self.session.commit()

        # user belongs to the wrong agency
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user_B.auth0_user_id, agency_ids=[report.source_id + 1]
            )
            response = self.client.get(f"/api/reports?agency_id={report.source_id}")

        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

        # user makes a request with no agencies, but belongs to an agency
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user_B.auth0_user_id, agency_ids=[report.source_id]
            )
            response = self.client.get("/api/reports")

        self.assertEqual(response.status_code, HTTPStatus.OK)

        # user makes a request with no agencies, but does not belong to an agency
        with self.app.test_request_context():
            g.user_context = UserContext(auth0_user_id=user_B.auth0_user_id)
            response = self.client.get("/api/reports")

        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

    def test_create_user_if_necessary(self) -> None:
        name = self.test_schema_objects.test_user_A.name
        auth0_user_id = self.test_schema_objects.test_user_A.auth0_user_id
        agency = self.test_schema_objects.test_agency_A
        self.session.add(agency)
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=auth0_user_id,
                agency_ids=[agency.id],
            )
            user_response = self.client.post(
                "/api/users",
                json={
                    "name": name,
                },
            )

        self.assertEqual(user_response.status_code, 200)
        response_json = assert_type(user_response.json, dict)
        self.assertEqual(
            response_json["agencies"],
            [
                {
                    "fips_county_code": agency.fips_county_code,
                    "id": agency.id,
                    "name": agency.name,
                    "system": agency.system.value,
                    "systems": [],
                    "state_code": agency.state_code,
                }
            ],
        )
        self.assertEqual(response_json["permissions"], [])
        # New user is added to the database
        db_item = self.session.query(UserAccount).one()
        self.assertEqual(db_item.name, name)
        self.assertEqual(db_item.auth0_user_id, auth0_user_id)

    def test_update_report(self) -> None:
        update_datetime = datetime.datetime(2022, 2, 1, 0, 0, 0)
        with freeze_time(update_datetime):
            report = self.test_schema_objects.test_report_monthly
            user = self.test_schema_objects.test_user_A
            self.session.add_all([user, report])
            self.session.commit()
            with self.app.test_request_context():
                user_account = UserAccountInterface.get_user_by_auth0_user_id(
                    session=self.session, auth0_user_id=user.auth0_user_id
                )
                g.user_context = UserContext(
                    auth0_user_id=user.auth0_user_id,
                    user_account=user_account,
                    agency_ids=[report.source_id],
                )
                value = 100
                endpoint = f"/api/reports/{report.id}"
                response = self.client.post(
                    endpoint,
                    json={
                        "status": "DRAFT",
                        "metrics": [
                            {
                                "key": law_enforcement.calls_for_service.key,
                                "value": value,
                            }
                        ],
                    },
                )
                self.assertEqual(response.status_code, 200)
                report = self.session.query(Report).one_or_none()
                self.assertEqual(report.status, ReportStatus.DRAFT)
                self.assertEqual(report.last_modified_at, update_datetime)
                self.assertEqual(report.datapoints[0].get_value(), value)
                self.assertEqual(
                    report.modified_by,
                    [user_account.id],
                )
                response = self.client.post(
                    endpoint,
                    json={
                        "status": "PUBLISHED",
                        "metrics": [
                            {
                                "key": law_enforcement.calls_for_service.key,
                                "value": value + 10,
                                "disaggregations": [
                                    {
                                        "key": CallType.dimension_identifier(),
                                        "dimensions": [
                                            {
                                                "key": CallType.EMERGENCY.value,
                                                "value": value,
                                            },
                                            {
                                                "key": CallType.NON_EMERGENCY.value,
                                                "value": 10,
                                            },
                                            {
                                                "key": CallType.UNKNOWN.value,
                                                "value": None,
                                            },
                                        ],
                                    }
                                ],
                                "contexts": [
                                    {
                                        "key": ContextKey.ALL_CALLS_OR_CALLS_RESPONDED.value,
                                        "value": CallsRespondedOptions.ALL_CALLS.value,
                                    },
                                ],
                            },
                            {
                                "key": law_enforcement.annual_budget.key,
                                "value": 2000000,
                                "disaggregations": [
                                    {
                                        "key": SheriffBudgetType.dimension_identifier(),
                                        "dimensions": [
                                            {
                                                "key": SheriffBudgetType.DETENTION.value,
                                                "value": 500000,
                                            },
                                            {
                                                "key": SheriffBudgetType.PATROL.value,
                                                "value": 1500000,
                                            },
                                        ],
                                    }
                                ],
                                "contexts": [
                                    {
                                        "key": ContextKey.PRIMARY_FUNDING_SOURCE.value,
                                        "value": "test context",
                                    },
                                ],
                            },
                        ],
                    },
                )
                self.assertEqual(response.status_code, 200)
                report = self.session.query(Report).one_or_none()
                self.assertEqual(report.status, ReportStatus.PUBLISHED)
                datapoints = report.datapoints
                self.assertEqual(report.datapoints[0].get_value(), 110)
                # Empty CallType dimension values
                self.assertEqual(datapoints[1].get_value(), value)
                self.assertEqual(datapoints[2].get_value(), 10)
                self.assertEqual(datapoints[3].get_value(), None)
                self.assertEqual(
                    datapoints[4].get_value(), CallsRespondedOptions.ALL_CALLS.value
                )
                # Empty contexts from calls_for_service metric
                self.assertEqual(datapoints[5].get_value(), None)
                self.assertEqual(datapoints[6].get_value(), None)

                self.assertEqual(datapoints[7].get_value(), 2000000)
                self.assertEqual(report.datapoints[8].get_value(), 1500000)
                self.assertEqual(report.datapoints[9].get_value(), 500000)
                self.assertEqual(report.datapoints[10].get_value(), "test context")
                self.assertEqual(
                    report.datapoints[11].get_value(), None
                )  # empty context from calls from budget metric

    def test_user_permissions(self) -> None:
        user_account = self.test_schema_objects.test_user_A
        self.session.add(user_account)
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user_account.auth0_user_id,
                permissions=[ControlPanelPermission.RECIDIVIZ_ADMIN.value],
            )
            user_response = self.client.post(
                "/api/users",
                json={
                    "name": user_account.name,
                    "auth0_user_id": user_account.auth0_user_id,
                },
            )
        self.assertEqual(user_response.status_code, 200)
        self.assertIsNotNone(user_response.json)
        self.assertEqual(
            user_response.json["permissions"] if user_response.json else [],
            [ControlPanelPermission.RECIDIVIZ_ADMIN.value],
        )

    def test_session(self) -> None:
        # Add data
        name = "Agency Alpha"
        self.session.add(Source(name=name))
        self.session.commit()

        # Query data
        source = self.session.query(Source).one_or_none()

        self.assertEqual(source.name, name)
