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
import os
from http import HTTPStatus
from pathlib import Path
from unittest import mock

import pytest
from flask import g, session
from freezegun import freeze_time
from mock import patch
from more_itertools import one
from sqlalchemy.engine import Engine

from recidiviz.auth.auth0_client import JusticeCountsAuth0AppMetadata
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.justice_counts import ContextKey
from recidiviz.justice_counts.bulk_upload.bulk_upload import BulkUploader
from recidiviz.justice_counts.control_panel.config import Config
from recidiviz.justice_counts.control_panel.constants import ControlPanelPermission
from recidiviz.justice_counts.control_panel.server import create_app
from recidiviz.justice_counts.control_panel.user_context import UserContext
from recidiviz.justice_counts.dimensions.law_enforcement import CallType
from recidiviz.justice_counts.metrics import law_enforcement
from recidiviz.justice_counts.metrics.metric_definition import CallsRespondedOptions
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    Agency,
    Datapoint,
    Report,
    ReportingFrequency,
    ReportStatus,
    Source,
    Spreadsheet,
    SpreadsheetStatus,
    System,
    UserAccount,
)
from recidiviz.tests.auth.utils import get_test_auth0_config
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
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
        self.now_time = datetime.datetime(
            2022, 2, 15, 0, 0, 0, 0, datetime.timezone.utc
        )
        self.freezer = freeze_time(self.now_time)
        self.freezer.start()
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        mock_project_id_fn = self.project_id_patcher.start()
        mock_project_id_fn.return_value = "justice-counts"

        self.bulk_upload_test_files = Path(
            "recidiviz/tests/justice_counts/bulk_upload/bulk_upload_fixtures"
        )
        self.client_patcher = patch("recidiviz.auth.auth0_client.Auth0")
        self.test_auth0_client = self.client_patcher.start().return_value
        self.secrets_patcher = patch("recidiviz.auth.auth0_client.secrets")
        self.mock_secrets = self.secrets_patcher.start()
        self.secrets = {
            "auth0_api_domain": "fake_api_domain",
            "auth0_api_client_id": "fake client id",
            "auth0_api_client_secret": "fake client secret",
        }
        self.mock_secrets.get_secret.side_effect = self.secrets.get
        test_config = Config(
            DB_URL=local_postgres_helpers.on_disk_postgres_db_url(),
            WTF_CSRF_ENABLED=False,
            AUTH_DECORATOR=passthrough_authorization_decorator(),
            AUTH0_CONFIGURATION=get_test_auth0_config(),
            AUTH0_CLIENT=self.test_auth0_client,
            SEGMENT_KEY="fake_segment_key",
        )
        self.fs = FakeGCSFileSystem()
        self.fs_patcher = mock.patch.object(GcsfsFactory, "build", return_value=self.fs)
        self.fs_patcher.start()

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
        response = self.client.get("/app_public_config.js")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.data,
            b"window.APP_CONFIG = {'audience': 'http://localhost', 'clientId': 'test_client_id', 'domain': 'auth0.localhost'}; window.SEGMENT_KEY = 'fake_segment_key';",
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
            response = self.client.get(f"/api/agencies/{report.source_id}/reports")

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

    def test_get_agency_metrics(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_A,
            ]
        )
        self.session.commit()
        agency = self.session.query(Agency).one()
        agency_datapoints = self.test_schema_objects.get_test_agency_datapoints(
            agency_id=agency.id
        )
        self.session.add_all(agency_datapoints)
        self.session.commit()
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
                agency_ids=[agency.id],
            )
            response = self.client.get(f"/api/agencies/{agency.id}/metrics")

        self.assertEqual(response.status_code, 200)
        metrics = assert_type(response.json, list)
        self.assertEqual(len(metrics), 7)
        # Annual Budget metric is turned off
        self.assertEqual(metrics[0]["key"], law_enforcement.annual_budget.key)
        self.assertEqual(metrics[0]["enabled"], False)
        # Police Officer metric has a prefilled context (ADDITIONAL_CONTEXT)
        self.assertEqual(metrics[1]["key"], law_enforcement.police_officers.key)
        self.assertEqual(
            metrics[1]["contexts"],
            [
                {
                    "display_name": "Please provide the land size (area) of the jurisdiction in "
                    "square miles.",
                    "key": "JURISDICTION_AREA",
                    "multiple_choice_options": [],
                    "reporting_note": None,
                    "required": False,
                    "type": "NUMBER",
                    "value": None,
                },
                {
                    "display_name": "Please provide additional context.",
                    "key": "ADDITIONAL_CONTEXT",
                    "multiple_choice_options": [],
                    "reporting_note": None,
                    "required": False,
                    "type": "TEXT",
                    "value": "this additional context provides contexts",
                },
            ],
        )
        # Calls for service metric is enabled but CallType disaggregation is disabled
        self.assertEqual(metrics[2]["key"], law_enforcement.calls_for_service.key)
        self.assertEqual(metrics[2]["enabled"], True)
        self.assertEqual(
            metrics[2]["disaggregations"],
            [
                {
                    "key": CallType.dimension_identifier(),
                    "enabled": False,
                    "required": True,
                    "helper_text": None,
                    "should_sum_to_total": False,
                    "display_name": CallType.display_name(),
                    "dimensions": [
                        {
                            "enabled": False,
                            "label": CallType.EMERGENCY.value,
                            "key": CallType.EMERGENCY.value,
                        },
                        {
                            "enabled": False,
                            "label": CallType.NON_EMERGENCY.value,
                            "key": CallType.NON_EMERGENCY.value,
                        },
                        {
                            "enabled": False,
                            "label": CallType.UNKNOWN.value,
                            "key": CallType.UNKNOWN.value,
                        },
                    ],
                }
            ],
        )
        self.assertEqual(metrics[3]["key"], law_enforcement.reported_crime.key)
        self.assertEqual(metrics[4]["key"], law_enforcement.total_arrests.key)
        self.assertEqual(
            metrics[5]["key"], law_enforcement.officer_use_of_force_incidents.key
        )
        self.assertEqual(
            metrics[6]["key"], law_enforcement.civilian_complaints_sustained.key
        )

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
            response = self.client.get(f"/api/agencies/{report.source_id}/reports")

        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

        # user makes a request with no agencies, but belongs to an agency
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user_B.auth0_user_id, agency_ids=[report.source_id]
            )
            response = self.client.get(f"/api/agencies/{report.source_id}/reports")

        self.assertEqual(response.status_code, HTTPStatus.OK)

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
            user_response = self.client.put(
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
                    "systems": agency.systems,
                    "state_code": agency.state_code,
                }
            ],
        )
        self.assertEqual(response_json["permissions"], [])
        # New user is added to the database
        db_item = self.session.query(UserAccount).one()
        self.assertEqual(db_item.name, name)
        self.assertEqual(db_item.auth0_user_id, auth0_user_id)

    def test_get_all_agencies_for_recidiviz_staff(self) -> None:
        auth0_user_id = self.test_schema_objects.test_user_A.auth0_user_id
        agency_A = self.test_schema_objects.test_agency_A
        agency_B = self.test_schema_objects.test_agency_B
        agency_C = self.test_schema_objects.test_agency_C
        self.session.add_all([agency_A, agency_B, agency_C])
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=auth0_user_id,
                agency_ids=[agency_A.id],
                permissions=[ControlPanelPermission.RECIDIVIZ_ADMIN.value],
            )
            user_response = self.client.put("/api/users", json={})

        self.assertEqual(user_response.status_code, 200)
        response_json = assert_type(user_response.json, dict)
        self.assertEqual(
            response_json["agencies"],
            [
                {
                    "fips_county_code": agency_A.fips_county_code,
                    "id": agency_A.id,
                    "name": agency_A.name,
                    "systems": agency_A.systems,
                    "state_code": agency_A.state_code,
                },
                {
                    "fips_county_code": agency_B.fips_county_code,
                    "id": agency_B.id,
                    "name": agency_B.name,
                    "systems": agency_B.systems,
                    "state_code": agency_B.state_code,
                },
                {
                    "fips_county_code": agency_C.fips_county_code,
                    "id": agency_C.id,
                    "name": agency_C.name,
                    "systems": agency_C.systems,
                    "state_code": agency_C.state_code,
                },
            ],
        )

    def test_update_user_name_and_email(self) -> None:
        new_email_address = "newuser@fake.com"
        new_name = "NEW NAME"
        auth0_user = self.test_schema_objects.test_auth0_user
        db_user = self.test_schema_objects.test_user_A
        self.session.add_all([self.test_schema_objects.test_agency_A, db_user])
        self.session.commit()
        agency = self.session.query(Agency).one_or_none()
        auth0_user["name"] = new_name
        auth0_user["email"] = new_email_address
        auth0_user["app_metadata"] = JusticeCountsAuth0AppMetadata(
            agency_ids=[agency.id], has_seen_onboarding={}
        )
        self.test_auth0_client.update_user.return_value = auth0_user
        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=auth0_user["user_id"], user_account=db_user
            )
            response = self.client.patch(
                "/api/users",
                json={
                    "name": new_name,
                    "email": new_email_address,
                    "auth0_user_id": auth0_user.get("id"),
                },
            )
        self.assertEqual(response.status_code, 200)
        self.test_auth0_client.update_user.assert_called_once_with(
            user_id=auth0_user.get("user_id"),
            name=new_name,
            email=new_email_address,
            email_verified=False,
        )
        self.test_auth0_client.send_verification_email.assert_called_once_with(
            user_id=auth0_user.get("user_id")
        )
        db_user = self.session.query(UserAccount).one()
        self.assertEqual(db_user.name, new_name)

    def test_update_report(self) -> None:
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
            response = self.client.patch(
                endpoint,
                json={
                    "status": "DRAFT",
                    "time_loaded": datetime.datetime.now(
                        tz=datetime.timezone.utc
                    ).timestamp(),
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
            self.assertEqual(
                report.last_modified_at.timestamp(), self.now_time.timestamp()
            )
            self.assertEqual(report.datapoints[0].get_value(), value)
            self.assertEqual(
                report.modified_by,
                [user_account.id],
            )
            response = self.client.patch(
                endpoint,
                json={
                    "status": "PUBLISHED",
                    "time_loaded": datetime.datetime.now(
                        tz=datetime.timezone.utc
                    ).timestamp(),
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
            self.assertEqual(len(datapoints), 7)
            self.assertEqual(report.datapoints[0].get_value(), 110)
            # Empty CallType dimension values
            self.assertEqual(datapoints[1].get_value(), value)
            self.assertEqual(datapoints[2].get_value(), 10)
            self.assertEqual(datapoints[3].get_value(), None)
            self.assertEqual(
                datapoints[4].get_value(), CallsRespondedOptions.ALL_CALLS.value
            )
            self.assertEqual(datapoints[5].get_value(), 2000000)

    def test_update_report_version_conflict(self) -> None:
        report = self.test_schema_objects.test_report_monthly
        user = self.test_schema_objects.test_user_A
        # report was modified on Feb 2 by someone else
        report.last_modified_at = datetime.datetime(2022, 2, 2, 0, 0, 0)
        report.modified_by = [10]
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
            endpoint = f"/api/reports/{report.id}"
            response = self.client.patch(
                endpoint,
                json={
                    "status": "DRAFT",
                    # client loaded the report on Feb 1
                    # so we should reject the update!
                    "time_loaded": datetime.datetime(2022, 2, 1, 0, 0, 0).timestamp(),
                    "metrics": [
                        {
                            "key": law_enforcement.calls_for_service.key,
                            "value": 100,
                        }
                    ],
                },
            )
            self.assertEqual(response.status_code, 400)

    def test_user_permissions(self) -> None:
        user_account = self.test_schema_objects.test_user_A
        self.session.add(user_account)
        self.session.commit()

        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user_account.auth0_user_id,
                permissions=[ControlPanelPermission.RECIDIVIZ_ADMIN.value],
            )
            user_response = self.client.put(
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

    def test_update_metric_settings(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_A,
            ]
        )
        self.session.commit()
        agency = self.session.query(Agency).one_or_none()
        request_body = self.test_schema_objects.get_agency_datapoints_request(
            agency_id=agency.id
        )
        with self.app.test_request_context():
            user_account = UserAccountInterface.get_user_by_auth0_user_id(
                session=self.session,
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )
            g.user_context = UserContext(
                user_account=user_account,
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
                agency_ids=[agency.id],
            )
            response = self.client.put(
                f"/api/agencies/{agency.id}/metrics",
                json=request_body,
            )
        self.assertEqual(response.status_code, 200)
        agency_datapoints = self.session.query(Datapoint).all()
        # 1 metric datapoint, 2 breakdown datapoints, and 1 and one context datapoint
        self.assertEqual(len(agency_datapoints), 4)

    def test_upload_spreadsheet_wrong_filetype(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_A,
            ]
        )
        self.session.commit()
        agency = self.session.query(Agency).one_or_none()
        with self.app.test_request_context():
            user_account = UserAccountInterface.get_user_by_auth0_user_id(
                session=self.session,
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )
            g.user_context = UserContext(
                user_account=user_account,
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
                agency_ids=[agency.id],
            )

            response = self.client.post(
                "/api/spreadsheets",
                data={
                    "agency_id": agency.id,
                    "system": System.LAW_ENFORCEMENT.value,
                    "file": (
                        self.bulk_upload_test_files / "law_enforcement/arrests.csv"
                    ).open("rb"),
                },
            )
            self.assertEqual(response.status_code, 400)

    def test_upload_spreadsheet(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_A,
            ]
        )
        self.session.commit()
        agency = self.session.query(Agency).one_or_none()
        with self.app.test_request_context():
            user_account = UserAccountInterface.get_user_by_auth0_user_id(
                session=self.session,
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )
            g.user_context = UserContext(
                user_account=user_account,
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
                agency_ids=[agency.id],
            )

            response = self.client.post(
                "/api/spreadsheets",
                data={
                    "agency_id": agency.id,
                    "system": System.LAW_ENFORCEMENT.value,
                    "file": (
                        self.bulk_upload_test_files
                        / "law_enforcement/law_enforcement_metrics.xlsx"
                    ).open("rb"),
                },
            )
            self.assertEqual(response.status_code, 200)
            response_dict = assert_type(response.json, dict)
            self.assertEqual(response_dict.get("name"), "law_enforcement_metrics.xlsx")
            self.assertEqual(
                response_dict.get("uploaded_at"),
                datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000,
            )
            self.assertEqual(
                response_dict.get("uploaded_by"),
                self.test_schema_objects.test_user_A.name,
            )
            self.assertEqual(response_dict.get("ingested_at"), None)
            self.assertEqual(response_dict.get("status"), "UPLOADED")
            self.assertEqual(response_dict.get("system"), "LAW_ENFORCEMENT")
            spreadsheet = self.session.query(Spreadsheet).one()
            self.assertEqual(spreadsheet.system, System.LAW_ENFORCEMENT)
            self.assertEqual(
                spreadsheet.uploaded_by,
                self.test_schema_objects.test_user_A.auth0_user_id,
            )
            self.assertEqual(
                spreadsheet.ingested_at,
                None,
            )
            self.assertEqual(spreadsheet.original_name, "law_enforcement_metrics.xlsx")
            standardized_name = f"{agency.id}:LAW_ENFORCEMENT:{datetime.datetime.now(tz=datetime.timezone.utc).timestamp()}.xlsx"
            self.assertEqual(
                spreadsheet.standardized_name,
                standardized_name,
            )
            self.assertEqual(1, len(self.fs.uploaded_paths))
            path = one(self.fs.uploaded_paths)
            self.assertEqual(
                GcsfsFilePath(
                    bucket_name="justice-counts-justice-counts-control-panel-ingest",
                    blob_name=standardized_name,
                ),
                path,
            )

    def test_upload_and_ingest_spreadsheet(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_A,
            ]
        )
        self.session.commit()
        agency = self.session.query(Agency).one_or_none()
        with self.app.test_request_context():
            user_account = UserAccountInterface.get_user_by_auth0_user_id(
                session=self.session,
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )
            g.user_context = UserContext(
                user_account=user_account,
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
                agency_ids=[agency.id],
                permissions=[ControlPanelPermission.RECIDIVIZ_ADMIN.value],
            )

            response = self.client.post(
                "/api/spreadsheets",
                data={
                    "agency_id": agency.id,
                    "system": System.LAW_ENFORCEMENT.value,
                    "ingest_on_upload": True,
                    "file": (
                        self.bulk_upload_test_files
                        / "law_enforcement/law_enforcement_metrics.xlsx"
                    ).open("rb"),
                },
            )
            self.assertEqual(response.status_code, 200)
            response_dict = assert_type(response.json, dict)
            self.assertEqual(response_dict.get("name"), "law_enforcement_metrics.xlsx")
            self.assertEqual(
                response_dict.get("uploaded_at"),
                datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000,
            )
            self.assertEqual(
                response_dict.get("uploaded_by"),
                self.test_schema_objects.test_user_A.name,
            )
            self.assertEqual(
                response_dict.get("ingested_at"), self.now_time.timestamp() * 1000
            )
            self.assertEqual(response_dict.get("status"), "INGESTED")
            self.assertEqual(response_dict.get("system"), "LAW_ENFORCEMENT")
            spreadsheet = self.session.query(Spreadsheet).one()
            self.assertEqual(spreadsheet.system, System.LAW_ENFORCEMENT)
            self.assertEqual(
                spreadsheet.uploaded_by,
                self.test_schema_objects.test_user_A.auth0_user_id,
            )
            self.assertEqual(
                spreadsheet.ingested_by,
                self.test_schema_objects.test_user_A.auth0_user_id,
            )
            self.assertEqual(
                spreadsheet.ingested_at.timestamp(),
                self.now_time.timestamp(),
            )
            self.assertEqual(spreadsheet.original_name, "law_enforcement_metrics.xlsx")
            standardized_name = f"{agency.id}:LAW_ENFORCEMENT:{datetime.datetime.now(tz=datetime.timezone.utc).timestamp()}.xlsx"
            self.assertEqual(
                spreadsheet.standardized_name,
                standardized_name,
            )
            self.assertEqual(1, len(self.fs.uploaded_paths))
            path = one(self.fs.uploaded_paths)
            self.assertEqual(
                GcsfsFilePath(
                    bucket_name="justice-counts-justice-counts-control-panel-ingest",
                    blob_name=standardized_name,
                ),
                path,
            )
            reports = ReportInterface.get_reports_by_agency_id(
                session=self.session,
                agency_id=agency.id,
                include_datapoints=True,
            )
            reports_by_instance = {report.instance: report for report in reports}
            self.assertEqual(
                set(reports_by_instance.keys()),
                {
                    "2021 Annual Metrics",
                    "2022 Annual Metrics",
                    "01 2021 Metrics",
                    "02 2021 Metrics",
                    "08 2021 Metrics",
                    "2020 Annual Metrics",
                    "07 2021 Metrics",
                    "04 2021 Metrics",
                    "05 2021 Metrics",
                    "06 2021 Metrics",
                },
            )

    def test_get_spreadsheets(self) -> None:
        user_agency = self.test_schema_objects.test_agency_E
        not_user_agency = self.test_schema_objects.test_agency_A
        user = self.test_schema_objects.test_user_A
        self.session.add_all([user_agency, user, not_user_agency])
        self.session.commit()
        self.session.refresh(user_agency)
        self.session.refresh(not_user_agency)
        self.session.add_all(
            [
                self.test_schema_objects.get_test_spreadsheet(
                    system=System.LAW_ENFORCEMENT,
                    user_id=user.auth0_user_id,
                    agency_id=not_user_agency.id,
                ),
                self.test_schema_objects.get_test_spreadsheet(
                    system=System.SUPERVISION,
                    user_id=user.auth0_user_id,
                    agency_id=user_agency.id,
                ),
                self.test_schema_objects.get_test_spreadsheet(
                    system=System.PAROLE,
                    user_id=user.auth0_user_id,
                    agency_id=user_agency.id,
                    upload_offset=25,
                ),
                self.test_schema_objects.get_test_spreadsheet(
                    system=System.PROBATION,
                    is_ingested=True,
                    user_id=user.auth0_user_id,
                    agency_id=user_agency.id,
                ),
            ]
        )
        self.session.commit()
        with self.app.test_request_context():
            user_account = UserAccountInterface.get_user_by_auth0_user_id(
                session=self.session,
                auth0_user_id=user.auth0_user_id,
            )
            g.user_context = UserContext(
                user_account=user_account,
                auth0_user_id=user.auth0_user_id,
                agency_ids=[user_agency.id],
            )

            response = self.client.get(
                f"/api/agencies/{user_agency.id}/spreadsheets",
            )
            self.assertEqual(response.status_code, 200)
            spreadsheets = assert_type(response.json, list)
            self.assertEqual(len(spreadsheets), 3)
            probation_spreadsheet = spreadsheets[0]
            self.assertEqual(
                probation_spreadsheet.get("name"), "PROBATION_metrics.xlsx"
            )
            self.assertEqual(
                probation_spreadsheet.get("uploaded_at"),
                self.now_time.timestamp() * 1000,
            )
            self.assertEqual(
                probation_spreadsheet.get("uploaded_by"),
                self.test_schema_objects.test_user_A.name,
            )

            self.assertEqual(
                probation_spreadsheet.get("ingested_at"),
                (self.now_time + (datetime.timedelta(50))).timestamp() * 1000,
            )
            self.assertEqual(probation_spreadsheet.get("status"), "INGESTED")
            self.assertEqual(probation_spreadsheet.get("system"), "PROBATION")
            parole_spreadsheet = spreadsheets[1]
            self.assertEqual(parole_spreadsheet.get("name"), "PAROLE_metrics.xlsx")

            self.assertEqual(
                parole_spreadsheet.get("uploaded_at"),
                (self.now_time + (datetime.timedelta(25))).timestamp() * 1000,
            )
            self.assertEqual(
                parole_spreadsheet.get("uploaded_by"),
                self.test_schema_objects.test_user_A.name,
            )
            self.assertEqual(parole_spreadsheet.get("ingested_at"), None)
            self.assertEqual(parole_spreadsheet.get("status"), "UPLOADED")
            self.assertEqual(parole_spreadsheet.get("system"), "PAROLE")
            supervision_spreadsheet = spreadsheets[2]
            self.assertEqual(
                supervision_spreadsheet.get("name"),
                "SUPERVISION_metrics.xlsx",
            )
            self.assertEqual(
                supervision_spreadsheet.get("uploaded_at"),
                self.now_time.timestamp() * 1000,
            )
            self.assertEqual(
                supervision_spreadsheet.get("uploaded_by"),
                self.test_schema_objects.test_user_A.name,
            )
            self.assertEqual(supervision_spreadsheet.get("ingested_at"), None)
            self.assertEqual(supervision_spreadsheet.get("status"), "UPLOADED")
            self.assertEqual(supervision_spreadsheet.get("system"), "SUPERVISION")

    def test_download_spreadsheet_fail_without_permissions(self) -> None:
        agency_A = self.test_schema_objects.test_agency_A
        agency_B = self.test_schema_objects.test_agency_B
        self.session.add_all([agency_A, agency_B])
        self.session.commit()
        self.session.refresh(agency_A)
        self.session.refresh(agency_B)
        spreadsheet = self.test_schema_objects.get_test_spreadsheet(
            system=System.LAW_ENFORCEMENT,
            user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            agency_id=agency_A.id,
        )
        self.session.add(spreadsheet)
        self.session.commit()
        self.session.refresh(spreadsheet)
        with self.app.test_request_context():
            user_account = UserAccountInterface.get_user_by_auth0_user_id(
                session=self.session,
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            )
            g.user_context = UserContext(
                user_account=user_account,
                auth0_user_id=self.test_schema_objects.test_user_A.auth0_user_id,
                agency_ids=[agency_B.id],
            )

            response = self.client.get(f"/api/spreadsheets/{spreadsheet.id}")
            self.assertEqual(response.status_code, 400)

    def test_download_spreadsheet(self) -> None:
        agency = self.test_schema_objects.test_agency_A
        user = self.test_schema_objects.test_user_A
        self.session.add_all([user, agency])
        self.session.commit()
        self.session.refresh(agency)
        spreadsheet = self.test_schema_objects.get_test_spreadsheet(
            system=System.LAW_ENFORCEMENT,
            user_id=self.test_schema_objects.test_user_A.auth0_user_id,
            agency_id=agency.id,
        )
        self.session.add(spreadsheet)
        self.session.commit()
        self.session.refresh(spreadsheet)
        with self.app.test_request_context():
            user_account = UserAccountInterface.get_user_by_auth0_user_id(
                session=self.session,
                auth0_user_id=user.auth0_user_id,
            )
            g.user_context = UserContext(
                user_account=user_account,
                auth0_user_id=user.auth0_user_id,
                agency_ids=[agency.id],
            )
            # Upload spreadsheet
            upload_response = self.client.post(
                "/api/spreadsheets",
                data={
                    "agency_id": agency.id,
                    "system": System.LAW_ENFORCEMENT.value,
                    "file": (
                        self.bulk_upload_test_files
                        / "law_enforcement/law_enforcement_metrics.xlsx"
                    ).open("rb"),
                },
            )
            self.assertEqual(upload_response.status_code, 200)
            upload_response_json = assert_type(upload_response.json, dict)
            spreadsheet_id = upload_response_json.get("id")
            # Download spreadsheet
            download_response = self.client.get(f"/api/spreadsheets/{spreadsheet_id}")
            self.assertEqual(download_response.status_code, 200)

    def test_update_spreadsheet(self) -> None:
        agency = self.test_schema_objects.test_agency_E
        user = self.test_schema_objects.test_user_A
        self.session.add_all([agency, user])
        self.session.commit()
        self.session.refresh(agency)
        self.session.refresh(user)
        spreadsheet = self.test_schema_objects.get_test_spreadsheet(
            system=System.SUPERVISION,
            user_id=user.auth0_user_id,
            agency_id=agency.id,
        )
        self.session.add(spreadsheet)
        self.session.commit()
        self.session.refresh(spreadsheet)

        with self.app.test_request_context():
            user_account = UserAccountInterface.get_user_by_auth0_user_id(
                session=self.session,
                auth0_user_id=user.auth0_user_id,
            )
            g.user_context = UserContext(
                user_account=user_account,
                auth0_user_id=user.auth0_user_id,
                agency_ids=[agency.id],
                permissions=[ControlPanelPermission.RECIDIVIZ_ADMIN.value],
            )

            response = self.client.patch(
                f"/api/spreadsheets/{spreadsheet.id}",
                json={"status": SpreadsheetStatus.INGESTED.value},
            )
            self.assertEqual(response.status_code, 200)
            spreadsheet_json = assert_type(response.json, dict)
            self.assertEqual(
                spreadsheet_json.get("status"), SpreadsheetStatus.INGESTED.value
            )
            db_spreadsheet = self.session.query(Spreadsheet).one()
            self.assertEqual(db_spreadsheet.ingested_by, user.auth0_user_id)
            self.assertEqual(
                db_spreadsheet.ingested_at.timestamp(), self.now_time.timestamp()
            )

    def test_delete_spreadsheet(self) -> None:
        agency = self.test_schema_objects.test_agency_A
        user = self.test_schema_objects.test_user_A
        self.session.add_all([agency, user])
        self.session.commit()
        self.session.refresh(agency)
        self.session.refresh(user)
        with self.app.test_request_context():
            user_account = UserAccountInterface.get_user_by_auth0_user_id(
                session=self.session,
                auth0_user_id=user.auth0_user_id,
            )
            g.user_context = UserContext(
                user_account=user_account,
                auth0_user_id=user.auth0_user_id,
                agency_ids=[agency.id],
            )
            upload_response = self.client.post(
                "/api/spreadsheets",
                data={
                    "agency_id": agency.id,
                    "system": System.LAW_ENFORCEMENT.value,
                    "file": (
                        self.bulk_upload_test_files
                        / "law_enforcement/law_enforcement_metrics.xlsx"
                    ).open("rb"),
                },
            )
            self.assertEqual(upload_response.status_code, 200)
            upload_response_json = assert_type(upload_response.json, dict)
            spreadsheet_id = upload_response_json.get("id")
            response = self.client.delete(f"/api/spreadsheets/{spreadsheet_id}")
            self.assertEqual(response.status_code, 200)
            db_spreadsheet = self.session.query(Spreadsheet).one_or_none()
            self.assertEqual(db_spreadsheet, None)
            path = GcsfsFilePath(
                bucket_name="justice-counts-justice-counts-control-panel-ingest",
                blob_name=f"{str(agency.id)}:{System.LAW_ENFORCEMENT.value}:{self.now_time.timestamp()}.xlsx",
            )
            self.assertEqual(
                self.fs.exists(path),
                False,
            )

    def test_get_datapoints_by_agency_id(self) -> None:
        user_A = self.test_schema_objects.test_user_A
        report = self.test_schema_objects.test_report_monthly
        self.session.add_all([report, user_A])
        self.session.commit()

        report_metric = self.test_schema_objects.reported_residents_metric
        ReportInterface.add_or_update_metric(
            session=self.session,
            report=report,
            report_metric=report_metric,
            user_account=user_A,
        )

        with self.app.test_request_context():
            g.user_context = UserContext(
                auth0_user_id=user_A.auth0_user_id, agency_ids=[report.source_id]
            )
            response = self.client.get(f"/api/agencies/{report.source_id}/datapoints")

        self.assertEqual(response.status_code, 200)
        agency_datapoints = self.session.query(Datapoint).all()
        response_json = assert_type(response.json, dict)
        response_json_datapoints = assert_type(response_json["datapoints"], list)
        self.assertEqual(len(agency_datapoints), len(response_json_datapoints))

        response_json_datapoint = assert_type(response_json_datapoints[0], dict)
        self.assertEqual(response_json_datapoint["dimension_display_name"], None)
        self.assertEqual(response_json_datapoint["disaggregation_display_name"], None)
        self.assertEqual(
            response_json_datapoint["end_date"], "Fri, 01 Jul 2022 00:00:00 GMT"
        )
        self.assertEqual(
            response_json_datapoint["frequency"], ReportingFrequency.MONTHLY.value
        )
        self.assertEqual(response_json_datapoint["is_published"], False)
        self.assertEqual(
            response_json_datapoint["metric_definition_key"],
            "LAW_ENFORCEMENT_RESIDENTS_global/gender/restricted,global/race_and_ethnicity",
        )
        self.assertEqual(
            response_json_datapoint["metric_display_name"], "Jurisdiction Residents"
        )
        self.assertEqual(
            response_json_datapoint["start_date"], "Wed, 01 Jun 2022 00:00:00 GMT"
        )
        self.assertEqual(response_json_datapoint["value"], "5000")

        response_json_dimensions = response_json[
            "dimension_names_by_metric_and_disaggregation"
        ]

        self.assertEqual(
            response_json_dimensions,
            {
                "LAW_ENFORCEMENT_ARRESTS_global/gender/restricted,global/race_and_ethnicity,metric/law_enforcement/reported_crime/type": {
                    "Gender": ["Male", "Female", "Other", "Non-Binary", "Unknown"],
                    "Offense Type": ["Person", "Property", "Drug", "Other", "Unknown"],
                    "Race / Ethnicity": [
                        "American Indian / Alaskan Native",
                        "Asian",
                        "Black",
                        "External / Unknown",
                        "Hispanic",
                        "Native Hawaiian / Pacific Islander",
                        "Other",
                        "White",
                    ],
                },
                "LAW_ENFORCEMENT_BUDGET_": {},
                "LAW_ENFORCEMENT_CALLS_FOR_SERVICE_metric/law_enforcement/calls_for_service/type": {
                    "Call Type": ["Emergency", "Non-emergency", "Unknown"]
                },
                "LAW_ENFORCEMENT_COMPLAINTS_SUSTAINED_": {},
                "LAW_ENFORCEMENT_REPORTED_CRIME_metric/law_enforcement/reported_crime/type": {
                    "Offense Type": ["Person", "Property", "Drug", "Other", "Unknown"]
                },
                "LAW_ENFORCEMENT_TOTAL_STAFF_": {},
                "LAW_ENFORCEMENT_USE_OF_FORCE_INCIDENTS_metric/law_enforcement/officer_use_of_force_incidents/type": {
                    "Force Type": [
                        "Physical",
                        "Restraint",
                        "Verbal",
                        "Weapon",
                        "Unknown",
                    ]
                },
            },
        )

    def test_session(self) -> None:
        # Add data
        name = "Agency Alpha"
        self.session.add(Source(name=name))
        self.session.commit()

        # Query data
        source = self.session.query(Source).one_or_none()

        self.assertEqual(source.name, name)

    def test_feed(self) -> None:
        self.session.add_all(
            [
                self.test_schema_objects.test_user_A,
                self.test_schema_objects.test_agency_A,
            ]
        )
        self.session.commit()
        self.session.flush()
        agency_id = self.test_schema_objects.test_agency_A.id

        law_enforcement_directory = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                "..",
                "bulk_upload/bulk_upload_fixtures/law_enforcement",
            )
        )
        BulkUploader(catch_errors=False).upload_directory(
            session=self.session,
            directory=law_enforcement_directory,
            agency_id=agency_id,
            system=schema.System.LAW_ENFORCEMENT,
            user_account=self.test_schema_objects.test_user_A,
        )

        feed_response_no_metric = self.client.get(f"/feed/{agency_id}")
        self.assertEqual(feed_response_no_metric.status_code, 200)

        feed_response_with_metric = self.client.get(
            f"/feed/{agency_id}", query_string={"metric": "arrests"}
        )
        self.assertEqual(feed_response_with_metric.status_code, 200)
