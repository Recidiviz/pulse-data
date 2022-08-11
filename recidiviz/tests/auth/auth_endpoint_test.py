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

"""Tests for auth/auth_endpoint.py."""
import json
from http import HTTPStatus
from typing import Any, Dict, Optional
from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

import flask
import pytest
from flask import Flask

from recidiviz.auth.auth_endpoint import auth_endpoint_blueprint
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.schema.case_triage.schema import (
    DashboardUserRestrictions,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.auth.helpers import (
    add_entity_to_database_session,
    generate_fake_default_permissions,
    generate_fake_permissions_overrides,
    generate_fake_rosters,
    generate_fake_user_overrides,
    generate_fake_user_restrictions,
)
from recidiviz.tools.postgres import local_postgres_helpers


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789"))
@patch(
    "recidiviz.utils.validate_jwt.validate_iap_jwt_from_app_engine",
    MagicMock(return_value=("test-user", "test-user@recidiviz.org", None)),
)
@pytest.mark.uses_db
class AuthEndpointTests(TestCase):
    """Integration tests of our flask auth endpoints"""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def setUp(self) -> None:
        self.app = Flask(__name__)
        self.app.register_blueprint(auth_endpoint_blueprint)
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

        self.headers: Dict[str, Dict[Any, Any]] = {"x-goog-iap-jwt-assertion": {}}
        self.region_code = "US_MO"
        self.bucket = "test-project-dashboard-user-restrictions"
        self.filename = "dashboard_user_restrictions.json"
        self.gcs_csv_uri = GcsfsFilePath.from_absolute_path(
            f"{self.bucket}/{self.region_code}/dashboard_user_restrictions.csv"
        )
        self.columns = [col.name for col in DashboardUserRestrictions.__table__.columns]
        self.maxDiff = None

        # Setup database
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        with self.app.test_request_context():
            self.handle_import_user_restrictions_csv_to_sql = flask.url_for(
                "auth_endpoint_blueprint.handle_import_user_restrictions_csv_to_sql"
            )
            self.import_user_restrictions_csv_to_sql_url = flask.url_for(
                "auth_endpoint_blueprint.import_user_restrictions_csv_to_sql"
            )
            self.dashboard_user_restrictions_by_email_url = flask.url_for(
                "auth_endpoint_blueprint.dashboard_user_restrictions_by_email"
            )
            self.dashboard_user_restrictions = flask.url_for(
                "auth_endpoint_blueprint.dashboard_user_restrictions"
            )
            self.users = flask.url_for("auth_endpoint_blueprint.users")
            self.add_user = flask.url_for(
                "auth_endpoint_blueprint.add_user",
                email="parameter@domain.org",
            )

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @patch("recidiviz.auth.auth_endpoint.CloudTaskQueueManager")
    def test_handle_import_user_restrictions_csv_to_sql(
        self, mock_task_manager: MagicMock
    ) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.handle_import_user_restrictions_csv_to_sql,
                headers=self.headers,
                json={
                    "message": {
                        "attributes": {
                            "bucketId": self.bucket,
                            "objectId": f"{self.region_code}/dashboard_user_restrictions.csv",
                        },
                    }
                },
            )
            self.assertEqual(b"", response.data)
            self.assertEqual(HTTPStatus.OK, response.status_code)

            mock_task_manager.return_value.create_task.assert_called_with(
                relative_uri=f"/auth{self.import_user_restrictions_csv_to_sql_url}",
                body={"region_code": self.region_code},
            )

    def test_handle_import_user_restrictions_csv_to_sql_invalid_pubsub(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.handle_import_user_restrictions_csv_to_sql,
                headers=self.headers,
                json={
                    "message": {
                        "attributes": {},
                    }
                },
            )
            self.assertEqual(b"Invalid Pub/Sub message", response.data)
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    @patch("recidiviz.auth.auth_endpoint.CloudTaskQueueManager")
    def test_handle_import_user_restrictions_csv_to_sql_missing_region(
        self, mock_task_manager: MagicMock
    ) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.handle_import_user_restrictions_csv_to_sql,
                headers=self.headers,
                json={
                    "message": {
                        "attributes": {
                            "bucketId": self.bucket,
                            "objectId": "dashboard_user_restrictions.csv",
                        },
                    }
                },
            )
            self.assertEqual(b"", response.data)
            self.assertEqual(HTTPStatus.OK, response.status_code)

            mock_task_manager.return_value.create_task.assert_not_called()

    @patch("recidiviz.auth.auth_endpoint.CloudTaskQueueManager")
    def test_handle_import_user_restrictions_csv_to_sql_invalid_file(
        self, mock_task_manager: MagicMock
    ) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.handle_import_user_restrictions_csv_to_sql,
                headers=self.headers,
                json={
                    "message": {
                        "attributes": {
                            "bucketId": self.bucket,
                            "objectId": f"{self.region_code}/invalid_file.csv",
                        },
                    }
                },
            )
            self.assertEqual(b"", response.data)
            self.assertEqual(HTTPStatus.OK, response.status_code)

            mock_task_manager.return_value.create_task.assert_not_called()

    @patch("recidiviz.auth.auth_endpoint.import_gcs_csv_to_cloud_sql")
    def test_import_user_restrictions_csv_to_sql_successful(
        self, mock_import_csv: MagicMock
    ) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.import_user_restrictions_csv_to_sql_url,
                headers=self.headers,
                json={
                    "region_code": self.region_code,
                },
            )
            mock_import_csv.assert_called_with(
                database_key=self.database_key,
                model=DashboardUserRestrictions,
                gcs_uri=self.gcs_csv_uri,
                columns=self.columns,
                region_code=self.region_code,
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(
                b"CSV US_MO/dashboard_user_restrictions.csv successfully imported to "
                b"Cloud SQL schema SchemaType.CASE_TRIAGE for region code US_MO",
                response.data,
            )

    def test_import_user_restrictions_csv_to_sql_missing_region_code(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.import_user_restrictions_csv_to_sql_url,
                headers=self.headers,
                json={},
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Missing region_code param",
                response.data,
            )

    def test_import_user_restrictions_csv_to_sql_invalid_region_code(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.import_user_restrictions_csv_to_sql_url,
                headers=self.headers,
                json={
                    "region_code": "MO",
                },
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Unknown region_code [MO] received, must be a valid state code.",
                response.data,
            )

    @patch("recidiviz.auth.auth_endpoint.import_gcs_csv_to_cloud_sql")
    def test_import_user_restrictions_csv_to_sql_exception(
        self, mock_import_csv: MagicMock
    ) -> None:
        mock_import_csv.side_effect = Exception("Error while importing CSV")
        with self.app.test_request_context():
            response = self.client.post(
                self.import_user_restrictions_csv_to_sql_url,
                headers=self.headers,
                json={
                    "region_code": "US_MO",
                },
            )
            self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)
            self.assertEqual(
                b"Error while importing CSV",
                response.data,
            )

    def test_dashboard_user_restrictions_by_email_invalid_email(self) -> None:
        with self.app.test_request_context():
            response = self.client.get(
                self.dashboard_user_restrictions_by_email_url,
                headers=self.headers,
                query_string={
                    "region_code": "US_MO",
                    "email_address": "not an email address",
                },
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Invalid email address format: [not an email address]",
                response.data,
            )

    def test_dashboard_user_restrictions_by_email_missing_email_address(self) -> None:
        with self.app.test_request_context():
            response = self.client.get(
                self.dashboard_user_restrictions_by_email_url,
                headers=self.headers,
                query_string={"region_code": "US_MO"},
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Missing email_address param",
                response.data,
            )

    def test_dashboard_user_restrictions_by_email_invalid_region_code(self) -> None:
        with self.app.test_request_context():
            for region_code in ["US_ZZ", "not a region code"]:
                response = self.client.get(
                    self.dashboard_user_restrictions_by_email_url,
                    headers=self.headers,
                    query_string={
                        "region_code": region_code,
                        "email_address": "test@domain.org",
                    },
                )
                self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
                self.assertEqual(
                    bytes(
                        f"Unknown region_code [{region_code}] received, must be a valid state code.",
                        encoding="utf-8",
                    ),
                    response.data,
                )

    def test_dashboard_user_restrictions_by_email_missing_region_code(self) -> None:
        with self.app.test_request_context():
            response = self.client.get(
                self.dashboard_user_restrictions_by_email_url,
                headers=self.headers,
                query_string={"email_address": "test@domain.org"},
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Missing region_code param",
                response.data,
            )

    def test_dashboard_user_restrictions_by_email_user_has_restrictions(self) -> None:
        user_1 = generate_fake_user_restrictions(
            self.region_code,
            "user-1@test.gov",
            allowed_supervision_location_ids="1,2",
        )
        user_2 = generate_fake_user_restrictions(
            self.region_code,
            "user-2@test.gov",
            allowed_supervision_location_ids="AB",
        )
        add_entity_to_database_session(self.database_key, [user_1, user_2])

        with self.app.test_request_context():
            expected_restrictions = {
                "allowed_supervision_location_ids": ["1", "2"],
                "allowed_supervision_location_level": "level_1_supervision_location",
                "can_access_case_triage": False,
                "can_access_leadership_dashboard": True,
                "should_see_beta_charts": False,
                "routes": None,
                "user_hash": "user-1@test.gov::hashed",
            }
            response = self.client.get(
                self.dashboard_user_restrictions_by_email_url,
                headers=self.headers,
                query_string={
                    "region_code": "US_MO",
                    "email_address": "user-1@test.gov",
                },
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(
                expected_restrictions,
                json.loads(response.data),
            )

    def test_dashboard_user_restrictions_by_email_hash_missing(self) -> None:
        user_1 = generate_fake_user_restrictions(
            self.region_code,
            "user-1@test.gov",
            allowed_supervision_location_ids="1,2",
            include_hash=False,
        )

        add_entity_to_database_session(self.database_key, [user_1])

        with self.app.test_request_context():
            expected_restrictions = {
                "allowed_supervision_location_ids": ["1", "2"],
                "allowed_supervision_location_level": "level_1_supervision_location",
                "can_access_case_triage": False,
                "can_access_leadership_dashboard": True,
                "should_see_beta_charts": False,
                "routes": None,
                "user_hash": None,
            }
            response = self.client.get(
                self.dashboard_user_restrictions_by_email_url,
                headers=self.headers,
                query_string={
                    "region_code": "US_MO",
                    "email_address": "user-1@test.gov",
                },
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(
                expected_restrictions,
                json.loads(response.data),
            )

    def test_dashboard_user_restrictions_by_email_no_user_found(self) -> None:
        with self.app.test_request_context():
            response = self.client.get(
                self.dashboard_user_restrictions_by_email_url,
                headers=self.headers,
                query_string={
                    "region_code": "US_MO",
                    "email_address": "nonexistent-user@domain.org",
                },
            )
            self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)
            self.assertEqual(
                b"User not found for email address nonexistent-user@domain.org and region code US_MO.",
                response.data,
            )

    @mock.patch(
        # TODO(#8046): Don't use the deprecated session fetcher
        "recidiviz.auth.auth_endpoint.SessionFactory.deprecated__for_database",
        return_value=MagicMock(),
    )
    def test_dashboard_user_restrictions_by_email_internal_error(
        self, mockSession: MagicMock
    ) -> None:
        with self.app.test_request_context():
            mockSession.return_value.query.side_effect = Exception("Session error")
            response = self.client.get(
                self.dashboard_user_restrictions_by_email_url,
                headers=self.headers,
                query_string={
                    "region_code": "US_MO",
                    "email_address": "test@domain.org",
                },
            )
            mockSession.assert_called_with(database_key=self.database_key)
            self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)
            self.assertEqual(
                b"An error occurred while fetching dashboard user restrictions with the email test@domain.org for "
                b"region_code US_MO: Session error",
                response.data,
            )

    def test_dashboard_user_restrictions_pass(self) -> None:
        user_1 = generate_fake_user_restrictions(
            email="test@domain.org",
            region_code="US_ND",
            allowed_supervision_location_ids="1, 2",
            include_hash=False,
        )
        add_entity_to_database_session(self.database_key, [user_1])
        with self.app.test_request_context():
            expected_restrictions = [
                {
                    "allowedSupervisionLocationIds": "1, 2",
                    "allowedSupervisionLocationLevel": "level_1_supervision_location",
                    "canAccessCaseTriage": False,
                    "canAccessLeadershipDashboard": True,
                    "restrictedUserEmail": "test@domain.org",
                    "routes": None,
                    "shouldSeeBetaCharts": False,
                    "stateCode": "US_ND",
                }
            ]
        response = self.client.get(
            self.dashboard_user_restrictions,
            headers=self.headers,
        )
        self.assertEqual(expected_restrictions, json.loads(response.data))

    def test_dashboard_user_restrictions_multiple_users(self) -> None:
        user_1 = generate_fake_user_restrictions(
            email="test@domain.org",
            region_code="US_ND",
            allowed_supervision_location_ids="1, 2",
            include_hash=False,
        )
        user_2 = generate_fake_user_restrictions(
            email="secondtest@domain.org",
            region_code="US_PA",
            allowed_supervision_location_ids="1",
            include_hash=False,
            should_see_beta_charts=True,
            routes={"A": "B", "B": "C"},
        )
        user_3 = generate_fake_user_restrictions(
            email="thirdtest@domain.org",
            region_code="US_ME",
            allowed_supervision_location_ids="A, B, C",
            can_access_leadership_dashboard=False,
            include_hash=False,
            can_access_case_triage=True,
        )
        add_entity_to_database_session(self.database_key, [user_1, user_2, user_3])
        with self.app.test_request_context():
            expected_restrictions = [
                {
                    "allowedSupervisionLocationIds": "1, 2",
                    "allowedSupervisionLocationLevel": "level_1_supervision_location",
                    "canAccessCaseTriage": False,
                    "canAccessLeadershipDashboard": True,
                    "restrictedUserEmail": "test@domain.org",
                    "routes": None,
                    "shouldSeeBetaCharts": False,
                    "stateCode": "US_ND",
                },
                {
                    "allowedSupervisionLocationIds": "1",
                    "allowedSupervisionLocationLevel": "level_1_supervision_location",
                    "canAccessCaseTriage": False,
                    "canAccessLeadershipDashboard": True,
                    "restrictedUserEmail": "secondtest@domain.org",
                    "routes": {"A": "B", "B": "C"},
                    "shouldSeeBetaCharts": True,
                    "stateCode": "US_PA",
                },
                {
                    "allowedSupervisionLocationIds": "A, B, C",
                    "allowedSupervisionLocationLevel": "level_1_supervision_location",
                    "canAccessCaseTriage": True,
                    "canAccessLeadershipDashboard": False,
                    "restrictedUserEmail": "thirdtest@domain.org",
                    "routes": None,
                    "shouldSeeBetaCharts": False,
                    "stateCode": "US_ME",
                },
            ]
        response = self.client.get(
            self.dashboard_user_restrictions,
            headers=self.headers,
        )
        self.assertEqual(expected_restrictions, json.loads(response.data))

    def test_dashboard_user_restrictions_no_users(self) -> None:
        with self.app.test_request_context():
            expected_restrictions: list[str] = []
        response = self.client.get(
            self.dashboard_user_restrictions,
            headers=self.headers,
        )
        self.assertEqual(expected_restrictions, json.loads(response.data))

    def test_users_some_overrides(self) -> None:
        user_1 = generate_fake_rosters(
            email="test@domain.org",
            region_code="US_ND",
            role="leadership_role",
            district="D1",
            first_name="Fake",
            last_name="User",
        )
        user_2 = generate_fake_rosters(
            email="secondtest@domain.org",
            region_code="US_ID",
            external_id="abc",
            role="line_staff",
            district="D3",
            first_name="John",
            last_name="Doe",
        )
        user_1_override = generate_fake_user_overrides(
            email="test@domain.org",
            region_code="US_ND",
            external_id="user_1_override.external_id",
            role="user_1_override.role",
            blocked=True,
        )
        default_1 = generate_fake_default_permissions(
            state="US_ND",
            role="leadership_role",
            can_access_leadership_dashboard=True,
            can_access_case_triage=False,
            should_see_beta_charts=True,
            routes={"A": False, "B": True},
        )
        default_2 = generate_fake_default_permissions(
            state="US_ND",
            role="user_1_override.role",
            can_access_leadership_dashboard=False,
            can_access_case_triage=True,
            should_see_beta_charts=True,
        )
        default_3 = generate_fake_default_permissions(
            state="US_ID",
            role="line_staff",
            can_access_leadership_dashboard=False,
            can_access_case_triage=True,
            should_see_beta_charts=True,
        )
        new_permissions = generate_fake_permissions_overrides(
            email="test@domain.org",
            routes={"overridden route": True},
        )
        add_entity_to_database_session(
            self.database_key,
            [
                user_1,
                user_2,
                user_1_override,
                default_1,
                default_2,
                default_3,
                new_permissions,
            ],
        )
        with self.app.test_request_context():
            expected = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": True,
                    "canAccessCaseTriage": True,
                    "canAccessLeadershipDashboard": False,
                    "district": "D1",
                    "emailAddress": "test@domain.org",
                    "externalId": "user_1_override.external_id",
                    "firstName": "Fake",
                    "lastName": "User",
                    "role": "user_1_override.role",
                    "stateCode": "US_ND",
                    "shouldSeeBetaCharts": True,
                    "routes": {"overridden route": True},
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": False,
                    "canAccessCaseTriage": True,
                    "canAccessLeadershipDashboard": False,
                    "district": "D3",
                    "emailAddress": "secondtest@domain.org",
                    "externalId": "abc",
                    "firstName": "John",
                    "lastName": "Doe",
                    "role": "line_staff",
                    "stateCode": "US_ID",
                    "shouldSeeBetaCharts": True,
                    "routes": None,
                },
            ]
        response = self.client.get(
            self.users,
            headers=self.headers,
        )
        self.assertEqual(expected, json.loads(response.data))

    def test_users_with_empty_overrides(self) -> None:
        user_1 = generate_fake_rosters(
            email="test_user@domain.org",
            region_code="US_MO",
            external_id="12345",
            role="leadership_role",
            district="4, 10A",
            first_name="Test A.",
            last_name="User",
        )
        default = generate_fake_default_permissions(
            state="US_MO",
            role="leadership_role",
            can_access_leadership_dashboard=True,
            can_access_case_triage=False,
            should_see_beta_charts=True,
        )
        add_entity_to_database_session(self.database_key, [user_1, default])
        with self.app.test_request_context():
            expected = [
                {
                    "allowedSupervisionLocationIds": "4, 10A",
                    "allowedSupervisionLocationLevel": "level_1_supervision_location",
                    "blocked": False,
                    "canAccessCaseTriage": False,
                    "canAccessLeadershipDashboard": True,
                    "district": "4, 10A",
                    "emailAddress": "test_user@domain.org",
                    "externalId": "12345",
                    "firstName": "Test A.",
                    "lastName": "User",
                    "role": "leadership_role",
                    "stateCode": "US_MO",
                    "shouldSeeBetaCharts": True,
                    "routes": None,
                },
            ]
        response = self.client.get(
            self.users,
            headers=self.headers,
        )
        self.assertEqual(expected, json.loads(response.data))

    def test_users_with_null_values(self) -> None:
        user_1 = generate_fake_rosters(
            email="user@domain.org",
            region_code="US_ME",
            role="leadership_role",
        )
        applicable_override = generate_fake_user_overrides(
            email="user@domain.org",
            region_code="US_ME",
            external_id="A1B2",
            blocked=True,
        )
        default_1 = generate_fake_default_permissions(
            state="US_ME",
            role="leadership_role",
            can_access_leadership_dashboard=True,
        )
        new_permissions = generate_fake_permissions_overrides(
            email="user@domain.org",
            routes={"A": True, "C": False},
        )
        add_entity_to_database_session(
            self.database_key, [user_1, applicable_override, default_1, new_permissions]
        )
        with self.app.test_request_context():
            expected = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": True,
                    "canAccessCaseTriage": False,
                    "canAccessLeadershipDashboard": True,
                    "district": None,
                    "emailAddress": "user@domain.org",
                    "externalId": "A1B2",
                    "firstName": None,
                    "lastName": None,
                    "role": "leadership_role",
                    "stateCode": "US_ME",
                    "shouldSeeBetaCharts": False,
                    "routes": {"A": True, "C": False},
                },
            ]
        response = self.client.get(
            self.users,
            headers=self.headers,
        )
        self.assertEqual(expected, json.loads(response.data))

    def test_users_no_users(self) -> None:
        with self.app.test_request_context():
            expected_restrictions: list[str] = []
        response = self.client.get(
            self.users,
            headers=self.headers,
        )
        self.assertEqual(expected_restrictions, json.loads(response.data))

    def test_users_no_permissions(self) -> None:
        user_1 = generate_fake_rosters(
            email="test_user@domain.org",
            region_code="US_CO",
            external_id="12345",
            role="leadership_role",
            district="District 4",
            first_name="Test A.",
            last_name="User",
        )
        add_entity_to_database_session(self.database_key, [user_1])
        with self.app.test_request_context():
            expected = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": False,
                    "canAccessCaseTriage": False,
                    "canAccessLeadershipDashboard": False,
                    "district": "District 4",
                    "emailAddress": "test_user@domain.org",
                    "externalId": "12345",
                    "firstName": "Test A.",
                    "lastName": "User",
                    "role": "leadership_role",
                    "stateCode": "US_CO",
                    "shouldSeeBetaCharts": False,
                    "routes": None,
                },
            ]
        response = self.client.get(
            self.users,
            headers=self.headers,
        )
        self.assertEqual(expected, json.loads(response.data))

    def test_users_add_user(self) -> None:
        user_1 = generate_fake_rosters(
            email="add_user@domain.org",
            region_code="US_CO",
            external_id="ABC",
            role="leadership_role",
            district="District",
        )
        default = generate_fake_default_permissions(
            state="US_MO",
            role="leadership_role",
            can_access_leadership_dashboard=True,
            can_access_case_triage=False,
            should_see_beta_charts=True,
            routes={"A": "B", "B": "C"},
        )
        add_entity_to_database_session(self.database_key, [user_1, default])
        self.client.post(
            self.add_user,
            headers=self.headers,
            json={
                "stateCode": "US_MO",
                "externalId": None,
                "role": "leadership_role",
                "district": "1, 2",
                "firstName": None,
                "lastName": None,
            },
        )
        with self.app.test_request_context():
            expected = [
                {  # handles MO's specific logic
                    "allowedSupervisionLocationIds": "1, 2",
                    "allowedSupervisionLocationLevel": "level_1_supervision_location",
                    "blocked": False,
                    "canAccessCaseTriage": False,
                    "canAccessLeadershipDashboard": True,
                    "district": "1, 2",
                    "emailAddress": "parameter@domain.org",
                    "externalId": None,
                    "firstName": None,
                    "lastName": None,
                    "role": "leadership_role",
                    "stateCode": "US_MO",
                    "shouldSeeBetaCharts": True,
                    "routes": {"A": "B", "B": "C"},
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": False,
                    "canAccessCaseTriage": False,
                    "canAccessLeadershipDashboard": False,
                    "district": "District",
                    "emailAddress": "add_user@domain.org",
                    "externalId": "ABC",
                    "firstName": None,
                    "lastName": None,
                    "role": "leadership_role",
                    "stateCode": "US_CO",
                    "shouldSeeBetaCharts": False,
                    "routes": None,
                },
            ]
        response = self.client.get(
            self.users,
            headers=self.headers,
        )
        self.assertEqual(expected, json.loads(response.data))

    def test_add_user_bad_request(self) -> None:
        no_state = self.client.post(
            self.add_user,
            headers=self.headers,
            json={
                "stateCode": None,
                "externalId": "XYZ",
                "role": "leadership_role",
                "district": "D1",
                "firstName": "Test",
                "lastName": "User",
            },
        )
        self.assertEqual(HTTPStatus.BAD_REQUEST, no_state.status_code)
        no_role = self.client.post(
            self.add_user,
            headers=self.headers,
            json={
                "stateCode": "US_ID",
                "externalId": "XYZ",
                "role": None,
                "district": "D1",
                "firstName": "Test",
                "lastName": "User",
            },
        )
        self.assertEqual(HTTPStatus.BAD_REQUEST, no_role.status_code)
        wrong_type = self.client.post(
            self.add_user,
            headers=self.headers,
            json={
                "stateCode": "US_ID",
                "externalId": "XYZ",
                "role": True,
                "district": "D1",
                "firstName": "Test",
                "lastName": "User",
            },
        )
        self.assertEqual(HTTPStatus.BAD_REQUEST, wrong_type.status_code)

    def test_add_user_repeat_email(self) -> None:
        with self.app.test_request_context():
            user_override_user = self.client.post(
                self.add_user,
                headers=self.headers,
                json={
                    "stateCode": "US_ID",
                    "externalId": "XYZ",
                    "role": "leadership_role",
                    "district": "D1",
                    "firstName": "Test",
                    "lastName": "User",
                },
            )
            expected = {  # no default permissions
                "allowedSupervisionLocationIds": "",
                "allowedSupervisionLocationLevel": "",
                "blocked": False,
                "canAccessCaseTriage": False,
                "canAccessLeadershipDashboard": False,
                "district": "D1",
                "emailAddress": "parameter@domain.org",
                "externalId": "XYZ",
                "firstName": "Test",
                "lastName": "User",
                "role": "leadership_role",
                "stateCode": "US_ID",
                "shouldSeeBetaCharts": False,
                "routes": None,
            }
            self.assertEqual(expected, json.loads(user_override_user.data))
            repeat_user_override_user = self.client.post(
                self.add_user,
                headers=self.headers,
                json={
                    "stateCode": "US_ND",
                    "externalId": None,
                    "role": "leadership_role",
                    "district": None,
                    "firstName": None,
                    "lastName": None,
                },
            )
            self.assertEqual(HTTPStatus.OK, user_override_user.status_code)
            self.assertEqual(
                HTTPStatus.UNPROCESSABLE_ENTITY, repeat_user_override_user.status_code
            )

    def test_add_user_repeat_roster_email(self) -> None:
        roster_user = generate_fake_rosters(
            email="parameter@domain.org",
            region_code="US_TN",
            role="leadership_role",
            district="40",
        )
        add_entity_to_database_session(self.database_key, [roster_user])
        with self.app.test_request_context():
            repeat_roster_user = self.client.post(
                self.add_user,
                headers=self.headers,
                json={
                    "stateCode": "US_TN",
                    "externalId": None,
                    "role": "leadership_role",
                    "district": "40",
                    "firstName": None,
                    "lastName": None,
                },
            )
            self.assertEqual(
                HTTPStatus.UNPROCESSABLE_ENTITY, repeat_roster_user.status_code
            )
