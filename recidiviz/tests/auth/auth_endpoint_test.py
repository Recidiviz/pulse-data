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
import os
from http import HTTPStatus
from typing import Any, Dict, List, Optional
from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

import flask
import pytest
from flask import Flask
from werkzeug.datastructures import FileStorage

from recidiviz.auth.auth_endpoint import auth_endpoint_blueprint
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.persistence.database.schema.case_triage.schema import (
    DashboardUserRestrictions,
    Roster,
    UserOverride,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
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

_FIXTURE_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "./fixtures/",
    )
)
_PARAMETER_USER_HASH = "flf+tuxZFuMOTgZf8aIZiDj/a4Cw4tIwRl7WcpVdCA0="
_ADD_USER_HASH = "0D1WiekUDUBhjVnqyNbbwGJP2xll0CS9vfsnPrxnmSE="
_LEADERSHIP_USER_HASH = "qKTCaVmWmjqbJX0SckE082QJKv6sE4W/bKzfHQZJNYk="
_SUPERVISION_STAFF_HASH = "EghmFPYcNI/RKWs9Cdt3P5nvGFhwM/uSkKKY1xVibvI="
_USER_HASH = "j8+pC9rc353XWt4x1fg+3Km9TQtr5XMZMT8Frl37H/o="


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
        self.ingested_users_bucket = "test-project-product-user-import"
        self.filename = "dashboard_user_restrictions.json"
        self.gcs_csv_uri = GcsfsFilePath.from_absolute_path(
            f"{self.bucket}/{self.region_code}/dashboard_user_restrictions.csv"
        )
        self.ingested_users_gcs_csv_uri = GcsfsFilePath.from_absolute_path(
            f"{self.ingested_users_bucket}/US_XX/ingested_product_users.csv"
        )
        self.fs = FakeGCSFileSystem()
        self.fs_patcher = patch.object(GcsfsFactory, "build", return_value=self.fs)
        self.fs_patcher.start()
        self.columns = [col.name for col in DashboardUserRestrictions.__table__.columns]
        self.roster_columns = [col.name for col in Roster.__table__.columns]
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
            self.user = flask.url_for(
                "auth_endpoint_blueprint.users",
                user_hash=_PARAMETER_USER_HASH,
            )
            self.add_user = flask.url_for(
                "auth_endpoint_blueprint.add_user",
            )
            self.upload_roster = lambda state_code, role=None: flask.url_for(
                "auth_endpoint_blueprint.upload_roster",
                state_code=state_code,
            )
            self.update_user = flask.url_for(
                "auth_endpoint_blueprint.update_user",
                user_hash=_PARAMETER_USER_HASH,
            )
            self.update_user_permissions = flask.url_for(
                "auth_endpoint_blueprint.update_user_permissions",
                user_hash=_USER_HASH,
            )
            self.delete_user_permissions = flask.url_for(
                "auth_endpoint_blueprint.delete_user_permissions",
                user_hash=_USER_HASH,
            )
            self.delete_user = flask.url_for(
                "auth_endpoint_blueprint.delete_user",
                user_hash=_PARAMETER_USER_HASH,
            )
            self.states = flask.url_for("auth_endpoint_blueprint.states")
            self.add_state_role = lambda state_code, role: flask.url_for(
                "auth_endpoint_blueprint.add_state_role",
                state_code=state_code,
                role=role,
            )
            self.update_state_role = lambda state_code, role: flask.url_for(
                "auth_endpoint_blueprint.update_state_role",
                state_code=state_code,
                role=role,
            )
            self.delete_state_role = lambda state_code, role: flask.url_for(
                "auth_endpoint_blueprint.delete_state_role",
                state_code=state_code,
                role=role,
            )
            self.import_ingested_users_async = flask.url_for(
                "auth_endpoint_blueprint.import_ingested_users_async"
            )
            self.import_ingested_users = flask.url_for(
                "auth_endpoint_blueprint.import_ingested_users"
            )

    def tearDown(self) -> None:
        self.fs_patcher.stop()
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    def assertReasonLog(self, log_messages: List[str], expected: str) -> None:
        self.assertIn(
            f"INFO:root:State User Permissions: [test-user@recidiviz.org] is {expected}",
            log_messages,
        )

    @patch("recidiviz.auth.auth_endpoint.SingleCloudTaskQueueManager")
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

    @patch("recidiviz.auth.auth_endpoint.SingleCloudTaskQueueManager")
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

    @patch("recidiviz.auth.auth_endpoint.SingleCloudTaskQueueManager")
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
                b"Unknown state_code [MO] received, must be a valid state code.",
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
                        f"Unknown state_code [{region_code}] received, must be a valid state code.",
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
                "role": "supervision_staff",
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
                "role": "supervision_staff",
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
                    "role": "supervision_staff",
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
                    "role": "supervision_staff",
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
                    "role": "supervision_staff",
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
                    "role": "supervision_staff",
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
            email="leadership@domain.org",
            region_code="US_ND",
            role="leadership_role",
            district="D1",
            first_name="Fake",
            last_name="User",
        )
        user_2 = generate_fake_rosters(
            email="supervision_staff@domain.org",
            region_code="US_ID",
            external_id="abc",
            role="supervision_staff",
            district="D3",
            first_name="John",
            last_name="Doe",
        )
        user_1_override = generate_fake_user_overrides(
            email="leadership@domain.org",
            region_code="US_ND",
            external_id="user_1_override.external_id",
            role="user_1_override.role",
            blocked=True,
        )
        default_1 = generate_fake_default_permissions(
            state="US_ND",
            role="leadership_role",
            routes={"A": False, "B": True},
            feature_variants={"C": False},
        )
        default_2 = generate_fake_default_permissions(
            state="US_ND",
            role="user_1_override.role",
            feature_variants={"C": False},
        )
        default_3 = generate_fake_default_permissions(
            state="US_ID",
            role="supervision_staff",
        )
        new_permissions = generate_fake_permissions_overrides(
            email="leadership@domain.org",
            routes={"overridden route": True},
            feature_variants={"overridden variant": True},
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
                    "district": "D1",
                    "emailAddress": "leadership@domain.org",
                    "externalId": "user_1_override.external_id",
                    "firstName": "Fake",
                    "lastName": "User",
                    "role": "user_1_override.role",
                    "stateCode": "US_ND",
                    "routes": {"overridden route": True},
                    "featureVariants": {"overridden variant": True},
                    "userHash": _LEADERSHIP_USER_HASH,
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": False,
                    "district": "D3",
                    "emailAddress": "supervision_staff@domain.org",
                    "externalId": "abc",
                    "firstName": "John",
                    "lastName": "Doe",
                    "role": "supervision_staff",
                    "stateCode": "US_ID",
                    "routes": None,
                    "featureVariants": None,
                    "userHash": _SUPERVISION_STAFF_HASH,
                },
            ]
        response = self.client.get(
            self.users,
            headers=self.headers,
        )
        self.assertEqual(expected, json.loads(response.data))

    def test_users_with_empty_overrides(self) -> None:
        user_1 = generate_fake_rosters(
            email="leadership@domain.org",
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
            routes={"A": True},
        )
        add_entity_to_database_session(self.database_key, [user_1, default])
        with self.app.test_request_context():
            expected = [
                {
                    "allowedSupervisionLocationIds": "4, 10A",
                    "allowedSupervisionLocationLevel": "level_1_supervision_location",
                    "blocked": False,
                    "district": "4, 10A",
                    "emailAddress": "leadership@domain.org",
                    "externalId": "12345",
                    "firstName": "Test A.",
                    "lastName": "User",
                    "role": "leadership_role",
                    "stateCode": "US_MO",
                    "routes": {"A": True},
                    "featureVariants": None,
                    "userHash": _LEADERSHIP_USER_HASH,
                },
            ]
        response = self.client.get(
            self.users,
            headers=self.headers,
        )
        self.assertEqual(expected, json.loads(response.data))

    def test_users_with_null_values(self) -> None:
        user_1 = generate_fake_rosters(
            email="leadership@domain.org",
            region_code="US_ME",
            role="leadership_role",
        )
        applicable_override = generate_fake_user_overrides(
            email="leadership@domain.org",
            region_code="US_ME",
            external_id="A1B2",
            blocked=True,
        )
        default_1 = generate_fake_default_permissions(
            state="US_ME",
            role="leadership_role",
            routes={"A": "B"},
        )
        new_permissions = generate_fake_permissions_overrides(
            email="leadership@domain.org",
            routes={"A": True, "C": False},
            feature_variants={"C": True},
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
                    "district": None,
                    "emailAddress": "leadership@domain.org",
                    "externalId": "A1B2",
                    "firstName": None,
                    "lastName": None,
                    "role": "leadership_role",
                    "stateCode": "US_ME",
                    "routes": {"A": True, "C": False},
                    "featureVariants": {"C": True},
                    "userHash": _LEADERSHIP_USER_HASH,
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
            email="leadership@domain.org",
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
                    "district": "District 4",
                    "emailAddress": "leadership@domain.org",
                    "externalId": "12345",
                    "firstName": "Test A.",
                    "lastName": "User",
                    "role": "leadership_role",
                    "stateCode": "US_CO",
                    "routes": None,
                    "featureVariants": None,
                    "userHash": _LEADERSHIP_USER_HASH,
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
            routes={"A": "B", "B": "C"},
            feature_variants={"D": "E"},
        )
        add_entity_to_database_session(self.database_key, [user_1, default])
        with self.assertLogs(level="INFO") as log:
            self.client.post(
                self.add_user,
                headers=self.headers,
                json={
                    "stateCode": "US_MO",
                    "emailAddress": "parameter@domain.org",
                    "externalId": None,
                    "role": "leadership_role",
                    "district": "1, 2",
                    "firstName": None,
                    "lastName": None,
                    "reason": "test",
                },
            )
            self.assertReasonLog(
                log.output, "adding user parameter@domain.org with reason: test"
            )

        with self.app.test_request_context():
            expected = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": False,
                    "district": "District",
                    "emailAddress": "add_user@domain.org",
                    "externalId": "ABC",
                    "firstName": None,
                    "lastName": None,
                    "role": "leadership_role",
                    "stateCode": "US_CO",
                    "routes": None,
                    "featureVariants": None,
                    "userHash": _ADD_USER_HASH,
                },
                {  # handles MO's specific logic
                    "allowedSupervisionLocationIds": "1, 2",
                    "allowedSupervisionLocationLevel": "level_1_supervision_location",
                    "blocked": False,
                    "district": "1, 2",
                    "emailAddress": "parameter@domain.org",
                    "externalId": None,
                    "firstName": None,
                    "lastName": None,
                    "role": "leadership_role",
                    "stateCode": "US_MO",
                    "routes": {"A": "B", "B": "C"},
                    "featureVariants": {"D": "E"},
                    "userHash": _PARAMETER_USER_HASH,
                },
            ]
        response = self.client.get(
            self.users,
            headers=self.headers,
        )
        self.assertEqual(expected, json.loads(response.data))

    def test_get_user(self) -> None:
        user_1 = generate_fake_rosters(
            email="parameter@domain.org",
            region_code="US_CO",
            external_id="ABC",
            role="leadership_role",
            district="District",
        )
        user_2 = generate_fake_rosters(
            email="user@domain.org",
            region_code="US_CO",
            external_id="XXXX",
            role="supervision_staff",
            district="District",
        )
        default = generate_fake_default_permissions(
            state="US_CO",
            role="leadership_role",
            routes={"A": "B", "B": "C"},
            feature_variants={"D": "E"},
        )
        add_entity_to_database_session(self.database_key, [user_1, user_2, default])

        expected = {
            "allowedSupervisionLocationIds": "",
            "allowedSupervisionLocationLevel": "",
            "blocked": False,
            "district": "District",
            "emailAddress": "parameter@domain.org",
            "externalId": "ABC",
            "firstName": None,
            "lastName": None,
            "role": "leadership_role",
            "stateCode": "US_CO",
            "routes": {"A": "B", "B": "C"},
            "featureVariants": {"D": "E"},
            "userHash": _PARAMETER_USER_HASH,
        }
        response = self.client.get(
            self.user,
            headers=self.headers,
        )
        self.assertEqual(expected, json.loads(response.data))

    def test_get_user_not_found(self) -> None:
        user = generate_fake_rosters(
            email="user@domain.org",
            region_code="US_CO",
            external_id="XXXX",
            role="supervision_staff",
            district="District",
        )

        add_entity_to_database_session(self.database_key, [user])

        response = self.client.get(
            self.user,
            headers=self.headers,
        )
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)
        error_message = f"User not found for email address hash {_PARAMETER_USER_HASH}, please file a bug"
        self.assertEqual(error_message, response.data.decode("UTF-8"))

    def test_add_user_bad_request(self) -> None:
        no_state = self.client.post(
            self.add_user,
            headers=self.headers,
            json={
                "stateCode": None,
                "emailAddress": "parameter@domain.org",
                "externalId": "XYZ",
                "role": "leadership_role",
                "district": "D1",
                "firstName": "Test",
                "lastName": "User",
                "reason": "test",
            },
        )
        self.assertEqual(HTTPStatus.BAD_REQUEST, no_state.status_code)
        wrong_type = self.client.post(
            self.add_user,
            headers=self.headers,
            json={
                "stateCode": "US_ID",
                "emailAddress": "parameter@domain.org",
                "externalId": "XYZ",
                "role": {"A": "B"},
                "district": "D1",
                "firstName": "Test",
                "lastName": "User",
                "reason": "test",
            },
        )
        self.assertEqual(HTTPStatus.BAD_REQUEST, wrong_type.status_code)

    def test_add_user_repeat_email(self) -> None:
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            user_override_user = self.client.post(
                self.add_user,
                headers=self.headers,
                json={
                    "stateCode": "US_ID",
                    "emailAddress": "parameter@domain.org",
                    "externalId": "XYZ",
                    "role": "leadership_role",
                    "district": "D1",
                    "firstName": "Test",
                    "lastName": "User",
                    "reason": "Test",
                },
            )
            expected = {  # no default permissions
                "district": "D1",
                "emailAddress": "parameter@domain.org",
                "externalId": "XYZ",
                "firstName": "Test",
                "lastName": "User",
                "role": "leadership_role",
                "stateCode": "US_ID",
                "userHash": _PARAMETER_USER_HASH,
            }
            self.assertEqual(HTTPStatus.OK, user_override_user.status_code)
            self.assertEqual(expected, json.loads(user_override_user.data))
            self.assertReasonLog(
                log.output, "adding user parameter@domain.org with reason: Test"
            )
            repeat_user_override_user = self.client.post(
                self.add_user,
                headers=self.headers,
                json={
                    "stateCode": "US_ND",
                    "emailAddress": "parameter@domain.org",
                    "externalId": None,
                    "role": "leadership_role",
                    "district": None,
                    "firstName": None,
                    "lastName": None,
                    "reason": "Test",
                },
            )
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
                    "emailAddress": "parameter@domain.org",
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

    def test_upload_roster(self) -> None:
        with open(os.path.join(_FIXTURE_PATH, "us_xx_roster.csv"), "rb") as fixture:
            file = FileStorage(fixture)
            data = {"file": file, "reason": "test"}

            with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
                # Create associated default permissions by role
                leadership_default = generate_fake_default_permissions(
                    state="US_XX",
                    role="leadership_role",
                    routes={"A": True},
                )
                supervision_staff_default = generate_fake_default_permissions(
                    state="US_XX",
                    role="supervision_staff",
                    routes={"B": True},
                )
                add_entity_to_database_session(
                    self.database_key, [leadership_default, supervision_staff_default]
                )

                self.client.put(
                    self.upload_roster("us_xx"),
                    headers=self.headers,
                    data=data,
                    follow_redirects=True,
                    content_type="multipart/form-data",
                )
                self.assertReasonLog(
                    log.output,
                    "uploading roster for state us_xx with reason: test",
                )
                expected = [
                    {
                        "allowedSupervisionLocationIds": "",
                        "allowedSupervisionLocationLevel": "",
                        "blocked": False,
                        "district": "",
                        "emailAddress": "leadership@domain.org",
                        "externalId": "3975",
                        "firstName": "leadership",
                        "lastName": "user",
                        "role": "leadership_role",
                        "stateCode": "US_XX",
                        "routes": {"A": True},
                        "featureVariants": None,
                        "userHash": _LEADERSHIP_USER_HASH,
                    },
                    {
                        "allowedSupervisionLocationIds": "",
                        "allowedSupervisionLocationLevel": "",
                        "blocked": False,
                        "district": "",
                        "emailAddress": "supervision_staff@domain.org",
                        "externalId": "3706",
                        "firstName": "supervision",
                        "lastName": "user",
                        "role": "supervision_staff",
                        "stateCode": "US_XX",
                        "routes": {"B": True},
                        "featureVariants": None,
                        "userHash": _SUPERVISION_STAFF_HASH,
                    },
                ]
            response = self.client.get(
                self.users,
                headers=self.headers,
            )
            self.assertEqual(expected, json.loads(response.data))

    def test_upload_roster_with_missing_email_address(self) -> None:
        roster_leadership_user = generate_fake_rosters(
            email="leadership@domain.org",
            region_code="US_XX",
            role="leadership_role",
            external_id="0000",
            district="",
        )
        add_entity_to_database_session(self.database_key, [roster_leadership_user])
        with open(
            os.path.join(_FIXTURE_PATH, "us_xx_roster_missing_email.csv"), "rb"
        ) as fixture:
            file = FileStorage(fixture)
            data = {"file": file, "reason": "test"}

            with self.app.test_request_context():
                response = self.client.put(
                    self.upload_roster("us_xx"),
                    headers=self.headers,
                    data=data,
                    follow_redirects=True,
                    content_type="multipart/form-data",
                )
                self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
                error_message = (
                    "Roster contains a row that is missing an email address (required)"
                )
                self.assertEqual(error_message, response.data.decode("UTF-8"))

                # Existing rows should not have been deleted
                expected = [
                    {
                        "allowedSupervisionLocationIds": "",
                        "allowedSupervisionLocationLevel": "",
                        "blocked": False,
                        "district": "",
                        "emailAddress": "leadership@domain.org",
                        "externalId": "0000",
                        "firstName": None,
                        "lastName": None,
                        "role": "leadership_role",
                        "stateCode": "US_XX",
                        "routes": None,
                        "featureVariants": None,
                        "userHash": _LEADERSHIP_USER_HASH,
                    },
                ]
                response = self.client.get(
                    self.users,
                    headers=self.headers,
                )
                self.assertEqual(expected, json.loads(response.data))

    def test_upload_roster_with_missing_associated_role(self) -> None:
        roster_leadership_user = generate_fake_rosters(
            email="leadership@domain.org",
            region_code="US_XX",
            role="leadership_role",
            external_id="0000",  # This should not change because user is not updated
            district="",
        )

        add_entity_to_database_session(self.database_key, [roster_leadership_user])
        with open(os.path.join(_FIXTURE_PATH, "us_xx_roster.csv"), "rb") as fixture:
            file = FileStorage(fixture)
            data = {"file": file, "reason": "test"}

            with self.app.test_request_context():
                response = self.client.put(
                    self.upload_roster("us_xx"),
                    headers=self.headers,
                    data=data,
                    follow_redirects=True,
                    content_type="multipart/form-data",
                )
                self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
                error_message = "Roster contains a row that with a role that does not exist in the default state role permissions. Offending row has email supervision_staff@domain.org"
                self.assertEqual(error_message, response.data.decode("UTF-8"))

                # Existing rows should not have been deleted
                expected = [
                    {
                        "allowedSupervisionLocationIds": "",
                        "allowedSupervisionLocationLevel": "",
                        "blocked": False,
                        "district": "",
                        "emailAddress": "leadership@domain.org",
                        "externalId": "0000",
                        "firstName": None,
                        "lastName": None,
                        "role": "leadership_role",
                        "stateCode": "US_XX",
                        "routes": None,
                        "featureVariants": None,
                        "userHash": _LEADERSHIP_USER_HASH,
                    },
                ]
                response = self.client.get(
                    self.users,
                    headers=self.headers,
                )
                self.assertEqual(expected, json.loads(response.data))

    def test_upload_roster_update_user(self) -> None:
        roster_leadership_user = generate_fake_rosters(
            email="leadership@domain.org",
            region_code="US_XX",
            role="leadership_role",
            external_id="0000",  # This should change with the new upload
            district="",
        )
        # This user will not change
        roster_supervision_staff = generate_fake_rosters(
            email="supervision_staff@domain.org",
            region_code="US_XX",
            role="supervision_staff",
            district="",
        )
        # Create associated default permissions by role
        leadership_default = generate_fake_default_permissions(
            state="US_XX",
            role="leadership_role",
            routes={"A": True},
        )
        supervision_staff_default = generate_fake_default_permissions(
            state="US_XX",
            role="supervision_staff",
            routes={"B": True},
        )
        add_entity_to_database_session(
            self.database_key,
            [
                roster_leadership_user,
                roster_supervision_staff,
                leadership_default,
                supervision_staff_default,
            ],
        )

        with open(
            os.path.join(_FIXTURE_PATH, "us_xx_roster_leadership_only.csv"), "rb"
        ) as fixture:
            file = FileStorage(fixture)
            data = {"file": file, "reason": "test"}

            with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
                self.client.put(
                    self.upload_roster("us_xx"),
                    headers=self.headers,
                    data=data,
                    follow_redirects=True,
                    content_type="multipart/form-data",
                )
                self.assertReasonLog(
                    log.output,
                    "uploading roster for state us_xx with reason: test",
                )
                expected = [
                    {
                        "allowedSupervisionLocationIds": "",
                        "allowedSupervisionLocationLevel": "",
                        "blocked": False,
                        "district": "",
                        "emailAddress": "leadership@domain.org",
                        "externalId": "3975",
                        "firstName": "leadership",
                        "lastName": "user",
                        "role": "leadership_role",
                        "stateCode": "US_XX",
                        "routes": {"A": True},
                        "featureVariants": None,
                        "userHash": _LEADERSHIP_USER_HASH,
                    },
                    {
                        "allowedSupervisionLocationIds": "",
                        "allowedSupervisionLocationLevel": "",
                        "blocked": False,
                        "district": "",
                        "emailAddress": "supervision_staff@domain.org",
                        "externalId": None,
                        "firstName": None,
                        "lastName": None,
                        "role": "supervision_staff",
                        "stateCode": "US_XX",
                        "routes": {"B": True},
                        "featureVariants": None,
                        "userHash": _SUPERVISION_STAFF_HASH,
                    },
                ]
            response = self.client.get(
                self.users,
                headers=self.headers,
            )
            self.assertEqual(expected, json.loads(response.data))

    def test_upload_roster_update_user_with_override(self) -> None:
        roster_leadership_user = generate_fake_rosters(
            email="leadership@domain.org",
            region_code="US_XX",
            role="leadership_role",
            external_id="0000",  # This should change with the new upload
            district="",
        )
        # Create associated default permissions by role
        leadership_default = generate_fake_default_permissions(
            state="US_XX",
            role="leadership_role",
            routes={"A": True},
        )
        # Create associated user_override - this should be deleted during the upload
        override = generate_fake_user_overrides(
            email="leadership@domain.org",
            region_code="US_XX",
            role="leadership_role",
            external_id="xxxx",
            district="XYZ",
        )
        add_entity_to_database_session(
            self.database_key,
            [
                roster_leadership_user,
                leadership_default,
                override,
            ],
        )

        with open(
            os.path.join(_FIXTURE_PATH, "us_xx_roster_leadership_only.csv"), "rb"
        ) as fixture:
            file = FileStorage(fixture)
            data = {"file": file, "reason": "test"}

            with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
                response = self.client.get(
                    self.users,
                    headers=self.headers,
                )
                self.client.put(
                    self.upload_roster("us_xx"),
                    headers=self.headers,
                    data=data,
                    follow_redirects=True,
                    content_type="multipart/form-data",
                )
                self.assertReasonLog(
                    log.output,
                    "uploading roster for state us_xx with reason: test",
                )
                expected = [
                    {
                        "allowedSupervisionLocationIds": "",
                        "allowedSupervisionLocationLevel": "",
                        "blocked": False,
                        "district": "",
                        "emailAddress": "leadership@domain.org",
                        "externalId": "3975",
                        "firstName": "leadership",
                        "lastName": "user",
                        "role": "leadership_role",
                        "stateCode": "US_XX",
                        "routes": {"A": True},
                        "featureVariants": None,
                        "userHash": _LEADERSHIP_USER_HASH,
                    },
                ]
            response = self.client.get(
                self.users,
                headers=self.headers,
            )
            self.assertEqual(expected, json.loads(response.data))
            with SessionFactory.using_database(self.database_key) as session:
                existing_user_override = (
                    session.query(UserOverride)
                    .filter(UserOverride.email_address == "leadership@domain.org")
                    .first()
                )
                self.assertEqual(existing_user_override, None)

    def test_update_user_in_roster(self) -> None:
        user = generate_fake_rosters(
            email="parameter@domain.org",
            region_code="US_CO",
            external_id="123",
            role="supervision_staff",
            district="D1",
            first_name="Test",
            last_name="User",
        )
        add_entity_to_database_session(self.database_key, [user])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            new_role = self.client.patch(
                self.update_user,
                headers=self.headers,
                json={
                    "stateCode": "US_CO",
                    "emailAddress": "parameter@domain.org",
                    "role": "leadership_role",
                    "reason": "test",
                },
            )
            expected_user = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": False,
                    "district": "D1",
                    "emailAddress": "parameter@domain.org",
                    "externalId": "123",
                    "firstName": "Test",
                    "lastName": "User",
                    "role": "leadership_role",
                    "stateCode": "US_CO",
                    "routes": None,
                    "featureVariants": None,
                    "userHash": _PARAMETER_USER_HASH,
                }
            ]
            response = self.client.get(
                self.users,
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.OK, new_role.status_code)
            self.assertEqual(expected_user, json.loads(response.data))
            self.assertReasonLog(
                log.output, "updating user parameter@domain.org with reason: test"
            )

    def test_update_user_in_user_override(self) -> None:
        user = generate_fake_user_overrides(
            email="parameter@domain.org",
            region_code="US_TN",
            external_id="Original",
            role="leadership_role",
            first_name="Original",
            last_name="Name",
        )
        add_entity_to_database_session(self.database_key, [user])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            new_name_id = self.client.patch(
                self.update_user,
                headers=self.headers,
                json={
                    "stateCode": "US_TN",
                    "emailAddress": "parameter@domain.org",
                    "externalId": "Updated ID",
                    "firstName": "Updated",
                    "reason": "test",
                },
            )
            expected_user = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": False,
                    "district": None,
                    "emailAddress": "parameter@domain.org",
                    "externalId": "Updated ID",
                    "firstName": "Updated",
                    "lastName": "Name",
                    "role": "leadership_role",
                    "stateCode": "US_TN",
                    "routes": None,
                    "featureVariants": None,
                    "userHash": _PARAMETER_USER_HASH,
                }
            ]
            response = self.client.get(
                self.users,
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.OK, new_name_id.status_code)
            self.assertEqual(expected_user, json.loads(response.data))
            self.assertReasonLog(
                log.output, "updating user parameter@domain.org with reason: test"
            )

    def test_update_user_missing_state_code(self) -> None:
        user = generate_fake_user_overrides(
            email="parameter@domain.org",
            region_code="US_TN",
            external_id="Original",
            role="leadership_role",
            first_name="Original",
            last_name="Name",
        )
        add_entity_to_database_session(self.database_key, [user])
        with self.app.test_request_context():
            response = self.client.patch(
                self.update_user,
                headers=self.headers,
                json={
                    "externalId": "Updated ID",
                    "emailAddress": "parameter@domain.org",
                    "firstName": "Updated",
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_update_user_permissions_roster(self) -> None:
        user = generate_fake_rosters(
            email="user@domain.org",
            region_code="US_CO",
            role="supervision_staff",
        )
        default_co = generate_fake_default_permissions(
            state="US_CO",
            role="supervision_staff",
            routes={"A": True},
        )
        add_entity_to_database_session(self.database_key, [user, default_co])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            update_routes = self.client.put(
                self.update_user_permissions,
                headers=self.headers,
                json={
                    "routes": {
                        "system_prisonToSupervision": "A",
                        "community_practices": "4",
                    },
                    "featureVariants": {
                        "variant1": "true",
                    },
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.OK, update_routes.status_code)
            self.assertReasonLog(
                log.output,
                "updating permissions for user user@domain.org with reason: test",
            )
            wrong_type = self.client.put(
                self.update_user_permissions,
                headers=self.headers,
                json={
                    "routes": "prisonToSupervision",
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, wrong_type.status_code)
            expected_response = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": False,
                    "district": None,
                    "emailAddress": "user@domain.org",
                    "externalId": None,
                    "firstName": None,
                    "lastName": None,
                    "role": "supervision_staff",
                    "stateCode": "US_CO",
                    "routes": {
                        "system_prisonToSupervision": "A",
                        "community_practices": "4",
                    },
                    "featureVariants": {
                        "variant1": "true",
                    },
                    "userHash": _USER_HASH,
                },
            ]
            response = self.client.get(
                self.users,
                headers=self.headers,
            )
            self.assertEqual(expected_response, json.loads(response.data))

    def test_update_user_permissions_user_override(self) -> None:
        added_user = generate_fake_rosters(
            email="user@domain.org",
            region_code="US_TN",
            role="leadership_role",
        )
        default_tn = generate_fake_default_permissions(
            state="US_TN",
            role="leadership_role",
            routes={"A": "B"},
            feature_variants={"C": "D"},
        )
        add_entity_to_database_session(self.database_key, [added_user, default_tn])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            update_tn_access = self.client.put(
                self.update_user_permissions,
                headers=self.headers,
                json={
                    "routes": {"C": "D"},
                    "reason": "test",
                },
            )
            self.assertEqual(
                HTTPStatus.OK, update_tn_access.status_code, update_tn_access.data
            )
            self.assertReasonLog(
                log.output,
                "updating permissions for user user@domain.org with reason: test",
            )
            wrong_type = self.client.put(
                self.update_user_permissions,
                headers=self.headers,
                json={
                    "routes": "Should not be a string",
                    "reason": "test",
                },
            )
            self.assertEqual(
                HTTPStatus.BAD_REQUEST, wrong_type.status_code, update_tn_access.data
            )
            expected_response = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": False,
                    "district": None,
                    "emailAddress": "user@domain.org",
                    "externalId": None,
                    "firstName": None,
                    "lastName": None,
                    "role": "leadership_role",
                    "routes": {"C": "D"},
                    "featureVariants": {"C": "D"},
                    "stateCode": "US_TN",
                    "userHash": _USER_HASH,
                },
            ]
        response = self.client.get(
            self.users,
            headers=self.headers,
        )
        self.assertEqual(expected_response, json.loads(response.data))

    def test_update_user_permissions_override(self) -> None:
        added_user = generate_fake_rosters(
            email="user@domain.org",
            region_code="US_CO",
            role="leadership_role",
        )
        override_permissions = generate_fake_permissions_overrides(
            email="user@domain.org",
            routes={"A": "B"},
            feature_variants={"C": "D"},
        )
        add_entity_to_database_session(
            self.database_key, [added_user, override_permissions]
        )
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            response = self.client.put(
                self.update_user_permissions,
                headers=self.headers,
                json={
                    "routes": {"E": "F"},
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertReasonLog(
                log.output,
                "updating permissions for user user@domain.org with reason: test",
            )
            expected = {
                "emailAddress": "user@domain.org",
                "routes": {"E": "F"},
                "featureVariants": {"C": "D"},
            }
            self.assertEqual(expected, json.loads(response.data))

    def test_delete_user_permissions(self) -> None:
        roster_user = generate_fake_rosters(
            email="user@domain.org",
            region_code="US_MO",
            role="leadership_role",
            district="D1",
        )
        default = generate_fake_default_permissions(
            state="US_MO",
            role="leadership_role",
            routes={"A": "B", "C": "D"},
            feature_variants={"E": "F"},
        )
        roster_user_override_permissions = generate_fake_permissions_overrides(
            email="user@domain.org",
            routes={"A": "B"},
            feature_variants={"C": "D"},
        )
        add_entity_to_database_session(
            self.database_key, [roster_user, default, roster_user_override_permissions]
        )
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            delete_roster_user = self.client.delete(
                self.delete_user_permissions,
                headers=self.headers,
                json={
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.OK, delete_roster_user.status_code)
            self.assertReasonLog(
                log.output,
                "removing custom permissions for user user@domain.org with reason: test",
            )
            expected_response = [
                {
                    "allowedSupervisionLocationIds": "D1",
                    "allowedSupervisionLocationLevel": "level_1_supervision_location",
                    "blocked": False,
                    "district": "D1",
                    "emailAddress": "user@domain.org",
                    "externalId": None,
                    "firstName": None,
                    "lastName": None,
                    "role": "leadership_role",
                    "routes": {"A": "B", "C": "D"},
                    "featureVariants": {"E": "F"},
                    "stateCode": "US_MO",
                    "userHash": _USER_HASH,
                },
            ]
        response = self.client.get(
            self.users,
            headers=self.headers,
        )
        self.assertEqual(expected_response, json.loads(response.data))

    def test_delete_added_user_permissions(self) -> None:
        user = generate_fake_user_overrides(
            email="user@domain.org",
            region_code="US_CO",
            role="leadership_role",
        )
        default = generate_fake_default_permissions(
            state="US_CO",
            role="leadership_role",
            routes={"A": "B", "C": "D"},
            feature_variants={"E": "F", "G": "H"},
        )
        add_entity_to_database_session(self.database_key, [user, default])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            self.client.put(
                self.update_user_permissions,
                headers=self.headers,
                json={
                    "routes": {"A": "B"},
                    "featureVariants": {"E": "F"},
                    "reason": "test",
                },
            )
            delete_roster_user = self.client.delete(
                self.delete_user_permissions,
                headers=self.headers,
                json={
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.OK, delete_roster_user.status_code)
            self.assertReasonLog(
                log.output,
                "removing custom permissions for user user@domain.org with reason: test",
            )
            expected_response = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": False,
                    "district": None,
                    "emailAddress": "user@domain.org",
                    "externalId": None,
                    "firstName": None,
                    "lastName": None,
                    "role": "leadership_role",
                    "routes": {"A": "B", "C": "D"},
                    "featureVariants": {"E": "F", "G": "H"},
                    "stateCode": "US_CO",
                    "userHash": _USER_HASH,
                },
            ]
        response = self.client.get(
            self.users,
            headers=self.headers,
        )
        self.assertEqual(expected_response, json.loads(response.data))

    def test_delete_nonexistent_user_permissions_error(self) -> None:
        user = generate_fake_user_overrides(
            email="user@domain.org",
            region_code="US_CO",
            role="leadership_role",
        )
        add_entity_to_database_session(self.database_key, [user])
        with self.app.test_request_context():
            delete_permissions = self.client.delete(
                self.delete_user_permissions,
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, delete_permissions.status_code)

    def test_delete_user_roster(self) -> None:
        user = generate_fake_rosters(
            email="parameter@domain.org",
            region_code="US_ID",
            role="leadership_role",
        )
        add_entity_to_database_session(self.database_key, [user])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            delete = self.client.delete(
                self.delete_user,
                headers=self.headers,
                json={
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.OK, delete.status_code)
            self.assertReasonLog(
                log.output, "blocking user parameter@domain.org with reason: test"
            )
            response = self.client.get(
                self.users,
                headers=self.headers,
            )
            expected_response = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": True,
                    "district": None,
                    "emailAddress": "parameter@domain.org",
                    "externalId": None,
                    "firstName": None,
                    "lastName": None,
                    "role": "leadership_role",
                    "routes": None,
                    "featureVariants": None,
                    "stateCode": "US_ID",
                    "userHash": _PARAMETER_USER_HASH,
                },
            ]
            self.assertEqual(expected_response, json.loads(response.data))

    def test_delete_user_user_override(self) -> None:
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            self.client.post(
                self.add_user,
                headers=self.headers,
                json={
                    "stateCode": "US_TN",
                    "emailAddress": "parameter@domain.org",
                    "role": "supervision_staff",
                    "reason": "test",
                },
            )
            delete = self.client.delete(
                self.delete_user,
                headers=self.headers,
                json={
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.OK, delete.status_code)
            self.assertReasonLog(
                log.output, "blocking user parameter@domain.org with reason: test"
            )
            response = self.client.get(
                self.users,
                headers=self.headers,
            )
            expected_response = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": True,
                    "district": None,
                    "emailAddress": "parameter@domain.org",
                    "externalId": None,
                    "firstName": None,
                    "lastName": None,
                    "role": "supervision_staff",
                    "routes": None,
                    "featureVariants": None,
                    "stateCode": "US_TN",
                    "userHash": _PARAMETER_USER_HASH,
                },
            ]
            self.assertEqual(expected_response, json.loads(response.data))

    def test_delete_nonexistent_user(self) -> None:
        with self.app.test_request_context():
            delete = self.client.delete(
                self.delete_user,
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, delete.status_code)

    def test_get_states(self) -> None:
        default = generate_fake_default_permissions(
            state="US_MO",
            role="leadership_role",
            routes={"A": "B", "B": "C"},
            feature_variants={"D": "E"},
        )
        add_entity_to_database_session(self.database_key, [default])
        with self.app.test_request_context():
            response = self.client.get(self.states, headers=self.headers)
            expected_response = [
                {
                    "stateCode": "US_MO",
                    "role": "leadership_role",
                    "routes": {"A": "B", "B": "C"},
                    "featureVariants": {"D": "E"},
                },
            ]
            self.assertEqual(expected_response, json.loads(response.data))

    def test_states_add_state_role(self) -> None:
        existing = generate_fake_default_permissions(
            state="US_MO",
            role="leadership_role",
            routes={"route_A": True, "routeB": True, "C": False},
        )
        add_entity_to_database_session(self.database_key, [existing])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            self.client.post(
                self.add_state_role("US_MO", "supervision_staff_role"),
                headers=self.headers,
                json={
                    "routes": {"route_A": True, "routeB": False},
                    "reason": "test",
                },
            )
            self.assertReasonLog(
                log.output,
                "adding permissions for state US_MO, role supervision_staff_role with reason: test",
            )
            expected = [
                {
                    "role": "leadership_role",
                    "stateCode": "US_MO",
                    "routes": {"route_A": True, "routeB": True, "C": False},
                    "featureVariants": None,
                },
                {
                    "role": "supervision_staff_role",
                    "stateCode": "US_MO",
                    "routes": {"route_A": True, "routeB": False},
                    "featureVariants": None,
                },
            ]
            response = self.client.get(
                self.states,
                headers=self.headers,
            )
            self.assertEqual(expected, json.loads(response.data))

    def test_states_add_state_role_missing_routes(self) -> None:
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            self.client.post(
                self.add_state_role("US_MO", "supervision_staff_role"),
                headers=self.headers,
                json={
                    "featureVariants": {"A": True},
                    "reason": "test",
                },
            )
            self.assertReasonLog(
                log.output,
                "adding permissions for state US_MO, role supervision_staff_role with reason: test",
            )
            expected = [
                {
                    "role": "supervision_staff_role",
                    "stateCode": "US_MO",
                    "routes": None,
                    "featureVariants": {"A": True},
                },
            ]
            response = self.client.get(
                self.states,
                headers=self.headers,
            )
            self.assertEqual(expected, json.loads(response.data))

    def test_add_state_existing(self) -> None:
        existing = generate_fake_default_permissions(
            state="US_MO",
            role="leadership_role",
            routes={"A": True, "B": True, "C": False},
        )
        add_entity_to_database_session(self.database_key, [existing])
        with self.app.test_request_context():
            response = self.client.post(
                self.add_state_role("US_MO", "leadership_role"),
                headers=self.headers,
                json={
                    "featureVariants": {"D": True},
                    "routes": {"A": True, "B": False},
                },
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_states_update_state(self) -> None:
        existing = generate_fake_default_permissions(
            state="US_MO",
            role="leadership_role",
            routes={"A": True, "B": True, "C": False},
            feature_variants={"C": True, "D": False},
        )
        add_entity_to_database_session(self.database_key, [existing])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            response = self.client.patch(
                self.update_state_role("US_MO", "leadership_role"),
                headers=self.headers,
                json={
                    "routes": {"C": True, "B": False},
                    "featureVariants": {"D": True, "E": False},
                    "reason": "test",
                },
            )

            expected = {
                "stateCode": "US_MO",
                "role": "leadership_role",
                "routes": {"A": True, "B": False, "C": True},
                "featureVariants": {"C": True, "D": True, "E": False},
            }

            self.assertEqual(expected, json.loads(response.data))
            self.assertReasonLog(
                log.output,
                "updating permissions for state US_MO, role leadership_role with reason: test",
            )

            response = self.client.get(
                self.states,
                headers=self.headers,
            )
            self.assertEqual([expected], json.loads(response.data))

    def test_states_update_state_role_routes_without_existing_routes(self) -> None:
        existing = generate_fake_default_permissions(
            state="US_MO",
            role="leadership_role",
            routes=None,
            feature_variants={"C": True, "D": False},
        )
        add_entity_to_database_session(self.database_key, [existing])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            response = self.client.patch(
                self.update_state_role("US_MO", "leadership_role"),
                headers=self.headers,
                json={
                    "routes": {"C": True, "B": False},
                    "featureVariants": {"D": True, "E": False},
                    "reason": "test",
                },
            )

            expected = {
                "stateCode": "US_MO",
                "role": "leadership_role",
                "routes": {"B": False, "C": True},
                "featureVariants": {"C": True, "D": True, "E": False},
            }

            self.assertEqual(expected, json.loads(response.data))
            self.assertReasonLog(
                log.output,
                "updating permissions for state US_MO, role leadership_role with reason: test",
            )

            response = self.client.get(
                self.states,
                headers=self.headers,
            )
            self.assertEqual([expected], json.loads(response.data))

    def test_states_update_state_code(self) -> None:
        existing = generate_fake_default_permissions(
            state="US_MO",
            role="leadership_role",
            routes={"A": True, "B": True, "C": False},
        )
        add_entity_to_database_session(self.database_key, [existing])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            response = self.client.patch(
                self.update_state_role("US_MO", "leadership_role"),
                headers=self.headers,
                json={
                    "stateCode": "US_TN",
                    "routes": {"C": True, "B": False},
                    "reason": "test",
                },
            )

            expected = {
                "stateCode": "US_TN",
                "role": "leadership_role",
                "routes": {"A": True, "B": False, "C": True},
                "featureVariants": None,
            }

            self.assertEqual(expected, json.loads(response.data))
            self.assertReasonLog(
                log.output,
                "updating permissions for state US_MO, role leadership_role with reason: test",
            )

            response = self.client.get(
                self.states,
                headers=self.headers,
            )
            self.assertEqual([expected], json.loads(response.data))

    def test_states_update_state_no_entry(self) -> None:
        existing = generate_fake_default_permissions(
            state="US_MO",
            role="leadership_role",
            routes={"A": True, "B": True, "C": False},
        )
        add_entity_to_database_session(self.database_key, [existing])
        with self.app.test_request_context():
            response = self.client.patch(
                self.update_state_role("US_MO", "supervision_staff_role"),
                headers=self.headers,
                json={
                    "routes": {"C": True, "B": False},
                },
            )
            self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_states_delete_role_no_entry(self) -> None:
        with self.app.test_request_context():
            response = self.client.delete(
                self.delete_state_role("US_MO", "supervision_staff_role"),
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_states_delete_role_with_active_roster_user(self) -> None:
        state_role = generate_fake_default_permissions(
            state="US_MO",
            role="leadership_role",
            routes={"A": True, "B": True, "C": False},
        )
        user = generate_fake_rosters(
            email="parameter@domain.org",
            region_code="US_MO",
            role="leadership_role",
        )
        add_entity_to_database_session(self.database_key, [state_role, user])
        with self.app.test_request_context():
            response = self.client.delete(
                self.delete_state_role("US_MO", "leadership_role"),
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_states_delete_role_with_active_override_user(self) -> None:
        state_role = generate_fake_default_permissions(
            state="US_MO",
            role="leadership_role",
            routes={"A": True, "B": True, "C": False},
        )
        user = generate_fake_user_overrides(
            email="parameter@domain.org",
            region_code="US_MO",
            role="leadership_role",
        )
        add_entity_to_database_session(self.database_key, [state_role, user])
        with self.app.test_request_context():
            response = self.client.delete(
                self.delete_state_role("US_MO", "leadership_role"),
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_states_delete_role_with_blocked_user(self) -> None:
        state_role_delete = generate_fake_default_permissions(
            state="US_MO",
            role="leadership_role",
            routes={"A": True, "B": True, "C": False},
        )
        state_role_keep = generate_fake_default_permissions(
            state="US_MO",
            role="supervision_staff_role",
            routes={"A": True, "B": False, "C": False},
            feature_variants={"D": True},
        )
        user_delete = generate_fake_rosters(
            email="parameter@domain.org",
            region_code="US_MO",
            role="leadership_role",
        )
        user_keep = generate_fake_rosters(
            email="supervision_staff@domain.org",
            region_code="US_MO",
            role="supervision_staff_role",
        )
        override_user_delete = generate_fake_user_overrides(
            email="parameter@domain.org",
            region_code="US_MO",
            blocked=True,
        )
        override_only_delete = generate_fake_user_overrides(
            email="user@domain.org",
            region_code="US_MO",
            role="leadership_role",
            blocked=True,
        )
        override_keep = generate_fake_user_overrides(
            email="supervision_staff_2@domain.org",
            region_code="US_MO",
            role="supervision_staff_role",
        )
        add_entity_to_database_session(
            self.database_key,
            [
                state_role_delete,
                state_role_keep,
                user_delete,
                user_keep,
                override_user_delete,
                override_only_delete,
                override_keep,
            ],
        )
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            response = self.client.delete(
                self.delete_state_role("US_MO", "leadership_role"),
                headers=self.headers,
                json={"reason": "test"},
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertReasonLog(
                log.output,
                "removing permissions and blocked users for state US_MO, role leadership_role with reason: test",
            )

            # Check that the leadership_role role no longer exists
            response = self.client.get(
                self.states,
                headers=self.headers,
            )
            expected_response = [
                {
                    "stateCode": "US_MO",
                    "role": "supervision_staff_role",
                    "routes": {"A": True, "B": False, "C": False},
                    "featureVariants": {"D": True},
                },
            ]
            self.assertEqual(expected_response, json.loads(response.data))

            # Check that the users with that role no longer exist
            response = self.client.get(
                self.users,
                headers=self.headers,
            )
            expected_users = [
                {
                    "emailAddress": "supervision_staff@domain.org",
                    "role": "supervision_staff_role",
                },
                {
                    "emailAddress": "supervision_staff_2@domain.org",
                    "role": "supervision_staff_role",
                },
            ]
            actual_users = [
                {"emailAddress": user["emailAddress"], "role": user["role"]}
                for user in json.loads(response.data)
            ]
            self.assertCountEqual(expected_users, actual_users)

    @patch("recidiviz.auth.auth_endpoint.SingleCloudTaskQueueManager")
    def test_import_ingested_users_async(self, mock_task_manager: MagicMock) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.import_ingested_users_async,
                headers=self.headers,
                json={
                    "message": {
                        "attributes": {
                            "bucketId": self.bucket,
                            "objectId": f"{self.region_code}/ingested_product_users.csv",
                        },
                    }
                },
            )
            self.assertEqual(b"", response.data)
            self.assertEqual(HTTPStatus.OK, response.status_code)

            mock_task_manager.return_value.create_task.assert_called_with(
                relative_uri=f"/auth{self.import_ingested_users}",
                body={"state_code": self.region_code},
            )

    def test_import_ingested_users_async_invalid_pubsub(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.import_ingested_users_async,
                headers=self.headers,
                json={
                    "message": {
                        "attributes": {},
                    }
                },
            )
            self.assertEqual(b"Invalid Pub/Sub message", response.data)
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    @patch("recidiviz.auth.auth_endpoint.SingleCloudTaskQueueManager")
    def test_import_ingested_users_async_missing_region(
        self, mock_task_manager: MagicMock
    ) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.import_ingested_users_async,
                headers=self.headers,
                json={
                    "message": {
                        "attributes": {
                            "bucketId": self.bucket,
                            "objectId": "ingested_product_users.csv",
                        },
                    }
                },
            )
            self.assertEqual(b"", response.data)
            self.assertEqual(HTTPStatus.OK, response.status_code)

            mock_task_manager.return_value.create_task.assert_not_called()

    @patch("recidiviz.auth.auth_endpoint.SingleCloudTaskQueueManager")
    def test_import_ingested_users_async_invalid_file(
        self, mock_task_manager: MagicMock
    ) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.import_ingested_users_async,
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
            # We return OK from these so that if someone does drop an invalid file in the bucket, we
            # don't end up in a situation where pub/sub keeps retrying the request because we keep
            # returning an error code.
            self.assertEqual(HTTPStatus.OK, response.status_code)

            mock_task_manager.return_value.create_task.assert_not_called()

    def test_import_ingested_users_successful(self) -> None:
        self.fs.upload_from_contents_handle_stream(
            self.ingested_users_gcs_csv_uri,
            contents_handle=LocalFileContentsHandle(
                local_file_path=os.path.join(_FIXTURE_PATH, "us_xx_ingested_users.csv"),
                cleanup_file=False,
            ),
            content_type="text/csv",
        )
        roster_leadership_user = generate_fake_rosters(
            email="leadership@domain.org",
            region_code="US_XX",
            role="leadership_role",
            external_id="0000",  # This should change with the new upload
            district="",
        )
        roster_leadership_user_override = generate_fake_user_overrides(
            email="leadership@domain.org",
            region_code="US_XX",
            first_name="override",  # This should not change with the new upload
        )
        # This user will be deleted
        roster_supervision_staff = generate_fake_rosters(
            email="parameter@domain.org",
            region_code="US_XX",
            role="supervision_staff",
            district="",
        )
        # Create associated default permissions by role
        leadership_default = generate_fake_default_permissions(
            state="US_XX",
            role="leadership_role",
        )
        supervision_staff_default = generate_fake_default_permissions(
            state="US_XX",
            role="supervision_staff",
        )
        add_entity_to_database_session(
            self.database_key,
            [
                roster_leadership_user,
                roster_leadership_user_override,
                roster_supervision_staff,
                leadership_default,
                supervision_staff_default,
            ],
        )

        with self.app.test_request_context():
            response = self.client.post(
                self.import_ingested_users,
                headers=self.headers,
                json={
                    "state_code": "US_XX",
                },
            )
            self.assertEqual(HTTPStatus.OK, response.status_code, response.data)
            self.assertEqual(
                b"CSV US_XX/ingested_product_users.csv successfully imported to "
                b"Cloud SQL schema SchemaType.CASE_TRIAGE for region code US_XX",
                response.data,
            )
            expected = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": False,
                    "district": None,
                    "emailAddress": "leadership@domain.org",
                    "externalId": None,
                    "firstName": "override",
                    "lastName": "user",
                    "role": "leadership_role",
                    "stateCode": "US_XX",
                    "routes": None,
                    "featureVariants": None,
                    "userHash": _LEADERSHIP_USER_HASH,
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": False,
                    "district": "D1",
                    "emailAddress": "supervision_staff@domain.org",
                    "externalId": "3706",
                    "firstName": "supervision",
                    "lastName": "user",
                    "role": "supervision_staff",
                    "stateCode": "US_XX",
                    "routes": None,
                    "featureVariants": None,
                    "userHash": _SUPERVISION_STAFF_HASH,
                },
            ]
            response = self.client.get(
                self.users,
                headers=self.headers,
            )
            self.assertEqual(expected, json.loads(response.data))

    def test_import_ingested_users_missing_region_code(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.import_ingested_users,
                headers=self.headers,
                json={},
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Missing state_code param",
                response.data,
            )

    def test_import_ingested_users_invalid_state_code(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.import_ingested_users,
                headers=self.headers,
                json={
                    "state_code": "MO",
                },
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Unknown state_code [MO] received, must be a valid state code.",
                response.data,
            )

    @patch(
        "recidiviz.auth.auth_endpoint.GcsfsCsvReader",
        side_effect=Exception("Error while reading CSV"),
    )
    def test_import_ingested_users_exception(self, _mock_csv_reader: MagicMock) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.import_ingested_users,
                headers=self.headers,
                json={
                    "state_code": "US_MO",
                },
            )
            self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)
            self.assertEqual(
                b"Error while reading CSV",
                response.data,
            )
