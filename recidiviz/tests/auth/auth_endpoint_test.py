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
from typing import Any, Dict, Optional
from unittest import TestCase, mock
from unittest.mock import MagicMock, call, patch

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
    add_users_to_database_session,
    generate_fake_user_restrictions,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tools.postgres import local_postgres_helpers


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789"))
@patch(
    "recidiviz.utils.validate_jwt.validate_iap_jwt_from_app_engine",
    MagicMock(return_value=("test-user", "test-user@recidiviz.org", None)),
)
@pytest.mark.uses_db
class AuthEndpointTests(TestCase):
    """ Integration tests of our flask auth endpoints """

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
        self.filename = "supervision_location_restricted_access_emails.json"
        self.update_query_params = {
            "bucket": self.bucket,
            "region_code": self.region_code,
            "filename": self.filename,
        }
        self.gcs_csv_uri = GcsfsFilePath.from_absolute_path(
            f"{self.bucket}/{self.region_code}/supervision_location_restricted_access_emails.csv"
        )
        self.columns = [col.name for col in DashboardUserRestrictions.__table__.columns]

        # Setup database
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        # Mock Auth0Client
        self.auth0_client_patcher = patch("recidiviz.auth.auth_endpoint.Auth0Client")
        self.mock_auth0_client = MagicMock()
        self.auth0_client_patcher.start().return_value = self.mock_auth0_client

        # Mock GcsfsFactory
        self.gcsfs_patcher = patch("recidiviz.auth.auth_endpoint.GcsfsFactory.build")
        self.mock_gcsfs = self.gcsfs_patcher.start()
        self.fake_gcsfs = FakeGCSFileSystem()
        self.mock_gcsfs.return_value = self.fake_gcsfs
        path = GcsfsFilePath.from_absolute_path(
            f"{self.bucket}/{self.region_code}/{self.filename}"
        )
        self.fake_gcsfs.test_add_path(
            path=path,
            local_path=os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "test_user_restrictions.jsonl",
            ),
        )

        with self.app.test_request_context():
            self.import_user_restrictions_csv_to_sql_url = flask.url_for(
                "auth_endpoint_blueprint.import_user_restrictions_csv_to_sql"
            )
            self.dashboard_user_restrictions_by_email_url = flask.url_for(
                "auth_endpoint_blueprint.dashboard_user_restrictions_by_email"
            )
            self.update_auth0_user_metadata_url = flask.url_for(
                "auth_endpoint_blueprint.update_auth0_user_metadata"
            )

    def tearDown(self) -> None:
        self.auth0_client_patcher.stop()
        self.gcsfs_patcher.stop()
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @patch("recidiviz.auth.auth_endpoint.import_gcs_csv_to_cloud_sql")
    def test_import_user_restrictions_csv_to_sql_successful(
        self, mock_import_csv: MagicMock
    ) -> None:
        with self.app.test_request_context():
            response = self.client.get(
                self.import_user_restrictions_csv_to_sql_url,
                headers=self.headers,
                query_string={
                    "region_code": self.region_code,
                },
            )
            mock_import_csv.assert_called_with(
                schema_type=SchemaType.CASE_TRIAGE,
                destination_table=DashboardUserRestrictions.__tablename__,
                gcs_uri=self.gcs_csv_uri,
                columns=self.columns,
                region_code=self.region_code,
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(
                b"CSV US_MO/supervision_location_restricted_access_emails.csv successfully imported to "
                b"Cloud SQL schema SchemaType.CASE_TRIAGE for region code US_MO",
                response.data,
            )

    def test_import_user_restrictions_csv_to_sql_missing_region_code(self) -> None:
        with self.app.test_request_context():
            response = self.client.get(
                self.import_user_restrictions_csv_to_sql_url,
                headers=self.headers,
                query_string={},
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Missing region_code param",
                response.data,
            )

    def test_import_user_restrictions_csv_to_sql_invalid_region_code(self) -> None:
        with self.app.test_request_context():
            response = self.client.get(
                self.import_user_restrictions_csv_to_sql_url,
                headers=self.headers,
                query_string={
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
            response = self.client.get(
                self.import_user_restrictions_csv_to_sql_url,
                headers=self.headers,
                query_string={
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
        add_users_to_database_session(self.database_key, [user_1, user_2])

        with self.app.test_request_context():
            expected_restrictions = {
                "allowed_supervision_location_ids": ["1", "2"],
                "allowed_supervision_location_level": "level_1_supervision_location",
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
        "recidiviz.auth.auth_endpoint.SessionFactory.for_database",
        side_effect=Exception("Session error"),
    )
    def test_dashboard_user_restrictions_by_email_internal_error(
        self, mockSession: MagicMock
    ) -> None:
        with self.app.test_request_context():
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

    def test_update_auth0_user_metadata_should_update_level(self) -> None:
        with self.app.test_request_context():
            self.mock_auth0_client.get_all_users_by_email_addresses.return_value = [
                {
                    "email": "test-user+0@test.org",
                    "user_id": "0",
                    "app_metadata": {
                        "allowed_supervision_location_ids": ["11", "EP"],
                        "allowed_supervision_location_level": "level_1_supervision_location",
                    },
                },
                {
                    "email": "test-user+1@test.org",
                    "user_id": "1",
                    "app_metadata": {
                        "allowed_supervision_location_ids": ["23"],
                        "allowed_supervision_location_level": "level_2_supervision_location",
                    },
                },
            ]

            response = self.client.get(
                self.update_auth0_user_metadata_url,
                headers=self.headers,
                query_string={**self.update_query_params},
            )
            self.mock_auth0_client.update_user_app_metadata.assert_has_calls(
                [
                    call(
                        user_id="1",
                        app_metadata={
                            "allowed_supervision_location_ids": ["23"],
                            "allowed_supervision_location_level": "level_1_supervision_location",
                        },
                    ),
                ]
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(
                b"Finished updating 1 auth0 users with restrictions for region US_MO",
                response.data,
            )

    def test_update_auth0_user_metadata_should_update_ids(self) -> None:
        with self.app.test_request_context():
            self.mock_auth0_client.get_all_users_by_email_addresses.return_value = [
                {
                    "email": "test-user+0@test.org",
                    "user_id": "0",
                    "app_metadata": {
                        "allowed_supervision_location_ids": ["11", "EP"],
                        "allowed_supervision_location_level": "level_1_supervision_location",
                    },
                },
                {
                    "email": "test-user+1@test.org",
                    "user_id": "1",
                    "app_metadata": {
                        "allowed_supervision_location_ids": ["44", "23"],
                        "allowed_supervision_location_level": "level_1_supervision_location",
                    },
                },
                {"email": "test-user+2@test.org", "user_id": "2", "app_metadata": {}},
            ]

            response = self.client.get(
                self.update_auth0_user_metadata_url,
                headers=self.headers,
                query_string={**self.update_query_params},
            )
            self.mock_auth0_client.update_user_app_metadata.assert_has_calls(
                [
                    call(
                        user_id="1",
                        app_metadata={
                            "allowed_supervision_location_ids": ["23"],
                            "allowed_supervision_location_level": "level_1_supervision_location",
                        },
                    ),
                    call(
                        user_id="2",
                        app_metadata={
                            "allowed_supervision_location_ids": ["11", "EP", "4E"],
                            "allowed_supervision_location_level": "level_1_supervision_location",
                        },
                    ),
                ]
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(
                b"Finished updating 2 auth0 users with restrictions for region US_MO",
                response.data,
            )

    def test_update_auth0_user_metadata_invalid_filename(self) -> None:
        with self.app.test_request_context():
            response = self.client.get(
                self.update_auth0_user_metadata_url,
                headers=self.headers,
                query_string={
                    **self.update_query_params,
                    "filename": "invalid_filename",
                },
            )

            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Auth endpoint update_auth0_user_metadata called with unexpected filename: invalid_filename",
                response.data,
            )

    def test_update_auth0_user_metadata_invalid_region_code(self) -> None:
        with self.app.test_request_context():
            for region_code in ["US_ZZ", "not a region code"]:
                response = self.client.get(
                    self.update_auth0_user_metadata_url,
                    headers=self.headers,
                    query_string={
                        **self.update_query_params,
                        "region_code": region_code,
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

    def test_update_auth0_user_metadata_invalid_gcs_path(self) -> None:
        with self.app.test_request_context():
            response = self.client.get(
                self.update_auth0_user_metadata_url,
                headers=self.headers,
                query_string={**self.update_query_params, "region_code": "US_PA"},
            )

            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"GCS path does not exist: bucket=test-project-dashboard-user-restrictions, region_code=US_PA, "
                b"filename=supervision_location_restricted_access_emails.json",
                response.data,
            )

    def test_update_auth0_user_metadata_no_users_returned(self) -> None:
        with self.app.test_request_context():
            self.mock_auth0_client.get_all_users_by_email_addresses.return_value = []
            response = self.client.get(
                self.update_auth0_user_metadata_url,
                headers=self.headers,
                query_string={**self.update_query_params},
            )

            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(
                b"Finished updating 0 auth0 users with restrictions for region US_MO",
                response.data,
            )

    def test_update_auth0_user_metadata_auth0_error(self) -> None:
        self.mock_auth0_client.get_all_users_by_email_addresses.side_effect = Exception(
            "Auth0Error"
        )
        with self.app.test_request_context() and self.assertLogs(level="ERROR"):
            response = self.client.get(
                self.update_auth0_user_metadata_url,
                headers=self.headers,
                query_string={**self.update_query_params},
            )
            self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)
            self.assertEqual(
                b"Error using Auth0 management API to update users: Auth0Error",
                response.data,
            )

    def test_update_auth0_user_metadata_with_users_returned(self) -> None:
        with self.app.test_request_context():
            query_params = {
                "bucket": self.bucket,
                "region_code": self.region_code,
                "filename": self.filename,
            }

            self.mock_auth0_client.get_all_users_by_email_addresses.return_value = [
                {"email": "test-user+0@test.org", "user_id": "0"},
                {"email": "test-user+1@test.org", "user_id": "1"},
                {"email": "test-user+2@test.org", "user_id": "2"},
                {"email": "test-user+3@test.org", "user_id": "3"},
                {"email": "test-user+4@test.org", "user_id": "4"},
            ]

            response = self.client.get(
                self.update_auth0_user_metadata_url,
                headers=self.headers,
                query_string={**query_params},
            )

            self.mock_auth0_client.get_all_users_by_email_addresses.assert_called_with(
                [
                    "test-user+0@test.org",
                    "test-user+1@test.org",
                    "test-user+2@test.org",
                    "test-user+3@test.org",
                    "test-user+4@test.org",
                ]
            )

            self.mock_auth0_client.update_user_app_metadata.assert_has_calls(
                [
                    call(
                        user_id="0",
                        app_metadata={
                            "allowed_supervision_location_ids": ["11", "EP"],
                            "allowed_supervision_location_level": "level_1_supervision_location",
                        },
                    ),
                    call(
                        user_id="1",
                        app_metadata={
                            "allowed_supervision_location_ids": ["23"],
                            "allowed_supervision_location_level": "level_1_supervision_location",
                        },
                    ),
                    call(
                        user_id="2",
                        app_metadata={
                            "allowed_supervision_location_ids": ["11", "EP", "4E"],
                            "allowed_supervision_location_level": "level_1_supervision_location",
                        },
                    ),
                    call(
                        user_id="3",
                        app_metadata={
                            "allowed_supervision_location_ids": ["12", "54"],
                            "allowed_supervision_location_level": "level_1_supervision_location",
                        },
                    ),
                    call(
                        user_id="4",
                        app_metadata={
                            "allowed_supervision_location_ids": ["4P"],
                            "allowed_supervision_location_level": "level_1_supervision_location",
                        },
                    ),
                ]
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertEqual(
                b"Finished updating 5 auth0 users with restrictions for region US_MO",
                response.data,
            )
