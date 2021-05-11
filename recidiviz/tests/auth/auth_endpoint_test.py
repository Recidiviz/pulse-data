# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
import os
from http import HTTPStatus
from typing import Any, Dict
from unittest import TestCase
from unittest.mock import patch, MagicMock, call

import flask
from flask import Flask

from recidiviz.auth.auth_endpoint import auth_endpoint_blueprint
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789"))
@patch(
    "recidiviz.utils.validate_jwt.validate_iap_jwt_from_app_engine",
    MagicMock(return_value=("test-user", "test-user@recidiviz.org", None)),
)
class AuthEndpointTests(TestCase):
    """ Integration tests of our flask auth endpoints """

    def setUp(self) -> None:
        self.app = Flask(__name__)
        self.app.register_blueprint(auth_endpoint_blueprint)
        self.app.config["TESTING"] = True

        self.client = self.app.test_client()
        self.headers: Dict[str, Dict[Any, Any]] = {"x-goog-iap-jwt-assertion": {}}
        self.auth0_client_patcher = patch("recidiviz.auth.auth_endpoint.Auth0Client")
        self.gcsfs_patcher = patch("recidiviz.auth.auth_endpoint.GcsfsFactory.build")
        self.fake_gcsfs = FakeGCSFileSystem()
        self.mock_gcsfs = self.gcsfs_patcher.start()
        self.mock_gcsfs.return_value = self.fake_gcsfs
        self.mock_auth0_client = MagicMock()
        self.auth0_client_patcher.start().return_value = self.mock_auth0_client
        self.region_code = "US_MO"
        self.bucket = "recidiviz-test-dashboard-user-restrictions"
        self.filename = "supervision_location_restricted_access_emails.json"
        self.query_params = {
            "bucket": self.bucket,
            "region_code": self.region_code,
            "filename": self.filename,
        }
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
            self.update_auth0_user_metadata_url = flask.url_for(
                "auth_endpoint_blueprint.update_auth0_user_metadata"
            )

    def tearDown(self) -> None:
        self.auth0_client_patcher.stop()
        self.gcsfs_patcher.stop()

    def test_update_auth0_user_metadata_should_update(self) -> None:
        with self.app.test_request_context():
            self.mock_auth0_client.get_all_users_by_email_addresses.return_value = [
                {
                    "email": "test-user+0@test.org",
                    "user_id": "0",
                    "app_metadata": {"allowed_supervision_location_ids": ["11", "EP"]},
                },
                {
                    "email": "test-user+1@test.org",
                    "user_id": "1",
                    "app_metadata": {"allowed_supervision_location_ids": ["44", "23"]},
                },
                {"email": "test-user+2@test.org", "user_id": "2", "app_metadata": {}},
            ]

            response = self.client.get(
                self.update_auth0_user_metadata_url,
                headers=self.headers,
                query_string={**self.query_params},
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

    def test_update_auth0_user_metadata_invalid_query_params(self) -> None:
        with self.app.test_request_context():
            response = self.client.get(
                self.update_auth0_user_metadata_url,
                headers=self.headers,
                query_string={**self.query_params, "filename": "invalid_filename"},
            )

            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Auth endpoint update_auth0_user_metadata called with unexpected filename: invalid_filename",
                response.data,
            )

    def test_update_auth0_user_metadata_invalid_gcs_path(self) -> None:
        with self.app.test_request_context():
            response = self.client.get(
                self.update_auth0_user_metadata_url,
                headers=self.headers,
                query_string={**self.query_params, "region_code": "US_PA"},
            )

            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"GCS path does not exist: bucket=recidiviz-test-dashboard-user-restrictions, region_code=US_PA, "
                b"filename=supervision_location_restricted_access_emails.json",
                response.data,
            )

    def test_update_auth0_user_metadata_no_users_returned(self) -> None:
        with self.app.test_request_context():
            self.mock_auth0_client.get_all_users_by_email_addresses.return_value = []
            response = self.client.get(
                self.update_auth0_user_metadata_url,
                headers=self.headers,
                query_string={**self.query_params},
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
                query_string={**self.query_params},
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
