# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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

"""Tests for auth/auth_users_endpoint.py."""
import json
import os
from http import HTTPStatus
from typing import Any, Dict, List, Optional
from unittest import TestCase
from unittest.mock import MagicMock, patch

import flask
import pytest
from flask import Flask
from flask_smorest import Api
from flask_sqlalchemy_session import current_session
from werkzeug.datastructures import FileStorage

from recidiviz.auth.auth_endpoint import auth_endpoint_blueprint
from recidiviz.auth.auth_users_endpoint import users_blueprint
from recidiviz.persistence.database.schema.case_triage.schema import UserOverride
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.tests.auth.auth_endpoint_test import _FIXTURE_PATH
from recidiviz.tests.auth.helpers import (
    add_entity_to_database_session,
    generate_fake_default_permissions,
    generate_fake_permissions_overrides,
    generate_fake_rosters,
    generate_fake_user_overrides,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers

_PARAMETER_USER_HASH = "flf+tuxZFuMOTgZf8aIZiDj/a4Cw4tIwRl7WcpVdCA0="
_ADD_USER_HASH = "0D1WiekUDUBhjVnqyNbbwGJP2xll0CS9vfsnPrxnmSE="
_LEADERSHIP_USER_HASH = "qKTCaVmWmjqbJX0SckE082QJKv6sE4W/bKzfHQZJNYk="
_SUPERVISION_STAFF_HASH = "EghmFPYcNI/RKWs9Cdt3P5nvGFhwM/uSkKKY1xVibvI="


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789"))
@patch(
    "recidiviz.utils.validate_jwt.validate_iap_jwt_from_app_engine",
    MagicMock(return_value=("test-user", "test-user@recidiviz.org", None)),
)
@pytest.mark.uses_db
class AuthUsersEndpointTestCase(TestCase):
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
        self.maxDiff = None
        self.app = Flask(__name__)
        self.app.register_blueprint(auth_endpoint_blueprint)
        self.app.config["TESTING"] = True
        api = Api(
            self.app,
            spec_kwargs={
                "title": "default",
                "version": "1.0.0",
                "openapi_version": "3.1.0",
            },
        )
        api.register_blueprint(users_blueprint, url_prefix="/auth/users")
        self.client = self.app.test_client()

        self.headers: Dict[str, Dict[Any, Any]] = {"x-goog-iap-jwt-assertion": {}}

        # Setup database
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars()
        )
        db_url = local_persistence_helpers.postgres_db_url_from_env_vars()
        engine = setup_scoped_sessions(self.app, SchemaType.CASE_TRIAGE, db_url)
        self.database_key.declarative_meta.metadata.create_all(engine)

        with self.app.test_request_context():
            self.users = lambda state_code=None: flask.url_for(
                "users.UsersAPI", state_code=state_code
            )
            self.user = flask.url_for(
                "users.UsersByHashAPI",
                user_hash=_PARAMETER_USER_HASH,
            )

    def tearDown(self) -> None:
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    def assertReasonLog(self, log_messages: List[str], expected: str) -> None:
        self.assertIn(
            f"INFO:root:State User Permissions: [test-user@recidiviz.org] is {expected}",
            log_messages,
        )

    ########
    # GET /users
    ########

    def test_get_users_some_overrides(self) -> None:
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
        with self.app.test_request_context():
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
        self.assertEqual(expected, json.loads(response.data))

    def test_get_users_with_empty_overrides(self) -> None:
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
        with self.app.test_request_context():
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
        self.assertEqual(expected, json.loads(response.data))

    def test_get_users_with_null_values(self) -> None:
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
            routes={"B": True},
        )
        new_permissions = generate_fake_permissions_overrides(
            email="leadership@domain.org",
            routes={"A": True, "C": False},
            feature_variants={"C": True},
        )
        add_entity_to_database_session(
            self.database_key, [user_1, applicable_override, default_1, new_permissions]
        )
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
        with self.app.test_request_context():
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
        self.assertEqual(expected, json.loads(response.data))

    def test_get_users_no_users(self) -> None:
        expected_restrictions: list[str] = []
        with self.app.test_request_context():
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
        self.assertEqual(expected_restrictions, json.loads(response.data))

    def test_get_users_no_permissions(self) -> None:
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
        with self.app.test_request_context():
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
        self.assertEqual(expected, json.loads(response.data))

    ########
    # GET /user/...
    ########

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
            routes={"A": True, "B": False},
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
            "routes": {"A": True, "B": False},
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
        self.assertEqual(error_message, json.loads(response.data)["message"])

    ########
    # POST /users
    ########

    def test_add_user(self) -> None:
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
            routes={"A": True, "B": False},
            feature_variants={"D": "E"},
        )
        add_entity_to_database_session(self.database_key, [user_1, default])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            self.client.post(
                self.users(),
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
                    "routes": {"A": True, "B": False},
                    "featureVariants": {"D": "E"},
                    "userHash": _PARAMETER_USER_HASH,
                },
            ]
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
        self.assertEqual(expected, json.loads(response.data))

    def test_add_user_bad_request(self) -> None:
        with self.app.test_request_context():
            no_state = self.client.post(
                self.users(),
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
                self.users(),
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
                self.users(),
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
            self.assertEqual(
                HTTPStatus.OK, user_override_user.status_code, user_override_user.data
            )
            self.assertEqual(expected, json.loads(user_override_user.data))
            self.assertReasonLog(
                log.output, "adding user parameter@domain.org with reason: Test"
            )
            repeat_user_override_user = self.client.post(
                self.users(),
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
                HTTPStatus.UNPROCESSABLE_ENTITY,
                repeat_user_override_user.status_code,
                repeat_user_override_user.data,
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
                self.users(),
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
            HTTPStatus.UNPROCESSABLE_ENTITY,
            repeat_roster_user.status_code,
            repeat_roster_user.data,
        )

    ########
    # PUT /users
    ########

    def test_upload_roster(self) -> None:
        with open(
            os.path.join(_FIXTURE_PATH, "us_xx_roster.csv"), "rb"
        ) as fixture, self.app.test_request_context(), self.assertLogs(
            level="INFO"
        ) as log:
            file = FileStorage(fixture)
            data = {"file": file, "reason": "test"}

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

            resp = self.client.put(
                self.users("us_xx"),
                headers=self.headers,
                data=data,
                follow_redirects=True,
                content_type="multipart/form-data",
            )
            self.assertEqual(resp.status_code, HTTPStatus.OK, resp.data)
            self.assertReasonLog(
                log.output,
                "uploading roster for state US_XX with reason: test",
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
                self.users(),
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
        ) as fixture, self.app.test_request_context():
            file = FileStorage(fixture)
            data = {"file": file, "reason": "test"}

            response = self.client.put(
                self.users("us_xx"),
                headers=self.headers,
                data=data,
                follow_redirects=True,
                content_type="multipart/form-data",
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            error_message = (
                "Roster contains a row that is missing an email address (required)"
            )
            self.assertEqual(error_message, json.loads(response.data)["message"])

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
                self.users(),
                headers=self.headers,
            )
            self.assertEqual(expected, json.loads(response.data))

    def test_upload_roster_with_malformed_email_address(self) -> None:
        roster_leadership_user = generate_fake_rosters(
            email="leadership@domain.org",
            region_code="US_XX",
            role="leadership_role",
            external_id="0000",
            district="",
        )
        add_entity_to_database_session(self.database_key, [roster_leadership_user])
        with open(
            os.path.join(_FIXTURE_PATH, "us_xx_roster_malformed_email.csv"), "rb"
        ) as fixture, self.app.test_request_context():
            file = FileStorage(fixture)
            data = {"file": file, "reason": "test"}

            response = self.client.put(
                self.users("us_xx"),
                headers=self.headers,
                data=data,
                follow_redirects=True,
                content_type="multipart/form-data",
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            error_message = "Invalid email address format: [email.gov]"
            self.assertEqual(error_message, json.loads(response.data)["message"])

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
                self.users(),
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
        with open(
            os.path.join(_FIXTURE_PATH, "us_xx_roster.csv"), "rb"
        ) as fixture, self.app.test_request_context():
            file = FileStorage(fixture)
            data = {"file": file, "reason": "test"}

            response = self.client.put(
                self.users("us_xx"),
                headers=self.headers,
                data=data,
                follow_redirects=True,
                content_type="multipart/form-data",
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            error_message = "Roster contains a row that with a role that does not exist in the default state role permissions. Offending row has email supervision_staff@domain.org"
            self.assertEqual(error_message, json.loads(response.data)["message"])

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
                self.users(),
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
        ) as fixture, self.app.test_request_context(), self.assertLogs(
            level="INFO"
        ) as log:
            file = FileStorage(fixture)
            data = {"file": file, "reason": "test"}

            self.client.put(
                self.users("us_xx"),
                headers=self.headers,
                data=data,
                follow_redirects=True,
                content_type="multipart/form-data",
            )
            self.assertReasonLog(
                log.output,
                "uploading roster for state US_XX with reason: test",
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
                self.users(),
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
        ) as fixture, self.app.test_request_context(), self.assertLogs(
            level="INFO"
        ) as log:
            file = FileStorage(fixture)
            data = {"file": file, "reason": "test"}

            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.client.put(
                self.users("us_xx"),
                headers=self.headers,
                data=data,
                follow_redirects=True,
                content_type="multipart/form-data",
            )
            self.assertReasonLog(
                log.output,
                "uploading roster for state US_XX with reason: test",
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
                self.users(),
                headers=self.headers,
            )
            self.assertEqual(expected, json.loads(response.data))
            existing_user_override = (
                current_session.query(UserOverride)
                .filter(UserOverride.email_address == "leadership@domain.org")
                .first()
            )
            self.assertEqual(existing_user_override, None)

    def test_upload_roster_missing_external_id(self) -> None:
        roster_leadership_user = generate_fake_rosters(
            email="leadership@domain.org",
            region_code="US_XX",
            role="leadership_role",
            external_id="1234",  # This should NOT change with the new upload
            district="OLD DISTRICT",  # This should change with the new upload
        )
        # Create associated default permissions by role
        leadership_default = generate_fake_default_permissions(
            state="US_XX",
            role="leadership_role",
            routes={"A": True},
        )
        add_entity_to_database_session(
            self.database_key,
            [
                roster_leadership_user,
                leadership_default,
            ],
        )

        with open(
            os.path.join(_FIXTURE_PATH, "us_xx_roster_missing_external_id.csv"), "rb"
        ) as fixture, self.app.test_request_context(), self.assertLogs(
            level="INFO"
        ) as log:
            file = FileStorage(fixture)
            data = {"file": file, "reason": "test"}

            self.client.put(
                self.users("us_xx"),
                headers=self.headers,
                data=data,
                follow_redirects=True,
                content_type="multipart/form-data",
            )
            self.assertReasonLog(
                log.output,
                "uploading roster for state US_XX with reason: test",
            )
            expected = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": False,
                    "district": "NEW DISTRICT",
                    "emailAddress": "leadership@domain.org",
                    "externalId": "1234",
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
                self.users(),
                headers=self.headers,
            )
            self.assertEqual(expected, json.loads(response.data))
