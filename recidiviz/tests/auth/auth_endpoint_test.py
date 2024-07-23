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
from unittest import TestCase
from unittest.mock import MagicMock, patch

import flask
import pytest
from flask import Flask
from flask_smorest import Api

from recidiviz.auth.auth_endpoint import auth_endpoint_blueprint
from recidiviz.auth.auth_users_endpoint import users_blueprint
from recidiviz.auth.helpers import replace_char_0_slash
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.persistence.database.schema.case_triage.schema import Roster
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.tests.auth.helpers import (
    add_entity_to_database_session,
    generate_fake_default_permissions,
    generate_fake_permissions_overrides,
    generate_fake_rosters,
    generate_fake_user_overrides,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers

_FIXTURE_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "./fixtures/",
    )
)
_PARAMETER_USER_HASH = "flf+tuxZFuMOTgZf8aIZiDj/a4Cw4tIwRl7WcpVdCA0="
_LEADERSHIP_USER_HASH = "qKTCaVmWmjqbJX0SckE082QJKv6sE4W/bKzfHQZJNYk="
_SUPERVISION_STAFF_HASH = "EghmFPYcNI/RKWs9Cdt3P5nvGFhwM/uSkKKY1xVibvI="
_USER_HASH = "j8+pC9rc353XWt4x1fg+3Km9TQtr5XMZMT8Frl37H/o="


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789"))
@patch(
    "recidiviz.utils.validate_jwt.validate_iap_jwt_from_compute_engine",
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
        self.region_code = "US_MO"
        self.bucket = "test-project-dashboard-user-restrictions"
        self.ingested_users_bucket = "test-project-product-user-import"
        self.ingested_users_gcs_csv_uri = GcsfsFilePath.from_absolute_path(
            f"{self.ingested_users_bucket}/US_XX/ingested_product_users.csv"
        )
        self.fs = FakeGCSFileSystem()
        self.fs_patcher = patch.object(GcsfsFactory, "build", return_value=self.fs)
        self.fs_patcher.start()
        self.roster_columns = [col.name for col in Roster.__table__.columns]
        self.maxDiff = None

        # Setup database
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars()
        )
        db_url = local_persistence_helpers.postgres_db_url_from_env_vars()
        engine = setup_scoped_sessions(self.app, SchemaType.CASE_TRIAGE, db_url)
        self.database_key.declarative_meta.metadata.create_all(engine)

        self.get_secret_patcher = patch("recidiviz.auth.helpers.get_secret")
        self.mock_get_secret = self.get_secret_patcher.start()
        self.mock_get_secret.return_value = "123"

        with self.app.test_request_context():
            self.users = flask.url_for("users.UsersAPI")
            self.user = flask.url_for(
                "users.UsersByHashAPI",
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
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )
        self.get_secret_patcher.stop()

    def assertReasonLog(self, log_messages: List[str], expected: str) -> None:
        self.assertIn(
            f"INFO:root:State User Permissions: [test-user@recidiviz.org] is {expected}",
            log_messages,
        )

    def test_replace_char_0_slash(self) -> None:
        bad_hash = "/aaabbbccc/ddd"
        expected = "_aaabbbccc/ddd"
        self.assertEqual(expected, replace_char_0_slash(bad_hash))

    def test_update_user_permissions_roster(self) -> None:
        user = generate_fake_rosters(
            email="user@domain.org",
            region_code="US_CO",
            role="supervision_staff",
            roles=["supervision_staff"],
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
                        "system_prisonToSupervision": True,
                        "community_practices": False,
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
                    "roles": ["supervision_staff"],
                    "stateCode": "US_CO",
                    "routes": {
                        "system_prisonToSupervision": True,
                        "community_practices": False,
                        "A": True,
                    },
                    "featureVariants": {
                        "variant1": "true",
                    },
                    "userHash": _USER_HASH,
                    "pseudonymizedId": None,
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
            roles=["leadership_role"],
        )
        default_tn = generate_fake_default_permissions(
            state="US_TN",
            role="leadership_role",
            routes={"A": True},
            feature_variants={"C": "D"},
        )
        add_entity_to_database_session(self.database_key, [added_user, default_tn])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            update_tn_access = self.client.put(
                self.update_user_permissions,
                headers=self.headers,
                json={
                    "routes": {"C": False},
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
                    "roles": ["leadership_role"],
                    "routes": {"A": True, "C": False},
                    "featureVariants": {"C": "D"},
                    "stateCode": "US_TN",
                    "userHash": _USER_HASH,
                    "pseudonymizedId": None,
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
            roles=["leadership_role"],
        )
        override_permissions = generate_fake_permissions_overrides(
            email="user@domain.org",
            routes={"A": True},
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
                    "routes": {"E": False},
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
                "routes": {"E": False},
                "featureVariants": {"C": "D"},
            }
            self.assertEqual(expected, json.loads(response.data))

    def test_delete_user_permissions(self) -> None:
        roster_user = generate_fake_rosters(
            email="user@domain.org",
            region_code="US_MO",
            role="leadership_role",
            roles=["leadership_role"],
            district="D1",
        )
        default = generate_fake_default_permissions(
            state="US_MO",
            role="leadership_role",
            routes={"A": True, "C": False},
            feature_variants={"E": "F"},
        )
        roster_user_override_permissions = generate_fake_permissions_overrides(
            email="user@domain.org",
            routes={"A": False},
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
                    "roles": ["leadership_role"],
                    "routes": {"A": True, "C": False},
                    "featureVariants": {"E": "F"},
                    "stateCode": "US_MO",
                    "userHash": _USER_HASH,
                    "pseudonymizedId": None,
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
            roles=["leadership_role"],
        )
        default = generate_fake_default_permissions(
            state="US_CO",
            role="leadership_role",
            routes={"A": True, "C": False},
            feature_variants={"E": "F", "G": "H"},
        )
        add_entity_to_database_session(self.database_key, [user, default])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            self.client.put(
                self.update_user_permissions,
                headers=self.headers,
                json={
                    "routes": {"A": True},
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
                    "roles": ["leadership_role"],
                    "routes": {"A": True, "C": False},
                    "featureVariants": {"E": "F", "G": "H"},
                    "stateCode": "US_CO",
                    "userHash": _USER_HASH,
                    "pseudonymizedId": None,
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
            roles=["leadership_role"],
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
                    "roles": ["leadership_role"],
                    "routes": {},
                    "featureVariants": {},
                    "stateCode": "US_ID",
                    "userHash": _PARAMETER_USER_HASH,
                    "pseudonymizedId": None,
                },
            ]
            self.assertEqual(expected_response, json.loads(response.data))

    def test_delete_user_user_override(self) -> None:
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            self.client.post(
                self.users,
                headers=self.headers,
                json={
                    "stateCode": "US_TN",
                    "emailAddress": "parameter@domain.org",
                    "role": "supervision_staff",
                    "roles": ["supervision_staff"],
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
                    "roles": ["supervision_staff"],
                    "routes": {},
                    "featureVariants": {},
                    "stateCode": "US_TN",
                    "userHash": _PARAMETER_USER_HASH,
                    "pseudonymizedId": None,
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
            routes={"A": True, "B": False},
            feature_variants={"D": "E"},
        )
        add_entity_to_database_session(self.database_key, [default])
        with self.app.test_request_context():
            response = self.client.get(self.states, headers=self.headers)
            expected_response = [
                {
                    "stateCode": "US_MO",
                    "role": "leadership_role",
                    "routes": {"A": True, "B": False},
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
        leadership_role = generate_fake_default_permissions(
            state="US_MO",
            role="leadership_role",
            routes={"A": True, "B": True, "C": False},
        )
        supervision_role = generate_fake_default_permissions(
            state="US_MO",
            role="supervision_staff_role",
            routes={"A": True, "B": False, "C": False},
            feature_variants={"D": True},
        )
        user = generate_fake_rosters(
            email="parameter@domain.org",
            region_code="US_MO",
            role="leadership_role",
            roles=["leadership_role", "supervision_staff_role"],
        )
        add_entity_to_database_session(
            self.database_key, [leadership_role, supervision_role, user]
        )
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
            roles=["leadership_role"],
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
            roles=["leadership_role"],
        )
        user_keep = generate_fake_rosters(
            email="supervision_staff@domain.org",
            region_code="US_MO",
            role="supervision_staff_role",
            roles=["supervision_staff_role"],
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
            roles=["leadership_role"],
            blocked=True,
        )
        override_keep = generate_fake_user_overrides(
            email="supervision_staff_2@domain.org",
            region_code="US_MO",
            role="supervision_staff_role",
            roles=["supervision_staff_role"],
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
                    "roles": ["supervision_staff_role"],
                },
                {
                    "emailAddress": "supervision_staff_2@domain.org",
                    "roles": ["supervision_staff_role"],
                },
            ]
            actual_users = [
                {"emailAddress": user["emailAddress"], "roles": user["roles"]}
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

    @patch(
        "recidiviz.auth.auth_endpoint.generate_pseudonymized_id",
    )
    def test_import_ingested_users_successful(
        self, mock_generate_pseudonymized_id: MagicMock
    ) -> None:
        mock_generate_pseudonymized_id.side_effect = lambda state_code, external_id: (
            f"hashed-{external_id}" if external_id else None
        )
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
            roles=["leadership_role"],
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
            roles=["supervision_staff"],
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
                    "roles": ["leadership_role"],
                    "stateCode": "US_XX",
                    "routes": {},
                    "featureVariants": {},
                    "userHash": _LEADERSHIP_USER_HASH,
                    "pseudonymizedId": None,
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
                    "roles": ["supervision_staff"],
                    "stateCode": "US_XX",
                    "routes": {},
                    "featureVariants": {},
                    "userHash": _SUPERVISION_STAFF_HASH,
                    "pseudonymizedId": "pseudo-3706",
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blocked": False,
                    "district": "D2",
                    "emailAddress": "user@domain.org",
                    "externalId": "98725",
                    "firstName": "supervision2",
                    "lastName": "user2",
                    "role": "supervision_staff",
                    "roles": ["supervision_staff"],
                    "stateCode": "US_XX",
                    "routes": {},
                    "featureVariants": {},
                    "userHash": _USER_HASH,
                    "pseudonymizedId": "hashed-98725",
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
