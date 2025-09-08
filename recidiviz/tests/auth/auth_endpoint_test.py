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
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from typing import Any, Dict, List
from unittest import TestCase
from unittest.mock import MagicMock, patch

import flask
import freezegun
import pytest
from dateutil.tz import tzlocal
from flask import Flask
from flask_smorest import Api

from recidiviz.auth.auth_endpoint import get_auth_endpoint_blueprint
from recidiviz.auth.auth_users_endpoint import get_users_blueprint
from recidiviz.auth.helpers import replace_char_0_slash
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.persistence.database.schema.case_triage.schema import Roster
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.tests.auth.helpers import (
    add_entity_to_database_session,
    generate_fake_default_permissions,
    generate_fake_rosters,
    generate_fake_user_overrides,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult
from recidiviz.utils.metadata import CloudRunMetadata

_FIXTURE_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "./fixtures/",
    )
)
_PARAMETER_USER_HASH = "Sb6c3tejhmTMDZ3RmPVuSz2pLS7Eo2H4i/zaMrYfEMU="
_LEADERSHIP_USER_HASH = "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM="
_SUPERVISION_STAFF_HASH = "_uYmjI0oMriD8yRXsTt1quVrTkZZuRHJ35X+szGMHJQ="
_USER_HASH = "U9/nAUB/dvfqwBERoVETtCxT66GclnELpsw9OPrE9Vk="
_FACILITIES_USER_HASH = "hAYT6YqEQZ2nuvlMgfr523mO4YE05n3wPcTCh9I6QBo="

LEADERSHIP_ROLE = "supervision_leadership"
SUPERVISION_STAFF = "supervision_line_staff"
FACILITIES_STAFF = "facilities_line_staff"

BLOCKED_ON_DATE = datetime.fromisoformat("2025-01-09T09:00:00").replace(
    tzinfo=tzlocal()
)


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
    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def setUp(self) -> None:
        self.app = Flask(__name__)
        self.app.register_blueprint(
            get_auth_endpoint_blueprint(
                authentication_middleware=None,
                cloud_run_metadata=CloudRunMetadata(
                    project_id="recidiviz-test",
                    region="us-central1",
                    url="https://example.com",
                    service_account_email="<EMAIL>",
                ),
            )
        )
        self.app.config["TESTING"] = True
        api = Api(
            self.app,
            spec_kwargs={
                "title": "default",
                "version": "1.0.0",
                "openapi_version": "3.1.0",
            },
        )
        api.register_blueprint(
            get_users_blueprint(authentication_middleware=None),
            url_prefix="/auth/users",
        )
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
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars(
                self.postgres_launch_result
            )
        )
        db_url = local_persistence_helpers.postgres_db_url_from_env_vars()
        engine = setup_scoped_sessions(self.app, SchemaType.CASE_TRIAGE, db_url)
        self.database_key.declarative_meta.metadata.create_all(engine)

        self.get_secret_patcher = patch("recidiviz.auth.helpers.get_secret")
        self.mock_get_secret = self.get_secret_patcher.start()
        self.mock_get_secret.return_value = "123"

        with self.app.test_request_context():
            self.users = flask.url_for("users.UsersAPI")
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

    def test_get_states(self) -> None:
        default = generate_fake_default_permissions(
            state="US_MO",
            role=LEADERSHIP_ROLE,
            routes={"A": True, "B": False},
            feature_variants={"D": "E"},
        )
        add_entity_to_database_session(self.database_key, [default])
        with self.app.test_request_context():
            response = self.client.get(self.states, headers=self.headers)
            expected_response = [
                {
                    "stateCode": "US_MO",
                    "role": LEADERSHIP_ROLE,
                    "routes": {"A": True, "B": False},
                    "featureVariants": {"D": "E"},
                },
            ]
            self.assertEqual(expected_response, json.loads(response.data))

    def test_states_add_state_role(self) -> None:
        existing = generate_fake_default_permissions(
            state="US_MO",
            role=LEADERSHIP_ROLE,
            routes={"route_A": True, "routeB": True, "C": False},
        )
        add_entity_to_database_session(self.database_key, [existing])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            self.client.post(
                self.add_state_role("US_MO", SUPERVISION_STAFF),
                headers=self.headers,
                json={
                    "routes": {"route_A": True, "routeB": False},
                    "reason": "test",
                },
            )
            self.assertReasonLog(
                log.output,
                f"adding permissions for state US_MO, role {SUPERVISION_STAFF} with reason: test",
            )
            expected = [
                {
                    "role": LEADERSHIP_ROLE,
                    "stateCode": "US_MO",
                    "routes": {"route_A": True, "routeB": True, "C": False},
                    "featureVariants": {},
                },
                {
                    "role": SUPERVISION_STAFF,
                    "stateCode": "US_MO",
                    "routes": {"route_A": True, "routeB": False},
                    "featureVariants": {},
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
                self.add_state_role("US_MO", SUPERVISION_STAFF),
                headers=self.headers,
                json={
                    "featureVariants": {"A": True},
                    "reason": "test",
                },
            )
            self.assertReasonLog(
                log.output,
                f"adding permissions for state US_MO, role {SUPERVISION_STAFF} with reason: test",
            )
            expected = [
                {
                    "role": SUPERVISION_STAFF,
                    "stateCode": "US_MO",
                    "routes": {},
                    "featureVariants": {"A": True},
                },
            ]
            response = self.client.get(
                self.states,
                headers=self.headers,
            )
            self.assertEqual(expected, json.loads(response.data))

    def test_states_add_state_unknown_role_with_permissions(self) -> None:
        with self.app.test_request_context():
            response = self.client.post(
                self.add_state_role("US_MO", "unknown"),
                headers=self.headers,
                json={
                    "routes": {"route_A": True, "routeB": False},
                    "featureVariants": {"A": True},
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_states_add_state_unknown_role_without_permissions(self) -> None:
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            self.client.post(
                self.add_state_role("US_MO", "Unknown"),
                headers=self.headers,
                json={
                    "reason": "test",
                },
            )
            self.assertReasonLog(
                log.output,
                "adding permissions for state US_MO, role unknown with reason: test",
            )
            expected = [
                {
                    "role": "unknown",
                    "stateCode": "US_MO",
                    "routes": {},
                    "featureVariants": {},
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
            role=LEADERSHIP_ROLE,
            routes={"A": True, "B": True, "C": False},
        )
        add_entity_to_database_session(self.database_key, [existing])
        with self.app.test_request_context():
            response = self.client.post(
                self.add_state_role("US_MO", LEADERSHIP_ROLE),
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
            role=LEADERSHIP_ROLE,
            routes={"A": True, "B": True, "C": False},
            feature_variants={"C": True, "D": False},
        )
        add_entity_to_database_session(self.database_key, [existing])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            response = self.client.patch(
                self.update_state_role("US_MO", LEADERSHIP_ROLE),
                headers=self.headers,
                json={
                    "routes": {"C": True, "B": False},
                    "featureVariants": {"D": True, "E": False},
                    "reason": "test",
                },
            )

            expected = {
                "stateCode": "US_MO",
                "role": LEADERSHIP_ROLE,
                "routes": {"A": True, "B": False, "C": True},
                "featureVariants": {"C": True, "D": True, "E": False},
            }

            self.assertEqual(expected, json.loads(response.data))
            self.assertReasonLog(
                log.output,
                f"updating permissions for state US_MO, role {LEADERSHIP_ROLE} with reason: test",
            )

            response = self.client.get(
                self.states,
                headers=self.headers,
            )
            self.assertEqual([expected], json.loads(response.data))

    def test_states_update_state_role_routes_without_existing_routes(self) -> None:
        existing = generate_fake_default_permissions(
            state="US_MO",
            role=LEADERSHIP_ROLE,
            routes=None,
            feature_variants={"C": True, "D": False},
        )
        add_entity_to_database_session(self.database_key, [existing])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            response = self.client.patch(
                self.update_state_role("US_MO", LEADERSHIP_ROLE),
                headers=self.headers,
                json={
                    "routes": {"C": True, "B": False},
                    "featureVariants": {"D": True, "E": False},
                    "reason": "test",
                },
            )

            expected = {
                "stateCode": "US_MO",
                "role": LEADERSHIP_ROLE,
                "routes": {"B": False, "C": True},
                "featureVariants": {"C": True, "D": True, "E": False},
            }

            self.assertEqual(expected, json.loads(response.data))
            self.assertReasonLog(
                log.output,
                f"updating permissions for state US_MO, role {LEADERSHIP_ROLE} with reason: test",
            )

            response = self.client.get(
                self.states,
                headers=self.headers,
            )
            self.assertEqual([expected], json.loads(response.data))

    def test_states_update_state_code(self) -> None:
        existing = generate_fake_default_permissions(
            state="US_MO",
            role=LEADERSHIP_ROLE,
            routes={"A": True, "B": True, "C": False},
        )
        add_entity_to_database_session(self.database_key, [existing])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            response = self.client.patch(
                self.update_state_role("US_MO", LEADERSHIP_ROLE),
                headers=self.headers,
                json={
                    "stateCode": "US_TN",
                    "routes": {"C": True, "B": False},
                    "reason": "test",
                },
            )

            expected = {
                "stateCode": "US_TN",
                "role": LEADERSHIP_ROLE,
                "routes": {"A": True, "B": False, "C": True},
                "featureVariants": {},
            }

            self.assertEqual(expected, json.loads(response.data))
            self.assertReasonLog(
                log.output,
                f"updating permissions for state US_MO, role {LEADERSHIP_ROLE} with reason: test",
            )

            response = self.client.get(
                self.states,
                headers=self.headers,
            )
            self.assertEqual([expected], json.loads(response.data))

    def test_states_update_state_no_entry(self) -> None:
        existing = generate_fake_default_permissions(
            state="US_MO",
            role=LEADERSHIP_ROLE,
            routes={"A": True, "B": True, "C": False},
        )
        add_entity_to_database_session(self.database_key, [existing])
        with self.app.test_request_context():
            response = self.client.patch(
                self.update_state_role("US_MO", SUPERVISION_STAFF),
                headers=self.headers,
                json={
                    "routes": {"C": True, "B": False},
                },
            )
            self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_states_update_unknown_role(self) -> None:
        existing = generate_fake_default_permissions(
            state="US_MO",
            role="unknown",
        )
        add_entity_to_database_session(self.database_key, [existing])
        with self.app.test_request_context():
            response = self.client.patch(
                self.update_state_role("US_MO", "unknown"),
                headers=self.headers,
                json={
                    "routes": {"C": True, "B": False},
                },
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_states_delete_role_no_entry(self) -> None:
        with self.app.test_request_context():
            response = self.client.delete(
                self.delete_state_role("US_MO", SUPERVISION_STAFF),
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_states_delete_role_with_active_roster_user(self) -> None:
        leadership_role = generate_fake_default_permissions(
            state="US_MO",
            role=LEADERSHIP_ROLE,
            routes={"A": True, "B": True, "C": False},
        )
        supervision_role = generate_fake_default_permissions(
            state="US_MO",
            role=SUPERVISION_STAFF,
            routes={"A": True, "B": False, "C": False},
            feature_variants={"D": True},
        )
        user = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_MO",
            roles=[LEADERSHIP_ROLE, SUPERVISION_STAFF],
        )
        add_entity_to_database_session(
            self.database_key, [leadership_role, supervision_role, user]
        )
        with self.app.test_request_context():
            response = self.client.delete(
                self.delete_state_role("US_MO", LEADERSHIP_ROLE),
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_states_delete_role_with_active_override_user(self) -> None:
        state_role = generate_fake_default_permissions(
            state="US_MO",
            role=LEADERSHIP_ROLE,
            routes={"A": True, "B": True, "C": False},
        )
        user = generate_fake_user_overrides(
            email="parameter@testdomain.com",
            region_code="US_MO",
            roles=[LEADERSHIP_ROLE],
        )
        add_entity_to_database_session(self.database_key, [state_role, user])
        with self.app.test_request_context():
            response = self.client.delete(
                self.delete_state_role("US_MO", LEADERSHIP_ROLE),
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_states_delete_role_with_blocked_user(self) -> None:
        state_role_delete = generate_fake_default_permissions(
            state="US_MO",
            role=LEADERSHIP_ROLE,
            routes={"A": True, "B": True, "C": False},
        )
        state_role_keep = generate_fake_default_permissions(
            state="US_MO",
            role=SUPERVISION_STAFF,
            routes={"A": True, "B": False, "C": False},
            feature_variants={"D": True},
        )
        user_delete = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_MO",
            roles=[LEADERSHIP_ROLE],
        )
        user_keep = generate_fake_rosters(
            email="supervision_staff@testdomain.com",
            region_code="US_MO",
            roles=[SUPERVISION_STAFF],
        )
        override_user_delete = generate_fake_user_overrides(
            email="parameter@testdomain.com",
            region_code="US_MO",
            blocked_on=BLOCKED_ON_DATE,
        )
        override_only_delete = generate_fake_user_overrides(
            email="user@testdomain.com",
            region_code="US_MO",
            roles=[LEADERSHIP_ROLE],
            blocked_on=BLOCKED_ON_DATE,
        )
        override_keep = generate_fake_user_overrides(
            email="supervision_staff_2@testdomain.com",
            region_code="US_MO",
            roles=[SUPERVISION_STAFF],
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
                self.delete_state_role("US_MO", LEADERSHIP_ROLE),
                headers=self.headers,
                json={"reason": "test"},
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertReasonLog(
                log.output,
                f"removing permissions and blocked users for state US_MO, role {LEADERSHIP_ROLE} with reason: test",
            )

            # Check that the leadership_role role no longer exists
            response = self.client.get(
                self.states,
                headers=self.headers,
            )
            expected_response = [
                {
                    "stateCode": "US_MO",
                    "role": SUPERVISION_STAFF,
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
                    "emailAddress": "supervision_staff@testdomain.com",
                    "roles": [SUPERVISION_STAFF],
                },
                {
                    "emailAddress": "supervision_staff_2@testdomain.com",
                    "roles": [SUPERVISION_STAFF],
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
                absolute_uri=f"https://example.com/auth{self.import_ingested_users}",
                body={"state_code": self.region_code},
                service_account_email="<EMAIL>",
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

    @freezegun.freeze_time(datetime.now(tzlocal()))
    @patch(
        "recidiviz.auth.auth_endpoint.generate_pseudonymized_id",
    )
    def test_import_ingested_users_remove_upcoming_block(
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
            email="leadership@testdomain.com",
            region_code="US_XX",
            roles=[LEADERSHIP_ROLE],
            external_id="0000",  # This should change with the new upload
            district="",
        )
        user_override_leadership_user = generate_fake_user_overrides(
            email="leadership@testdomain.com",
            region_code="US_XX",
            first_name="override",
            blocked_on=datetime.now(tzlocal())
            + timedelta(days=5),  # This should be set to null with the new upload
        )
        # Create associated default permissions by role
        leadership_default = generate_fake_default_permissions(
            state="US_XX",
            role=LEADERSHIP_ROLE,
        )
        supervision_staff_default = generate_fake_default_permissions(
            state="US_XX",
            role=SUPERVISION_STAFF,
        )
        add_entity_to_database_session(
            self.database_key,
            [
                roster_leadership_user,
                user_override_leadership_user,
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
            expected: List[Dict[str, Any]] = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": None,
                    "emailAddress": "leadership@testdomain.com",
                    "externalId": None,
                    "firstName": "override",
                    "lastName": "user",
                    "roles": [LEADERSHIP_ROLE],
                    "stateCode": "US_XX",
                    "routes": {},
                    "featureVariants": {},
                    "userHash": _LEADERSHIP_USER_HASH,
                    "pseudonymizedId": None,
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": "D1",
                    "emailAddress": "supervision_staff@testdomain.com",
                    "externalId": "3706",
                    "firstName": "supervision",
                    "lastName": "user",
                    "roles": [SUPERVISION_STAFF],
                    "stateCode": "US_XX",
                    "routes": {},
                    "featureVariants": {},
                    "userHash": _SUPERVISION_STAFF_HASH,
                    "pseudonymizedId": "pseudo-3706",
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": "D2",
                    "emailAddress": "user@testdomain.com",
                    "externalId": "98725",
                    "firstName": "supervision2",
                    "lastName": "user2",
                    "roles": [SUPERVISION_STAFF],
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

    @patch(
        "recidiviz.auth.auth_endpoint.generate_pseudonymized_id",
    )
    def test_import_ingested_users_keep_existing_block(
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
            email="leadership@testdomain.com",
            region_code="US_XX",
            roles=[LEADERSHIP_ROLE],
            external_id="0000",
            district="",
        )
        user_override_leadership_user = generate_fake_user_overrides(
            email="leadership@testdomain.com",
            region_code="US_XX",
            first_name="override",
            blocked_on=BLOCKED_ON_DATE,  # This should stay the same with the new upload
        )
        # Create associated default permissions by role
        leadership_default = generate_fake_default_permissions(
            state="US_XX",
            role=LEADERSHIP_ROLE,
        )
        supervision_staff_default = generate_fake_default_permissions(
            state="US_XX",
            role=SUPERVISION_STAFF,
        )
        add_entity_to_database_session(
            self.database_key,
            [
                roster_leadership_user,
                user_override_leadership_user,
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
            expected: List[Dict[str, Any]] = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": BLOCKED_ON_DATE.astimezone(timezone.utc).isoformat(),
                    "district": None,
                    "emailAddress": "leadership@testdomain.com",
                    "externalId": None,
                    "firstName": "override",
                    "lastName": "user",
                    "roles": [LEADERSHIP_ROLE],
                    "stateCode": "US_XX",
                    "routes": {},
                    "featureVariants": {},
                    "userHash": _LEADERSHIP_USER_HASH,
                    "pseudonymizedId": None,
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": "D1",
                    "emailAddress": "supervision_staff@testdomain.com",
                    "externalId": "3706",
                    "firstName": "supervision",
                    "lastName": "user",
                    "roles": [SUPERVISION_STAFF],
                    "stateCode": "US_XX",
                    "routes": {},
                    "featureVariants": {},
                    "userHash": _SUPERVISION_STAFF_HASH,
                    "pseudonymizedId": "pseudo-3706",
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": "D2",
                    "emailAddress": "user@testdomain.com",
                    "externalId": "98725",
                    "firstName": "supervision2",
                    "lastName": "user2",
                    "roles": [SUPERVISION_STAFF],
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

    @freezegun.freeze_time(datetime.now(tzlocal()))
    @patch(
        "recidiviz.auth.auth_endpoint.generate_pseudonymized_id",
    )
    def test_import_ingested_users_insert_user_override(
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
        # User will be inserted in UserOverride with info from Roster and
        # future block date added
        roster_supervision_staff = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_XX",
            roles=[SUPERVISION_STAFF],
            district="",
        )
        # Create associated default permissions by role
        leadership_default = generate_fake_default_permissions(
            state="US_XX",
            role=LEADERSHIP_ROLE,
        )
        supervision_staff_default = generate_fake_default_permissions(
            state="US_XX",
            role=SUPERVISION_STAFF,
        )
        add_entity_to_database_session(
            self.database_key,
            [
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
            expected: List[Dict[str, Any]] = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": None,
                    "emailAddress": "leadership@testdomain.com",
                    "externalId": None,
                    "firstName": "leadership",
                    "lastName": "user",
                    "roles": [LEADERSHIP_ROLE],
                    "stateCode": "US_XX",
                    "routes": {},
                    "featureVariants": {},
                    "userHash": _LEADERSHIP_USER_HASH,
                    "pseudonymizedId": None,
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": (datetime.now(tzlocal()) + timedelta(weeks=1))
                    .astimezone(timezone.utc)
                    .isoformat(),
                    "district": "",
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": None,
                    "featureVariants": {},
                    "firstName": None,
                    "lastName": None,
                    "pseudonymizedId": None,
                    "roles": [SUPERVISION_STAFF],
                    "routes": {},
                    "stateCode": "US_XX",
                    "userHash": _PARAMETER_USER_HASH,
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": "D1",
                    "emailAddress": "supervision_staff@testdomain.com",
                    "externalId": "3706",
                    "firstName": "supervision",
                    "lastName": "user",
                    "roles": [SUPERVISION_STAFF],
                    "stateCode": "US_XX",
                    "routes": {},
                    "featureVariants": {},
                    "userHash": _SUPERVISION_STAFF_HASH,
                    "pseudonymizedId": "pseudo-3706",
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": "D2",
                    "emailAddress": "user@testdomain.com",
                    "externalId": "98725",
                    "firstName": "supervision2",
                    "lastName": "user2",
                    "roles": [SUPERVISION_STAFF],
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

    @freezegun.freeze_time(datetime.now(tzlocal()))
    @patch(
        "recidiviz.auth.auth_endpoint.generate_pseudonymized_id",
    )
    def test_import_ingested_users_update_user_override(
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
        # Non-null fields will be updated in UserOverride with info from Roster and
        # future block date added
        roster_supervision_staff = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_XX",
            roles=[SUPERVISION_STAFF],
            district="",
            first_name="Supervision Staff",
            last_name="User",
        )
        user_override_supervision_staff = generate_fake_user_overrides(
            email="parameter@testdomain.com",
            region_code="US_XX",
            district="District A",
        )
        # Create associated default permissions by role
        leadership_default = generate_fake_default_permissions(
            state="US_XX",
            role=LEADERSHIP_ROLE,
        )
        supervision_staff_default = generate_fake_default_permissions(
            state="US_XX",
            role=SUPERVISION_STAFF,
        )
        add_entity_to_database_session(
            self.database_key,
            [
                roster_supervision_staff,
                user_override_supervision_staff,
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
            expected: List[Dict[str, Any]] = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": None,
                    "emailAddress": "leadership@testdomain.com",
                    "externalId": None,
                    "firstName": "leadership",
                    "lastName": "user",
                    "roles": [LEADERSHIP_ROLE],
                    "stateCode": "US_XX",
                    "routes": {},
                    "featureVariants": {},
                    "userHash": _LEADERSHIP_USER_HASH,
                    "pseudonymizedId": None,
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": (datetime.now(tzlocal()) + timedelta(weeks=1))
                    .astimezone(timezone.utc)
                    .isoformat(),
                    "district": "District A",
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": None,
                    "featureVariants": {},
                    "firstName": "Supervision Staff",
                    "lastName": "User",
                    "pseudonymizedId": None,
                    "roles": [SUPERVISION_STAFF],
                    "routes": {},
                    "stateCode": "US_XX",
                    "userHash": _PARAMETER_USER_HASH,
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": "D1",
                    "emailAddress": "supervision_staff@testdomain.com",
                    "externalId": "3706",
                    "firstName": "supervision",
                    "lastName": "user",
                    "roles": [SUPERVISION_STAFF],
                    "stateCode": "US_XX",
                    "routes": {},
                    "featureVariants": {},
                    "userHash": _SUPERVISION_STAFF_HASH,
                    "pseudonymizedId": "pseudo-3706",
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": "D2",
                    "emailAddress": "user@testdomain.com",
                    "externalId": "98725",
                    "firstName": "supervision2",
                    "lastName": "user2",
                    "roles": [SUPERVISION_STAFF],
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

    @patch(
        "recidiviz.auth.auth_endpoint.generate_pseudonymized_id",
    )
    def test_import_ingested_users_keep_facilities_users(
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
        # User will not be blocked because their user override role is a facilities role
        roster_facilities_staff = generate_fake_rosters(
            email="facilities_staff@testdomain.com",
            region_code="US_XX",
            roles=[SUPERVISION_STAFF],
            district="",
            first_name="Facilities",
            last_name="User",
        )
        user_override_facilities_staff = generate_fake_user_overrides(
            email="facilities_staff@testdomain.com",
            region_code="US_XX",
            roles=[FACILITIES_STAFF],
            district="",
        )
        # Create associated default permissions by role
        leadership_default = generate_fake_default_permissions(
            state="US_XX",
            role=LEADERSHIP_ROLE,
        )
        supervision_staff_default = generate_fake_default_permissions(
            state="US_XX",
            role=SUPERVISION_STAFF,
        )
        facilities_staff_default = generate_fake_default_permissions(
            state="US_XX",
            role=FACILITIES_STAFF,
        )
        add_entity_to_database_session(
            self.database_key,
            [
                roster_facilities_staff,
                user_override_facilities_staff,
                leadership_default,
                supervision_staff_default,
                facilities_staff_default,
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
            expected: List[Dict[str, Any]] = [
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": "",
                    "emailAddress": "facilities_staff@testdomain.com",
                    "externalId": None,
                    "featureVariants": {},
                    "firstName": "Facilities",
                    "lastName": "User",
                    "pseudonymizedId": None,
                    "roles": [FACILITIES_STAFF],
                    "routes": {},
                    "stateCode": "US_XX",
                    "userHash": _FACILITIES_USER_HASH,
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": None,
                    "emailAddress": "leadership@testdomain.com",
                    "externalId": None,
                    "firstName": "leadership",
                    "lastName": "user",
                    "roles": [LEADERSHIP_ROLE],
                    "stateCode": "US_XX",
                    "routes": {},
                    "featureVariants": {},
                    "userHash": _LEADERSHIP_USER_HASH,
                    "pseudonymizedId": None,
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": "D1",
                    "emailAddress": "supervision_staff@testdomain.com",
                    "externalId": "3706",
                    "firstName": "supervision",
                    "lastName": "user",
                    "roles": [SUPERVISION_STAFF],
                    "stateCode": "US_XX",
                    "routes": {},
                    "featureVariants": {},
                    "userHash": _SUPERVISION_STAFF_HASH,
                    "pseudonymizedId": "pseudo-3706",
                },
                {
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": "D2",
                    "emailAddress": "user@testdomain.com",
                    "externalId": "98725",
                    "firstName": "supervision2",
                    "lastName": "user2",
                    "roles": [SUPERVISION_STAFF],
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
            self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)
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
            self.assertEqual(HTTPStatus.INTERNAL_SERVER_ERROR, response.status_code)
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
