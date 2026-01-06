# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
import tempfile
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from typing import Any, Dict, List, Optional
from unittest import TestCase, mock
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import flask
import freezegun
import pytest
from dateutil.tz import tzlocal
from flask import Flask
from flask_smorest import Api
from werkzeug.datastructures import FileStorage

from recidiviz.auth.auth_endpoint import get_auth_endpoint_blueprint
from recidiviz.auth.auth_users_endpoint import get_users_blueprint
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.tests.auth.auth_endpoint_test import _FIXTURE_PATH, BLOCKED_ON_DATE
from recidiviz.tests.auth.helpers import (
    add_entity_to_database_session,
    generate_fake_default_permissions,
    generate_fake_permissions_overrides,
    generate_fake_rosters,
    generate_fake_user_overrides,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult
from recidiviz.utils.metadata import CloudRunMetadata

_PARAMETER_USER_HASH = "Sb6c3tejhmTMDZ3RmPVuSz2pLS7Eo2H4i/zaMrYfEMU="
_USER_HASH = "U9/nAUB/dvfqwBERoVETtCxT66GclnELpsw9OPrE9Vk="
# Hash values that might show up in snapshots, for verification
# add_user@testdomain.com => "yRfBcQIOiTFhlzX/Erh5NLDygGJxoGUlTm7iVPyC5CY="
# leadership@testdomain.com => "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM="
# supervision_staff@testdomain.com => "_uYmjI0oMriD8yRXsTt1quVrTkZZuRHJ35X+szGMHJQ="

LEADERSHIP_ROLE = "supervision_leadership"
SUPERVISION_STAFF = "supervision_line_staff"
FACILITIES_STAFF = "facilities_line_staff"
NON_PREDEFINED_ROLE = "supervision_officer"


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789"))
@patch(
    "recidiviz.utils.validate_jwt.validate_iap_jwt_from_compute_engine",
    MagicMock(return_value=("test-user", "test-user@recidiviz.org", None)),
)
@pytest.mark.uses_db
@pytest.mark.usefixtures("snapshottest_snapshot")
class AuthUsersEndpointTestCase(TestCase):
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
        self.maxDiff = None
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

        def mock_generate_pseudonymized_id(
            _state_code: str, external_id: str
        ) -> Optional[str]:
            return f"pseudo-{external_id}" if external_id else None

        self.generate_pseudonymized_ids_auth_endpoint_patcher = mock.patch(
            "recidiviz.auth.auth_endpoint.generate_pseudonymized_id",
            new=mock_generate_pseudonymized_id,
        )
        self.generate_pseudonymized_ids_auth_endpoint_patcher.start()
        self.generate_pseudonymized_ids_auth_users_endpoint_patcher = mock.patch(
            "recidiviz.auth.auth_users_endpoint.generate_pseudonymized_id",
            new=mock_generate_pseudonymized_id,
        )
        self.generate_pseudonymized_ids_auth_users_endpoint_patcher.start()

        self.get_secret_patcher = patch("recidiviz.auth.helpers.get_secret")
        self.mock_get_secret = self.get_secret_patcher.start()
        self.mock_get_secret.return_value = "123"

        self.fs = FakeGCSFileSystem()
        self.fs_patcher = patch.object(GcsfsFactory, "build", return_value=self.fs)
        self.fs_patcher.start()
        self.ingested_users_bucket = "test-project-product-user-import"
        self.ingested_users_gcs_csv_uri = GcsfsFilePath.from_absolute_path(
            f"{self.ingested_users_bucket}/US_XX/ingested_supervision_product_users.csv"
        )

        self.active_date1 = "2024-04-30T14:45:09.865Z"
        self.eastern_timezone = ZoneInfo("America/New_York")
        self.blocked_on_date = datetime.fromisoformat("2025-01-09T09:00:00").replace(
            tzinfo=self.eastern_timezone
        )

        with self.app.test_request_context():
            self.users = lambda state_code=None: flask.url_for(
                "users.UsersAPI", state_code=state_code
            )
            self.user = flask.url_for(
                "users.UsersByHashAPI",
                user_hash=_PARAMETER_USER_HASH,
            )
            self.user_permissions = flask.url_for(
                "users.UserPermissionsAPI",
                user_hash=_PARAMETER_USER_HASH,
            )
            self.import_ingested_users = flask.url_for(
                "auth_endpoint_blueprint.import_ingested_users"
            )
            self.feature_variants = flask.url_for(
                "users.FeatureVariantAPI",
                feature_variant="E",
            )

    def tearDown(self) -> None:
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )
        self.generate_pseudonymized_ids_auth_endpoint_patcher.stop()
        self.generate_pseudonymized_ids_auth_users_endpoint_patcher.stop()
        self.get_secret_patcher.stop()
        self.fs_patcher.stop()

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
            email="leadership@testdomain.com",
            region_code="US_ND",
            roles=[LEADERSHIP_ROLE],
            district="D1",
            first_name="Fake",
            last_name="User",
        )
        user_2 = generate_fake_rosters(
            email="supervision_staff@testdomain.com",
            region_code="US_ID",
            external_id="abc",
            roles=[SUPERVISION_STAFF],
            district="D3",
            first_name="John",
            last_name="Doe",
            pseudonymized_id="pseudo-abc",
        )
        user_1_override = generate_fake_user_overrides(
            email="leadership@testdomain.com",
            region_code="US_ND",
            external_id="user_1_override.external_id",
            roles=["user_1_override.role"],
            blocked_on=self.blocked_on_date,
            pseudonymized_id="hashed-user_1_override",
        )
        default_1 = generate_fake_default_permissions(
            state="US_ND",
            role=LEADERSHIP_ROLE,
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
            role=SUPERVISION_STAFF,
        )
        new_permissions = generate_fake_permissions_overrides(
            email="leadership@testdomain.com",
            routes={"overridden route": True},
            feature_variants={"C": {}, "new variant": False},
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
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
        self.snapshot.assert_match(json.loads(response.data), name="test_get_users_some_overrides")  # type: ignore[attr-defined]

    def test_get_users_with_empty_overrides(self) -> None:
        user_1 = generate_fake_rosters(
            email="leadership@testdomain.com",
            region_code="US_MO",
            external_id="12345",
            roles=[LEADERSHIP_ROLE],
            district="4, 10A",
            first_name="Test A.",
            last_name="User",
        )
        default = generate_fake_default_permissions(
            state="US_MO",
            role=LEADERSHIP_ROLE,
            routes={"A": True},
        )
        add_entity_to_database_session(self.database_key, [user_1, default])
        with self.app.test_request_context():
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
        self.snapshot.assert_match(json.loads(response.data), name="test_get_users_with_empty_overrides")  # type: ignore[attr-defined]

    def test_get_users_with_null_values(self) -> None:
        user_1 = generate_fake_rosters(
            email="leadership@testdomain.com",
            region_code="US_ME",
            roles=[LEADERSHIP_ROLE],
        )
        applicable_override = generate_fake_user_overrides(
            email="leadership@testdomain.com",
            region_code="US_ME",
            external_id="A1B2",
            blocked_on=self.blocked_on_date,
        )
        default_1 = generate_fake_default_permissions(
            state="US_ME",
            role=LEADERSHIP_ROLE,
            routes={"B": True},
        )
        new_permissions = generate_fake_permissions_overrides(
            email="leadership@testdomain.com",
            routes={"A": True, "C": False},
            feature_variants={"C": {}},
        )
        add_entity_to_database_session(
            self.database_key, [user_1, applicable_override, default_1, new_permissions]
        )
        with self.app.test_request_context():
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
        self.snapshot.assert_match(json.loads(response.data), name="test_get_users_with_null_values")  # type: ignore[attr-defined]

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
            email="leadership@testdomain.com",
            region_code="US_CO",
            external_id="12345",
            roles=[LEADERSHIP_ROLE],
            district="District 4",
            first_name="Test A.",
            last_name="User",
        )
        add_entity_to_database_session(self.database_key, [user_1])
        with self.app.test_request_context():
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
        self.snapshot.assert_match(json.loads(response.data), name="test_get_users_no_permissions")  # type: ignore[attr-defined]

    def test_get_users_with_multiple_roles_no_conflicts(self) -> None:
        user_1 = generate_fake_rosters(
            email="leadership@testdomain.com",
            region_code="US_CO",
            roles=[LEADERSHIP_ROLE, SUPERVISION_STAFF],
        )
        leadership_permissions = generate_fake_default_permissions(
            state="US_CO",
            role=LEADERSHIP_ROLE,
            routes={"B": True},
        )
        supervision_permissions = generate_fake_default_permissions(
            state="US_CO",
            role=SUPERVISION_STAFF,
            routes={"A": True},
            feature_variants={"feature1": {}},
        )
        add_entity_to_database_session(
            self.database_key, [user_1, leadership_permissions, supervision_permissions]
        )
        with self.app.test_request_context():
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
        self.snapshot.assert_match(json.loads(response.data), name="test_get_users_with_multiple_roles_no_conflicts")  # type: ignore[attr-defined]

    def test_get_users_with_multiple_roles_with_conflicts(self) -> None:

        user_1 = generate_fake_rosters(
            email="leadership@testdomain.com",
            region_code="US_CO",
            roles=[LEADERSHIP_ROLE, SUPERVISION_STAFF],
        )
        leadership_permissions = generate_fake_default_permissions(
            state="US_CO",
            role=LEADERSHIP_ROLE,
            routes={"A": False, "B": True},
            feature_variants={
                "feature1": {"activeDate": f"{self.active_date1}"},
                "feature2": {"activeDate": f"{self.active_date1}"},
            },
        )
        supervision_permissions = generate_fake_default_permissions(
            state="US_CO",
            role=SUPERVISION_STAFF,
            routes={"A": True},
            feature_variants={
                "feature1": False,
                "feature2": {},
                "feature3": {"activeDate": f"{self.active_date1}"},
            },
        )
        add_entity_to_database_session(
            self.database_key, [user_1, leadership_permissions, supervision_permissions]
        )
        with self.app.test_request_context():
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
        self.snapshot.assert_match(json.loads(response.data), name="test_get_users_with_multiple_roles_with_conflicts")  # type: ignore[attr-defined]

    ########
    # GET /user/...
    ########

    def test_get_user(self) -> None:
        user_1 = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_CO",
            external_id="ABC",
            roles=[LEADERSHIP_ROLE],
            district="District",
            pseudonymized_id="pseudo-ABC",
        )
        user_2 = generate_fake_rosters(
            email="user@testdomain.com",
            region_code="US_CO",
            external_id="XXXX",
            roles=[SUPERVISION_STAFF],
            district="District",
        )
        default = generate_fake_default_permissions(
            state="US_CO",
            role=LEADERSHIP_ROLE,
            routes={"A": True, "B": False},
            feature_variants={"D": {}},
        )
        add_entity_to_database_session(self.database_key, [user_1, user_2, default])

        response = self.client.get(
            self.user,
            headers=self.headers,
        )
        self.snapshot.assert_match(json.loads(response.data), name="test_get_user")  # type: ignore[attr-defined]

    def test_get_user_not_found(self) -> None:
        user = generate_fake_rosters(
            email="user@testdomain.com",
            region_code="US_CO",
            external_id="XXXX",
            roles=[SUPERVISION_STAFF],
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

    def test_get_user_blocked_user(self) -> None:
        user = generate_fake_user_overrides(
            email="parameter@testdomain.com",
            region_code="US_CO",
            external_id="ABC",
            roles=["leadership_role"],
            district="District",
            blocked_on=datetime.fromisoformat("2023-01-01"),
        )

        add_entity_to_database_session(self.database_key, [user])

        response = self.client.get(
            self.user,
            headers=self.headers,
        )
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)
        error_message = f"User not found for email address hash {_PARAMETER_USER_HASH}, please file a bug"
        self.assertEqual(error_message, json.loads(response.data)["message"])

    # Need to set blocked_on to a future date so that blocked_on will always be in the
    # future compared to the current datetime. The time freeze does not apply to the endpoint;
    # instead it returns the user only if blocked_on is after the actual datetime the
    # test is run, so we can't simply freeze the time to a past date.
    @freezegun.freeze_time(datetime.now(tzlocal()))
    def test_get_user_upcoming_block(self) -> None:
        user = generate_fake_user_overrides(
            email="parameter@testdomain.com",
            region_code="US_CO",
            external_id="XXXX",
            roles=[SUPERVISION_STAFF],
            blocked_on=datetime.now(tzlocal()) + timedelta(weeks=1),
        )

        add_entity_to_database_session(self.database_key, [user])

        response = self.client.get(
            self.user,
            headers=self.headers,
        )
        expected: Dict[str, Any] = {
            "allowedApps": {},
            "allowedSupervisionLocationIds": "",
            "allowedSupervisionLocationLevel": "",
            "blockedOn": (datetime.now(tzlocal()) + timedelta(weeks=1))
            .astimezone(timezone.utc)
            .isoformat(),
            "district": None,
            "emailAddress": "parameter@testdomain.com",
            "externalId": "XXXX",
            "firstName": None,
            "lastName": None,
            "roles": [SUPERVISION_STAFF],
            "stateCode": "US_CO",
            "routes": {},
            "jiiPermissions": {},
            "featureVariants": {},
            "userHash": _PARAMETER_USER_HASH,
            "pseudonymizedId": None,
        }
        self.assertEqual(expected, json.loads(response.data))

    ########
    # PATCH /user/...
    ########

    def test_update_user_in_roster(self) -> None:
        user = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_CO",
            external_id="123",
            roles=[SUPERVISION_STAFF],
            district="D1",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-123",
        )
        add_entity_to_database_session(self.database_key, [user])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            new_role = self.client.patch(
                self.user,
                headers=self.headers,
                json={
                    "stateCode": "US_CO",
                    "emailAddress": "parameter@testdomain.com",
                    "roles": [LEADERSHIP_ROLE],
                    "reason": "test",
                },
            )
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.OK, new_role.status_code)
            self.snapshot.assert_match(json.loads(response.data), name="test_update_user_in_roster")  # type: ignore[attr-defined]
            self.assertReasonLog(
                log.output, "updating user parameter@testdomain.com with reason: test"
            )

    def test_update_user_in_user_override(self) -> None:
        user = generate_fake_user_overrides(
            email="parameter@testdomain.com",
            region_code="US_TN",
            external_id="Original",
            roles=[LEADERSHIP_ROLE],
            first_name="Original",
            last_name="Name",
            pseudonymized_id="hashed-Original",
        )
        add_entity_to_database_session(self.database_key, [user])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            new_name_id = self.client.patch(
                self.user,
                headers=self.headers,
                json={
                    "stateCode": "US_TN",
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": "Updated ID",
                    "firstName": "Updated",
                    "reason": "test",
                },
            )
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.OK, new_name_id.status_code)
            self.snapshot.assert_match(json.loads(response.data), name="test_update_user_in_user_override")  # type: ignore[attr-defined]
            self.assertReasonLog(
                log.output, "updating user parameter@testdomain.com with reason: test"
            )

    def test_update_user_missing_state_code(self) -> None:
        user = generate_fake_user_overrides(
            email="parameter@testdomain.com",
            region_code="US_TN",
            external_id="Original",
            roles=[LEADERSHIP_ROLE],
            first_name="Original",
            last_name="Name",
        )
        add_entity_to_database_session(self.database_key, [user])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            response = self.client.patch(
                self.user,
                headers=self.headers,
                json={
                    "externalId": "Updated ID",
                    "emailAddress": "parameter@testdomain.com",
                    "firstName": "Updated",
                    "reason": "test",
                },
            )
            # This succeeds because we can look up the state code based on the user_hash
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.snapshot.assert_match(json.loads(response.data), name="test_update_user_missing_state_code")  # type: ignore[attr-defined]
            self.assertReasonLog(
                log.output, "updating user parameter@testdomain.com with reason: test"
            )

    def test_update_user_add_role(self) -> None:
        user_1 = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_CO",
            roles=[SUPERVISION_STAFF],
        )
        leadership_permissions = generate_fake_default_permissions(
            state="US_CO",
            role=LEADERSHIP_ROLE,
            routes={"A": False, "B": True},
            feature_variants={"feature2": {}},
        )
        supervision_permissions = generate_fake_default_permissions(
            state="US_CO",
            role=SUPERVISION_STAFF,
            routes={"A": True},
            feature_variants={"feature1": {}, "feature2": False},
        )
        add_entity_to_database_session(
            self.database_key, [user_1, leadership_permissions, supervision_permissions]
        )
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            response = self.client.patch(
                self.user,
                headers=self.headers,
                json={
                    "emailAddress": "parameter@testdomain.com",
                    "roles": [SUPERVISION_STAFF, LEADERSHIP_ROLE],
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.snapshot.assert_match(json.loads(response.data), name="test_update_user_add_role")  # type: ignore[attr-defined]
            self.assertReasonLog(
                log.output, "updating user parameter@testdomain.com with reason: test"
            )

    def test_update_user_without_predefined_role(self) -> None:
        user_1 = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_CO",
            roles=[SUPERVISION_STAFF],
        )
        supervision_permissions = generate_fake_default_permissions(
            state="US_CO",
            role=SUPERVISION_STAFF,
            routes={"A": True},
            feature_variants={"feature1": {}, "feature2": False},
        )
        add_entity_to_database_session(
            self.database_key, [user_1, supervision_permissions]
        )
        with self.app.test_request_context():
            response = self.client.patch(
                self.user,
                headers=self.headers,
                json={
                    "emailAddress": "parameter@testdomain.com",
                    "roles": [NON_PREDEFINED_ROLE],
                    "reason": "test",
                },
            )

            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            error_message = "User parameter@testdomain.com must have at least one of the following roles:"
            self.assertIn(error_message, json.loads(response.data)["message"])

    ########
    # DELETE /users/...
    ########

    @freezegun.freeze_time(BLOCKED_ON_DATE)
    def test_delete_user_roster(self) -> None:
        user = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_ID",
            roles=[LEADERSHIP_ROLE],
        )
        add_entity_to_database_session(self.database_key, [user])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            delete = self.client.delete(
                self.user,
                headers=self.headers,
                json={
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.OK, delete.status_code)
            self.assertReasonLog(
                log.output, "blocking user parameter@testdomain.com with reason: test"
            )
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            expected_response: List[Dict[str, Any]] = [
                {
                    "allowedApps": {},
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": BLOCKED_ON_DATE.astimezone(timezone.utc).isoformat(),
                    "district": None,
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": None,
                    "firstName": None,
                    "lastName": None,
                    "roles": [LEADERSHIP_ROLE],
                    "routes": {},
                    "featureVariants": {},
                    "jiiPermissions": {},
                    "stateCode": "US_ID",
                    "userHash": _PARAMETER_USER_HASH,
                    "pseudonymizedId": None,
                },
            ]
            self.assertEqual(expected_response, json.loads(response.data))

    @freezegun.freeze_time(BLOCKED_ON_DATE)
    def test_delete_user_user_override(self) -> None:
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            self.client.post(
                self.users(),
                headers=self.headers,
                json={
                    "stateCode": "US_TN",
                    "emailAddress": "parameter@testdomain.com",
                    "roles": [SUPERVISION_STAFF],
                    "reason": "test",
                },
            )
            delete = self.client.delete(
                self.user,
                headers=self.headers,
                json={
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.OK, delete.status_code)
            self.assertReasonLog(
                log.output, "blocking user parameter@testdomain.com with reason: test"
            )
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            expected_response: List[Dict[str, Any]] = [
                {
                    "allowedApps": {},
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": BLOCKED_ON_DATE.astimezone(timezone.utc).isoformat(),
                    "district": None,
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": None,
                    "firstName": None,
                    "lastName": None,
                    "roles": [SUPERVISION_STAFF],
                    "routes": {},
                    "featureVariants": {},
                    "jiiPermissions": {},
                    "stateCode": "US_TN",
                    "userHash": _PARAMETER_USER_HASH,
                    "pseudonymizedId": None,
                },
            ]
            self.assertEqual(expected_response, json.loads(response.data))

    def test_delete_nonexistent_user(self) -> None:
        with self.app.test_request_context():
            delete = self.client.delete(
                self.user,
                headers=self.headers,
                json={},
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, delete.status_code)

    ########
    # PUT /users/.../permissions
    ########

    def test_update_user_permissions_roster(self) -> None:
        user = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_CO",
            roles=[SUPERVISION_STAFF],
        )
        default_co = generate_fake_default_permissions(
            state="US_CO",
            role=SUPERVISION_STAFF,
            routes={"A": True},
        )
        add_entity_to_database_session(self.database_key, [user, default_co])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            update_routes = self.client.put(
                self.user_permissions,
                headers=self.headers,
                json={
                    "routes": {
                        "system_prisonToSupervision": True,
                        "community_practices": False,
                    },
                    "featureVariants": {
                        "variant1": "true",
                    },
                    "allowedApps": {"app1": True},
                    "jiiPermissions": {"permission1": True},
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.OK, update_routes.status_code)
            self.assertReasonLog(
                log.output,
                "updating permissions for user parameter@testdomain.com with reason: test",
            )
            wrong_type = self.client.put(
                self.user_permissions,
                headers=self.headers,
                json={
                    "routes": "prisonToSupervision",
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, wrong_type.status_code)
            expected_response = [
                {
                    "allowedApps": {"app1": True},
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": None,
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": None,
                    "firstName": None,
                    "lastName": None,
                    "roles": [SUPERVISION_STAFF],
                    "stateCode": "US_CO",
                    "jiiPermissions": {"permission1": True},
                    "routes": {
                        "system_prisonToSupervision": True,
                        "community_practices": False,
                        "A": True,
                    },
                    "featureVariants": {
                        "variant1": "true",
                    },
                    "userHash": _PARAMETER_USER_HASH,
                    "pseudonymizedId": None,
                },
            ]
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.assertEqual(expected_response, json.loads(response.data))

    def test_update_user_permissions_user_override(self) -> None:
        added_user = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_TN",
            roles=[LEADERSHIP_ROLE],
        )
        default_tn = generate_fake_default_permissions(
            state="US_TN",
            role=LEADERSHIP_ROLE,
            routes={"A": True},
            feature_variants={"C": "D"},
            jii_permissions={"permission1": True},
        )
        add_entity_to_database_session(self.database_key, [added_user, default_tn])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            update_tn_access = self.client.put(
                self.user_permissions,
                headers=self.headers,
                json={
                    "routes": {"C": False},
                    "reason": "test",
                    "allowedApps": {"app1": True},
                    "jiiPermissions": {"permission2": True},
                },
            )
            self.assertEqual(
                HTTPStatus.OK, update_tn_access.status_code, update_tn_access.data
            )
            self.assertReasonLog(
                log.output,
                "updating permissions for user parameter@testdomain.com with reason: test",
            )
            wrong_type = self.client.put(
                self.user_permissions,
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
                    "allowedApps": {"app1": True},
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": None,
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": None,
                    "firstName": None,
                    "lastName": None,
                    "jiiPermissions": {"permission1": True, "permission2": True},
                    "roles": [LEADERSHIP_ROLE],
                    "routes": {"A": True, "C": False},
                    "featureVariants": {"C": "D"},
                    "stateCode": "US_TN",
                    "userHash": _PARAMETER_USER_HASH,
                    "pseudonymizedId": None,
                },
            ]
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.assertEqual(expected_response, json.loads(response.data))

    def test_update_user_permissions_override(self) -> None:
        added_user = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_CO",
            roles=[LEADERSHIP_ROLE],
        )
        override_permissions = generate_fake_permissions_overrides(
            email="parameter@testdomain.com",
            routes={"A": True},
            feature_variants={"C": "D"},
            allowed_apps={"app1": True},
            jii_permissions={"permission1": True},
        )
        add_entity_to_database_session(
            self.database_key, [added_user, override_permissions]
        )
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            response = self.client.put(
                self.user_permissions,
                headers=self.headers,
                json={
                    "routes": {"E": False},
                    "reason": "test",
                    "allowedApps": {"app2": True},
                    "jiiPermissions": {"permission1": False, "permission2": True},
                },
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertReasonLog(
                log.output,
                "updating permissions for user parameter@testdomain.com with reason: test",
            )
            expected = {
                "emailAddress": "parameter@testdomain.com",
                "routes": {"E": False},
                "featureVariants": {"C": "D"},
                "allowedApps": {"app2": True},
                "jiiPermissions": {"permission1": False, "permission2": True},
            }
            self.assertEqual(expected, json.loads(response.data))

    ########
    # DELETE /users/.../permissions
    ########

    def test_delete_user_permissions(self) -> None:
        roster_user = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_MO",
            roles=[LEADERSHIP_ROLE],
            district="D1",
        )
        default = generate_fake_default_permissions(
            state="US_MO",
            role=LEADERSHIP_ROLE,
            routes={"A": True, "C": False},
            feature_variants={"E": "F"},
        )
        roster_user_override_permissions = generate_fake_permissions_overrides(
            email="parameter@testdomain.com",
            routes={"A": False},
            feature_variants={"C": "D"},
        )
        add_entity_to_database_session(
            self.database_key, [roster_user, default, roster_user_override_permissions]
        )
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            delete_roster_user = self.client.delete(
                self.user_permissions,
                headers=self.headers,
                json={
                    "reason": "test",
                },
            )
            self.assertEqual(
                HTTPStatus.OK, delete_roster_user.status_code, delete_roster_user.data
            )
            self.assertReasonLog(
                log.output,
                "removing custom permissions for user parameter@testdomain.com with reason: test",
            )
            expected_response = [
                {
                    "allowedApps": {},
                    "allowedSupervisionLocationIds": "D1",
                    "allowedSupervisionLocationLevel": "level_1_supervision_location",
                    "blockedOn": None,
                    "district": "D1",
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": None,
                    "firstName": None,
                    "lastName": None,
                    "roles": [LEADERSHIP_ROLE],
                    "routes": {"A": True, "C": False},
                    "featureVariants": {"E": "F"},
                    "jiiPermissions": {},
                    "stateCode": "US_MO",
                    "userHash": _PARAMETER_USER_HASH,
                    "pseudonymizedId": None,
                },
            ]
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.assertEqual(expected_response, json.loads(response.data))

    def test_delete_added_user_permissions(self) -> None:
        user = generate_fake_user_overrides(
            email="parameter@testdomain.com",
            region_code="US_CO",
            roles=[LEADERSHIP_ROLE],
        )
        default = generate_fake_default_permissions(
            state="US_CO",
            role=LEADERSHIP_ROLE,
            routes={"A": True, "C": False},
            feature_variants={"E": "F", "G": "H"},
        )
        add_entity_to_database_session(self.database_key, [user, default])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            self.client.put(
                self.user_permissions,
                headers=self.headers,
                json={
                    "routes": {"A": True},
                    "featureVariants": {"E": "F"},
                    "reason": "test",
                },
            )
            delete_roster_user = self.client.delete(
                self.user_permissions,
                headers=self.headers,
                json={
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.OK, delete_roster_user.status_code)
            self.assertReasonLog(
                log.output,
                "removing custom permissions for user parameter@testdomain.com with reason: test",
            )
            expected_response = [
                {
                    "allowedApps": {},
                    "allowedSupervisionLocationIds": "",
                    "allowedSupervisionLocationLevel": "",
                    "blockedOn": None,
                    "district": None,
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": None,
                    "firstName": None,
                    "lastName": None,
                    "roles": [LEADERSHIP_ROLE],
                    "routes": {"A": True, "C": False},
                    "jiiPermissions": {},
                    "featureVariants": {"E": "F", "G": "H"},
                    "stateCode": "US_CO",
                    "userHash": _PARAMETER_USER_HASH,
                    "pseudonymizedId": None,
                },
            ]
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.assertEqual(expected_response, json.loads(response.data))

    def test_delete_nonexistent_user_permissions_error(self) -> None:
        user = generate_fake_user_overrides(
            email="parameter@testdomain.com",
            region_code="US_CO",
            roles=[LEADERSHIP_ROLE],
        )
        add_entity_to_database_session(self.database_key, [user])
        with self.app.test_request_context():
            delete_permissions = self.client.delete(
                self.user_permissions,
                headers=self.headers,
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, delete_permissions.status_code)

    ########
    # POST /users
    ########

    def test_add_user(self) -> None:
        user_1 = generate_fake_rosters(
            email="add_user@testdomain.com",
            region_code="US_CO",
            external_id="ABC",
            roles=[LEADERSHIP_ROLE],
            district="District",
        )
        default = generate_fake_default_permissions(
            state="US_MO",
            role=LEADERSHIP_ROLE,
            routes={"A": True, "B": False},
            feature_variants={"D": {}},
            allowed_apps={"app1": True, "app2": False},
            jii_permissions={"perm1": True, "perm2": False},
        )
        add_entity_to_database_session(self.database_key, [user_1, default])
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            self.client.post(
                self.users(),
                headers=self.headers,
                json={
                    "stateCode": "US_MO",
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": None,
                    "roles": [LEADERSHIP_ROLE],
                    "district": "1, 2",
                    "firstName": None,
                    "lastName": None,
                    "reason": "test",
                },
            )
            self.assertReasonLog(
                log.output, "adding user parameter@testdomain.com with reason: test"
            )
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.snapshot.assert_match(json.loads(response.data), name="test_add_user")  # type: ignore[attr-defined]

    def test_add_user_no_state(self) -> None:
        with self.app.test_request_context():
            no_state = self.client.post(
                self.users(),
                headers=self.headers,
                json={
                    "stateCode": None,
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": "XYZ",
                    "roles": [LEADERSHIP_ROLE],
                    "district": "D1",
                    "firstName": "Test",
                    "lastName": "User",
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, no_state.status_code)

    def test_add_user_wrong_type(self) -> None:
        with self.app.test_request_context():
            wrong_type = self.client.post(
                self.users(),
                headers=self.headers,
                json={
                    "stateCode": "US_ID",
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": "XYZ",
                    "roles": [{"A": "B"}],
                    "district": "D1",
                    "firstName": "Test",
                    "lastName": "User",
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, wrong_type.status_code)

    def test_add_user_without_predefined_role(self) -> None:
        with self.app.test_request_context():
            no_predefined_role = self.client.post(
                self.users(),
                headers=self.headers,
                json={
                    "stateCode": "US_ID",
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": "XYZ",
                    "roles": [NON_PREDEFINED_ROLE],
                    "district": "D1",
                    "firstName": "Test",
                    "lastName": "User",
                    "reason": "test",
                },
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, no_predefined_role.status_code)
            error_message = "User parameter@testdomain.com must have at least one of the following roles:"
            self.assertIn(error_message, json.loads(no_predefined_role.data)["message"])

    def test_add_user_repeat_email(self) -> None:
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            user_override_user = self.client.post(
                self.users(),
                headers=self.headers,
                json={
                    "stateCode": "US_ID",
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": "XYZ",
                    "roles": [LEADERSHIP_ROLE],
                    "district": "D1",
                    "firstName": "Test",
                    "lastName": "User",
                    "reason": "Test",
                },
            )
            self.assertEqual(
                HTTPStatus.OK, user_override_user.status_code, user_override_user.data
            )
            self.snapshot.assert_match(json.loads(user_override_user.data), name="test_add_user_repeat_email")  # type: ignore[attr-defined]
            self.assertReasonLog(
                log.output, "adding user parameter@testdomain.com with reason: Test"
            )
            repeat_user_override_user = self.client.post(
                self.users(),
                headers=self.headers,
                json={
                    "stateCode": "US_ND",
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": None,
                    "roles": [LEADERSHIP_ROLE],
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
            email="parameter@testdomain.com",
            region_code="US_TN",
            roles=[LEADERSHIP_ROLE],
            district="40",
        )
        add_entity_to_database_session(self.database_key, [roster_user])
        with self.app.test_request_context():
            repeat_roster_user = self.client.post(
                self.users(),
                headers=self.headers,
                json={
                    "stateCode": "US_TN",
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": None,
                    "roles": [LEADERSHIP_ROLE],
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

    def test_add_user_multiple_roles(self) -> None:
        user_1 = generate_fake_rosters(
            email="add_user@testdomain.com",
            region_code="US_CO",
            external_id="ABC",
            roles=[LEADERSHIP_ROLE],
            district="District",
        )
        leadership_permissions = generate_fake_default_permissions(
            state="US_MO",
            role=LEADERSHIP_ROLE,
            routes={"B": True},
        )
        supervision_permissions = generate_fake_default_permissions(
            state="US_MO",
            role=SUPERVISION_STAFF,
            routes={"A": True},
            feature_variants={"feature1": {}},
        )
        add_entity_to_database_session(
            self.database_key, [user_1, leadership_permissions, supervision_permissions]
        )
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            self.client.post(
                self.users(),
                headers=self.headers,
                json={
                    "stateCode": "US_MO",
                    "emailAddress": "parameter@testdomain.com",
                    "externalId": None,
                    "roles": [LEADERSHIP_ROLE, SUPERVISION_STAFF],
                    "district": "1, 2",
                    "firstName": None,
                    "lastName": None,
                    "reason": "test",
                },
            )
            self.assertReasonLog(
                log.output, "adding user parameter@testdomain.com with reason: test"
            )

            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.snapshot.assert_match(json.loads(response.data), name="test_add_user_multiple_roles")  # type: ignore[attr-defined]

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
                role=LEADERSHIP_ROLE,
                routes={"A": True},
                allowed_apps={"app1": True},
                jii_permissions={"perm1": True},
            )
            supervision_staff_default = generate_fake_default_permissions(
                state="US_XX",
                role=SUPERVISION_STAFF,
                routes={"B": True},
                allowed_apps={"app2": True},
                jii_permissions={"perm2": True},
            )
            facilities_staff_default = generate_fake_default_permissions(
                state="US_XX",
                role=FACILITIES_STAFF,
                routes={"C": True},
                allowed_apps={"app3": True},
                jii_permissions={"perm3": True},
            )
            add_entity_to_database_session(
                self.database_key,
                [
                    leadership_default,
                    supervision_staff_default,
                    facilities_staff_default,
                ],
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
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.snapshot.assert_match(json.loads(response.data), name="test_upload_roster")  # type: ignore[attr-defined]

    def test_upload_roster_incorrect_columns(self) -> None:
        with tempfile.NamedTemporaryFile() as fixture, self.app.test_request_context():
            fixture.write(b"email_address,roles,district")
            fixture.seek(0)
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
            error_message = "CSV columns must be exactly email_address,roles,district,external_id,first_name,last_name"
            self.assertEqual(error_message, json.loads(response.data)["message"])

    def test_upload_roster_with_missing_email_address(self) -> None:
        roster_leadership_user = generate_fake_rosters(
            email="leadership@testdomain.com",
            region_code="US_XX",
            roles=[LEADERSHIP_ROLE],
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
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.snapshot.assert_match(json.loads(response.data), name="test_upload_roster_with_missing_email_address")  # type: ignore[attr-defined]

    def test_upload_roster_with_malformed_email_address(self) -> None:
        roster_leadership_user = generate_fake_rosters(
            email="leadership@testdomain.com",
            region_code="US_XX",
            roles=[LEADERSHIP_ROLE],
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
            error_message = "Invalid email address format: [testdomain.com]"
            self.assertEqual(error_message, json.loads(response.data)["message"])

            # Existing rows should not have been deleted
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.snapshot.assert_match(json.loads(response.data), name="test_upload_roster_with_malformed_email_address")  # type: ignore[attr-defined]

    def test_upload_roster_with_missing_associated_role(self) -> None:
        roster_leadership_user = generate_fake_rosters(
            email="leadership@testdomain.com",
            region_code="US_XX",
            roles=[LEADERSHIP_ROLE],
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
            error_message = "Roster contains a row that with a role that does not exist in the default state role permissions. Offending row has email supervision_staff@testdomain.com"
            self.assertEqual(error_message, json.loads(response.data)["message"])

            # Existing rows should not have been deleted
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.snapshot.assert_match(json.loads(response.data), name="test_upload_roster_with_missing_associated_role")  # type: ignore[attr-defined]

    def test_upload_roster_update_user(self) -> None:
        roster_leadership_user = generate_fake_rosters(
            email="leadership@testdomain.com",
            region_code="US_XX",
            roles=[LEADERSHIP_ROLE],  # This should change with the new upload
            external_id="0000",  # This should change with the new upload
            district="",
        )
        # This user will not change
        roster_supervision_staff = generate_fake_rosters(
            email="supervision_staff@testdomain.com",
            region_code="US_XX",
            roles=[SUPERVISION_STAFF],
            district="",
        )
        # Create associated default permissions by role
        leadership_default = generate_fake_default_permissions(
            state="US_XX",
            role=LEADERSHIP_ROLE,
            routes={"A": True},
        )
        supervision_staff_default = generate_fake_default_permissions(
            state="US_XX",
            role=SUPERVISION_STAFF,
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

            response = self.client.put(
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
            self.assertEqual(HTTPStatus.OK, response.status_code, response.data)

            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.snapshot.assert_match(json.loads(response.data), name="test_upload_roster_update_user")  # type: ignore[attr-defined]

    def test_upload_roster_then_sync_roster(self) -> None:
        # Create default permissions by role
        leadership_default = generate_fake_default_permissions(
            state="US_XX",
            role=LEADERSHIP_ROLE,
            routes={"A": True},
        )
        supervision_staff_default = generate_fake_default_permissions(
            state="US_XX",
            role=SUPERVISION_STAFF,
            routes={"B": True},
        )
        facilities_staff_default = generate_fake_default_permissions(
            state="US_XX",
            role=FACILITIES_STAFF,
            routes={"C": True},
        )
        add_entity_to_database_session(
            self.database_key,
            [
                leadership_default,
                supervision_staff_default,
                facilities_staff_default,
            ],
        )
        with open(
            os.path.join(_FIXTURE_PATH, "us_xx_roster.csv"), "rb"
        ) as fixture, self.app.test_request_context(), self.assertLogs(
            level="INFO"
        ) as log:
            file = FileStorage(fixture)
            data = {"file": file, "reason": "test"}

            # upload user: should get put in overrides
            response = self.client.put(
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
            self.assertEqual(HTTPStatus.OK, response.status_code, response.data)

            self.fs.upload_from_contents_handle_stream(
                self.ingested_users_gcs_csv_uri,
                contents_handle=LocalFileContentsHandle(
                    local_file_path=os.path.join(
                        _FIXTURE_PATH, "us_xx_ingested_supervision_product_users.csv"
                    ),
                    cleanup_file=False,
                ),
                content_type="text/csv",
            )
            response = self.client.post(
                self.import_ingested_users,
                headers=self.headers,
                json={
                    "state_code": "US_XX",
                    "filename": "ingested_supervision_product_users.csv",
                },
            )
            self.assertEqual(HTTPStatus.OK, response.status_code, response.data)

            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.snapshot.assert_match(json.loads(response.data), name="test_upload_roster_then_sync_roster")  # type: ignore[attr-defined]

    def test_upload_roster_missing_external_id(self) -> None:
        """Test that uploading a CSV with empty external_id preserves the existing
        external_id for a user in the Roster table. See also
        test_upload_roster_missing_external_id_preserves_existing_override which
        tests the same behavior for users in the UserOverride table."""
        roster_leadership_user = generate_fake_rosters(
            email="leadership@testdomain.com",
            region_code="US_XX",
            roles=[LEADERSHIP_ROLE],
            external_id="1234",  # This should NOT change with the new upload
            district="OLD DISTRICT",  # This should change with the new upload
        )
        # Create associated default permissions by role
        leadership_default = generate_fake_default_permissions(
            state="US_XX",
            role=LEADERSHIP_ROLE,
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

            response = self.client.put(
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
            self.assertEqual(HTTPStatus.OK, response.status_code, response.data)

            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.snapshot.assert_match(json.loads(response.data), name="test_upload_roster_missing_external_id")  # type: ignore[attr-defined]

    def test_upload_roster_missing_external_id_preserves_existing_override(
        self,
    ) -> None:
        """Test that uploading a CSV with empty external_id does NOT wipe out
        an existing external_id in the UserOverride table. This prevents orphaned
        pseudonymized_ids where external_id is cleared but pseudonymized_id remains."""
        # Create a UserOverride with existing external_id and pseudonymized_id
        override_user = generate_fake_user_overrides(
            email="leadership@testdomain.com",
            region_code="US_XX",
            roles=[LEADERSHIP_ROLE],
            external_id="EXISTING_EXTERNAL_ID",
            pseudonymized_id="existing_pseudo_id",
            district="OLD DISTRICT",
        )
        # Create associated default permissions by role
        leadership_default = generate_fake_default_permissions(
            state="US_XX",
            role=LEADERSHIP_ROLE,
            routes={"A": True},
        )
        add_entity_to_database_session(
            self.database_key,
            [
                override_user,
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

            response = self.client.put(
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
            self.assertEqual(HTTPStatus.OK, response.status_code, response.data)

            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            result = json.loads(response.data)

            # Verify the existing external_id and pseudonymized_id were preserved
            self.assertEqual(len(result), 1)
            user = result[0]
            self.assertEqual(user["externalId"], "EXISTING_EXTERNAL_ID")
            self.assertEqual(user["pseudonymizedId"], "existing_pseudo_id")
            # District should be updated from the CSV
            self.assertEqual(user["district"], "NEW DISTRICT")

    def test_upload_roster_multiple_roles(self) -> None:
        leadership_default = generate_fake_default_permissions(
            state="US_XX",
            role=LEADERSHIP_ROLE,
            routes={"A": True},
        )
        supervision_staff_default = generate_fake_default_permissions(
            state="US_XX",
            role=SUPERVISION_STAFF,
            routes={"B": True},
        )
        facilities_staff_default = generate_fake_default_permissions(
            state="US_XX",
            role=FACILITIES_STAFF,
            routes={"C": True},
        )
        add_entity_to_database_session(
            self.database_key,
            [leadership_default, supervision_staff_default, facilities_staff_default],
        )

        with open(
            os.path.join(_FIXTURE_PATH, "us_xx_roster_multiple_roles.csv"), "rb"
        ) as fixture, self.app.test_request_context(), self.assertLogs(
            level="INFO"
        ) as log:
            file = FileStorage(fixture)
            data = {"file": file, "reason": "test"}

            response = self.client.put(
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
            self.assertEqual(HTTPStatus.OK, response.status_code, response.data)

            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.snapshot.assert_match(json.loads(response.data), name="test_upload_roster_multiple_roles")  # type: ignore[attr-defined]

    def test_upload_roster_without_predefined_role(self) -> None:
        leadership_default = generate_fake_default_permissions(
            state="US_XX",
            role=LEADERSHIP_ROLE,
            routes={"A": True},
        )
        supervision_staff_default = generate_fake_default_permissions(
            state="US_XX",
            role=SUPERVISION_STAFF,
            routes={"B": True},
        )
        add_entity_to_database_session(
            self.database_key,
            [leadership_default, supervision_staff_default],
        )

        with open(
            os.path.join(_FIXTURE_PATH, "us_xx_roster_without_predefined_role.csv"),
            "rb",
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
            error_message = "User supervision_staff@testdomain.com must have at least one of the following roles:"
            self.assertIn(error_message, json.loads(response.data)["message"])

    ########
    # PATCH /users
    ########

    def test_update_users(self) -> None:
        roster_user = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_CO",
            external_id="123",
            roles=[FACILITIES_STAFF],
            district="D1",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-123",
        )
        override_user = generate_fake_user_overrides(
            email="user@testdomain.com",
            region_code="US_TN",
            external_id="456",
            roles=[LEADERSHIP_ROLE],
            first_name="Original",
            last_name="Name",
        )
        add_entity_to_database_session(self.database_key, [roster_user, override_user])

        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            updates = self.client.patch(
                self.users(),
                headers=self.headers,
                json=[
                    {
                        "stateCode": "US_CO",
                        "userHash": _PARAMETER_USER_HASH,
                        "roles": [SUPERVISION_STAFF],
                        "reason": "test",
                    },
                    {
                        "stateCode": "US_TN",
                        "userHash": _USER_HASH,
                        "roles": [SUPERVISION_STAFF],
                        "reason": "test",
                    },
                ],
            )
            self.assertEqual(HTTPStatus.OK, updates.status_code, updates.data)

            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.snapshot.assert_match(json.loads(response.data), name="test_update_users")  # type: ignore[attr-defined]
            self.assertReasonLog(
                log.output, "updating user parameter@testdomain.com with reason: test"
            )
            self.assertReasonLog(
                log.output, "updating user user@testdomain.com with reason: test"
            )

    def test_update_users_without_predefined_role(self) -> None:
        roster_user = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_CO",
            external_id="123",
            roles=[FACILITIES_STAFF],
            district="D1",
            first_name="Test",
            last_name="User",
            pseudonymized_id="pseudo-123",
        )
        override_user = generate_fake_user_overrides(
            email="user@testdomain.com",
            region_code="US_TN",
            external_id="456",
            roles=[LEADERSHIP_ROLE],
            first_name="Original",
            last_name="Name",
        )
        add_entity_to_database_session(self.database_key, [roster_user, override_user])

        with self.app.test_request_context():
            updates = self.client.patch(
                self.users(),
                headers=self.headers,
                json=[
                    {
                        "stateCode": "US_CO",
                        "userHash": _PARAMETER_USER_HASH,
                        "roles": [NON_PREDEFINED_ROLE],
                        "reason": "test",
                    },
                    {
                        "stateCode": "US_TN",
                        "userHash": _USER_HASH,
                        "roles": [SUPERVISION_STAFF],
                        "reason": "test",
                    },
                ],
            )

            self.assertEqual(HTTPStatus.BAD_REQUEST, updates.status_code)
            error_message = "User parameter@testdomain.com must have at least one of the following roles:"
            self.assertIn(error_message, json.loads(updates.data)["message"])

    ########
    # DELETE /users/feature_variants/...
    ########

    def test_delete_feature_variant_from_permissions_overrides(self) -> None:
        leadership_user = generate_fake_rosters(
            email="parameter@testdomain.com",
            region_code="US_MO",
            roles=[LEADERSHIP_ROLE],
        )
        supervision_user = generate_fake_rosters(
            email="user@testdomain.com",
            region_code="US_MO",
            roles=[SUPERVISION_STAFF],
        )
        leadership_role = generate_fake_default_permissions(
            state="US_MO",
            role=LEADERSHIP_ROLE,
            routes={"A": True, "B": True, "C": False},
            feature_variants={"A": True, "D": True},
        )
        supervision_staff = generate_fake_default_permissions(
            state="US_MO",
            role=SUPERVISION_STAFF,
            routes={"A": True},
            feature_variants={"A": True, "B": True},
        )
        leadership_user_permissions_override = generate_fake_permissions_overrides(
            email="parameter@testdomain.com",
            feature_variants={"E": True},
        )
        supervision_user_permissions_override = generate_fake_permissions_overrides(
            email="user@testdomain.com",
            feature_variants={"E": True, "F": True},
        )
        add_entity_to_database_session(
            self.database_key,
            [
                leadership_user,
                supervision_user,
                leadership_role,
                supervision_staff,
                leadership_user_permissions_override,
                supervision_user_permissions_override,
            ],
        )
        with self.app.test_request_context(), self.assertLogs(level="INFO") as log:
            response = self.client.delete(
                self.feature_variants,
                headers=self.headers,
                json={"reason": "test"},
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            self.assertReasonLog(
                log.output,
                "removing feature variant E from 2 user permissions overrides with reason: test",
            )

            # Check that the feature variant no longer exists
            response = self.client.get(
                self.users(),
                headers=self.headers,
            )
            self.snapshot.assert_match(json.loads(response.data), name="test_delete_feature_variant_from_permissions_overrides")  # type: ignore[attr-defined]

    def test_delete_nonexistent_feature_variant_from_permissions_overrides(
        self,
    ) -> None:
        with self.app.test_request_context():
            response = self.client.delete(
                self.feature_variants,
                headers=self.headers,
                json={"reason": "test"},
            )
            self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)
