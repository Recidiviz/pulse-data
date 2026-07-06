# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Integration tests for the Edovo course-completion Flask blueprint.

External dependencies (HMAC verification, BigQuery person existence check) are
mocked. The database layer uses a real local Postgres instance.
"""
import json
from http import HTTPStatus
from unittest import TestCase
from unittest.mock import MagicMock, patch

from flask import Flask
from flask.testing import FlaskClient
from werkzeug.test import TestResponse

from recidiviz.case_triage.edovo.edovo_routes import create_edovo_api_blueprint
from recidiviz.case_triage.edovo.person_existence import PersonNotFoundError
from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult
from recidiviz.utils.auth.auth0 import AuthorizationError

MODULE = "recidiviz.case_triage.edovo.edovo_routes"

_DOC_ID = "A123456"

_VALID_BODY: dict[str, object] = {
    "person_external_id": _DOC_ID,
    "state_code": "US_CO",
    "course_id": "course-001",
    "course_name": "Introduction to Reading",
    "content_hours": 3.5,
    "completed_at": "2026-04-23T17:42:00Z",
}

_AUTH_HEADER = "HMAC-SHA256 KeyId=k1, Signature=sig, Timestamp=1745000000"

_IDEMPOTENCY_KEY = "11111111-1111-4111-8111-111111111111"
_OTHER_IDEMPOTENCY_KEY = "22222222-2222-4222-8222-222222222222"


class TestEdovoRoutes(TestCase):
    """Integration tests for the Edovo course-completion Flask blueprint."""

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
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars(
                self.postgres_launch_result
            )
        )
        db_url = local_persistence_helpers.postgres_db_url_from_env_vars()

        self.test_app = Flask(__name__)
        register_error_handlers(self.test_app)
        engine = setup_scoped_sessions(self.test_app, SchemaType.CASE_TRIAGE, db_url)
        self.database_key.declarative_meta.metadata.create_all(engine)
        self.test_app.register_blueprint(
            create_edovo_api_blueprint(), url_prefix="/edovo"
        )
        self.client: FlaskClient = self.test_app.test_client()

        self.hmac_patcher = patch(f"{MODULE}.load_secret_and_verify")
        self.mock_hmac = self.hmac_patcher.start()

        self.bq_patcher = patch(f"{MODULE}.BigQueryClientImpl")
        mock_bq_cls = self.bq_patcher.start()
        self.mock_bq_client = MagicMock()
        mock_bq_cls.return_value = self.mock_bq_client

        self.resolve_patcher = patch(f"{MODULE}.assert_person_exists")
        self.mock_resolve = self.resolve_patcher.start()
        self.mock_resolve.return_value = None

    def tearDown(self) -> None:
        self.hmac_patcher.stop()
        self.bq_patcher.stop()
        self.resolve_patcher.stop()
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    def _post(
        self,
        body: object = None,
        auth: str = _AUTH_HEADER,
        idempotency_key: str | None = _IDEMPOTENCY_KEY,
    ) -> TestResponse:
        headers = {"Authorization": auth}
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        return self.client.post(
            "/edovo/course-completions",
            data=json.dumps(body if body is not None else _VALID_BODY),
            content_type="application/json",
            headers=headers,
        )

    def test_new_completion_returns_201(self) -> None:
        response = self._post()
        self.assertEqual(response.status_code, HTTPStatus.CREATED)
        data = response.get_json()
        self.assertEqual(data["status"], "accepted")
        self.assertIsNotNone(data["completion_id"])

    def test_retry_with_same_idempotency_key_returns_200(self) -> None:
        self._post()
        response = self._post()
        self.assertEqual(response.status_code, HTTPStatus.OK)
        data = response.get_json()
        self.assertEqual(data["status"], "duplicate")

    def test_duplicate_idempotency_key_returns_same_completion_id(self) -> None:
        first = self._post()
        second = self._post()
        self.assertEqual(
            first.get_json()["completion_id"], second.get_json()["completion_id"]
        )

    def test_missing_idempotency_key_returns_400(self) -> None:
        response = self._post(idempotency_key=None)
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        data = response.get_json()
        self.assertEqual(data["error_code"], "VALIDATION_ERROR")
        self.assertEqual(data["details"]["field"], "Idempotency-Key")
        self.assertEqual(data["details"]["constraint"], "required")

    def test_malformed_idempotency_key_returns_400(self) -> None:
        response = self._post(idempotency_key="not-a-uuid")
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        data = response.get_json()
        self.assertEqual(data["error_code"], "VALIDATION_ERROR")
        self.assertEqual(data["details"]["field"], "Idempotency-Key")
        self.assertEqual(data["details"]["constraint"], "invalid")

    def test_invalid_signature_returns_401(self) -> None:
        self.mock_hmac.side_effect = AuthorizationError(
            code="invalid_signature", description="HMAC signature does not match"
        )
        response = self._post()
        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

    def test_person_not_found_returns_422(self) -> None:
        self.mock_resolve.side_effect = PersonNotFoundError(_DOC_ID)
        response = self._post()
        self.assertEqual(response.status_code, HTTPStatus.UNPROCESSABLE_ENTITY)
        data = response.get_json()
        self.assertEqual(data["error_code"], "PERSON_NOT_FOUND")

    def test_already_completed_returns_422(self) -> None:
        self._post()
        # The same person+course pair submitted under a *different* idempotency
        # key is a genuinely new request, so it trips the no_double_credit
        # constraint rather than the idempotent-replay path.
        response = self._post(idempotency_key=_OTHER_IDEMPOTENCY_KEY)
        self.assertEqual(response.status_code, HTTPStatus.UNPROCESSABLE_ENTITY)
        data = response.get_json()
        self.assertEqual(data["error_code"], "ALREADY_COMPLETED")

    def test_missing_required_field_returns_400(self) -> None:
        body = {k: v for k, v in _VALID_BODY.items() if k != "course_id"}
        response = self._post(body=body)
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        data = response.get_json()
        self.assertEqual(data["error_code"], "VALIDATION_ERROR")
        self.assertEqual(data["details"]["constraint"], "required")
        self.assertEqual(data["details"]["field"], "course_id")

    def test_invalid_state_code_returns_400(self) -> None:
        response = self._post(body={**_VALID_BODY, "state_code": "US_INVALID"})
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        data = response.get_json()
        self.assertEqual(data["error_code"], "VALIDATION_ERROR")
        self.assertEqual(data["details"]["constraint"], "invalid_state_code")

    def test_non_positive_content_hours_returns_400(self) -> None:
        response = self._post(body={**_VALID_BODY, "content_hours": 0})
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        data = response.get_json()
        self.assertEqual(data["error_code"], "VALIDATION_ERROR")
        self.assertEqual(data["details"]["constraint"], "gt_zero")

    def test_timezone_naive_datetime_returns_400(self) -> None:
        response = self._post(
            body={**_VALID_BODY, "completed_at": "2026-04-23T17:42:00"}
        )
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        data = response.get_json()
        self.assertEqual(data["error_code"], "VALIDATION_ERROR")
        self.assertEqual(data["details"]["constraint"], "timezone_aware")

    def test_unmapped_field_failure_reports_generic_constraint(self) -> None:
        # course_name is required but has no specific constraint mapping.
        # The previous mapper would have mislabeled this as "invalid_state_code".
        response = self._post(body={**_VALID_BODY, "course_name": 12345})
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        data = response.get_json()
        self.assertEqual(data["error_code"], "VALIDATION_ERROR")
        self.assertEqual(data["details"]["field"], "course_name")
        self.assertEqual(data["details"]["constraint"], "invalid")

    def test_person_resolution_error_audits_and_returns_500(self) -> None:
        # A non-PersonNotFound failure (e.g. BigQuery error) must still emit an
        # audit record before the error handler returns 500.
        self.mock_resolve.side_effect = RuntimeError("BigQuery exploded")
        with patch(f"{MODULE}._log_audit") as mock_audit:
            response = self._post()
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)
        reasons = [call.kwargs.get("reason") for call in mock_audit.call_args_list]
        self.assertIn("person_resolution_error", reasons)

    def test_commit_error_audits_and_returns_500(self) -> None:
        # A commit failure must also be audited before returning 500. Patch
        # Session.commit (not the current_session proxy, which can't be patched
        # outside an app context) and stub persistence so nothing is flushed.
        fake_record = MagicMock()
        fake_record.id = 123
        with patch(
            f"{MODULE}.persist_completion", return_value=(fake_record, True)
        ), patch(
            "sqlalchemy.orm.Session.commit", side_effect=RuntimeError("db down")
        ), patch(
            f"{MODULE}._log_audit"
        ) as mock_audit:
            response = self._post()
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)
        reasons = [call.kwargs.get("reason") for call in mock_audit.call_args_list]
        self.assertIn("commit_error", reasons)
