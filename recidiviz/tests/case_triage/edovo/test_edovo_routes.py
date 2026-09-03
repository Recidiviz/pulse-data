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

External dependencies (WIF token verification, BigQuery person identity check) are
mocked. The database layer uses a real local Postgres instance.
"""
import json
import os
from http import HTTPStatus
from unittest import TestCase
from unittest.mock import MagicMock, patch

from flask import Flask
from flask.testing import FlaskClient
from werkzeug.test import TestResponse

from recidiviz.case_triage.edovo.edovo_routes import (
    RequestOutcome,
    _redacted_body,
    create_edovo_api_blueprint,
)
from recidiviz.case_triage.edovo.person_verification import (
    PersonNameMismatchError,
    PersonNotFoundError,
    verify_person_identity,
)
from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.persistence.database.schema.case_triage.schema import (
    EdovoCourseCompletion,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult
from recidiviz.utils.auth.auth0 import AuthorizationError
from recidiviz.utils.flask_exception import FlaskException

MODULE = "recidiviz.case_triage.edovo.edovo_routes"
WIF_MODULE = "recidiviz.case_triage.edovo.wif_verifier"
PERSON_VERIFICATION_MODULE = "recidiviz.case_triage.edovo.person_verification"

_DOC_ID = "A123456"
_WIF_SA_UNIQUE_ID = "123456789012345678901"
_FIRST_NAME = "Jane"
_LAST_NAME = "Doe"

_VALID_BODY: dict[str, object] = {
    "person_external_id": _DOC_ID,
    "state_code": "US_CO",
    "course_id": "course-001",
    "course_name": "Introduction to Reading",
    "content_hours": 3.5,
    "completed_at": "2026-04-23T17:42:00Z",
    "first_name": _FIRST_NAME,
    "last_name": _LAST_NAME,
    "facility": "CDOC-XYZ",
}

_AUTH_HEADER = "Bearer some.jwt.token"

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

        self.wif_patcher = patch(f"{MODULE}.verify_bearer_token")
        self.mock_wif = self.wif_patcher.start()

        self.bq_patcher = patch(f"{MODULE}.BigQueryClientImpl")
        mock_bq_cls = self.bq_patcher.start()
        self.mock_bq_client = MagicMock()
        mock_bq_cls.return_value = self.mock_bq_client

        self.resolve_patcher = patch(f"{MODULE}.verify_person_identity")
        self.mock_resolve = self.resolve_patcher.start()
        self.mock_resolve.return_value = None

    def tearDown(self) -> None:
        self.wif_patcher.stop()
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

    def test_wif_forbidden_returns_403(self) -> None:
        # A valid token for the wrong identity/audience is a 403, and is audited.
        self.mock_wif.side_effect = FlaskException(
            code="wrong_identity",
            description="wrong service account",
            status_code=HTTPStatus.FORBIDDEN,
        )
        with patch(f"{MODULE}._log_audit") as mock_audit:
            response = self._post()
        self.assertEqual(response.status_code, HTTPStatus.FORBIDDEN)
        data = response.get_json()
        self.assertEqual(data["status"], "error")
        self.assertEqual(data["error_code"], "FORBIDDEN")
        # error_code stays the stable machine value.
        self.assertIn("wrong service account", data["message"])
        self.assertIn("wrong_identity", data["message"])
        mock_audit.assert_called_once()
        self.assertEqual(mock_audit.call_args.kwargs["reason"], "auth:wrong_identity")

    def test_audit_log_redacts_the_learner_name(self) -> None:
        """The audit record needs the identifier, course and timestamp to
        reconcile with Edovo — not the learner's name.

        Asserted against what the handler actually emitted, so that wiring
        ``_log_audit`` to something other than ``_redacted_body`` fails here.
        """
        with self.assertLogs(level="INFO") as logs:
            self._post()
        emitted = "\n".join(logs.output)

        self.assertIn("Edovo course-completion request", emitted)
        self.assertNotIn(_FIRST_NAME, emitted)
        self.assertNotIn(_LAST_NAME, emitted)
        self.assertIn("[REDACTED]", emitted)

    def test_audit_log_keeps_the_course_and_identifier(self) -> None:
        """Redaction must not cost the fields reconciliation runs on."""
        with self.assertLogs(level="INFO") as logs:
            self._post()
        emitted = "\n".join(logs.output)

        self.assertIn(_DOC_ID, emitted)
        self.assertIn("course-001", emitted)

    def test_redaction_leaves_the_rest_of_the_body_byte_for_byte(self) -> None:
        """The record exists to settle what Edovo sent, so nothing but the name
        may change — no renumbering, reordering, or dropped duplicates."""
        self.assertEqual(
            '{"content_hours": 1e2, "first_name": "[REDACTED]", "course_id": "c1"}',
            _redacted_body(
                b'{"content_hours": 1e2, "first_name": "Jane", "course_id": "c1"}'
            ),
        )

    def test_redaction_covers_a_name_cut_off_mid_value(self) -> None:
        """A payload truncated inside the name still carries the part that
        arrived, so the prefix must not reach the log either."""
        self.assertEqual(
            '{"first_name": "[REDACTED]"', _redacted_body(b'{"first_name": "Jan')
        )

    def test_audit_log_redacts_a_name_in_a_body_that_does_not_parse(self) -> None:
        """A truncated payload still carries a name, so the fallback path has to
        redact it too."""
        logged_body = _redacted_body(
            b'{"first_name": "Jane", "last_name": "Doe", "course_id": "c1"'
        )

        self.assertNotIn("Jane", logged_body)
        self.assertNotIn("Doe", logged_body)
        self.assertIn("c1", logged_body)

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

    def test_invalid_token_returns_401(self) -> None:
        self.mock_wif.side_effect = AuthorizationError(
            code="invalid_bearer_token", description="Bearer token failed verification"
        )
        response = self._post()
        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)
        data = response.get_json()
        self.assertEqual(data["status"], "error")
        self.assertEqual(data["error_code"], "UNAUTHENTICATED")
        self.assertIn("Bearer token failed verification", data["message"])
        self.assertIn("invalid_bearer_token", data["message"])

    def test_person_not_found_returns_422(self) -> None:
        self.mock_resolve.side_effect = PersonNotFoundError(_DOC_ID)
        response = self._post()
        self.assertEqual(response.status_code, HTTPStatus.UNPROCESSABLE_ENTITY)
        data = response.get_json()
        self.assertEqual(data["error_code"], "PERSON_NOT_FOUND")

    def test_name_mismatch_returns_422(self) -> None:
        self.mock_resolve.side_effect = PersonNameMismatchError(
            person_external_id=_DOC_ID, mismatched_fields=["last_name"]
        )
        response = self._post()
        self.assertEqual(response.status_code, HTTPStatus.UNPROCESSABLE_ENTITY)
        data = response.get_json()
        self.assertEqual(data["error_code"], "PERSON_NAME_MISMATCH")
        self.assertEqual(["last_name"], data["mismatched_fields"])

    def test_name_mismatch_does_not_echo_the_submitted_name_or_id(self) -> None:
        self.mock_resolve.side_effect = PersonNameMismatchError(
            person_external_id=_DOC_ID,
            mismatched_fields=["first_name", "last_name"],
        )
        response = self._post()
        body = response.get_data(as_text=True)
        self.assertNotIn(_DOC_ID, body)
        self.assertNotIn(_FIRST_NAME, body)
        self.assertNotIn(_LAST_NAME, body)

    def test_name_mismatch_is_audited_with_the_mismatched_fields(self) -> None:
        self.mock_resolve.side_effect = PersonNameMismatchError(
            person_external_id=_DOC_ID,
            mismatched_fields=["first_name", "last_name"],
        )
        with patch(f"{MODULE}._log_audit") as mock_audit:
            self._post()
        self.assertEqual(
            "person_name_mismatch:first_name,last_name",
            mock_audit.call_args.kwargs["reason"],
        )

    def test_name_mismatch_persists_nothing(self) -> None:
        self.mock_resolve.side_effect = PersonNameMismatchError(
            person_external_id=_DOC_ID, mismatched_fields=["last_name"]
        )
        self._post()
        self.assertEqual([], self._stored_external_ids())

    def test_already_completed_returns_422(self) -> None:
        self._post()
        # The same person+course under a different key is a new request, so it
        # trips no_double_credit rather than the replay path.
        response = self._post(idempotency_key=_OTHER_IDEMPOTENCY_KEY)
        self.assertEqual(response.status_code, HTTPStatus.UNPROCESSABLE_ENTITY)
        data = response.get_json()
        self.assertEqual(data["error_code"], "ALREADY_COMPLETED")

    def test_already_completed_points_at_the_original_completion(self) -> None:
        """Edovo asked to be told what we already hold, not just that something
        exists, so the rejection carries the first submission's id and time."""
        first = self._post()
        response = self._post(idempotency_key=_OTHER_IDEMPOTENCY_KEY)

        data = response.get_json()
        self.assertEqual(first.get_json()["completion_id"], data["completion_id"])
        self.assertIsNotNone(data["originally_received_at"])

    def test_replay_is_answered_without_reverifying_identity(self) -> None:
        """A key we already accepted is answered from what we recorded, so a
        replay costs no BigQuery call."""
        self._post()
        self.mock_resolve.reset_mock()

        response = self._post()

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual("duplicate", response.get_json()["status"])
        self.mock_resolve.assert_not_called()

    def test_replay_survives_a_name_change_on_our_side(self) -> None:
        """Our stored name can change after we accept a completion (a
        correction, a re-ingest). Re-verifying a replay would turn a settled
        completion into a 422, breaking the idempotency guarantee."""
        self._post()
        self.mock_resolve.side_effect = PersonNameMismatchError(
            person_external_id=_DOC_ID, mismatched_fields=["last_name"]
        )

        response = self._post()

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual("duplicate", response.get_json()["status"])

    def test_replay_of_a_malformed_body_still_returns_the_original(self) -> None:
        """The Idempotency-Key identifies the request, so once a key names a
        completion we recorded, the recorded answer is returned even if the
        replayed body no longer validates."""
        first = self._post()

        response = self._post(body={"nonsense": True})

        self.assertEqual(response.status_code, HTTPStatus.OK)
        data = response.get_json()
        self.assertEqual("duplicate", data["status"])
        self.assertEqual(first.get_json()["completion_id"], data["completion_id"])

    def test_a_malformed_body_under_a_new_key_is_still_rejected(self) -> None:
        """Answering replays early must not weaken validation for a key we have
        not seen."""
        response = self._post(
            body={"nonsense": True}, idempotency_key=_OTHER_IDEMPOTENCY_KEY
        )

        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual("VALIDATION_ERROR", response.get_json()["error_code"])

    def test_replay_is_audited_as_a_duplicate(self) -> None:
        self._post()
        with patch(f"{MODULE}._log_audit") as mock_audit:
            self._post()
        self.assertEqual(
            RequestOutcome.DUPLICATE, mock_audit.call_args.kwargs["outcome"]
        )

    def test_duplicate_reports_when_the_original_was_received(self) -> None:
        """A replay is distinguishable from a fresh retry only if we say when we
        first recorded it."""
        self._post()
        response = self._post()

        data = response.get_json()
        self.assertEqual("duplicate", data["status"])
        self.assertIsNotNone(data["originally_received_at"])

    def _assert_rejected_as_required(self, response: TestResponse, field: str) -> None:
        """Asserts |response| is a 400 naming |field| as a required field."""
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        data = response.get_json()
        self.assertEqual(data["error_code"], "VALIDATION_ERROR")
        self.assertEqual(data["details"]["field"], field)
        self.assertEqual(data["details"]["constraint"], "required")

    def test_blank_first_name_returns_400_as_a_missing_field(self) -> None:
        response = self._post(body={**_VALID_BODY, "first_name": "   "})
        self._assert_rejected_as_required(response, "first_name")

    def test_blank_last_name_returns_400_as_a_missing_field(self) -> None:
        response = self._post(body={**_VALID_BODY, "last_name": "   "})
        self._assert_rejected_as_required(response, "last_name")

    def test_blank_facility_returns_400_as_a_missing_field(self) -> None:
        response = self._post(body={**_VALID_BODY, "facility": "   "})
        self._assert_rejected_as_required(response, "facility")

    def test_null_first_name_returns_400_as_a_missing_field(self) -> None:
        """A JSON null is as absent as a blank string, so it is reported the
        same way rather than as a generic type error."""
        response = self._post(body={**_VALID_BODY, "first_name": None})
        self._assert_rejected_as_required(response, "first_name")

    def test_null_last_name_returns_400_as_a_missing_field(self) -> None:
        response = self._post(body={**_VALID_BODY, "last_name": None})
        self._assert_rejected_as_required(response, "last_name")

    def test_null_facility_returns_400_as_a_missing_field(self) -> None:
        response = self._post(body={**_VALID_BODY, "facility": None})
        self._assert_rejected_as_required(response, "facility")

    def test_missing_first_name_returns_400(self) -> None:
        body = {k: v for k, v in _VALID_BODY.items() if k != "first_name"}
        self._assert_rejected_as_required(self._post(body=body), "first_name")

    def test_missing_last_name_returns_400(self) -> None:
        body = {k: v for k, v in _VALID_BODY.items() if k != "last_name"}
        self._assert_rejected_as_required(self._post(body=body), "last_name")

    def test_missing_facility_returns_400(self) -> None:
        body = {k: v for k, v in _VALID_BODY.items() if k != "facility"}
        self._assert_rejected_as_required(self._post(body=body), "facility")

    def test_name_and_facility_are_persisted(self) -> None:
        self._post()
        with SessionFactory.using_database(self.database_key) as session:
            record = session.query(EdovoCourseCompletion).one()
            self.assertEqual(_FIRST_NAME, record.first_name)
            self.assertEqual(_LAST_NAME, record.last_name)
            self.assertEqual("CDOC-XYZ", record.facility)

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
        response = self._post(body={**_VALID_BODY, "course_name": 12345})
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        data = response.get_json()
        self.assertEqual(data["error_code"], "VALIDATION_ERROR")
        self.assertEqual(data["details"]["field"], "course_name")
        self.assertEqual(data["details"]["constraint"], "invalid")

    def test_person_resolution_error_audits_and_returns_500(self) -> None:
        # A non-PersonNotFound failure must still be audited before the 500.
        self.mock_resolve.side_effect = RuntimeError("BigQuery exploded")
        with patch(f"{MODULE}._log_audit") as mock_audit:
            response = self._post()
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)
        reasons = [call.kwargs.get("reason") for call in mock_audit.call_args_list]
        self.assertIn("person_resolution_error", reasons)

    def _post_through_real_verification(
        self,
        *,
        person_exists: bool,
        submitted_external_id: str,
        stored_given_names: str,
        stored_surname: str,
    ) -> TestResponse:
        """POSTs a completion with the real ``verify_person_identity`` in the path.

        BigQuery returns a fixed result, so this covers the handler wiring and the
        persistence write, not the comparison — that is emulator-tested in
        ``test_person_verification.py``.
        """
        self.mock_bq_client.run_query_async.return_value = iter(
            [{"given_names": stored_given_names, "surname": stored_surname}]
            if person_exists
            else []
        )
        with patch(f"{MODULE}.verify_person_identity", verify_person_identity), patch(
            f"{PERSON_VERIFICATION_MODULE}.project_id", return_value="recidiviz-123"
        ):
            return self._post(
                {**_VALID_BODY, "person_external_id": submitted_external_id}
            )

    def _stored_external_ids(self) -> list[str]:
        with SessionFactory.using_database(self.database_key) as session:
            return [
                record.person_external_id
                for record in session.query(EdovoCourseCompletion).all()
            ]

    def test_end_to_end_padded_id_is_persisted_verbatim(self) -> None:
        """The padded value Edovo sent is what lands in the database — the
        comparison-only normalization must not reach the stored value."""
        response = self._post_through_real_verification(
            person_exists=True,
            submitted_external_id="000123456",
            stored_given_names=_FIRST_NAME,
            stored_surname=_LAST_NAME,
        )

        self.assertEqual(response.status_code, HTTPStatus.CREATED)
        self.assertEqual(response.get_json()["status"], "accepted")
        self.assertEqual(["000123456"], self._stored_external_ids())

    def test_end_to_end_name_mismatch_returns_422_and_persists_nothing(self) -> None:
        """The id resolves, but to someone else: the drift signal Edovo asked
        for, and no credit captured."""
        response = self._post_through_real_verification(
            person_exists=True,
            submitted_external_id="000123456",
            stored_given_names="Robert",
            stored_surname="Smith",
        )

        self.assertEqual(response.status_code, HTTPStatus.UNPROCESSABLE_ENTITY)
        data = response.get_json()
        self.assertEqual("PERSON_NAME_MISMATCH", data["error_code"])
        self.assertEqual(["first_name", "last_name"], data["mismatched_fields"])
        self.assertEqual([], self._stored_external_ids())

    def test_end_to_end_unknown_person_returns_422_and_persists_nothing(self) -> None:
        response = self._post_through_real_verification(
            person_exists=False,
            submitted_external_id="000123456",
            stored_given_names=_FIRST_NAME,
            stored_surname=_LAST_NAME,
        )

        self.assertEqual(response.status_code, HTTPStatus.UNPROCESSABLE_ENTITY)
        self.assertEqual(response.get_json()["error_code"], "PERSON_NOT_FOUND")
        self.assertEqual([], self._stored_external_ids())

    def test_idempotency_lookup_error_audits_and_returns_500(self) -> None:
        # The lookup runs before the body is parsed, so its failure would
        # otherwise be the one terminal outcome with no audit record.
        with patch(
            f"{MODULE}.find_completion_by_idempotency_key",
            side_effect=RuntimeError("db down"),
        ), patch(f"{MODULE}._log_audit") as mock_audit:
            response = self._post()
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)
        reasons = [call.kwargs.get("reason") for call in mock_audit.call_args_list]
        self.assertIn("idempotency_lookup_error", reasons)

    def test_persist_error_audits_and_returns_500(self) -> None:
        # A persist failure that is not AlreadyCompletedError must still be
        # audited before the 500.
        with patch(
            f"{MODULE}.persist_completion", side_effect=RuntimeError("db down")
        ), patch(f"{MODULE}._log_audit") as mock_audit:
            response = self._post()
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)
        reasons = [call.kwargs.get("reason") for call in mock_audit.call_args_list]
        self.assertIn("persist_error", reasons)

    def test_commit_error_audits_and_returns_500(self) -> None:
        # Patch Session.commit, not the current_session proxy, which cannot be
        # patched outside an app context.
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


class TestEdovoAuthErrors(TestCase):
    """Auth-path responses driven through the real ``verify_bearer_token``.

    Auth is the first thing the handler checks, before any DB or BigQuery
    access, so these need only the blueprint and the shared error handlers — no
    Postgres. External calls are faked exactly as the smoke test does: patch the
    verifier's ``requests.get`` (Google tokeninfo) and set the SA unique-id env.
    """

    def setUp(self) -> None:
        self.test_app = Flask(__name__)
        register_error_handlers(self.test_app)
        self.test_app.register_blueprint(
            create_edovo_api_blueprint(), url_prefix="/edovo"
        )
        self.client: FlaskClient = self.test_app.test_client()
        self.unique_id_env = patch.dict(
            os.environ, {"EDOVO_WIF_SA_UNIQUE_ID": _WIF_SA_UNIQUE_ID}
        )
        self.unique_id_env.start()

    def tearDown(self) -> None:
        self.unique_id_env.stop()

    def _post(self, *, auth: str | None) -> TestResponse:
        headers = {"Idempotency-Key": _IDEMPOTENCY_KEY}
        if auth is not None:
            headers["Authorization"] = auth
        return self.client.post(
            "/edovo/course-completions",
            data=json.dumps(_VALID_BODY),
            content_type="application/json",
            headers=headers,
        )

    def _assert_unauthenticated(
        self, response: TestResponse, verifier_code: str
    ) -> None:
        self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)
        data = response.get_json()
        self.assertEqual(data["status"], "error")
        self.assertEqual(data["error_code"], "UNAUTHENTICATED")
        # A human-readable message that carries — but is not merely — the code.
        self.assertIn(verifier_code, data["message"])
        self.assertNotEqual(data["message"], verifier_code)

    @staticmethod
    def _tokeninfo(
        *, status_code: HTTPStatus, payload: dict[str, object] | None = None
    ) -> MagicMock:
        response = MagicMock()
        response.status_code = status_code
        response.json.return_value = payload or {}
        return response

    def test_missing_authorization_header_returns_401(self) -> None:
        with patch(f"{MODULE}._log_audit") as mock_audit:
            response = self._post(auth=None)
        self._assert_unauthenticated(response, "invalid_authorization_header")
        self.assertEqual(
            mock_audit.call_args.kwargs["outcome"], RequestOutcome.REJECTED
        )
        self.assertEqual(
            mock_audit.call_args.kwargs["reason"], "auth:invalid_authorization_header"
        )

    def test_non_bearer_scheme_returns_401(self) -> None:
        self._assert_unauthenticated(
            self._post(auth="Basic dXNlcjpwYXNz"), "invalid_authorization_header"
        )

    def test_empty_bearer_token_returns_401(self) -> None:
        self._assert_unauthenticated(
            self._post(auth="Bearer "), "invalid_authorization_header"
        )

    def test_tokeninfo_rejected_returns_401(self) -> None:
        with patch(f"{WIF_MODULE}.requests.get") as mock_get:
            mock_get.return_value = self._tokeninfo(status_code=HTTPStatus.BAD_REQUEST)
            response = self._post(auth="Bearer rejected.token")
        self._assert_unauthenticated(response, "invalid_bearer_token")

    def test_wrong_identity_returns_403(self) -> None:
        with patch(f"{WIF_MODULE}.requests.get") as mock_get, patch(
            f"{MODULE}._log_audit"
        ) as mock_audit:
            mock_get.return_value = self._tokeninfo(
                status_code=HTTPStatus.OK,
                payload={"aud": "some-other-service-account", "expires_in": 3600},
            )
            response = self._post(auth="Bearer valid.but.wrong.identity")
        self.assertEqual(response.status_code, HTTPStatus.FORBIDDEN)
        data = response.get_json()
        self.assertEqual(data["status"], "error")
        self.assertEqual(data["error_code"], "FORBIDDEN")
        self.assertIn("wrong_identity", data["message"])
        self.assertNotEqual(data["message"], "wrong_identity")
        self.assertEqual(mock_audit.call_args.kwargs["reason"], "auth:wrong_identity")

    def test_missing_unique_id_env_returns_500_in_shared_shape(self) -> None:
        # A 5xx keeps the shared handler's shape, not the partner shape.
        with patch.dict(os.environ, {"EDOVO_WIF_SA_UNIQUE_ID": ""}), patch(
            f"{MODULE}._log_audit"
        ) as mock_audit:
            response = self._post(auth="Bearer any.well.formed.token")
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)
        data = response.get_json()
        self.assertIn("code", data)
        self.assertNotIn("error_code", data)
        self.assertNotIn("status", data)
        self.assertEqual(
            mock_audit.call_args.kwargs["reason"],
            "auth:token_verification_misconfigured",
        )
