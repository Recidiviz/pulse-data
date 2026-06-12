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
"""Integration tests for Edovo course-completion persistence and idempotency.

These tests write to a real local Postgres database — no DB layer mocks.
"""
import uuid
from unittest import TestCase

from flask import Flask

from recidiviz.case_triage.edovo.course_completion_models import CourseCompletionRequest
from recidiviz.case_triage.edovo.persistence import (
    AlreadyCompletedError,
    acquire_person_credit_lock,
    persist_completion,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

# DOC number as Edovo sends it; distinct from the platform-internal _PERSON_ID.
_DOC_ID = "A123456"
_PERSON_ID = "9876543"

_VALID_PAYLOAD: dict[str, object] = {
    "person_id": _DOC_ID,
    "state_code": "US_CO",
    "course_id": "course-001",
    "course_name": "Introduction to Reading",
    "content_hours": 3.5,
    "completed_at": "2026-04-23T17:42:00Z",
}


class TestPersistCompletion(TestCase):
    """Integration tests — real Postgres, no DB mocks."""

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
        engine = setup_scoped_sessions(Flask(__name__), SchemaType.CASE_TRIAGE, db_url)
        self.database_key.declarative_meta.metadata.create_all(engine)

    def tearDown(self) -> None:
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    def test_new_request_is_written_and_returns_is_new(self) -> None:
        request = CourseCompletionRequest.model_validate(_VALID_PAYLOAD)
        idempotency_key = uuid.uuid4()

        with SessionFactory.using_database(self.database_key) as session:
            record, is_new = persist_completion(
                session, request, _PERSON_ID, idempotency_key
            )
            self.assertTrue(is_new)
            self.assertIsNotNone(record.id)
            self.assertEqual(record.person_id, _PERSON_ID)
            self.assertEqual(record.state_code, "US_CO")
            self.assertEqual(record.course_id, "course-001")
            self.assertEqual(record.idempotency_key, idempotency_key)

    def test_retry_with_same_key_returns_original_record(self) -> None:
        request = CourseCompletionRequest.model_validate(_VALID_PAYLOAD)
        idempotency_key = uuid.uuid4()

        with SessionFactory.using_database(self.database_key) as session:
            original, is_new = persist_completion(
                session, request, _PERSON_ID, idempotency_key
            )
            self.assertTrue(is_new)
            original_id = original.id

        with SessionFactory.using_database(self.database_key) as session:
            retry, is_new = persist_completion(
                session, request, _PERSON_ID, idempotency_key
            )
            self.assertFalse(is_new)
            self.assertEqual(retry.id, original_id)

    def test_same_person_course_different_key_raises(self) -> None:
        request = CourseCompletionRequest.model_validate(_VALID_PAYLOAD)

        with SessionFactory.using_database(self.database_key) as session:
            persist_completion(session, request, _PERSON_ID, uuid.uuid4())

        with self.assertRaises(AlreadyCompletedError):
            with SessionFactory.using_database(self.database_key) as session:
                persist_completion(session, request, _PERSON_ID, uuid.uuid4())

    def test_different_courses_for_same_person_are_independent(self) -> None:
        request_a = CourseCompletionRequest.model_validate(_VALID_PAYLOAD)
        request_b = CourseCompletionRequest.model_validate(
            {**_VALID_PAYLOAD, "course_id": "course-002"}
        )

        with SessionFactory.using_database(self.database_key) as session:
            record_a, is_new_a = persist_completion(
                session, request_a, _PERSON_ID, uuid.uuid4()
            )
            self.assertTrue(is_new_a)
            id_a = record_a.id

        with SessionFactory.using_database(self.database_key) as session:
            record_b, is_new_b = persist_completion(
                session, request_b, _PERSON_ID, uuid.uuid4()
            )
            self.assertTrue(is_new_b)
            id_b = record_b.id

        self.assertNotEqual(id_a, id_b)

    def test_acquire_person_credit_lock_succeeds(self) -> None:
        # Smoke test: the advisory lock SQL is valid against Postgres and a
        # subsequent acquire for a different key in the same transaction does
        # not block (advisory locks are scoped per (lock_key, transaction)).
        with SessionFactory.using_database(self.database_key) as session:
            acquire_person_credit_lock(session, "US_CO", _PERSON_ID)
            acquire_person_credit_lock(session, "US_CO", "different-person")
