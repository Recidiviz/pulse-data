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
"""Tests for the Edovo credit-calculation consumer driving a sink."""
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any
from unittest import TestCase
from unittest.mock import MagicMock, patch

from flask import Flask

from recidiviz.case_triage.edovo.credit_calculation_job import run_credit_calculation
from recidiviz.case_triage.edovo.external_id_matching import strip_leading_zeros
from recidiviz.case_triage.edovo.pending_credit_sink import InMemoryPendingCreditSink
from recidiviz.persistence.database.schema.case_triage.schema import (
    EdovoCourseCompletion,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

CALCULATOR_MODULE = "recidiviz.case_triage.edovo.credit_calculator"

_PERSON_EXTERNAL_ID = "9876543"
_PERSON_ID = 111
_ID_TYPE = "US_CO_ADCNUMBER"
_STATE_CODE = "US_CO"
_COMPLETED_AT = datetime(2026, 4, 1, tzinfo=timezone.utc)


def _make_bq_client() -> MagicMock:
    client = MagicMock()

    def _run_query_async(**_kwargs: Any) -> Iterator[dict[str, Any]]:
        return iter(
            [
                {
                    "stripped_external_id": strip_leading_zeros(_PERSON_EXTERNAL_ID),
                    "person_id": _PERSON_ID,
                }
            ]
        )

    client.run_query_async.side_effect = _run_query_async
    return client


def _make_completion(course_id: str, content_hours: float) -> EdovoCourseCompletion:
    return EdovoCourseCompletion(
        idempotency_key=uuid.uuid4(),
        person_external_id=_PERSON_EXTERNAL_ID,
        id_type=_ID_TYPE,
        state_code=_STATE_CODE,
        course_id=course_id,
        course_name="Test Course",
        content_hours=content_hours,
        completed_at=_COMPLETED_AT,
    )


class TestRunCreditCalculation(TestCase):
    """Drives the consumer against a local Postgres and an in-memory sink."""

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
        self.project_id_patcher = patch(
            f"{CALCULATOR_MODULE}.project_id", return_value="recidiviz-123"
        )
        self.project_id_patcher.start()
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
        self.project_id_patcher.stop()
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    def _run(self, sink: InMemoryPendingCreditSink) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            run_credit_calculation(
                session=session, bq_client=_make_bq_client(), sink=sink
            )

    def test_records_pooled_credits_to_sink(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion("c1", 3.0))
            session.add(_make_completion("c2", 4.0))

        sink = InMemoryPendingCreditSink()
        self._run(sink)

        self.assertEqual(sink.run_count, 1)
        self.assertEqual(len(sink.latest_results), 1)
        self.assertEqual(sink.latest_results[0].credits_earned, 1)

    def test_rerun_hands_off_equal_results(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion("c1", 3.0))
            session.add(_make_completion("c2", 4.0))

        sink = InMemoryPendingCreditSink()
        self._run(sink)
        first_results = sink.latest_results
        self._run(sink)

        self.assertEqual(sink.run_count, 2)
        self.assertEqual(first_results, sink.latest_results)

    def test_no_completions_records_empty(self) -> None:
        sink = InMemoryPendingCreditSink()
        self._run(sink)

        self.assertEqual(sink.run_count, 1)
        self.assertEqual(sink.latest_results, [])
