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
"""Tests for the Edovo earned-time credit calculator."""
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase

from flask import Flask

from recidiviz.case_triage.edovo.credit_calculator import (
    new_credits_for_completion,
    query_total_hours,
)
from recidiviz.persistence.database.schema.case_triage.schema import (
    EdevoCourseCompletion,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

_PERSON_ID = "9876543"
_STATE_CODE = "US_CO"
_COMPLETED_AT = datetime(2026, 4, 1, tzinfo=timezone.utc)


def _make_completion(
    person_id: str,
    state_code: str,
    course_id: str,
    content_hours: float,
) -> EdevoCourseCompletion:
    return EdevoCourseCompletion(
        idempotency_key=uuid.uuid4(),
        person_id=person_id,
        state_code=state_code,
        course_id=course_id,
        course_name="Test Course",
        content_hours=content_hours,
        completed_at=_COMPLETED_AT,
    )


class TestNewCreditsForCompletion(TestCase):
    def test_no_credit_when_total_stays_below_threshold(self) -> None:
        self.assertEqual(new_credits_for_completion(Decimal("0"), Decimal("5")), 0)

    def test_exactly_one_credit_at_boundary(self) -> None:
        self.assertEqual(new_credits_for_completion(Decimal("0"), Decimal("6")), 1)

    def test_no_credit_when_prior_total_already_crossed_boundary(self) -> None:
        self.assertEqual(new_credits_for_completion(Decimal("4"), Decimal("1")), 0)

    def test_credit_when_new_hours_push_over_boundary(self) -> None:
        self.assertEqual(new_credits_for_completion(Decimal("4"), Decimal("3")), 1)

    def test_remainder_carries_forward(self) -> None:
        self.assertEqual(new_credits_for_completion(Decimal("5.5"), Decimal("2")), 1)

    def test_multiple_credits_from_large_completion(self) -> None:
        self.assertEqual(new_credits_for_completion(Decimal("0"), Decimal("18")), 3)

    def test_multiple_credits_when_prior_total_has_remainder(self) -> None:
        # 12 prior (2 credits earned) + 7 new = 19 total → 3 total credits, 1 new
        self.assertEqual(new_credits_for_completion(Decimal("12"), Decimal("7")), 1)

    def test_multiple_credits_from_zero_prior(self) -> None:
        # 0 + 13 = 13 → 2 credits with 1 hour remainder
        self.assertEqual(new_credits_for_completion(Decimal("0"), Decimal("13")), 2)

    def test_credit_from_accumulated_remainder(self) -> None:
        self.assertEqual(new_credits_for_completion(Decimal("0"), Decimal("2")), 0)
        self.assertEqual(new_credits_for_completion(Decimal("2"), Decimal("2")), 0)
        self.assertEqual(new_credits_for_completion(Decimal("4"), Decimal("2")), 1)

    def test_fractional_hours_do_not_earn_partial_credits(self) -> None:
        self.assertEqual(new_credits_for_completion(Decimal("0"), Decimal("5.9")), 0)

    def test_non_positive_hours_raises(self) -> None:
        with self.assertRaises(ValueError):
            new_credits_for_completion(Decimal("0"), Decimal("0"))
        with self.assertRaises(ValueError):
            new_credits_for_completion(Decimal("0"), Decimal("-1"))


class TestQueryTotalHours(TestCase):
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

    def test_returns_zero_when_no_completions(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            total = query_total_hours(session, _PERSON_ID, _STATE_CODE)
        self.assertEqual(total, Decimal("0"))

    def test_sums_hours_for_person(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(_PERSON_ID, _STATE_CODE, "c1", 3.5))
            session.add(_make_completion(_PERSON_ID, _STATE_CODE, "c2", 2.5))

        with SessionFactory.using_database(self.database_key) as session:
            total = query_total_hours(session, _PERSON_ID, _STATE_CODE)
        self.assertEqual(total, Decimal("6"))

    def test_excludes_other_persons(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(_PERSON_ID, _STATE_CODE, "c1", 4.0))
            session.add(_make_completion("other-person", _STATE_CODE, "c1", 10.0))

        with SessionFactory.using_database(self.database_key) as session:
            total = query_total_hours(session, _PERSON_ID, _STATE_CODE)
        self.assertEqual(total, Decimal("4"))
