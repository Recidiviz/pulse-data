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
from collections.abc import Iterator
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest import TestCase
from unittest.mock import MagicMock, patch

from flask import Flask

from recidiviz.case_triage.edovo.credit_calculator import (
    CourseHoursConflict,
    EdovoCreditCalculation,
    PersonCreditResult,
    calculate_all_pending_credits,
    pooled_credits_for_hours,
)
from recidiviz.persistence.database.schema.case_triage.schema import (
    EdovoCourseCompletion,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

MODULE = "recidiviz.case_triage.edovo.credit_calculator"

# External (DOC-facing) ids and the internal person_ids they resolve to.
_EXT_A = "9876543"
_EXT_B = "1234567"
_PID_A = 111
_PID_B = 222
_ID_TYPE = "US_CO_ADCNUMBER"
_STATE_CODE = "US_CO"
_COMPLETED_AT = datetime(2026, 4, 1, tzinfo=timezone.utc)


def _make_completion(
    person_external_id: str,
    state_code: str,
    course_id: str,
    content_hours: float,
) -> EdovoCourseCompletion:
    return EdovoCourseCompletion(
        idempotency_key=uuid.uuid4(),
        person_external_id=person_external_id,
        id_type=_ID_TYPE,
        state_code=state_code,
        course_id=course_id,
        course_name="Test Course",
        content_hours=content_hours,
        completed_at=_COMPLETED_AT,
    )


def _make_bq_client(person_id_by_external_id: dict[str, int]) -> MagicMock:
    """A mock BQ client whose resolution query returns the configured mapping,
    filtered to the external ids each call requests (ids absent from the map
    resolve to nothing)."""
    client = MagicMock()

    def _run_query_async(**kwargs: Any) -> Iterator[dict[str, Any]]:
        requested = {
            v
            for p in kwargs["query_parameters"]
            if p.name == "external_ids"
            for v in p.values
        }
        return iter(
            [
                {"external_id": ext, "person_id": pid}
                for ext, pid in person_id_by_external_id.items()
                if ext in requested
            ]
        )

    client.run_query_async.side_effect = _run_query_async
    return client


_HOURS_PER_CREDIT = Decimal("6")


class TestPooledCreditsForHours(TestCase):
    """Unit tests for the pure pooling math, total-hours form."""

    def test_below_threshold_earns_nothing(self) -> None:
        self.assertEqual(
            pooled_credits_for_hours(Decimal("5"), hours_per_credit=_HOURS_PER_CREDIT),
            0,
        )

    def test_exact_boundary_earns_one(self) -> None:
        self.assertEqual(
            pooled_credits_for_hours(Decimal("6"), hours_per_credit=_HOURS_PER_CREDIT),
            1,
        )

    def test_remainder_carries_forward(self) -> None:
        self.assertEqual(
            pooled_credits_for_hours(
                Decimal("11.9"), hours_per_credit=_HOURS_PER_CREDIT
            ),
            1,
        )

    def test_multiple_thresholds(self) -> None:
        self.assertEqual(
            pooled_credits_for_hours(Decimal("18"), hours_per_credit=_HOURS_PER_CREDIT),
            3,
        )

    def test_zero_hours(self) -> None:
        self.assertEqual(
            pooled_credits_for_hours(Decimal("0"), hours_per_credit=_HOURS_PER_CREDIT),
            0,
        )

    def test_negative_hours_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be non-negative"):
            pooled_credits_for_hours(Decimal("-1"), hours_per_credit=_HOURS_PER_CREDIT)


class TestCalculateAllPendingCredits(TestCase):
    """DB-backed tests for pooling stored completions per resolved person."""

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
            f"{MODULE}.project_id", return_value="recidiviz-123"
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

    def _calculate(
        self, person_id_by_external_id: dict[str, int]
    ) -> EdovoCreditCalculation:
        bq_client = _make_bq_client(person_id_by_external_id)
        with SessionFactory.using_database(self.database_key) as session:
            return calculate_all_pending_credits(session=session, bq_client=bq_client)

    def _results_by_person(
        self, person_id_by_external_id: dict[str, int]
    ) -> dict[tuple[int, str], PersonCreditResult]:
        return {
            (r.person_id, r.state_code): r
            for r in self._calculate(person_id_by_external_id).person_credits
        }

    def test_no_completions_yields_no_results(self) -> None:
        self.assertEqual(self._results_by_person({}), {})

    def test_spec_example_pooling(self) -> None:
        # Spec example: 3h + 4h = 7h earns 1 credit; a later 5h (12h) earns a 2nd.
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(_EXT_A, _STATE_CODE, "c1", 3.0))
            session.add(_make_completion(_EXT_A, _STATE_CODE, "c2", 4.0))

        result = self._results_by_person({_EXT_A: _PID_A})[(_PID_A, _STATE_CODE)]
        self.assertEqual(result.total_pooled_hours, Decimal("7"))
        self.assertEqual(result.credits_earned, 1)

        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(_EXT_A, _STATE_CODE, "c3", 5.0))

        result = self._results_by_person({_EXT_A: _PID_A})[(_PID_A, _STATE_CODE)]
        self.assertEqual(result.total_pooled_hours, Decimal("12"))
        self.assertEqual(result.credits_earned, 2)

    def test_multiple_thresholds_in_single_completion(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(_EXT_A, _STATE_CODE, "c1", 18.0))

        result = self._results_by_person({_EXT_A: _PID_A})[(_PID_A, _STATE_CODE)]
        self.assertEqual(result.credits_earned, 3)

    def test_exact_six_hour_boundary(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(_EXT_A, _STATE_CODE, "c1", 2.5))
            session.add(_make_completion(_EXT_A, _STATE_CODE, "c2", 3.5))

        result = self._results_by_person({_EXT_A: _PID_A})[(_PID_A, _STATE_CODE)]
        self.assertEqual(result.total_pooled_hours, Decimal("6"))
        self.assertEqual(result.credits_earned, 1)

    def test_float_drift_row_quantizes_up_to_boundary(self) -> None:
        # Float-export drift (5.9999999999 for a 6.0h course) rounds to 6.00 -> 1 credit.
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(_EXT_A, _STATE_CODE, "c1", 5.9999999999))

        result = self._results_by_person({_EXT_A: _PID_A})[(_PID_A, _STATE_CODE)]
        self.assertEqual(result.total_pooled_hours, Decimal("6.00"))
        self.assertEqual(result.credits_earned, 1)

    def test_per_row_quantization_pushes_pool_over_boundary(self) -> None:
        # Two 2.995h rows round per-row to 3.00 -> 6.00 -> 1 credit; summing raw
        # first (5.99) would earn 0. Locks in per-row (not post-sum) rounding.
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(_EXT_A, _STATE_CODE, "c1", 2.995))
            session.add(_make_completion(_EXT_A, _STATE_CODE, "c2", 2.995))

        result = self._results_by_person({_EXT_A: _PID_A})[(_PID_A, _STATE_CODE)]
        self.assertEqual(result.total_pooled_hours, Decimal("6.00"))
        self.assertEqual(result.credits_earned, 1)

    def test_remainder_carries_but_earns_no_partial_credit(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(_EXT_A, _STATE_CODE, "c1", 5.9))

        result = self._results_by_person({_EXT_A: _PID_A})[(_PID_A, _STATE_CODE)]
        self.assertEqual(result.credits_earned, 0)

    def test_pools_per_person_independently(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(_EXT_A, _STATE_CODE, "c1", 7.0))
            session.add(_make_completion(_EXT_B, _STATE_CODE, "c1", 5.0))

        results = self._results_by_person({_EXT_A: _PID_A, _EXT_B: _PID_B})
        self.assertEqual(results[(_PID_A, _STATE_CODE)].credits_earned, 1)
        self.assertEqual(results[(_PID_B, _STATE_CODE)].credits_earned, 0)

    def test_pools_across_a_persons_multiple_external_ids(self) -> None:
        # Two external ids for one person (3.0h each) pool to 6.0h -> 1 credit;
        # pooling by external id would leave each at 3.0h -> 0.
        ext_id_1 = "D44444"
        ext_id_2 = "D44445"
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(ext_id_1, _STATE_CODE, "c1", 3.0))
            session.add(_make_completion(ext_id_2, _STATE_CODE, "c2", 3.0))

        results = self._results_by_person({ext_id_1: 444, ext_id_2: 444})
        self.assertEqual(len(results), 1)
        result = results[(444, _STATE_CODE)]
        self.assertEqual(result.total_pooled_hours, Decimal("6.00"))
        self.assertEqual(result.credits_earned, 1)

    def test_same_course_under_multiple_external_ids_counted_once(self) -> None:
        # Same course under two of a person's external ids counts once (6.0h -> 1),
        # not 12.0h -> 2; the capture constraint can't catch this (keyed on external id).
        ext_id_1 = "D44444"
        ext_id_2 = "D44445"
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(ext_id_1, _STATE_CODE, "same-course", 6.0))
            session.add(_make_completion(ext_id_2, _STATE_CODE, "same-course", 6.0))

        results = self._results_by_person({ext_id_1: 444, ext_id_2: 444})
        self.assertEqual(len(results), 1)
        result = results[(444, _STATE_CODE)]
        self.assertEqual(result.total_pooled_hours, Decimal("6.00"))
        self.assertEqual(result.credits_earned, 1)

    def test_conflicting_hours_for_same_course_surfaced_not_credited(self) -> None:
        # Same course under two of a person's external ids reports different hours
        # (5h vs 7h) — a broken 1:1 course/content invariant. The course is
        # surfaced in `conflicts` and excluded from crediting (not silently max'd).
        ext_id_1 = "D44444"
        ext_id_2 = "D44445"
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(ext_id_1, _STATE_CODE, "conflicted", 5.0))
            session.add(_make_completion(ext_id_2, _STATE_CODE, "conflicted", 7.0))
            session.add(_make_completion(ext_id_1, _STATE_CODE, "clean", 6.0))

        calculation = self._calculate({ext_id_1: 444, ext_id_2: 444})

        # Only the clean 6h course is credited; the conflicted course is excluded.
        self.assertEqual(len(calculation.person_credits), 1)
        credit = calculation.person_credits[0]
        self.assertEqual(credit.person_id, 444)
        self.assertEqual(credit.total_pooled_hours, Decimal("6.00"))
        self.assertEqual(credit.credits_earned, 1)

        self.assertEqual(
            calculation.conflicts,
            [
                CourseHoursConflict(
                    state_code=_STATE_CODE,
                    person_id=444,
                    course_id="conflicted",
                    content_hours_values=[Decimal("5.00"), Decimal("7.00")],
                )
            ],
        )

    def test_unsupported_state_raises(self) -> None:
        # The calculator runs over the whole table; a state with no configured
        # earned-time policy must fail loudly rather than be credited at CO's rate.
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(_EXT_A, "US_WW", "c1", 6.0))

        with self.assertRaisesRegex(
            ValueError, r"No Edovo earned-time credit policy configured for state"
        ):
            self._calculate({_EXT_A: _PID_A})

    def test_person_credits_sorted_by_state_and_person(self) -> None:
        # Re-runs must hand the audit output in a stable (state_code, person_id) order
        # regardless of query/insertion order.
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion("EXT_HI", _STATE_CODE, "c1", 6.0))
            session.add(_make_completion("EXT_LO", _STATE_CODE, "c2", 6.0))

        calculation = self._calculate({"EXT_HI": 900, "EXT_LO": 100})

        self.assertEqual(
            [(c.state_code, c.person_id) for c in calculation.person_credits],
            [(_STATE_CODE, 100), (_STATE_CODE, 900)],
        )

    def test_unresolvable_external_id_is_reported_not_credited(self) -> None:
        # An unresolvable external id is surfaced in `unresolved`, not credited.
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(_EXT_A, _STATE_CODE, "c1", 7.0))
            session.add(_make_completion("GHOST", _STATE_CODE, "c1", 7.0))

        calculation = self._calculate({_EXT_A: _PID_A})

        self.assertEqual(len(calculation.person_credits), 1)
        self.assertEqual(calculation.person_credits[0].person_id, _PID_A)
        self.assertEqual(calculation.person_credits[0].credits_earned, 1)
        self.assertEqual(len(calculation.unresolved), 1)
        unresolved = calculation.unresolved[0]
        self.assertEqual(unresolved.person_external_id, "GHOST")
        self.assertEqual(unresolved.id_type, _ID_TYPE)
        self.assertEqual(unresolved.state_code, _STATE_CODE)

    def test_rerun_is_idempotent(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            session.add(_make_completion(_EXT_A, _STATE_CODE, "c1", 3.0))
            session.add(_make_completion(_EXT_A, _STATE_CODE, "c2", 4.0))

        first = self._results_by_person({_EXT_A: _PID_A})
        second = self._results_by_person({_EXT_A: _PID_A})
        self.assertEqual(first, second)
        self.assertEqual(first[(_PID_A, _STATE_CODE)].credits_earned, 1)
