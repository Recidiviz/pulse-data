#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests us_az_sentence_normalization_delegate.py."""
import unittest
from datetime import date, datetime

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSentence,
    StateSentenceStatusSnapshot,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_sentence_normalization_delegate import (
    UsAzSentenceNormalizationDelegate,
)

_STATE_CODE = StateCode.US_AZ.value


class TestUsAzSentenceNormalizationDelegate(unittest.TestCase):
    """Tests functions in UsAzSentenceNormalizationDelegate."""

    @staticmethod
    def _build_incarceration_period(
        admission_date: date,
        release_date: date | None,
    ) -> StateIncarcerationPeriod:
        """Helper to create test incarceration periods."""
        return StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="ip1",
            admission_date=admission_date,
            release_date=release_date,
        )

    @staticmethod
    def _build_sentence_status(
        status: StateSentenceStatus,
        status_raw_text: str,
        status_date: date,
    ) -> StateSentenceStatusSnapshot:
        """Helper to create test sentence status snapshots."""
        return StateSentenceStatusSnapshot.new_with_defaults(
            state_code=_STATE_CODE,
            status=status,
            status_raw_text=status_raw_text,
            status_update_datetime=datetime.combine(status_date, datetime.min.time()),
        )

    @staticmethod
    def _build_sentence(
        sentence_id: int,
        statuses: list[StateSentenceStatusSnapshot],
    ) -> StateSentence:
        """Helper to create test sentences."""
        sentence = StateSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=f"sent{sentence_id}",
            sentence_status_snapshots=statuses,
        )
        sentence.sentence_id = sentence_id
        return sentence

    def test_filters_most_recent_completed_when_person_incarcerated(self) -> None:
        """Test that COMPLETED status is filtered for most recent sentence when person is incarcerated."""
        # Person still incarcerated
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=None,  # Still incarcerated
        )

        # Sentence with SERVING and inferred COMPLETED statuses
        serving = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2020, 1, 1),
        )
        completed = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2024, 1, 1),  # Projected completion date (in the past)
        )
        sentence = self._build_sentence(
            sentence_id=1,
            statuses=[serving, completed],
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            sentences=[sentence],
        )

        # COMPLETED status should be filtered out
        result = delegate.update_sentence_status_snapshot(
            sentence=sentence,
            snapshot=completed,
        )

        self.assertIsNone(result, "COMPLETED status should be filtered out")

        # SERVING status should remain unchanged
        result_serving = delegate.update_sentence_status_snapshot(
            sentence=sentence,
            snapshot=serving,
        )

        self.assertEqual(
            result_serving, serving, "SERVING status should remain unchanged"
        )

    def test_keeps_completed_when_person_not_incarcerated(self) -> None:
        """Test that COMPLETED status is kept as-is when person is not incarcerated."""
        # Person released
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=date(2024, 6, 1),  # Released
        )

        # Sentence with COMPLETED status
        serving = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2020, 1, 1),
        )
        completed = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2024, 1, 1),  # Projected completion date
        )
        sentence = self._build_sentence(
            sentence_id=1,
            statuses=[serving, completed],
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            sentences=[sentence],
        )

        # COMPLETED status should be kept as-is
        result = delegate.update_sentence_status_snapshot(
            sentence=sentence,
            snapshot=completed,
        )

        self.assertEqual(result, completed, "COMPLETED status should be kept as-is")

    def test_multiple_sentences_only_most_recent_filtered(self) -> None:
        """Test that only the sentence with most recent SERVING date is affected."""
        # Person still incarcerated
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=None,
        )

        # Sentence A: older SERVING date
        serving_a = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2015, 1, 1),  # Older SERVING date
        )
        completed_a = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2018, 1, 1),
        )
        sentence_a = self._build_sentence(
            sentence_id=1,
            statuses=[serving_a, completed_a],
        )

        # Sentence B: more recent SERVING date
        serving_b = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2020, 1, 1),  # More recent SERVING date
        )
        completed_b = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2024, 1, 1),
        )
        sentence_b = self._build_sentence(
            sentence_id=2,
            statuses=[serving_b, completed_b],
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            sentences=[sentence_a, sentence_b],
        )

        # Sentence A's COMPLETED should be kept (not most recent SERVING)
        result_a = delegate.update_sentence_status_snapshot(
            sentence=sentence_a,
            snapshot=completed_a,
        )
        self.assertEqual(
            result_a,
            completed_a,
            "Older sentence COMPLETED status should be kept as-is",
        )

        # Sentence B's COMPLETED should be filtered (most recent SERVING)
        result_b = delegate.update_sentence_status_snapshot(
            sentence=sentence_b,
            snapshot=completed_b,
        )
        self.assertIsNone(
            result_b,
            "Most recent sentence COMPLETED status should be filtered out",
        )

    def test_person_releases_after_projected_date(self) -> None:
        """Test behavior when person releases after their projected completion date.

        While incarcerated: COMPLETED status filtered out
        After release: COMPLETED status reappears with original projected date
        """
        # Sentence with projected completion in past
        serving = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2020, 1, 1),
        )
        completed = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2024, 1, 1),  # Projected completion date (in past)
        )
        sentence = self._build_sentence(
            sentence_id=1,
            statuses=[serving, completed],
        )

        # Scenario 1: Person still incarcerated today
        period_still_incarcerated = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=None,  # Still incarcerated
        )

        delegate_incarcerated = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[period_still_incarcerated],
            sentences=[sentence],
        )

        result_incarcerated = delegate_incarcerated.update_sentence_status_snapshot(
            sentence=sentence,
            snapshot=completed,
        )

        self.assertIsNone(
            result_incarcerated,
            "While incarcerated: COMPLETED should be filtered out",
        )

        # Scenario 2: Person releases in future
        period_released = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=date(2025, 6, 15),  # Released after projected date
        )

        delegate_released = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[period_released],
            sentences=[sentence],
        )

        result_released = delegate_released.update_sentence_status_snapshot(
            sentence=sentence,
            snapshot=completed,
        )

        self.assertEqual(
            result_released,
            completed,
            "After release: COMPLETED should reappear with original projected date",
        )

    def test_ignores_future_release_dates(self) -> None:
        """Test that future release dates are treated as open periods."""
        # Person has "release date" in future (should be treated as still incarcerated)
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=date(2030, 1, 1),  # Future date
        )

        serving = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2020, 1, 1),
        )
        completed = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2024, 1, 1),
        )
        sentence = self._build_sentence(
            sentence_id=1,
            statuses=[serving, completed],
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            sentences=[sentence],
        )

        # Should filter COMPLETED (person still considered incarcerated)
        result = delegate.update_sentence_status_snapshot(
            sentence=sentence,
            snapshot=completed,
        )

        self.assertIsNone(
            result,
            "Future release date should be treated as open period, COMPLETED filtered",
        )

    def test_does_not_affect_non_inferred_completed_statuses(self) -> None:
        """Test that explicit (non-inferred) COMPLETED statuses are not affected."""
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=None,  # Still incarcerated
        )

        # Explicit COMPLETED status (not inferred)
        serving = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2020, 1, 1),
        )
        completed_explicit = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "DISCHARGE",  # Not "RECIDIVIZ MARKED COMPLETED"
            date(2024, 1, 1),
        )
        sentence = self._build_sentence(
            sentence_id=1,
            statuses=[serving, completed_explicit],
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            sentences=[sentence],
        )

        # Explicit COMPLETED should not be filtered
        result = delegate.update_sentence_status_snapshot(
            sentence=sentence,
            snapshot=completed_explicit,
        )

        self.assertEqual(
            result,
            completed_explicit,
            "Explicit COMPLETED status should not be filtered",
        )

    def test_no_sentences_no_error(self) -> None:
        """Test that delegate handles case with no sentences gracefully."""
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=None,
        )

        # Should not error when no sentences provided
        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            sentences=[],
        )

        self.assertEqual(len(delegate.most_recent_serving_sentence_ids), 0)

    def test_no_incarceration_periods_no_error(self) -> None:
        """Test that delegate handles case with no incarceration periods gracefully."""
        serving = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2020, 1, 1),
        )
        completed = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2024, 1, 1),
        )
        sentence = self._build_sentence(
            sentence_id=1,
            statuses=[serving, completed],
        )

        # Should not error when no incarceration periods provided
        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[],
            sentences=[sentence],
        )

        self.assertFalse(delegate.is_currently_incarcerated)

        # COMPLETED should be kept (no open periods)
        result = delegate.update_sentence_status_snapshot(
            sentence=sentence,
            snapshot=completed,
        )

        self.assertEqual(result, completed)

    def test_tied_most_recent_serving_dates_both_filtered(self) -> None:
        """Test that when multiple sentences have same most recent SERVING date, all are filtered."""
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=None,  # Still incarcerated
        )

        # Sentence A: SERVING on 2020-06-01
        serving_a = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2020, 6, 1),  # Same SERVING date as B
        )
        completed_a = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2024, 1, 1),
        )
        sentence_a = self._build_sentence(
            sentence_id=1,
            statuses=[serving_a, completed_a],
        )

        # Sentence B: SERVING on 2020-06-01 (same as A)
        serving_b = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2020, 6, 1),  # Same SERVING date as A
        )
        completed_b = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2024, 1, 1),
        )
        sentence_b = self._build_sentence(
            sentence_id=2,
            statuses=[serving_b, completed_b],
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            sentences=[sentence_a, sentence_b],
        )

        # Both COMPLETED statuses should be filtered (tied for most recent SERVING)
        result_a = delegate.update_sentence_status_snapshot(
            sentence=sentence_a,
            snapshot=completed_a,
        )
        result_b = delegate.update_sentence_status_snapshot(
            sentence=sentence_b,
            snapshot=completed_b,
        )

        self.assertIsNone(result_a, "Sentence A COMPLETED should be filtered")
        self.assertIsNone(result_b, "Sentence B COMPLETED should be filtered")
