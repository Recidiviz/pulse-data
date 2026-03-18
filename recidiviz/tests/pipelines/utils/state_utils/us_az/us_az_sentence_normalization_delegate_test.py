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
import json
import unittest
from datetime import date, datetime

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSentence,
    StateSentenceStatusSnapshot,
    StateSupervisionPeriod,
)
from recidiviz.pipelines.utils.state_utils.us_az.us_az_sentence_normalization_delegate import (
    UsAzSentenceNormalizationDelegate,
)

_STATE_CODE = StateCode.US_AZ.value


def _metadata_with_doc_number(doc_number: str) -> str:
    """Helper to create sentence_metadata JSON with a doc_number."""
    return json.dumps({"doc_number": doc_number})


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
    def _build_supervision_period(
        start_date: date,
        termination_date: date | None,
    ) -> StateSupervisionPeriod:
        """Helper to create test incarceration periods."""
        return StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp1",
            start_date=start_date,
            termination_date=termination_date,
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
        sentence_group_external_id: str | None = None,
        sentence_metadata: str | None = None,
    ) -> StateSentence:
        """Helper to create test sentences."""
        sentence = StateSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=f"sent{sentence_id}",
            sentence_group_external_id=sentence_group_external_id,
            sentence_metadata=sentence_metadata,
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
            sentence_group_external_id="100",
            sentence_metadata=_metadata_with_doc_number("1"),
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            supervision_periods=[],
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

    def test_filters_most_recent_completed_when_person_supervised(self) -> None:
        """Test that COMPLETED status is filtered for most recent sentence when person is on supervision."""
        # Person released
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=date(2024, 6, 1),  # Released
        )

        supervision_period = self._build_supervision_period(
            start_date=date(2024, 6, 1), termination_date=None  # Still supervised
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
            sentence_group_external_id="100",
            sentence_metadata=_metadata_with_doc_number("1"),
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
            sentences=[sentence],
        )

        # COMPLETED status should be filtered out
        result = delegate.update_sentence_status_snapshot(
            sentence=sentence,
            snapshot=completed,
        )

        self.assertIsNone(result, "COMPLETED status should be filtered out")

        # SERVING status should be filtered out
        result_serving = delegate.update_sentence_status_snapshot(
            sentence=sentence,
            snapshot=serving,
        )

        self.assertEqual(
            result_serving, serving, "SERVING status should remain unchanged"
        )

    def test_keeps_completed_when_person_not_incarcerated_or_supervised(self) -> None:
        """Test that COMPLETED status is kept as-is when person is not incarcerated or supervised."""
        # Person released
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=date(2024, 6, 1),  # Released
        )

        supervision_period = self._build_supervision_period(
            start_date=date(2024, 6, 1),
            termination_date=date(2024, 8, 1),  # Released
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
            supervision_periods=[supervision_period],
            sentences=[sentence],
        )

        # COMPLETED status should be kept as-is
        result = delegate.update_sentence_status_snapshot(
            sentence=sentence,
            snapshot=completed,
        )

        self.assertEqual(result, completed, "COMPLETED status should be kept as-is")

    def test_multiple_sentences_only_most_recent_filtered(self) -> None:
        """Test that only the sentence in the most recent group is affected."""
        # Person still incarcerated
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=None,
        )

        # Sentence A: older group
        serving_a = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2015, 1, 1),
        )
        completed_a = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2018, 1, 1),
        )
        sentence_a = self._build_sentence(
            sentence_id=1,
            statuses=[serving_a, completed_a],
            sentence_group_external_id="100",
            sentence_metadata=_metadata_with_doc_number("1"),
        )

        # Sentence B: newer group
        serving_b = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2020, 1, 1),
        )
        completed_b = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2024, 1, 1),
        )
        sentence_b = self._build_sentence(
            sentence_id=2,
            statuses=[serving_b, completed_b],
            sentence_group_external_id="200",
            sentence_metadata=_metadata_with_doc_number("2"),
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            supervision_periods=[],
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
            "Older group sentence COMPLETED status should be kept as-is",
        )

        # Sentence B's COMPLETED should be filtered (most recent group)
        result_b = delegate.update_sentence_status_snapshot(
            sentence=sentence_b,
            snapshot=completed_b,
        )
        self.assertIsNone(
            result_b,
            "Most recent group sentence COMPLETED status should be filtered out",
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
            sentence_group_external_id="100",
            sentence_metadata=_metadata_with_doc_number("1"),
        )

        # Scenario 1: Person still incarcerated today
        period_still_incarcerated = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=None,  # Still incarcerated
        )

        delegate_incarcerated = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[period_still_incarcerated],
            supervision_periods=[],
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
            supervision_periods=[],
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
            sentence_group_external_id="100",
            sentence_metadata=_metadata_with_doc_number("1"),
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            supervision_periods=[],
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
            sentence_group_external_id="100",
            sentence_metadata=_metadata_with_doc_number("1"),
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            supervision_periods=[],
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
            supervision_periods=[],
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
            sentence_group_external_id="100",
            sentence_metadata=_metadata_with_doc_number("1"),
        )

        # Should not error when no incarceration periods provided
        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[],
            sentences=[sentence],
        )

        self.assertFalse(delegate.is_currently_incarcerated)

        # COMPLETED should be kept (no open periods)
        result = delegate.update_sentence_status_snapshot(
            sentence=sentence,
            snapshot=completed,
        )

        self.assertEqual(result, completed)

    def test_tied_most_recent_serving_dates_same_group_both_filtered(self) -> None:
        """Test that when multiple sentences in the same group have same SERVING date, all are filtered."""
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=None,  # Still incarcerated
        )

        # Sentence A: SERVING on 2020-06-01, same group as B
        serving_a = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2020, 6, 1),
        )
        completed_a = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2024, 1, 1),
        )
        sentence_a = self._build_sentence(
            sentence_id=1,
            statuses=[serving_a, completed_a],
            sentence_group_external_id="100",
            sentence_metadata=_metadata_with_doc_number("1"),
        )

        # Sentence B: SERVING on 2020-06-01 (same as A), same group
        serving_b = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2020, 6, 1),
        )
        completed_b = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2024, 1, 1),
        )
        sentence_b = self._build_sentence(
            sentence_id=2,
            statuses=[serving_b, completed_b],
            sentence_group_external_id="100",
            sentence_metadata=_metadata_with_doc_number("1"),
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            supervision_periods=[],
            sentences=[sentence_a, sentence_b],
        )

        # Both COMPLETED statuses should be filtered (both in same most recent group)
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

    def test_group_aware_filtering_only_most_recent_group(self) -> None:
        """Test that only the most recent group's inferred COMPLETED is filtered."""
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=None,  # Still incarcerated
        )

        # Sentence A: older group
        serving_a = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2015, 1, 1),
        )
        completed_a = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2018, 1, 1),
        )
        sentence_a = self._build_sentence(
            sentence_id=1,
            statuses=[serving_a, completed_a],
            sentence_group_external_id="100",
            sentence_metadata=_metadata_with_doc_number("1"),
        )

        # Sentence B: newer group
        serving_b = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2020, 1, 1),
        )
        completed_b = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2024, 1, 1),
        )
        sentence_b = self._build_sentence(
            sentence_id=2,
            statuses=[serving_b, completed_b],
            sentence_group_external_id="200",
            sentence_metadata=_metadata_with_doc_number("2"),
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            supervision_periods=[],
            sentences=[sentence_a, sentence_b],
        )

        # Sentence A's COMPLETED should be kept (older group)
        result_a = delegate.update_sentence_status_snapshot(
            sentence=sentence_a,
            snapshot=completed_a,
        )
        self.assertEqual(
            result_a,
            completed_a,
            "Older group sentence COMPLETED should be kept",
        )

        # Sentence B's COMPLETED should be filtered (most recent group)
        result_b = delegate.update_sentence_status_snapshot(
            sentence=sentence_b,
            snapshot=completed_b,
        )
        self.assertIsNone(
            result_b,
            "Most recent group sentence COMPLETED should be filtered",
        )

    def test_group_aware_same_serving_date_different_groups(self) -> None:
        """Test that same serving start date but different groups only filters the newer group.

        This is the key fix: previously, both sentences would have their COMPLETED filtered
        because they shared the same serving date. With group-aware logic, only the newer
        group's sentence gets filtered.
        """
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=None,  # Still incarcerated
        )

        # Sentence A: older group, same serving start date as B
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
            sentence_group_external_id="100",  # Older group
            sentence_metadata=_metadata_with_doc_number("1"),
        )

        # Sentence B: newer group, same serving start date as A
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
            sentence_group_external_id="200",  # Newer group
            sentence_metadata=_metadata_with_doc_number("2"),
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            supervision_periods=[],
            sentences=[sentence_a, sentence_b],
        )

        # Sentence A's COMPLETED should be KEPT (older group, even with same serving date)
        result_a = delegate.update_sentence_status_snapshot(
            sentence=sentence_a,
            snapshot=completed_a,
        )
        self.assertEqual(
            result_a,
            completed_a,
            "Older group sentence COMPLETED should be kept even with same serving date",
        )

        # Sentence B's COMPLETED should be filtered (newer group)
        result_b = delegate.update_sentence_status_snapshot(
            sentence=sentence_b,
            snapshot=completed_b,
        )
        self.assertIsNone(
            result_b,
            "Newer group sentence COMPLETED should be filtered",
        )

    def test_inverted_episode_id_ordering(self) -> None:
        """Test that the delegate correctly identifies the most recent group even when
        SC_EPISODE_ID ordering is inverted relative to chronological ordering.

        In some AZ data, higher SC_EPISODE_IDs correspond to older episodes. The
        delegate uses DOC_NUMBER (cycle number) from sentence_metadata, which is
        the only reliable chronological ordering.
        """
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2009, 12, 16),
            release_date=None,  # Still incarcerated
        )

        # Sentence A: old episode with HIGHER SC_EPISODE_ID but LOWER DOC_NUMBER
        serving_a = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(1975, 5, 30),
        )
        completed_a = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(1985, 5, 29),
        )
        sentence_a = self._build_sentence(
            sentence_id=1,
            statuses=[serving_a, completed_a],
            sentence_group_external_id="469120",  # Higher ID but older episode
            sentence_metadata=_metadata_with_doc_number("1"),  # DOC_NUMBER 1 = oldest
        )

        # Sentence B: new episode with LOWER SC_EPISODE_ID but HIGHER DOC_NUMBER
        serving_b = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2009, 12, 16),
        )
        completed_b = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2025, 12, 16),
        )
        sentence_b = self._build_sentence(
            sentence_id=2,
            statuses=[serving_b, completed_b],
            sentence_group_external_id="469115",  # Lower ID but newer episode
            sentence_metadata=_metadata_with_doc_number("6"),  # DOC_NUMBER 6 = newest
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            supervision_periods=[],
            sentences=[sentence_a, sentence_b],
        )

        # Sentence A (old episode, high ID, low DOC_NUMBER) should KEEP its COMPLETED
        result_a = delegate.update_sentence_status_snapshot(
            sentence=sentence_a,
            snapshot=completed_a,
        )
        self.assertEqual(
            result_a,
            completed_a,
            "Old episode sentence should keep COMPLETED (DOC_NUMBER 1 < 6)",
        )

        # Sentence B (new episode, low ID, high DOC_NUMBER) should have COMPLETED filtered
        result_b = delegate.update_sentence_status_snapshot(
            sentence=sentence_b,
            snapshot=completed_b,
        )
        self.assertIsNone(
            result_b,
            "New episode sentence should have COMPLETED filtered (DOC_NUMBER 6 = most recent)",
        )

    def test_within_group_only_latest_serving_filtered(self) -> None:
        """Test that within the most recent group, only the sentence(s) with the latest
        serving start date have their COMPLETED filtered. Other sentences in the same
        group that started serving earlier should keep their COMPLETED status, since
        they legitimately completed while the person continues serving the later sentence.
        """
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2020, 1, 1),
            release_date=None,  # Still incarcerated
        )

        # Sentence A: same group, earlier serving start date (legitimately completed)
        serving_a = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2020, 1, 1),  # Earlier serving date
        )
        completed_a = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2023, 6, 1),  # Projected end passed
        )
        sentence_a = self._build_sentence(
            sentence_id=1,
            statuses=[serving_a, completed_a],
            sentence_group_external_id="100",
            sentence_metadata=_metadata_with_doc_number("1"),
        )

        # Sentence B: same group, later serving start date (still actively serving)
        serving_b = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2021, 3, 1),  # Later serving date
        )
        completed_b = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2025, 6, 1),  # Projected end passed
        )
        sentence_b = self._build_sentence(
            sentence_id=2,
            statuses=[serving_b, completed_b],
            sentence_group_external_id="100",  # Same group as A
            sentence_metadata=_metadata_with_doc_number("1"),
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            supervision_periods=[],
            sentences=[sentence_a, sentence_b],
        )

        # Sentence A's COMPLETED should be KEPT (earlier serving date, legitimately completed)
        result_a = delegate.update_sentence_status_snapshot(
            sentence=sentence_a,
            snapshot=completed_a,
        )
        self.assertEqual(
            result_a,
            completed_a,
            "Earlier-serving sentence in same group should keep COMPLETED",
        )

        # Sentence B's COMPLETED should be FILTERED (latest serving date, person still incarcerated)
        result_b = delegate.update_sentence_status_snapshot(
            sentence=sentence_b,
            snapshot=completed_b,
        )
        self.assertIsNone(
            result_b,
            "Latest-serving sentence in same group should have COMPLETED filtered",
        )

    def test_older_doc_number_with_later_serving_date(self) -> None:
        """Test that DOC_NUMBER takes precedence over serving start dates.

        In some cases, an older episode (lower DOC_NUMBER) has sentences with later
        serving dates than the newer episode. The delegate should still pick the
        higher DOC_NUMBER as most recent.
        """
        incarceration_period = self._build_incarceration_period(
            admission_date=date(2010, 7, 28),
            release_date=None,
        )

        # Sentence A: older episode (DOC_NUMBER 1) but LATER serving date
        serving_a = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2011, 10, 20),  # Later serving date
        )
        completed_a = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2024, 1, 1),
        )
        sentence_a = self._build_sentence(
            sentence_id=1,
            statuses=[serving_a, completed_a],
            sentence_group_external_id="829537",
            sentence_metadata=_metadata_with_doc_number("1"),
        )

        # Sentence B: newer episode (DOC_NUMBER 2) but EARLIER serving date
        serving_b = self._build_sentence_status(
            StateSentenceStatus.SERVING,
            "IMPOSED",
            date(2010, 7, 28),  # Earlier serving date
        )
        completed_b = self._build_sentence_status(
            StateSentenceStatus.COMPLETED,
            "RECIDIVIZ MARKED COMPLETED",
            date(2024, 6, 1),
        )
        sentence_b = self._build_sentence(
            sentence_id=2,
            statuses=[serving_b, completed_b],
            sentence_group_external_id="926276",
            sentence_metadata=_metadata_with_doc_number("2"),
        )

        delegate = UsAzSentenceNormalizationDelegate(
            incarceration_periods=[incarceration_period],
            supervision_periods=[],
            sentences=[sentence_a, sentence_b],
        )

        # Sentence A (DOC_NUMBER 1) should KEEP its COMPLETED
        result_a = delegate.update_sentence_status_snapshot(
            sentence=sentence_a,
            snapshot=completed_a,
        )
        self.assertEqual(
            result_a,
            completed_a,
            "Older episode should keep COMPLETED despite later serving date",
        )

        # Sentence B (DOC_NUMBER 2) should have COMPLETED filtered
        result_b = delegate.update_sentence_status_snapshot(
            sentence=sentence_b,
            snapshot=completed_b,
        )
        self.assertIsNone(
            result_b,
            "Newer episode should have COMPLETED filtered despite earlier serving date",
        )
