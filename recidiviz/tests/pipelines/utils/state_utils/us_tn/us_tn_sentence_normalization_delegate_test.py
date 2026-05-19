#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests us_tn_sentence_normalization_delegate.py."""
import json
import unittest
from datetime import date, datetime

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.activity.entities import (
    StateIncarcerationPeriod,
    StateSentence,
    StateSentenceLength,
    StateSentenceStatusSnapshot,
    StateSupervisionPeriod,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_sentence_normalization_delegate import (
    UsTnSentenceNormalizationDelegate,
)

_STATE_CODE = StateCode.US_TN.value


class TestUsTnSentenceNormalizationDelegate(unittest.TestCase):
    """Tests UsTnSentenceNormalizationDelegate's keep-open behavior for the
    most-recently-active sentence(s) of a person currently in DOC custody
    (on supervision OR incarcerated)."""

    @staticmethod
    def _build_supervision_period(
        start_date: date,
        termination_date: date | None,
    ) -> StateSupervisionPeriod:
        return StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp1",
            start_date=start_date,
            termination_date=termination_date,
        )

    @staticmethod
    def _build_incarceration_period(
        admission_date: date,
        release_date: date | None,
    ) -> StateIncarcerationPeriod:
        return StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="ip1",
            admission_date=admission_date,
            release_date=release_date,
        )

    @staticmethod
    def _build_status(
        status: StateSentenceStatus,
        status_date: date,
        status_raw_text: str | None = None,
    ) -> StateSentenceStatusSnapshot:
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
        imposed_date: date | None = None,
        sentence_lengths: list[StateSentenceLength] | None = None,
        is_life: bool = False,
        sentence_metadata: str | None = None,
    ) -> StateSentence:
        sentence = StateSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id=f"sent{sentence_id}",
            sentence_status_snapshots=statuses,
            imposed_date=imposed_date,
            sentence_lengths=sentence_lengths or [],
            is_life=is_life,
            sentence_metadata=sentence_metadata,
        )
        sentence.sentence_id = sentence_id
        return sentence

    @staticmethod
    def _build_length(
        update_date: date,
        proj_max: date | None,
    ) -> StateSentenceLength:
        return StateSentenceLength.new_with_defaults(
            state_code=_STATE_CODE,
            length_update_datetime=datetime.combine(update_date, datetime.min.time()),
            projected_completion_date_max_external=proj_max,
        )

    # ---- Core scenarios ----

    def test_filters_completed_for_most_recent_sentence_when_supervised(self) -> None:
        """When the person is on supervision, COMPLETED is filtered for the
        most-recently-active sentence so it stays "open" downstream."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)
        serving = self._build_status(StateSentenceStatus.SERVING, date(2020, 1, 1))
        completed = self._build_status(StateSentenceStatus.COMPLETED, date(2024, 8, 1))
        sentence = self._build_sentence(1, [serving, completed])

        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[sentence],
        )

        self.assertIsNone(
            delegate.update_sentence_status_snapshot(sentence, completed),
            "COMPLETED status should be filtered out",
        )
        self.assertEqual(
            delegate.update_sentence_status_snapshot(sentence, serving),
            serving,
            "SERVING status should pass through unchanged",
        )

    def test_keeps_completed_when_not_currently_supervised(self) -> None:
        """When the person is no longer on supervision, COMPLETED is preserved."""
        sp = self._build_supervision_period(date(2020, 1, 1), date(2024, 6, 1))
        serving = self._build_status(StateSentenceStatus.SERVING, date(2020, 1, 1))
        completed = self._build_status(StateSentenceStatus.COMPLETED, date(2024, 6, 1))
        sentence = self._build_sentence(1, [serving, completed])

        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[sentence],
        )

        self.assertEqual(
            delegate.update_sentence_status_snapshot(sentence, completed),
            completed,
            "COMPLETED should be preserved when person is no longer supervised",
        )

    def test_only_most_recent_sentence_is_protected(self) -> None:
        """Older sentences' COMPLETED snapshots are preserved; only the
        most-recently-active sentence is "kept open"."""
        sp = self._build_supervision_period(date(2025, 1, 1), None)

        # Older sentence: SERVING in 2015, COMPLETED in 2018
        old_serving = self._build_status(StateSentenceStatus.SERVING, date(2015, 1, 1))
        old_completed = self._build_status(
            StateSentenceStatus.COMPLETED, date(2018, 1, 1)
        )
        old_sentence = self._build_sentence(1, [old_serving, old_completed])

        # Newer sentence: SERVING in 2020, COMPLETED in 2024
        new_serving = self._build_status(StateSentenceStatus.SERVING, date(2020, 1, 1))
        new_completed = self._build_status(
            StateSentenceStatus.COMPLETED, date(2024, 8, 1)
        )
        new_sentence = self._build_sentence(2, [new_serving, new_completed])

        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[old_sentence, new_sentence],
        )

        # Older sentence: COMPLETED preserved
        self.assertEqual(
            delegate.update_sentence_status_snapshot(old_sentence, old_completed),
            old_completed,
            "Older sentence's COMPLETED should be preserved",
        )

        # Newer sentence: COMPLETED filtered
        self.assertIsNone(
            delegate.update_sentence_status_snapshot(new_sentence, new_completed),
            "Newer (most-recent-active) sentence's COMPLETED should be filtered",
        )

    def test_ties_at_latest_active_datetime_protect_all(self) -> None:
        """When multiple sentences share the latest SERVING snapshot datetime,
        all of them are protected."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)
        same_dt = date(2020, 1, 1)

        serving_a = self._build_status(StateSentenceStatus.SERVING, same_dt)
        completed_a = self._build_status(
            StateSentenceStatus.COMPLETED, date(2024, 1, 1)
        )
        sentence_a = self._build_sentence(1, [serving_a, completed_a])

        serving_b = self._build_status(StateSentenceStatus.SERVING, same_dt)
        completed_b = self._build_status(
            StateSentenceStatus.COMPLETED, date(2024, 6, 1)
        )
        sentence_b = self._build_sentence(2, [serving_b, completed_b])

        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[sentence_a, sentence_b],
        )

        self.assertIsNone(
            delegate.update_sentence_status_snapshot(sentence_a, completed_a),
            "Tied sentence A's COMPLETED should be filtered",
        )
        self.assertIsNone(
            delegate.update_sentence_status_snapshot(sentence_b, completed_b),
            "Tied sentence B's COMPLETED should be filtered",
        )

    def test_ips_status_qualifies_as_most_recent(self) -> None:
        """An IMPOSED_PENDING_SERVING status counts toward "most recent" — TN
        often synthesizes IPS for sentences without explicit raw-data SERVING
        snapshots, so they represent active sentences whose dates we want
        surfaced."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)

        # Older sentence: SERVING + COMPLETED
        old_serving = self._build_status(StateSentenceStatus.SERVING, date(2015, 1, 1))
        old_completed = self._build_status(
            StateSentenceStatus.COMPLETED, date(2018, 1, 1)
        )
        old_sentence = self._build_sentence(1, [old_serving, old_completed])

        # Newer sentence: only IPS (no explicit SERVING)
        ips = self._build_status(
            StateSentenceStatus.IMPOSED_PENDING_SERVING,
            date(2024, 1, 1),
        )
        completed = self._build_status(StateSentenceStatus.COMPLETED, date(2025, 1, 1))
        new_sentence = self._build_sentence(2, [ips, completed])

        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[old_sentence, new_sentence],
        )

        # The IPS-newer sentence should be picked as most recent and have its
        # COMPLETED filtered.
        self.assertIsNone(
            delegate.update_sentence_status_snapshot(new_sentence, completed),
            "IPS-newer sentence should be picked as most recent and have its "
            "COMPLETED filtered",
        )
        # The older sentence's COMPLETED should be preserved.
        self.assertEqual(
            delegate.update_sentence_status_snapshot(old_sentence, old_completed),
            old_completed,
            "Older sentence's COMPLETED should be preserved",
        )

    def test_serving_beats_ips_when_both_present(self) -> None:
        """When a sentence has both IPS and SERVING snapshots, the SERVING
        snapshot's datetime is what determines "most recent" if it's later
        than any IPS snapshot in another sentence."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)

        # Sentence A: IPS only at 2024-01
        ips_a = self._build_status(
            StateSentenceStatus.IMPOSED_PENDING_SERVING,
            date(2024, 1, 1),
        )
        completed_a = self._build_status(
            StateSentenceStatus.COMPLETED, date(2025, 1, 1)
        )
        sentence_a = self._build_sentence(1, [ips_a, completed_a])

        # Sentence B: IPS at 2023-01, then SERVING at 2025-01
        ips_b = self._build_status(
            StateSentenceStatus.IMPOSED_PENDING_SERVING,
            date(2023, 1, 1),
        )
        serving_b = self._build_status(StateSentenceStatus.SERVING, date(2025, 1, 1))
        completed_b = self._build_status(
            StateSentenceStatus.COMPLETED, date(2025, 6, 1)
        )
        sentence_b = self._build_sentence(2, [ips_b, serving_b, completed_b])

        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[sentence_a, sentence_b],
        )

        # Sentence B has the latest active snapshot (SERVING 2025-01) so its
        # COMPLETED should be filtered.
        self.assertIsNone(
            delegate.update_sentence_status_snapshot(sentence_b, completed_b),
            "Sentence with later SERVING should be picked as most recent",
        )
        # Sentence A is older — its COMPLETED preserved.
        self.assertEqual(
            delegate.update_sentence_status_snapshot(sentence_a, completed_a),
            completed_a,
            "Sentence with older IPS should not be picked as most recent",
        )

    def test_non_completed_status_passes_through(self) -> None:
        """Non-COMPLETED status snapshots are returned unchanged regardless of
        whether the sentence is most-recent or the person is supervised."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)
        serving = self._build_status(StateSentenceStatus.SERVING, date(2020, 1, 1))
        sentence = self._build_sentence(1, [serving])

        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[sentence],
        )

        self.assertEqual(
            delegate.update_sentence_status_snapshot(sentence, serving),
            serving,
            "SERVING status should always pass through",
        )

    # ---- Edge cases ----

    def test_no_sentences(self) -> None:
        """Delegate constructs cleanly with no sentences."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)
        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[],
        )
        self.assertEqual(delegate.most_recent_sentence_ids, set())
        self.assertTrue(delegate.is_currently_in_doc_custody)

    def test_no_supervision_periods(self) -> None:
        """Without any supervision period, the delegate doesn't filter anything."""
        serving = self._build_status(StateSentenceStatus.SERVING, date(2020, 1, 1))
        completed = self._build_status(StateSentenceStatus.COMPLETED, date(2024, 1, 1))
        sentence = self._build_sentence(1, [serving, completed])

        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[],
            sentences=[sentence],
        )

        self.assertFalse(delegate.is_currently_in_doc_custody)
        self.assertEqual(
            delegate.update_sentence_status_snapshot(sentence, completed),
            completed,
            "COMPLETED preserved when person is not in DOC custody",
        )

    def test_filters_completed_for_currently_incarcerated_person(self) -> None:
        """When the person is currently incarcerated (open incarceration
        period, no supervision period), COMPLETED is filtered for the
        most-recently-active sentence — same rule that applies to currently-
        supervised people. This is the resident-side analog of the supervision
        case."""
        ip = self._build_incarceration_period(date(2020, 1, 1), None)
        serving = self._build_status(StateSentenceStatus.SERVING, date(2020, 1, 1))
        completed = self._build_status(StateSentenceStatus.COMPLETED, date(2024, 8, 1))
        sentence = self._build_sentence(1, [serving, completed])

        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[ip],
            supervision_periods=[],
            sentences=[sentence],
        )

        self.assertTrue(delegate.is_currently_in_doc_custody)
        self.assertIsNone(
            delegate.update_sentence_status_snapshot(sentence, completed),
            "COMPLETED filtered when person is currently incarcerated",
        )

    def test_imposed_date_used_as_fallback_anchor(self) -> None:
        """When a sentence has only COMPLETED snapshots in raw data (with
        SERVING/IPS to be synthesized later in the same normalization
        pipeline), imposed_date is used as the fallback anchor for
        "most recent" identification. Without this fallback, the override
        silently no-ops on these sentences."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)

        # Older sentence: only COMPLETED, imposed in 2010
        old_completed = self._build_status(
            StateSentenceStatus.COMPLETED, date(2018, 1, 1)
        )
        old_sentence = self._build_sentence(
            1, [old_completed], imposed_date=date(2010, 1, 1)
        )

        # Newer sentence: only COMPLETED, imposed in 2020
        new_completed = self._build_status(
            StateSentenceStatus.COMPLETED, date(2024, 8, 1)
        )
        new_sentence = self._build_sentence(
            2, [new_completed], imposed_date=date(2020, 1, 1)
        )

        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[old_sentence, new_sentence],
        )

        # Newer sentence (later imposed_date) is "most recent" — its COMPLETED
        # is filtered.
        self.assertIsNone(
            delegate.update_sentence_status_snapshot(new_sentence, new_completed),
            "Newer sentence's COMPLETED should be filtered (imposed_date fallback)",
        )
        # Older sentence's COMPLETED preserved.
        self.assertEqual(
            delegate.update_sentence_status_snapshot(old_sentence, old_completed),
            old_completed,
            "Older sentence's COMPLETED should be preserved",
        )

    def test_serving_anchor_beats_imposed_date_anchor(self) -> None:
        """When one sentence has a SERVING snapshot and another only has
        imposed_date, the SERVING anchor wins if it's later than the
        imposed_date — confirming the per-sentence anchor logic prefers
        explicit serving statuses over imposed_date when both are present."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)

        # Sentence A: only COMPLETED + imposed_date 2024-01-01
        completed_a = self._build_status(
            StateSentenceStatus.COMPLETED, date(2025, 1, 1)
        )
        sentence_a = self._build_sentence(
            1, [completed_a], imposed_date=date(2024, 1, 1)
        )

        # Sentence B: SERVING in 2025-06 (later than A's imposed_date)
        serving_b = self._build_status(StateSentenceStatus.SERVING, date(2025, 6, 1))
        completed_b = self._build_status(
            StateSentenceStatus.COMPLETED, date(2026, 1, 1)
        )
        sentence_b = self._build_sentence(
            2, [serving_b, completed_b], imposed_date=date(2023, 1, 1)
        )

        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[sentence_a, sentence_b],
        )

        # Sentence B has the latest anchor (SERVING 2025-06 > A's imposed_date 2024-01)
        self.assertIsNone(
            delegate.update_sentence_status_snapshot(sentence_b, completed_b),
            "Sentence with later SERVING anchor should be picked",
        )
        self.assertEqual(
            delegate.update_sentence_status_snapshot(sentence_a, completed_a),
            completed_a,
            "Sentence with earlier imposed_date anchor should not be picked",
        )

    def test_future_termination_date_treated_as_supervised(self) -> None:
        """A supervision period with a future termination_date is still 'open'."""
        sp = self._build_supervision_period(date(2020, 1, 1), date(2099, 1, 1))
        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[],
        )
        self.assertTrue(delegate.is_currently_in_doc_custody)

    # ---- Carry-forward (should_carry_forward_projected_dates) ----

    def test_carry_forward_pattern_a(self) -> None:
        """Pattern A: most-recent active sentence on supervision, no newer
        sentence, not LIFE — carry-forward enabled."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)
        serving = self._build_status(StateSentenceStatus.SERVING, date(2020, 1, 1))
        sentence = self._build_sentence(
            1,
            [serving],
            imposed_date=date(2020, 1, 1),
            sentence_lengths=[
                self._build_length(date(2020, 1, 1), date(2024, 1, 1)),
                self._build_length(date(2024, 6, 1), None),
            ],
        )
        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[sentence],
        )
        self.assertTrue(delegate.should_carry_forward_projected_dates(sentence))

    def test_carry_forward_disabled_when_not_supervised(self) -> None:
        """Off-supervision: no carry-forward."""
        sp = self._build_supervision_period(date(2020, 1, 1), date(2024, 1, 1))
        serving = self._build_status(StateSentenceStatus.SERVING, date(2020, 1, 1))
        sentence = self._build_sentence(
            1,
            [serving],
            imposed_date=date(2020, 1, 1),
            sentence_lengths=[
                self._build_length(date(2020, 1, 1), date(2024, 1, 1)),
            ],
        )
        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[sentence],
        )
        self.assertFalse(delegate.should_carry_forward_projected_dates(sentence))

    def test_carry_forward_disabled_for_older_sentence(self) -> None:
        """Older (not-most-recent) sentence: no carry-forward."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)
        old_serving = self._build_status(StateSentenceStatus.SERVING, date(2015, 1, 1))
        old_sentence = self._build_sentence(
            1,
            [old_serving],
            imposed_date=date(2015, 1, 1),
            sentence_lengths=[
                self._build_length(date(2015, 1, 1), date(2018, 1, 1)),
            ],
        )
        new_serving = self._build_status(StateSentenceStatus.SERVING, date(2024, 1, 1))
        new_sentence = self._build_sentence(
            2,
            [new_serving],
            imposed_date=date(2024, 1, 1),
            sentence_lengths=[
                self._build_length(date(2024, 1, 1), date(2030, 1, 1)),
            ],
        )
        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[old_sentence, new_sentence],
        )
        # Older sentence: no carry-forward
        self.assertFalse(delegate.should_carry_forward_projected_dates(old_sentence))
        # Newer (most-recent) sentence: carry-forward enabled
        self.assertTrue(delegate.should_carry_forward_projected_dates(new_sentence))

    def test_carry_forward_disabled_for_superseded_sentence(self) -> None:
        """Pattern B: sentence's contributor is most-recent BUT another
        sentence was imposed AFTER this one's last populated snapshot.
        Carry-forward should be suppressed (avoid stale dates).

        We simulate this by giving sentence 1 a long IPS history and then
        imposing sentence 2 AFTER sentence 1's last populated date. Both end
        up tied as "most recent active" by SERVING-considered status, but
        sentence 1 is superseded by sentence 2's later imposed_date."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)
        # Sentence 1: IPS at 2020, populated proj_max only at 2020-01-01
        ips_1 = self._build_status(
            StateSentenceStatus.IMPOSED_PENDING_SERVING, date(2020, 1, 1)
        )
        sentence_1 = self._build_sentence(
            1,
            [ips_1],
            imposed_date=date(2020, 1, 1),
            sentence_lengths=[
                self._build_length(date(2020, 1, 1), date(2024, 1, 1)),
                self._build_length(date(2022, 1, 1), None),
            ],
        )
        # Sentence 2: imposed 2025-01-01, AFTER sentence 1's last populated
        # snapshot (2020-01-01). Has the same IPS datetime so it's tied as
        # most-recent.
        ips_2 = self._build_status(
            StateSentenceStatus.IMPOSED_PENDING_SERVING, date(2020, 1, 1)
        )
        sentence_2 = self._build_sentence(
            2,
            [ips_2],
            imposed_date=date(2025, 1, 1),
            sentence_lengths=[
                self._build_length(date(2025, 1, 1), date(2030, 1, 1)),
            ],
        )
        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[sentence_1, sentence_2],
        )
        # Sentence 1 is in most_recent_sentence_ids but is superseded by
        # sentence 2 (imposed after sentence 1's last populated snapshot).
        self.assertIn(1, delegate.most_recent_sentence_ids)
        self.assertIn(1, delegate.superseded_sentence_ids)
        self.assertFalse(delegate.should_carry_forward_projected_dates(sentence_1))
        # Sentence 2 is not superseded — its imposed date is the latest.
        self.assertNotIn(2, delegate.superseded_sentence_ids)

    def test_carry_forward_not_suppressed_when_newer_sentence_has_no_projection(
        self,
    ) -> None:
        """Pattern A edge case: a newer sentence was imposed after this
        sentence's last populated snapshot, but that newer sentence has NEVER
        had a non-null projected_completion_date_max_external of its own. It
        shouldn't be treated as superseding — otherwise we drop the older
        sentence's known date to NULL when the "successor" offers no
        replacement projection.

        Real-world example: TDOC 00519804 (sample #22 in FOIL validation).
        Sentence 2020CR158 #1 had projected_date 2023-12-11 in 2021-04-07,
        then TN nulled it out. Sentence 21CR426 #1 was imposed 2021-12-03
        (later than 2021-04-07) but has never had a projected_date. Without
        this gate, we mark 2020CR158 as superseded and surface NULL; with
        this gate, we carry forward 2023-12-11."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)
        # Sentence 1: had a projected_date populated in 2020, then nulled.
        ips_1 = self._build_status(
            StateSentenceStatus.IMPOSED_PENDING_SERVING, date(2020, 1, 1)
        )
        sentence_1 = self._build_sentence(
            1,
            [ips_1],
            imposed_date=date(2020, 1, 1),
            sentence_lengths=[
                self._build_length(date(2020, 1, 1), date(2024, 1, 1)),
                self._build_length(date(2022, 1, 1), None),
            ],
        )
        # Sentence 2: imposed after sentence 1's last-populated snapshot, but
        # has NEVER had a projected_date — all length rows are NULL.
        ips_2 = self._build_status(
            StateSentenceStatus.IMPOSED_PENDING_SERVING, date(2020, 1, 1)
        )
        sentence_2 = self._build_sentence(
            2,
            [ips_2],
            imposed_date=date(2025, 1, 1),
            sentence_lengths=[
                self._build_length(date(2025, 1, 1), None),
            ],
        )
        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[sentence_1, sentence_2],
        )
        # Sentence 2 is later-imposed but offers no replacement projection, so
        # sentence 1 should NOT be marked as superseded.
        self.assertNotIn(1, delegate.superseded_sentence_ids)
        self.assertTrue(delegate.should_carry_forward_projected_dates(sentence_1))

    # ---- Past-date hygiene ----

    def test_close_serving_at_passed_projected_date_fires_for_non_ac_status(
        self,
    ) -> None:
        """Past-date hygiene fires when sentence_metadata's
        CURRENT_SENTENCE_STATUS is not 'AC' (e.g., PB for probation)."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)
        sentence = self._build_sentence(
            1,
            [self._build_status(StateSentenceStatus.SERVING, date(2020, 1, 1))],
            imposed_date=date(2020, 1, 1),
            sentence_lengths=[
                self._build_length(date(2020, 1, 1), date(2024, 1, 1)),
            ],
            sentence_metadata=json.dumps({"CURRENT_SENTENCE_STATUS": "PB"}),
        )
        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[sentence],
        )
        self.assertTrue(
            delegate.close_serving_period_at_passed_projected_date(sentence)
        )

    def test_close_serving_at_passed_projected_date_skipped_for_ac_status(
        self,
    ) -> None:
        """Past-date hygiene is gated off for sentences whose current raw
        SentenceStatus is 'AC' (Active). This matches v1's behavior — for
        sentences TN still considers Active, v1 keeps them open in
        projected_date_spans even past the projected end. Common case:
        person paroled into a probation tail; raw status stays AC and the
        sentence is operationally still being served past the original
        projected end."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)
        sentence = self._build_sentence(
            1,
            [self._build_status(StateSentenceStatus.SERVING, date(2020, 1, 1))],
            imposed_date=date(2020, 1, 1),
            sentence_lengths=[
                self._build_length(date(2020, 1, 1), date(2024, 1, 1)),
            ],
            sentence_metadata=json.dumps({"CURRENT_SENTENCE_STATUS": "AC"}),
        )
        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[sentence],
        )
        self.assertFalse(
            delegate.close_serving_period_at_passed_projected_date(sentence)
        )

    def test_close_serving_at_passed_projected_date_skipped_when_carry_forward_fires(
        self,
    ) -> None:
        """When carry-forward is firing on a sentence, that means the latest raw
        `projected_completion_date_max_external` is NULL — TN has stopped emitting
        a projection. Closing the serving period at a historical date TN no longer
        considers authoritative would undo carry-forward's work, so past-date
        hygiene defers and lets the sentence stay open with the carried-forward
        date.

        Real-world example: FOIL sample #22 (TDOC 00519804) sentence 2020CR158#1.
        Raw `PB`, projected_date 2023-12-11 was emitted in 2021 and then nulled
        in every subsequent length row. Person is still on probation. Without
        this guard, past-date hygiene closes at 2023-12-11 even though TN has
        stopped emitting that date as authoritative."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)
        # Sentence with carry-forward criteria: in custody (sp open), latest status
        # SERVING-considered, not superseded. Length history has a date populated
        # then nulled — exactly the pattern carry-forward exists to handle.
        sentence = self._build_sentence(
            1,
            [self._build_status(StateSentenceStatus.SERVING, date(2021, 1, 1))],
            imposed_date=date(2021, 1, 1),
            sentence_lengths=[
                self._build_length(date(2021, 4, 7), date(2023, 12, 11)),
                self._build_length(date(2022, 1, 1), None),
            ],
            sentence_metadata=json.dumps({"CURRENT_SENTENCE_STATUS": "PB"}),
        )
        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[sentence],
        )
        # Sanity: carry-forward IS firing on this sentence.
        self.assertTrue(delegate.should_carry_forward_projected_dates(sentence))
        # Past-date hygiene defers to carry-forward.
        self.assertFalse(
            delegate.close_serving_period_at_passed_projected_date(sentence)
        )

    def test_close_serving_at_passed_projected_date_fires_when_metadata_missing(
        self,
    ) -> None:
        """When sentence_metadata is missing or doesn't contain
        CURRENT_SENTENCE_STATUS (e.g., diversion sentences from a separate
        ingest path), past-date hygiene fires as a fallback. This ensures
        stale diversion sentences not closed via raw status still get
        cleaned up."""
        sp = self._build_supervision_period(date(2020, 1, 1), None)
        sentence_no_metadata = self._build_sentence(
            1,
            [self._build_status(StateSentenceStatus.SERVING, date(2020, 1, 1))],
            imposed_date=date(2020, 1, 1),
            sentence_lengths=[
                self._build_length(date(2020, 1, 1), date(2024, 1, 1)),
            ],
        )
        sentence_metadata_no_field = self._build_sentence(
            2,
            [self._build_status(StateSentenceStatus.SERVING, date(2020, 1, 1))],
            imposed_date=date(2020, 1, 1),
            sentence_lengths=[
                self._build_length(date(2020, 1, 1), date(2024, 1, 1)),
            ],
            sentence_metadata=json.dumps({"SENTENCE_SOURCE": "DIVERSION"}),
        )
        delegate = UsTnSentenceNormalizationDelegate(
            incarceration_periods=[],
            supervision_periods=[sp],
            sentences=[sentence_no_metadata, sentence_metadata_no_field],
        )
        self.assertTrue(
            delegate.close_serving_period_at_passed_projected_date(sentence_no_metadata)
        )
        self.assertTrue(
            delegate.close_serving_period_at_passed_projected_date(
                sentence_metadata_no_field
            )
        )
