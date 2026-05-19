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
"""Contains US_TN implementation of the StateSpecificSentenceNormalizationDelegate."""
import datetime
import json
from datetime import date

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.entity.activity.entities import (
    StateIncarcerationPeriod,
    StateSentence,
    StateSentenceLength,
    StateSentenceStatusSnapshot,
    StateSupervisionPeriod,
)
from recidiviz.pipelines.ingest.activity.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)


class UsTnSentenceNormalizationDelegate(StateSpecificSentenceNormalizationDelegate):
    """US_TN implementation of the StateSpecificSentenceNormalizationDelegate.

    TN's raw sentence data has several hygiene issues this delegate corrects
    during normalization:

      - Stale "Active" status on sentences whose projected end has passed
        (close-stale: synthesizes a COMPLETED snapshot at the projected end).
      - Premature COMPLETED status on a person's last active sentence while
        they're still in DOC custody (narrow override: keep the sentence
        active).
      - NULL projected_completion_date on the latest StateSentenceLength
        once the date has passed (carry-forward: fill from the most recent
        prior non-null value, gated on the sentence still being active and
        not superseded).

    Each correction's gating logic is documented on the relevant override
    method. Diversion-sentence completion inference happens primarily at
    ingest (us_tn_diversion_sentence.yaml); the close-stale hook here
    handles non-diversion sentences and any diversion edge cases the ingest
    branch didn't close.
    """

    def __init__(
        self,
        incarceration_periods: list[StateIncarcerationPeriod],
        supervision_periods: list[StateSupervisionPeriod],
        sentences: list[StateSentence],
    ) -> None:
        super().__init__()

        today = date.today()

        # Person is "currently in DOC custody" if they have any open
        # supervision period (termination_date is None or in the future) OR
        # any open incarceration period (release_date is None or in the
        # future). Both populations have the same TN data hygiene problems
        # (unreliable COMPLETED statuses, nullified projected dates), so the
        # delegate applies to both.
        self.is_currently_in_doc_custody = any(
            sp.termination_date is None or sp.termination_date > today
            for sp in supervision_periods
        ) or any(
            ip.release_date is None or ip.release_date > today
            for ip in incarceration_periods
        )

        # Identify the most-recently-active sentence(s) for this person. We
        # consider both `is_considered_serving_status` statuses (SERVING,
        # AMENDED, COMMUTED, SANCTIONED) AND IMPOSED_PENDING_SERVING — IPS is
        # not formally a "serving" status but in TN it is commonly synthesized
        # for sentences that have been imposed and are effectively being served
        # without explicit raw-data SERVING snapshots. The "most recent"
        # sentence is the one with the latest such snapshot's
        # status_update_datetime; ties are protected together.
        #
        # Fallback to `imposed_date` when a sentence has no SERVING-or-IPS
        # snapshot in raw data: this delegate runs BEFORE the framework injects
        # synthetic SERVING / IPS snapshots from `current_state_provided_start_date`
        # or `imposed_date` (see normalize_snapshots_for_single_sentence). For
        # TN sentences whose raw data has only a COMPLETED snapshot (with the
        # SERVING/IPS to be synthesized later in the same pipeline), we use
        # imposed_date as the anchor so they're still eligible to be "most
        # recent" — otherwise the override silently no-ops for these sentences.
        latest_active_dt: datetime.datetime | None = None
        latest_active_sentence_ids: set[int] = set()
        for sentence in sentences:
            if sentence.sentence_id is None:
                continue
            sentence_anchor_dt: datetime.datetime | None = None
            for snapshot in sentence.sentence_status_snapshots:
                if not (
                    snapshot.status.is_considered_serving_status
                    or snapshot.status == StateSentenceStatus.IMPOSED_PENDING_SERVING
                ):
                    continue
                if (
                    sentence_anchor_dt is None
                    or snapshot.status_update_datetime > sentence_anchor_dt
                ):
                    sentence_anchor_dt = snapshot.status_update_datetime
            if sentence_anchor_dt is None and sentence.imposed_date is not None:
                sentence_anchor_dt = datetime.datetime.combine(
                    sentence.imposed_date, datetime.time.min
                )
            if sentence_anchor_dt is None:
                continue
            if latest_active_dt is None or sentence_anchor_dt > latest_active_dt:
                latest_active_dt = sentence_anchor_dt
                latest_active_sentence_ids = {sentence.sentence_id}
            elif sentence_anchor_dt == latest_active_dt:
                latest_active_sentence_ids.add(sentence.sentence_id)
        self.most_recent_sentence_ids = latest_active_sentence_ids

        # For carry-forward: identify sentences that are "superseded" by a
        # newer-imposed sentence. v1's spans capture a projected date once and
        # keep it; raw TN data later nulls the date out (housekeeping after the
        # date passes), but the sentence is often still relevant unless the
        # person has had new sentences imposed in the meantime that themselves
        # offer a replacement projection. We detect that by checking whether
        # any other sentence's imposed_date is later than this sentence's most
        # recent populated length_update_datetime AND that other sentence has
        # ever had a non-null projected_completion_date_max_external of its
        # own. If the "newer" sentence has never had a projection (e.g., a
        # freshly imposed sentence that TN hasn't fully populated yet), we
        # don't treat it as superseding, so we preserve the older sentence's
        # last-known date rather than dropping to NULL.
        sentence_last_populated: dict[int, datetime.datetime] = {}
        sentences_with_any_projected_date: set[int] = set()
        for sentence in sentences:
            if sentence.sentence_id is None:
                continue
            for length in sentence.sentence_lengths:
                if length.projected_completion_date_max_external is None:
                    continue
                sentences_with_any_projected_date.add(sentence.sentence_id)
                current = sentence_last_populated.get(sentence.sentence_id)
                if current is None or length.length_update_datetime > current:
                    sentence_last_populated[
                        sentence.sentence_id
                    ] = length.length_update_datetime

        self.superseded_sentence_ids: set[int] = set()
        for sentence in sentences:
            if sentence.sentence_id is None:
                continue
            last_populated = sentence_last_populated.get(sentence.sentence_id)
            if last_populated is None:
                continue
            for other in sentences:
                if (
                    other.sentence_id is None
                    or other.sentence_id == sentence.sentence_id
                ):
                    continue
                if other.imposed_date is None:
                    continue
                if other.sentence_id not in sentences_with_any_projected_date:
                    continue
                if (
                    datetime.datetime.combine(other.imposed_date, datetime.time.min)
                    > last_populated
                ):
                    self.superseded_sentence_ids.add(sentence.sentence_id)
                    break

        # Sentences eligible for carry-forward: any sentence whose LATEST
        # status snapshot is "active" (SERVING-considered or IPS). Broader
        # than `most_recent_sentence_ids` (which is the single latest-active
        # sentence used for COMPLETED-filtering) — carry-forward applies to
        # all currently-active sentences in parallel so each can
        # independently get its NULL projected date filled.
        self.carry_forward_candidate_ids: set[int] = set()
        for sentence in sentences:
            if sentence.sentence_id is None:
                continue
            latest_status_snapshot: StateSentenceStatusSnapshot | None = None
            for snapshot in sentence.sentence_status_snapshots:
                if (
                    latest_status_snapshot is None
                    or snapshot.status_update_datetime
                    > latest_status_snapshot.status_update_datetime
                ):
                    latest_status_snapshot = snapshot
            if latest_status_snapshot is None:
                # No status snapshots in raw data — IPS will be synthesized
                # downstream from imposed_date. Treat as carry-forward-eligible
                # if the sentence has an imposed_date.
                if sentence.imposed_date is not None:
                    self.carry_forward_candidate_ids.add(sentence.sentence_id)
                continue
            if (
                latest_status_snapshot.status.is_considered_serving_status
                or latest_status_snapshot.status
                == StateSentenceStatus.IMPOSED_PENDING_SERVING
            ):
                self.carry_forward_candidate_ids.add(sentence.sentence_id)

    @property
    def infer_imposed_pending_serving_from_imposed_date(self) -> bool:
        """TN provides an effective date (SentenceEffectiveDate) as the serving start,
        but many sentences have null or future effective dates. We create
        IMPOSED_PENDING_SERVING from the imposed date to cover the gap."""
        return True

    @property
    def allow_non_credit_serving(self) -> bool:
        """TN has an 'oversight board' that revokes credit for time served, so we allow non-credit serving sentences."""
        return True

    # NOTE: We don't really understand why TN gives us data like this (it could be that
    # a sentence really changes, there's an acute data issue, or the originating process
    # is flawed)
    @property
    def correct_early_completed_statuses(self) -> bool:
        """
        If True, if we see a StateSentenceStatusSnapshot that is not the last status for a sentence which
        has status COMPLETED, correct that status to SERVING. Otherwise, we'll throw if we see a COMPLETED
        status that is followed by other statuses.
        """
        return True

    def should_carry_forward_projected_dates(self, sentence: StateSentence) -> bool:
        """Carry-forward NULL projected dates in this sentence's
        StateSentenceLength snapshots when:

        1. The person is currently in DOC custody (supervised or incarcerated).
        2. This sentence has an active SERVING-considered or IPS latest
           status (so its dates are relevant for the person's current legal
           status).
        3. The sentence has NOT been superseded by a later-imposed sentence
           (avoids surfacing a stale date from an old sentence that's been
           replaced by newer ones).

        Why: TN raw data routinely nulls out projected dates after the date
        has passed, but the sentence often remains relevant — the person
        stays in DOC custody past the original projection. Carry-forward at
        the sentence_length normalization layer fills the NULL with the most
        recent prior non-null value so the date stays visible downstream.
        """
        if sentence.sentence_id is None:
            return False
        if not self.is_currently_in_doc_custody:
            return False
        if sentence.sentence_id not in self.carry_forward_candidate_ids:
            return False
        if sentence.sentence_id in self.superseded_sentence_ids:
            return False
        return True

    def close_serving_period_at_passed_projected_date(
        self, sentence: StateSentence
    ) -> bool:
        """Close the sentence_serving_period at the latest
        projected_completion_date_max_external when that date is in the past,
        for TN sentences whose current raw SentenceStatus is NOT 'AC' (Active).

        Why: TN's raw SentenceStatus is unreliable — sentences frequently lack
        a COMPLETED snapshot even after the projected end date has passed,
        leaving the v2 sentence_serving_period open indefinitely. v1's
        `us_tn_sentence_status_raw_text_sessions` view coalesces
        projected_completion_date_max into the effective completion_date
        only when raw status is not 'AC'; for sentences still raw-marked
        Active, v1 keeps the serving period open. This delegate method
        replicates that gating.

        Net effect:
          - Raw status = 'AC' + past projected date → don't fire (matches v1
            keeping the sentence active; person is still considered serving
            per TN raw, often because they're on parole/probation that
            extends past the original projected end).
          - Carry-forward is firing on this sentence → don't fire. If
            `should_carry_forward_projected_dates` returns True, the latest
            raw `projected_completion_date_max_external` is NULL — TN has
            stopped emitting a projection. Closing at a historical value
            TN no longer considers authoritative would undo the work of
            carry-forward; we let the sentence stay open with the carried-
            forward date so it can surface in person_projected_date_sessions.
          - Raw status non-'AC' (IN, PB, CC, etc.) + past projected date,
            carry-forward NOT firing → fire (matches v1 closing these stale
            sentences). Raw 'IN' is already handled via raw COMPLETED
            snapshot at ingest, so this method effectively only injects for
            PB/CC/other statuses.
          - Diversion sentences (no CURRENT_SENTENCE_STATUS in metadata) →
            fire as a fallback for any past-date diversions not already
            closed by the diversion completion inference.
        """
        if sentence.sentence_id is None:
            return True
        current_raw_status = self._current_sentence_status_raw(sentence)
        if current_raw_status == "AC":
            return False
        if self.should_carry_forward_projected_dates(
            sentence
        ) and self._latest_length_has_null_projected_date(sentence):
            return False
        return True

    @staticmethod
    def _latest_length_has_null_projected_date(sentence: StateSentence) -> bool:
        """True if the sentence's most recent (by `length_update_datetime`)
        `state_sentence_length` row has a NULL `projected_completion_date_max_external`.
        Used to distinguish sentences where carry-forward is doing real work
        (latest raw was NULL, we filled it) vs sentences where the latest row
        already has a populated value and carry-forward is a no-op."""
        latest_length: StateSentenceLength | None = None
        for length in sentence.sentence_lengths:
            if (
                latest_length is None
                or length.length_update_datetime > latest_length.length_update_datetime
            ):
                latest_length = length
        if latest_length is None:
            return False
        return latest_length.projected_completion_date_max_external is None

    @staticmethod
    def _current_sentence_status_raw(sentence: StateSentence) -> str | None:
        """Returns the CURRENT_SENTENCE_STATUS value stored in this sentence's
        sentence_metadata JSON dict, or None if not present.

        Populated by `us_tn_sentence_and_charge.yaml` from the latest known
        raw SentenceStatus value for non-diversion sentences. Diversion
        sentences don't carry this field; for them the method returns None
        and past-date hygiene fires as a fallback.
        """
        if sentence.sentence_metadata is None:
            return None
        try:
            metadata = json.loads(sentence.sentence_metadata)
        except (TypeError, ValueError):
            return None
        if not isinstance(metadata, dict):
            return None
        value = metadata.get("CURRENT_SENTENCE_STATUS")
        return value if isinstance(value, str) else None

    def update_sentence_status_snapshot(
        self,
        sentence: StateSentence,
        snapshot: StateSentenceStatusSnapshot,
    ) -> StateSentenceStatusSnapshot | None:
        """Filter COMPLETED status snapshots for the most-recently-active
        sentence(s) when the person is currently in DOC custody.

        Background: TN's raw SentenceStatus is unreliable — people frequently
        remain in custody past the date their sentence is marked COMPLETED.
        Without this override, sentence_serving_period closes on the COMPLETED
        snapshot and the sentence drops out of `person_projected_date_sessions`,
        which removes the projected date from downstream views like
        client_record / resident_record even though the person is still in
        DOC custody.

        This override drops the COMPLETED snapshot on the person's
        most-recently-active sentence when they are still in DOC custody,
        keeping the sentence "open" so its projected dates surface downstream.

        Logic:
        1. If the snapshot is not COMPLETED, return unchanged.
        2. If the sentence is not in the most-recently-active set, return
           unchanged.
        3. If the person is not currently in DOC custody, return unchanged.
        4. Otherwise, return None (filter the COMPLETED snapshot out).
        """
        if snapshot.status != StateSentenceStatus.COMPLETED:
            return snapshot
        if sentence.sentence_id not in self.most_recent_sentence_ids:
            return snapshot
        if not self.is_currently_in_doc_custody:
            return snapshot
        return None
