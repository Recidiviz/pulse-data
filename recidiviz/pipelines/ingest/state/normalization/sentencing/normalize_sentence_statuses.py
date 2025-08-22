# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""
This module contains code to normalize StateSentenceStatusSnapshot entities.
We do this separately from sentences and as the very first step in
sentencing entity normalization, because sentence status information is
usually the source of truth for what is true of a sentence
for a given point in time.

We normalize statuses for parent sentences first, because sometimes we need
to know the final status of a parent sentence to determine the status of a child sentence.
"""
import datetime

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.date import as_datetime
from recidiviz.persistence.entity.state.entities import (
    StateSentence,
    StateSentenceStatusSnapshot,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSentenceStatusSnapshot,
)
from recidiviz.persistence.entity.state.state_entity_utils import (
    build_unique_sentence_status_snapshot_key,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)
from recidiviz.utils.types import assert_type


def _termination_datetime_for_given_sentences(
    list_of_snapshots: list[list[NormalizedStateSentenceStatusSnapshot]],
) -> datetime.datetime | None:
    """
    Returns the termination datetime for the given sentences, which is the
    latest status_update_datetime of all final snapshots of all sentences.
    If no final snapshot exists, returns None.
    """
    termination_dt: datetime.datetime | None = None
    for snapshot_list in list_of_snapshots:
        # TODO(#45385): Ensure parents and children either both have statuses, or both do not
        if not snapshot_list:
            continue
        final_snapshot = max(snapshot_list, key=lambda s: s.partition_key)
        if final_snapshot.status.is_terminating_status and (
            termination_dt is None
            or final_snapshot.status_update_datetime > termination_dt
        ):
            termination_dt = final_snapshot.status_update_datetime
    return termination_dt


def normalize_sentence_status_snapshots(
    delegate: StateSpecificSentenceNormalizationDelegate,
    unprocessed_sentences: dict[str, StateSentence],
    processing_order: list[str],
) -> dict[str, list[NormalizedStateSentenceStatusSnapshot]]:
    """
    Normalizes StateSentenceStatusSnapshot entities for the given sentences,
    processing sentences with no parents first.

    The graph of consecutive sentences (child -> parent) does not have a cycle.
    """
    if len(processing_order) != len(unprocessed_sentences):
        raise ValueError(
            "Processing order must match the number of unprocessed sentences."
        )
    normalized_snapshots: dict[str, list[NormalizedStateSentenceStatusSnapshot]] = {}
    for external_id in processing_order:
        sentence = unprocessed_sentences[external_id]

        parent_sentence_snapshots = []
        if sentence.parent_sentence_external_id_array:
            for _id in sentence.parent_sentence_external_ids:
                parent_sentence_snapshots.append(normalized_snapshots[_id])

        normalized_snapshots[external_id] = normalize_snapshots_for_single_sentence(
            delegate=delegate,
            sentence=sentence,
            parent_sentences_final_terminating_status_dt=_termination_datetime_for_given_sentences(
                parent_sentence_snapshots
            ),
        )
    if set(normalized_snapshots.keys()) != set(unprocessed_sentences.keys()):
        raise ValueError(
            "Normalized snapshots keys do not match unprocessed sentences keys."
        )
    return normalized_snapshots


def _add_end_datetime_and_normalize_sequence_num(
    snapshots: list[StateSentenceStatusSnapshot],
) -> list[NormalizedStateSentenceStatusSnapshot]:
    sorted_snapshots = sorted(snapshots, key=lambda s: s.partition_key)
    N = len(sorted_snapshots) - 1
    normalized_snapshots: list[NormalizedStateSentenceStatusSnapshot] = []
    for idx, snapshot in enumerate(sorted_snapshots):
        normalized_snapshots.append(
            NormalizedStateSentenceStatusSnapshot(
                state_code=snapshot.state_code,
                status=snapshot.status,
                status_raw_text=snapshot.status_raw_text,
                status_update_datetime=snapshot.status_update_datetime,
                status_end_datetime=(
                    None
                    if idx == N
                    else sorted_snapshots[idx + 1].status_update_datetime
                ),
                # We always re-assign sequence_num here to make sure it is
                # contiguous and increasing.
                sequence_num=idx + 1,
                sentence_status_snapshot_id=build_unique_sentence_status_snapshot_key(
                    snapshot=snapshot,
                ),
            )
        )
    return normalized_snapshots


def _correct_and_create_serving_statuses_from_watermark(
    sentence: StateSentence,
    all_snapshots: list[StateSentenceStatusSnapshot],
    serving_watermark_datetime: datetime.datetime,
    status_value_to_relace_serving: StateSentenceStatus,
    raw_text_for_new_serving_status: str,
) -> list[StateSentenceStatusSnapshot]:
    """
    This function updates any serving statuses to be the given new status
    value if their status_update_datetime is before the watermark_datetime.
    If there is not a serving status on the date of the watermark_datetime,
    we create a new serving snapshot at that watermark_datetime.

    For example, consider:

    status_update_datetime | status
    -----------------------|--------
    2024-01-01 00:00:00    | SERVING
    2024-01-04 00:00:00    | SERVING
    2024-01-08 00:00:00    | SERVING

    If the watermark_datetime is 2024-01-05, and the new_status is IMPOSED_PENDING_SERVING,
    the given statuses will become

    sequence_num | status_update_datetime | status
    -------------|------------------------|--------
         1       | 2024-01-01 00:00:00    | IMPOSED_PENDING_SERVING
         2       | 2024-01-04 00:00:00    | IMPOSED_PENDING_SERVING
         3       | 2024-01-08 00:00:00    | SERVING

    Since there is not a serving status on the date of the watermark_datetime,
    we would create a new StateSentenceStatusSnapshot with the
    status_update_datetime set to the 2024-01-05 and a status value of SERVING.

    The list of status snapshots returned by this function would then look like:

    sequence_num | status_update_datetime | status
    -------------|------------------------|--------
         1       | 2024-01-01 00:00:00    | IMPOSED_PENDING_SERVING
         2       | 2024-01-04 00:00:00    | IMPOSED_PENDING_SERVING
         3       | 2024-01-05 00:00:00    | SERVING <--- New! Created by this function
         4       | 2024-01-08 00:00:00    | SERVING

    For that example, if the watermark_datetime was 2024-01-08, we would not have
    made a new StateSentenceStatusSnapshot.
    """
    snapshots_to_return: list[StateSentenceStatusSnapshot] = []

    serving_snapshot_exists_at_watermark = False

    for snapshot in sorted(all_snapshots, key=lambda s: s.partition_key):

        if snapshot.status_update_datetime < serving_watermark_datetime:

            # If serving before the watermark, update to the new status.
            if snapshot.status.is_considered_serving_status:
                snapshot.status = status_value_to_relace_serving

            # If we have a terminating status before the serving_watermark,
            # something has gone wrong in ingest. Note we've already corrected
            # early completion statuses before calling this function!
            if snapshot.status.is_terminating_status:
                raise ValueError(
                    f"{sentence.limited_pii_repr()} has a terminating status before we expected it to begin serving!"
                )

        # We only create net-new serving statuses if there isn't a serving status on the serving watermark date
        if snapshot.status_update_datetime.date() == serving_watermark_datetime.date():
            if snapshot.status.is_considered_serving_status:
                serving_snapshot_exists_at_watermark = True

        # If we're past the watermark and we haven't created a serving snapshot yet,
        # we create a new one with the serving_watermark_datetime.
        if (
            snapshot.status_update_datetime >= serving_watermark_datetime
            and not serving_snapshot_exists_at_watermark
        ):
            snapshots_to_return.append(
                StateSentenceStatusSnapshot(
                    state_code=snapshot.state_code,
                    status=StateSentenceStatus.SERVING,
                    status_raw_text=raw_text_for_new_serving_status,
                    status_update_datetime=serving_watermark_datetime,
                    sentence=snapshot.sentence,
                    sequence_num=len(snapshots_to_return) + 1,
                )
            )
            serving_snapshot_exists_at_watermark = True

        snapshot.sequence_num = len(snapshots_to_return) + 1
        snapshots_to_return.append(snapshot)

    # If there were only statuses before the watermark, we haven't made a net-new
    # snapshot yet.
    if not serving_snapshot_exists_at_watermark:
        snapshots_to_return.append(
            StateSentenceStatusSnapshot(
                state_code=sentence.state_code,
                status=StateSentenceStatus.SERVING,
                status_raw_text=raw_text_for_new_serving_status,
                status_update_datetime=serving_watermark_datetime,
                sentence=sentence,
                sequence_num=len(snapshots_to_return) + 1,
            )
        )

    return snapshots_to_return


def _validate_terminating_statuses(
    sorted_snapshots: list[StateSentenceStatusSnapshot],
    correct_early_completed_statuses: bool,
) -> list[StateSentenceStatusSnapshot]:
    """
    Validate that if a terminating status exists in the given snapshots,
    then it is the only terminating status and it is the last one in the list.

    If correct_early_completed_statuses is True, we will correct
    any early COMPLETED statuses to SERVING. Note that we only correct
    COMPLETED and not other terminating statuses (accessed via status.is_terminating_status).
    For example, if a DEATH status is not the final status, we fail.
    """
    if not sorted_snapshots:
        return []
    *initial_snapshots, final_snapshot = sorted_snapshots
    for snapshot in initial_snapshots:
        if snapshot.status.is_terminating_status:
            if (
                snapshot.status == StateSentenceStatus.COMPLETED
                and correct_early_completed_statuses
            ):
                snapshot.status = StateSentenceStatus.SERVING
            else:
                raise ValueError(
                    f"Found [{snapshot.status.value}] status that is not the final status. {snapshot.limited_pii_repr()}"
                )
    return initial_snapshots + [final_snapshot]


def normalize_snapshots_for_single_sentence(
    delegate: StateSpecificSentenceNormalizationDelegate,
    sentence: StateSentence,
    parent_sentences_final_terminating_status_dt: datetime.datetime | None,
) -> list[NormalizedStateSentenceStatusSnapshot]:
    """
    This helper function *actually* normalizes the given SentenceStatusSnapshot entities (for a single sentence).
    Changes depend on the configuration of the delegate, as well as when
    the parent sentences for these snapshots' sentence were finalized (if they exist).

    Depending on these factors, we may :
      - correct early completed statuses
      - designate NON_CREDIT_SERVING statuses
      - TODO(#44525) change statuses to IMPOSED_PENDING_SERVING
    """
    all_snapshots = sorted(
        sentence.sentence_status_snapshots, key=lambda s: s.partition_key
    )
    if delegate.correct_early_completed_statuses:
        all_snapshots = _validate_terminating_statuses(
            all_snapshots, correct_early_completed_statuses=True
        )

    # This should never happen but we check again just in case.
    if (
        delegate.allow_non_credit_serving
        and delegate.correct_imposed_pending_serving_statuses
    ):
        raise ValueError(
            "Both allow_non_credit_serving and correct_imposed_pending_serving_statuses return True. "
            "We can only have one of these properties return True (if a state is providing "
            "explicit serving start dates, then we should be using them). "
            "If this is too strict, please ping #platform-team"
        )

    if (
        delegate.correct_imposed_pending_serving_statuses
        and parent_sentences_final_terminating_status_dt is not None
    ):
        all_snapshots = _correct_and_create_serving_statuses_from_watermark(
            sentence=sentence,
            all_snapshots=all_snapshots,
            serving_watermark_datetime=parent_sentences_final_terminating_status_dt,
            status_value_to_relace_serving=StateSentenceStatus.IMPOSED_PENDING_SERVING,
            raw_text_for_new_serving_status="Created during normalization from parent sentences' final terminating statuses",
        )

    if (
        delegate.allow_non_credit_serving
        and sentence.current_state_provided_start_date is not None
    ):
        all_snapshots = _correct_and_create_serving_statuses_from_watermark(
            sentence=sentence,
            all_snapshots=all_snapshots,
            serving_watermark_datetime=as_datetime(
                assert_type(sentence.current_state_provided_start_date, datetime.date)
            ),
            status_value_to_relace_serving=StateSentenceStatus.NON_CREDIT_SERVING,
            raw_text_for_new_serving_status="Created during normalization from current_state_provided_start_date",
        )

    # We have possibly created new snapshots, and so we should validate that
    # there's at most one terminating status (and it is the last one).
    all_snapshots = _validate_terminating_statuses(
        sorted_snapshots=all_snapshots, correct_early_completed_statuses=False
    )
    return _add_end_datetime_and_normalize_sequence_num(snapshots=all_snapshots)
