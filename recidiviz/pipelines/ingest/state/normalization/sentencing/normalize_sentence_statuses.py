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
            for _id in sentence.parent_sentence_external_id_array.split(","):
                parent_sentence_snapshots.append(normalized_snapshots[_id])

        normalized_snapshots[external_id] = normalize_snapshots_for_single_sentence(
            delegate=delegate,
            sentence=sentence,
            # TODO(#44525): Make use of parent sentence statuses for
            # IMPOSED_PENDING_SERVING
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
                sequence_num=idx + 1,
                sentence_status_snapshot_id=build_unique_sentence_status_snapshot_key(
                    snapshot=snapshot,
                ),
            )
        )
    return normalized_snapshots


def _update_serving_statuses_before_watermark(
    sentence: StateSentence,
    all_snapshots: list[StateSentenceStatusSnapshot],
    watermark_datetime: datetime.datetime,
    new_status: StateSentenceStatus,
    new_status_raw_text: str,
) -> StateSentenceStatusSnapshot | None:
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

    status_update_datetime | status
    -----------------------|--------
    2024-01-01 00:00:00    | IMPOSED_PENDING_SERVING
    2024-01-04 00:00:00    | IMPOSED_PENDING_SERVING
    2024-01-08 00:00:00    | SERVING

    Since there is not a serving status on the date of the watermark_datetime,
    we would return a new StateSentenceStatusSnapshot with the
    status_update_datetime set to the 2024-01-05 and a status value of SERVING.

    When considered with the existing status snapshots downstream of this function,
    that would then look like

    status_update_datetime | status
    -----------------------|--------
    2024-01-01 00:00:00    | IMPOSED_PENDING_SERVING
    2024-01-04 00:00:00    | IMPOSED_PENDING_SERVING
    2024-01-05 00:00:00    | SERVING <--- New! Returned by this function
    2024-01-08 00:00:00    | SERVING

    For that example, if the watermark_datetime was 2024-01-08, we would not have
    made a new StateSentenceStatusSnapshot and this function would have returned None
    """

    make_net_new_serving_status = True
    for snapshot in all_snapshots:
        if not snapshot.status.is_considered_serving_status:
            continue
        if snapshot.status_update_datetime < watermark_datetime:
            snapshot.status = new_status
        if snapshot.status_update_datetime.date() == watermark_datetime.date():
            make_net_new_serving_status = False

    if make_net_new_serving_status:
        return StateSentenceStatusSnapshot(
            state_code=sentence.state_code,
            status_update_datetime=watermark_datetime,
            status=StateSentenceStatus.SERVING,
            status_raw_text=new_status_raw_text,
            sentence=sentence,
        )
    return None


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
    if not sentence.sentence_status_snapshots:
        return []

    # TODO(#44525): Make use of parent sentence statuses for IMPOSED_PENDING_SERVING
    _ = parent_sentences_final_terminating_status_dt
    *initial_snapshots, final_snapshot = sorted(
        sentence.sentence_status_snapshots, key=lambda s: s.partition_key
    )

    # Validate (or correct) non-final statuses are not terminating
    # Note that we only correct COMPLETED and not other terminating statuses
    # (accessed via status.is_terminating_status). For example, if a DEATH status
    # is not the final status, we fail.
    for snapshot in initial_snapshots:
        if snapshot.status.is_terminating_status:
            if (
                snapshot.status == StateSentenceStatus.COMPLETED
                and delegate.correct_early_completed_statuses
            ):
                snapshot.status = StateSentenceStatus.SERVING
            else:
                raise ValueError(
                    f"Found [{snapshot.status.value}] status that is not the final status. {snapshot.limited_pii_repr()}"
                )

    all_snapshots = initial_snapshots + [final_snapshot]

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

    net_new_snapshot: StateSentenceStatusSnapshot | None = None
    if (
        delegate.correct_imposed_pending_serving_statuses
        and parent_sentences_final_terminating_status_dt is not None
    ):
        net_new_snapshot = _update_serving_statuses_before_watermark(
            sentence=sentence,
            all_snapshots=all_snapshots,
            watermark_datetime=parent_sentences_final_terminating_status_dt,
            new_status=StateSentenceStatus.IMPOSED_PENDING_SERVING,
            new_status_raw_text="Created during normalization from parent sentences' final terminating statuses",
        )

    if (
        delegate.allow_non_credit_serving
        and sentence.current_state_provided_start_date is not None
    ):
        net_new_snapshot = _update_serving_statuses_before_watermark(
            sentence=sentence,
            all_snapshots=all_snapshots,
            watermark_datetime=as_datetime(
                assert_type(sentence.current_state_provided_start_date, datetime.date)
            ),
            new_status=StateSentenceStatus.NON_CREDIT_SERVING,
            new_status_raw_text="Created during normalization from current_state_provided_start_date",
        )

    if net_new_snapshot is not None:
        all_snapshots.append(net_new_snapshot)

    return _add_end_datetime_and_normalize_sequence_num(snapshots=all_snapshots)
