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
from recidiviz.persistence.entity.state.entities import (
    StateSentence,
    StateSentenceStatusSnapshot,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSentenceStatusSnapshot,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)
from recidiviz.utils.types import assert_type


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
        normalized_snapshots[external_id] = _normalize_given_snapshots(
            delegate=delegate,
            snapshots=sentence.sentence_status_snapshots,
            # TODO(#44525): Make use of parent sentence statuses for
            # IMPOSED_PENDING_SERVING
            parent_sentences_final_terminating_status_dt=None,
        )
    if set(normalized_snapshots.keys()) != set(unprocessed_sentences.keys()):
        raise ValueError(
            "Normalized snapshots keys do not match unprocessed sentences keys."
        )
    return normalized_snapshots


def _normalize_given_snapshots(
    delegate: StateSpecificSentenceNormalizationDelegate,
    snapshots: list[StateSentenceStatusSnapshot],
    parent_sentences_final_terminating_status_dt: datetime.datetime | None,
) -> list[NormalizedStateSentenceStatusSnapshot]:
    """
    This helper function *actually* normalizes the given SentenceStatusSnapshot entities (for a single sentence).
    Changes depend on the configuration of the delegate, as well as when
    the parent sentences for these snapshots' sentence were finalized (if they exist).

    Depending on these factors, we may :
      - correct early completed statuses
      - TODO(#44525) change statuses to IMPOSED_PENDING_SERVING
    """

    # TODO(#44525): Make use of parent sentence statuses for IMPOSED_PENDING_SERVING
    _ = parent_sentences_final_terminating_status_dt
    snapshots = sorted(snapshots, key=lambda s: s.partition_key)
    n_snapshots = len(snapshots)
    normalized_snapshots = []
    for idx, snapshot in enumerate(snapshots):
        # The final snapshot has no end date.
        if idx == n_snapshots - 1:
            end_dt = None
        else:
            end_dt = snapshots[idx + 1].status_update_datetime
            if (
                delegate.correct_early_completed_statuses
                and snapshot.status == StateSentenceStatus.COMPLETED
            ):
                snapshot.status = StateSentenceStatus.SERVING
            if snapshot.status.is_terminating_status:
                raise ValueError(
                    f"Found [{snapshot.status.value}] status that is not the final status. {snapshot.limited_pii_repr()}"
                )
        normalized_snapshots.append(
            NormalizedStateSentenceStatusSnapshot(
                state_code=snapshot.state_code,
                status_update_datetime=snapshot.status_update_datetime,
                status_end_datetime=end_dt,
                status=snapshot.status,
                status_raw_text=snapshot.status_raw_text,
                sentence_status_snapshot_id=assert_type(
                    snapshot.sentence_status_snapshot_id, int
                ),
                sequence_num=idx + 1,
            )
        )
    return normalized_snapshots
