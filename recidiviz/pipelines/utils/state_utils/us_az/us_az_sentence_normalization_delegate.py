# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Contains US_AZ implementation of the StateSpecificSentenceNormalizationDelegate."""
import datetime
import json
from collections import defaultdict
from datetime import date

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.ingest.direct.regions.us_az.ingest_views.common_sentencing_views_and_utils import (
    RECIDIVIZ_MARKED_COMPLETED,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSentence,
    StateSentenceStatusSnapshot,
    StateSupervisionPeriod,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)


class UsAzSentenceNormalizationDelegate(StateSpecificSentenceNormalizationDelegate):
    """US_AZ implementation of the StateSpecificSentenceNormalizationDelegate.

    Handles AZ-specific sentence normalization logic, including filtering of inferred
    COMPLETED sentence status snapshots for people still incarcerated.

    Uses group-aware filtering: first identifies the most recent sentence group
    by DOC_NUMBER, then within that group only filters COMPLETED for the
    sentence(s) with the latest serving start date. This ensures that older
    sentence groups are properly closed even when their sentences share a serving
    start date with the most recent group, while also allowing individual
    sentences within the most recent group to complete normally.

    The most recent group is determined by DOC_NUMBER (the cycle number from the
    DOC_EPISODE table), which is the only reliable chronological ordering of
    episodes per person. DOC_NUMBER is stored in sentence_metadata as doc_number.
    """

    def __init__(
        self,
        incarceration_periods: list[StateIncarcerationPeriod],
        supervision_periods: list[StateSupervisionPeriod],
        sentences: list[StateSentence],
    ) -> None:
        super().__init__()

        today = date.today()

        # Check if person has any open incarceration periods
        # A period is "open" if release_date is None OR if release_date is in the future
        has_open_incarceration_period = any(
            ip.release_date is None or ip.release_date > today
            for ip in incarceration_periods
        )
        self.is_currently_incarcerated = has_open_incarceration_period

        # Check if person has any open supervision periods
        # A period is "open" if termination_date is None OR if termination_date is in the future
        has_open_supervision_period = any(
            sp.termination_date is None or sp.termination_date > today
            for sp in supervision_periods
        )
        self.is_currently_supervised = has_open_supervision_period

        # Group SERVING sentences by sentence_group_external_id (SC_EPISODE_ID)
        # and track the DOC_NUMBER (cycle number) per group for ordering.
        serving_sentences_by_group: dict[str, list[StateSentence]] = defaultdict(list)
        group_doc_number: dict[str, int] = {}
        for sentence in sentences:
            if not self._has_serving_status(sentence):
                continue
            if (
                sentence.sentence_id is None
                or sentence.sentence_group_external_id is None
            ):
                continue

            group_id = sentence.sentence_group_external_id
            serving_sentences_by_group[group_id].append(sentence)

            if group_id not in group_doc_number:
                doc_number = self._get_doc_number_from_metadata(sentence)
                if doc_number is not None:
                    group_doc_number[group_id] = doc_number

        # Pick the most recent group by DOC_NUMBER (cycle number), with
        # SC_EPISODE_ID as a fallback for data that predates the doc_number
        # metadata field. Within that group, only protect the sentence(s) with
        # the latest serving start date from COMPLETED filtering — other
        # sentences in the group that have legitimately completed should stay
        # completed.
        self.most_recent_serving_sentence_ids: set[int] = set()
        if serving_sentences_by_group:
            most_recent_group = max(
                serving_sentences_by_group,
                key=lambda g: (group_doc_number.get(g, -1), self._safe_int(g)),
            )
            # Within the most recent group, find the latest serving start date
            # and only protect those sentences from COMPLETED filtering.
            sentence_ids_by_serving_date: dict[datetime.date, set[int]] = defaultdict(
                set
            )
            for sentence in serving_sentences_by_group[most_recent_group]:
                latest_serving = self._get_sentence_latest_serving_start_date(sentence)
                if latest_serving and sentence.sentence_id is not None:
                    sentence_ids_by_serving_date[latest_serving].add(
                        sentence.sentence_id
                    )
            if sentence_ids_by_serving_date:
                latest_date = max(sentence_ids_by_serving_date)
                self.most_recent_serving_sentence_ids = sentence_ids_by_serving_date[
                    latest_date
                ]

    def update_sentence_status_snapshot(
        self,
        sentence: StateSentence,
        snapshot: StateSentenceStatusSnapshot,
    ) -> StateSentenceStatusSnapshot | None:
        """Filter inferred COMPLETED status snapshots for people still incarcerated or supervised.

        Arizona does not provide explicit COMPLETED statuses in source data, so we infer
        them from projected completion dates in the raw data. However, this causes issues
        when a person is still incarcerated but their projected completion date has passed.

        Logic:
        1. Identify the most recent sentence group by DOC_NUMBER (cycle number)
        2. Within that group, identify the sentence(s) with the latest serving start date
        3. If person is still incarcerated or supervised: filter out COMPLETED for
           only those latest-serving sentences
           - Ensures at least one sentence remains open when person is actively serving
           - Other sentences in the group that have legitimately completed stay completed
           - Only affects the most recent group, leaving older groups properly closed
        4. If person is NOT incarcerated or supervised: leave all COMPLETED statuses as-is
           - Once released, data reverts to original projected dates

        In AZ, we have projected dates at the sentence group level, so as long as one
        sentence in that group is left open, the group will be considered open.
        """
        # Only process inferred COMPLETED statuses
        is_inferred_completed = (
            snapshot.status == StateSentenceStatus.COMPLETED
            and snapshot.status_raw_text == RECIDIVIZ_MARKED_COMPLETED
        )
        if not is_inferred_completed:
            return snapshot

        # Only filter for the latest-serving sentences in the most recent group
        if sentence.sentence_id not in self.most_recent_serving_sentence_ids:
            return snapshot

        # Filter COMPLETED if person is still incarcerated or supervised, otherwise leave as-is
        if self.is_currently_incarcerated or self.is_currently_supervised:
            return None  # Filter out COMPLETED status

        return snapshot  # Leave as-is

    @staticmethod
    def _has_serving_status(sentence: StateSentence) -> bool:
        """Returns True if this sentence has any serving status snapshots."""
        return any(
            s.status.is_considered_serving_status
            for s in sentence.sentence_status_snapshots
        )

    @staticmethod
    def _get_sentence_latest_serving_start_date(
        sentence: StateSentence,
    ) -> datetime.date | None:
        """Returns the latest date that we see a serving status for this sentence, or
        None if there are no serving statuses.
        """
        serving_statuses = [
            s
            for s in sentence.sentence_status_snapshots
            if s.status.is_considered_serving_status
        ]
        if not serving_statuses:
            return None
        latest_serving = max(
            serving_statuses,
            key=lambda s: s.status_update_datetime,
        )
        return latest_serving.status_update_datetime.date()

    @staticmethod
    def _safe_int(value: str) -> int:
        """Attempts to parse a string as an integer, returning 0 if it fails.

        In AZ, sentence_group_external_id is SC_EPISODE_ID which is always
        numeric. This fallback handles test data or unexpected formats.
        """
        try:
            return int(value)
        except ValueError:
            return 0

    @staticmethod
    def _get_doc_number_from_metadata(sentence: StateSentence) -> int | None:
        """Extracts the DOC_NUMBER (cycle number) from sentence_metadata JSON.

        DOC_NUMBER is a sequential integer assigned per person in DOC_EPISODE,
        representing each cycle through the system. It is the only reliable
        chronological ordering of episodes.
        """
        if not sentence.sentence_metadata:
            return None
        metadata = json.loads(sentence.sentence_metadata)
        doc_number_str = metadata["doc_number"]
        if not doc_number_str:
            return None
        return int(doc_number_str)
