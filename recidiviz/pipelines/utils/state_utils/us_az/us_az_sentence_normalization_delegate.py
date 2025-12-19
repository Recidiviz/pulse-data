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
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)


class UsAzSentenceNormalizationDelegate(StateSpecificSentenceNormalizationDelegate):
    """US_AZ implementation of the StateSpecificSentenceNormalizationDelegate.

    Handles AZ-specific sentence normalization logic, including filtering of inferred
    COMPLETED sentence status snapshots for people still incarcerated.
    """

    def __init__(
        self,
        incarceration_periods: list[StateIncarcerationPeriod],
        sentences: list[StateSentence],
    ) -> None:
        super().__init__()

        today = date.today()

        # Check if person has any open incarceration periods
        # A period is "open" if release_date is None OR if release_date is in the future
        # We only look at incarceration periods for AZ, not supervision periods
        has_open_incarceration_period = any(
            ip.release_date is None or ip.release_date > today
            for ip in incarceration_periods
        )

        self.is_currently_incarcerated = has_open_incarceration_period

        # Find the sentence(s) with the most recent SERVING status start timestamp
        # This identifies which sentence(s) are associated with the person's most recent period of incarceration
        # We track sentence IDs rather than dates to handle multiple concurrent sentences
        sentence_ids_by_latest_serving_start: dict[
            datetime.date, set[int]
        ] = defaultdict(set)
        for sentence in sentences:
            if (
                latest_serving := self._get_sentence_latest_serving_start_date(sentence)
            ) and sentence.sentence_id is not None:
                sentence_ids_by_latest_serving_start[latest_serving].add(
                    sentence.sentence_id
                )

        if sentence_ids_by_latest_serving_start:
            overall_latest_serving_start_date = max(
                sentence_ids_by_latest_serving_start
            )
            self.most_recent_serving_sentence_ids = (
                sentence_ids_by_latest_serving_start[overall_latest_serving_start_date]
            )
        else:
            self.most_recent_serving_sentence_ids = set()

    def update_sentence_status_snapshot(
        self,
        sentence: StateSentence,
        snapshot: StateSentenceStatusSnapshot,
    ) -> StateSentenceStatusSnapshot | None:
        """Filter inferred COMPLETED status snapshots for people still incarcerated.

        Arizona does not provide explicit COMPLETED statuses in source data, so we infer
        them from projected completion dates in the raw data. However, this causes issues
        when a person is still incarcerated but their projected completion date has passed.

        Logic:
        1. Identify the sentence(s) with the most recent SERVING status timestamp
           - This identifies the sentence associated with the person's most recent period of incarceration
        2. If person is still incarcerated: filter out COMPLETED for that sentence
           - Ensures at least one sentence remains open when person is actively serving
           - Only affects the most recent sentence, leaving older sentences from previous
             periods of incarceration untouched
        3. If person is NOT incarcerated: leave all COMPLETED statuses as-is
           - Once released, data reverts to original projected dates

        In AZ, we have projected dates at the sentence group level, so as long as one sentence
        in that group is left open, the group will be considered open, and we will show the
        correct dates associated with that person.

        Examples:
        - Person has Sentence A (served 2015) and Sentence B (served 2021), still
          incarcerated today:
          → Only Sentence B (most recent SERVING) gets COMPLETED filtered, Sentence A stays completed
        - Person releases after their projected completion date:
          → While incarcerated: COMPLETED filtered out
          → After release: COMPLETED reappears with original projected date
        """
        # Only process inferred COMPLETED statuses
        is_inferred_completed = (
            snapshot.status == StateSentenceStatus.COMPLETED
            and snapshot.status_raw_text == RECIDIVIZ_MARKED_COMPLETED
        )
        if not is_inferred_completed:
            return snapshot

        # Only process sentences with the most recent SERVING timestamp
        if sentence.sentence_id not in self.most_recent_serving_sentence_ids:
            return snapshot

        # Filter COMPLETED if person is still incarcerated, otherwise leave as-is
        if self.is_currently_incarcerated:
            return None  # Filter out COMPLETED status

        return snapshot  # Leave as-is

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
