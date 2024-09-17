# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Contains US_MI implementation of the StateSpecificSentenceNormalizationDelegate."""

from datetime import timedelta

from recidiviz.persistence.entity.state.entities import StateIncarcerationSentence
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)


class UsMiSentenceNormalizationDelegate(StateSpecificSentenceNormalizationDelegate):
    """US_MI implementation of the StateSpecificSentenceNormalizationDelegate."""

    def update_incarceration_sentence(
        self, incarceration_sentence: StateIncarcerationSentence
    ) -> StateIncarcerationSentence:
        if (
            incarceration_sentence.projected_max_release_date is None
            and incarceration_sentence.effective_date is not None
            and incarceration_sentence.max_length_days is not None
        ):
            incarceration_sentence.projected_max_release_date = (
                incarceration_sentence.effective_date
                + timedelta(days=incarceration_sentence.max_length_days)
            )

        return incarceration_sentence
