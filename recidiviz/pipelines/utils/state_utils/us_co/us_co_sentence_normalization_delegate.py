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
"""Contains US_CO implementation of the StateSpecificSentenceNormalizationDelegate."""
import datetime
from typing import Optional

from recidiviz.persistence.entity.entity_utils import deep_entity_update
from recidiviz.persistence.entity.state.entities import StateIncarcerationSentence
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    StateSpecificSentenceNormalizationDelegate,
)


class UsCoSentenceNormalizationDelegate(StateSpecificSentenceNormalizationDelegate):
    """US_CO implementation of the StateSpecificSentenceNormalizationDelegate."""

    def update_incarceration_sentence(
        self, incarceration_sentence: StateIncarcerationSentence
    ) -> StateIncarcerationSentence:
        for index, charge in enumerate(incarceration_sentence.charges):
            incarceration_sentence.charges[index] = deep_entity_update(
                charge,
                offense_date=charge.offense_date
                if self._is_date_valid(charge.offense_date)
                else None,
                date_charged=charge.date_charged
                if self._is_date_valid(charge.date_charged)
                else None,
            )
        return deep_entity_update(
            incarceration_sentence,
            date_imposed=incarceration_sentence.date_imposed
            if self._is_date_valid(incarceration_sentence.date_imposed)
            else None,
            effective_date=incarceration_sentence.effective_date
            if self._is_date_valid(incarceration_sentence.effective_date)
            else None,
            projected_min_release_date=incarceration_sentence.projected_min_release_date
            if self._is_date_valid(incarceration_sentence.projected_min_release_date)
            else None,
            projected_max_release_date=incarceration_sentence.projected_max_release_date
            if self._is_date_valid(incarceration_sentence.projected_max_release_date)
            else None,
            completion_date=incarceration_sentence.completion_date
            if self._is_date_valid(incarceration_sentence.completion_date)
            else None,
            parole_eligibility_date=incarceration_sentence.parole_eligibility_date
            if self._is_date_valid(incarceration_sentence.parole_eligibility_date)
            else None,
        )

    @staticmethod
    def _is_date_valid(given_date: Optional[datetime.date]) -> bool:
        """For CO, we see many occurrences of dates in incarceration sentences having
        years that are less than 1900. Therefore, any date that is null or past 1900
        is considered a valid date."""
        return not given_date or given_date.year >= 1900
