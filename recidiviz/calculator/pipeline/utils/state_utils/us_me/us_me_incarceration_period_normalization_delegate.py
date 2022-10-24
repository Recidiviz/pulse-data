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
"""Contains state-specific logic for certain aspects of pre-processing US_ME
StateIncarcerationPeriod entities so that they are ready to be used in pipeline
calculations."""
import json
from typing import List, Optional

from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.date import safe_strptime
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
)

INCARCERATION_SENTENCE_PERIOD_LOOKBACK = 7


class UsMeIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_ME implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    def normalization_relies_on_incarceration_sentences(self) -> bool:
        return True

    def incarceration_admission_reason_override(
        self,
        incarceration_period: StateIncarcerationPeriod,
        incarceration_sentences: Optional[List[StateIncarcerationSentence]],
    ) -> Optional[StateIncarcerationPeriodAdmissionReason]:
        """If there is a revocation sentence with an intake date within a week of the same start date of this
        period, then we assume the period's admission reason was a revocation."""
        if not incarceration_sentences:
            return incarceration_period.admission_reason

        for incarceration_sentence in incarceration_sentences:
            sentence_metadata = (
                json.loads(incarceration_sentence.sentence_metadata)
                if incarceration_sentence.sentence_metadata
                else None
            )
            term_intake_date = safe_strptime(
                sentence_metadata["TERM_INTAKE_DATE"], "%Y-%m-%d %I:%M:%S"
            )

            is_revocation_sentence = sentence_metadata["IS_REVOCATION_SENTENCE"] == "Y"

            if (
                term_intake_date
                and incarceration_period.start_date_inclusive
                and (
                    abs(
                        term_intake_date.date()
                        - incarceration_period.start_date_inclusive
                    ).days
                    <= INCARCERATION_SENTENCE_PERIOD_LOOKBACK
                )
                and is_revocation_sentence
            ):
                return StateIncarcerationPeriodAdmissionReason.REVOCATION

        return incarceration_period.admission_reason
