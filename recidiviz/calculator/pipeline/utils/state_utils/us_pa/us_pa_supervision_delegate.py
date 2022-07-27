# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""US_PA implementation of the supervision delegate"""
from datetime import date
from typing import List, Optional, Tuple

from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
)
from recidiviz.common.date import DateRange, DateRangeDiff
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationSentence,
    StateSupervisionPeriod,
    StateSupervisionSentence,
)


class UsPaSupervisionDelegate(StateSpecificSupervisionDelegate):
    """US_PA implementation of the supervision delegate"""

    def supervision_location_from_supervision_site(
        self, supervision_site: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """In US_PA, supervision_site follows format
        {supervision district}|{supervision suboffice}|{supervision unit org code}"""
        # TODO(#3829): Remove this helper once we've built level 1/level 2 supervision
        #  location distinction directly into our schema.
        level_1_supervision_location = None
        level_2_supervision_location = None
        if supervision_site:
            (
                level_2_supervision_location,
                level_1_supervision_location,
                _org_code,
            ) = supervision_site.split("|")
        return (
            level_1_supervision_location or None,
            level_2_supervision_location or None,
        )

    # pylint: disable=unused-argument
    def supervision_period_in_supervision_population_in_non_excluded_date_range(
        self,
        supervision_period: StateSupervisionPeriod,
        supervising_officer_external_id: Optional[str],
    ) -> bool:
        """In US_PA, a supervision period only counts towards the supervision population
        if it is not a period indicating absconsion."""
        return (
            supervision_period.admission_reason
            != StateSupervisionPeriodAdmissionReason.ABSCONSION
        )

    def assessment_types_to_include_for_class(
        self, assessment_class: StateAssessmentClass
    ) -> Optional[List[StateAssessmentType]]:
        """In US_PA, only LSIR assessments are supported."""
        if assessment_class == StateAssessmentClass.RISK:
            return [StateAssessmentType.LSIR]
        return None

    # pylint: disable=unused-argument
    def set_lsir_assessment_score_bucket(
        self,
        assessment_score: Optional[int],
        assessment_level: Optional[StateAssessmentLevel],
    ) -> Optional[str]:
        """In US_PA, the score buckets for LSIR have changed over time, so in order to
        set the bucket, the assessment level is used instead."""
        if assessment_level:
            return assessment_level.value
        return None

    # pylint: disable=unused-argument
    def get_projected_completion_date(
        self,
        supervision_period: StateSupervisionPeriod,
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
    ) -> Optional[date]:
        """In US_PA, we only consider incarceration sentences for the projected completion
        dates of periods of supervision."""
        if not incarceration_sentences:
            return None

        relevant_max_release_dates = [
            incarceration_sentence.projected_max_release_date
            for incarceration_sentence in incarceration_sentences
            if incarceration_sentence.start_date
            and DateRangeDiff(
                supervision_period.duration,
                DateRange.from_maybe_open_range(
                    incarceration_sentence.start_date,
                    incarceration_sentence.completion_date,
                ),
            ).overlapping_range
            and incarceration_sentence.projected_max_release_date
        ]

        return max(relevant_max_release_dates) if relevant_max_release_dates else None
