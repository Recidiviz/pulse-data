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
"""US_ME implementation of the supervision pre-processing delegate"""
from collections import OrderedDict
from typing import List, Optional

from recidiviz.common.constants.state.state_assessment import StateAssessmentLevel
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateSupervisionPeriod,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionSentence,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)

SUPERVISION_SENTENCE_PERIOD_LOOKBACK = 7


class UsMeSupervisionNormalizationDelegate(
    StateSpecificSupervisionNormalizationDelegate
):
    """US_ME implementation of the supervision pre-processing delegate"""

    def __init__(
        self,
        assessments: List[StateAssessment],
        supervision_sentences: List[NormalizedStateSupervisionSentence],
    ):
        self._assessments = assessments
        self._supervision_sentences = supervision_sentences

    def supervision_termination_reason_override(
        self, supervision_period: StateSupervisionPeriod
    ) -> Optional[StateSupervisionPeriodTerminationReason]:
        """If there was a revocation sentence status with a completion date within a week of the supervision period's
        end date, then we assume the period's termination reason was a revocation."""
        if not self._supervision_sentences:
            return supervision_period.termination_reason

        for supervision_sentence in self._supervision_sentences:
            if (
                supervision_sentence.completion_date
                and supervision_period.end_date_exclusive
                and abs(
                    (
                        supervision_sentence.completion_date
                        - supervision_period.end_date_exclusive
                    ).days
                )
                <= SUPERVISION_SENTENCE_PERIOD_LOOKBACK
            ) and supervision_sentence.status == StateSentenceStatus.REVOKED:
                return StateSupervisionPeriodTerminationReason.REVOCATION
        return supervision_period.termination_reason

    def supervision_level_override(
        self,
        supervision_period_list_index: int,
        sorted_supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionLevel]:
        """US_ME specific logic for determining supervision level from assessment scores."""
        if not self._assessments:
            return StateSupervisionLevel.INTERNAL_UNKNOWN

        assessment_type_raw_texts = [
            "SPIN-W",
            "ADULT, FEMALE, COMMUNITY",
            "ADULT, MALE, COMMUNITY",
            "STATIC 99",
            "STATIC 99 R",
        ]
        # Ordered low to high
        assessment_level_to_supervision_level = OrderedDict(
            {
                StateAssessmentLevel.MINIMUM: StateSupervisionLevel.LIMITED,
                StateAssessmentLevel.LOW: StateSupervisionLevel.MINIMUM,
                StateAssessmentLevel.MODERATE: StateSupervisionLevel.MEDIUM,
                StateAssessmentLevel.HIGH: StateSupervisionLevel.HIGH,
                StateAssessmentLevel.VERY_HIGH: StateSupervisionLevel.HIGH,
                StateAssessmentLevel.MAXIMUM: StateSupervisionLevel.MAXIMUM,
            }
        )

        assessments_before_period_ends: List[StateAssessment] = []

        supervision_period = sorted_supervision_periods[supervision_period_list_index]

        for assessment in self._assessments:
            # Only include assessments done before the supervision period's end date
            if (
                supervision_period.end_date_exclusive is None
                or (
                    supervision_period.end_date_exclusive
                    and assessment.assessment_date
                    and assessment.assessment_date
                    <= supervision_period.end_date_exclusive
                )
            ) and assessment.assessment_type_raw_text in assessment_type_raw_texts:
                assessments_before_period_ends.append(assessment)

        if not assessments_before_period_ends:
            return StateSupervisionLevel.INTERNAL_UNKNOWN

        # Always take the level of most recent Static 99 first
        static_99_assessments = [
            a
            for a in assessments_before_period_ends
            if a.assessment_type_raw_text in ("STATIC 99", "STATIC 99 R")
        ]
        if static_99_assessments:
            most_recent_assessment_level = self._find_the_most_recent_assessment_level(
                static_99_assessments
            )
            if most_recent_assessment_level:
                return assessment_level_to_supervision_level[
                    most_recent_assessment_level
                ]

        # Next check for a SPIN-W or Adult,Female,Community LSIR assessment
        spin_w_or_adult_female_assessments = [
            a
            for a in assessments_before_period_ends
            if a.assessment_type_raw_text in ("SPIN-W", "ADULT, FEMALE, COMMUNITY")
        ]
        if spin_w_or_adult_female_assessments:
            most_recent_assessment_level = self._find_the_most_recent_assessment_level(
                spin_w_or_adult_female_assessments
            )
            if most_recent_assessment_level:
                return assessment_level_to_supervision_level[
                    most_recent_assessment_level
                ]

        # If there is not a Static 99 or Spin-W, take the most recent assessment level
        most_recent_assessment_level = self._find_the_most_recent_assessment_level(
            assessments_before_period_ends
        )
        if most_recent_assessment_level:
            return assessment_level_to_supervision_level[most_recent_assessment_level]

        return StateSupervisionLevel.INTERNAL_UNKNOWN

    @staticmethod
    def _find_the_most_recent_assessment_level(
        assessments: List[StateAssessment],
    ) -> StateAssessmentLevel | None:
        most_recent_date = max(
            assessment.assessment_date
            for assessment in assessments
            if assessment.assessment_date
        )
        # Sort to deterministically pick an assessment when there are multiple
        # on a given day.
        assessments_on_date = sorted(
            [
                assessment
                for assessment in assessments
                if assessment.assessment_date == most_recent_date
            ],
            key=lambda a: a.external_id,
        )

        assessment = assessments_on_date[0]
        return assessment.assessment_level
