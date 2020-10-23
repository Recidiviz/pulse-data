# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utils for events that are a product of each pipeline's identifier step."""
import logging

import attr

from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.state.state_assessment import StateAssessmentType


@attr.s
class IdentifierEvent(BuildableAttr):
    """Base class for events created by the identifier step of each pipeline."""
    # The state where the event took place
    state_code: str = attr.ib()


@attr.s
class AssessmentEventMixin:
    """Attribute that enables an event to be able to calculate the score bucket from assessment information."""

    DEFAULT_ASSESSMENT_SCORE_BUCKET = 'NOT_ASSESSED'

    @property
    def assessment_score_bucket(self) -> str:
        """Calculates the assessment score bucket that applies to measurement.

        NOTE: Only LSIR and ORAS buckets are currently supported
        TODO(#2742): Add calculation support for all supported StateAssessmentTypes

        Returns:
            A string representation of the assessment score for the person.
            DEFAULT_ASSESSMENT_SCORE_BUCKET if the assessment type is not supported or if the object is missing
                assessment information.
        """
        state_code = getattr(self, 'state_code')
        assessment_score = getattr(self, 'assessment_score')
        assessment_level = getattr(self, 'assessment_level')
        assessment_type = getattr(self, 'assessment_type')

        if assessment_type:
            if assessment_type == StateAssessmentType.LSIR:
                if state_code == 'US_PA':
                    # The score buckets for US_PA have changed over time, so we defer to the assessment_level
                    if assessment_level:
                        return assessment_level.value
                else:
                    if assessment_score:
                        if assessment_score < 24:
                            return '0-23'
                        if assessment_score <= 29:
                            return '24-29'
                        if assessment_score <= 38:
                            return '30-38'
                        return '39+'

            elif assessment_type in [
                    StateAssessmentType.ORAS,
                    StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                    StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
                    StateAssessmentType.ORAS_MISDEMEANOR_ASSESSMENT,
                    StateAssessmentType.ORAS_MISDEMEANOR_SCREENING,
                    StateAssessmentType.ORAS_PRE_TRIAL,
                    StateAssessmentType.ORAS_PRISON_SCREENING,
                    StateAssessmentType.ORAS_PRISON_INTAKE,
                    StateAssessmentType.ORAS_REENTRY,
                    StateAssessmentType.ORAS_STATIC,
                    StateAssessmentType.ORAS_SUPPLEMENTAL_REENTRY
            ]:
                if assessment_level:
                    return assessment_level.value
            elif assessment_type in [
                    StateAssessmentType.INTERNAL_UNKNOWN,
                    StateAssessmentType.ASI,
                    StateAssessmentType.CSSM,
                    StateAssessmentType.HIQ,
                    StateAssessmentType.PA_RST,
                    StateAssessmentType.PSA,
                    StateAssessmentType.SORAC,
                    StateAssessmentType.STATIC_99,
                    StateAssessmentType.TCU_DRUG_SCREEN
            ]:
                logging.warning("Assessment type %s is unsupported.", assessment_type)
            else:
                raise ValueError(f"Unexpected unsupported StateAssessmentType: {assessment_type}")

        return self.DEFAULT_ASSESSMENT_SCORE_BUCKET
