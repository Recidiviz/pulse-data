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
from typing import List, Optional, Tuple

from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)


class UsPaSupervisionDelegate(StateSpecificSupervisionDelegate):
    """US_PA implementation of the supervision delegate"""

    def supervision_location_from_supervision_site(
        self,
        supervision_site: Optional[str],
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
