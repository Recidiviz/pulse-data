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
"""Contains generic implementation of the StateSpecificSupervisionDelegate."""
from typing import List, Optional

from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
)


class UsCaSupervisionDelegate(StateSpecificSupervisionDelegate):
    """US_CA implementation of the StateSpecificSupervisionDelegate."""

    def assessment_types_to_include_for_class(
        self, assessment_class: StateAssessmentClass
    ) -> Optional[List[StateAssessmentType]]:
        """For unit tests, we support all types of assessments."""
        if assessment_class == StateAssessmentClass.RISK:
            return [
                StateAssessmentType.LSIR,
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            ]
        return None
