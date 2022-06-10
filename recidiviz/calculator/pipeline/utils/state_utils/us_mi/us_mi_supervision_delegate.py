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


class UsMiSupervisionDelegate(StateSpecificSupervisionDelegate):
    """US_MI implementation of the StateSpecificSupervisionDelegate."""

    def assessment_types_to_include_for_class(
        self, assessment_class: StateAssessmentClass
    ) -> Optional[List[StateAssessmentType]]:
        """Defines the assessment types to refer to in calculations for a given
        assessment class. Each state needs to override this with the assessment type
        (e.g. LSIR) that they support."""
        return None
