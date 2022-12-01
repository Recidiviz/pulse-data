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
"""US_IX implementation of the supervision delegate"""
from typing import List, Optional

from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
)

_OUT_OF_STATE_EXTERNAL_ID_IDENTIFIERS: List[str] = [
    "INTERSTATE PROBATION",
    "PAROLE COMMISSION OFFICE",
]


class UsIxSupervisionDelegate(StateSpecificSupervisionDelegate):
    """US_IX implementation of the StateSpecificSupervisionDelegate."""

    def supervision_types_mutually_exclusive(self) -> bool:
        """In US_IX, people on DUAL supervision are tracked as mutually exclusive from groups of people
        on PAROLE or PROBATION."""
        return True

    def is_supervision_location_out_of_state(
        self,
        deprecated_supervising_district_external_id: Optional[str],
    ) -> bool:
        """For Idaho, we look at the supervision district identifier to see if it's a non-Idaho
        entity/jurisdiction."""
        # TODO(#4713): Rely on level_2_supervising_district_external_id, once it is populated.
        return (
            deprecated_supervising_district_external_id is not None
            and deprecated_supervising_district_external_id.startswith(
                tuple(_OUT_OF_STATE_EXTERNAL_ID_IDENTIFIERS)
            )
        )

    def assessment_types_to_include_for_class(
        self, assessment_class: StateAssessmentClass
    ) -> Optional[List[StateAssessmentType]]:
        if assessment_class == StateAssessmentClass.RISK:
            return [StateAssessmentType.LSIR]
        return None
