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
"""US_MO implementation of the supervision delegate"""
# pylint: disable=unused-argument
from typing import List, Optional

from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)


class UsMoSupervisionDelegate(StateSpecificSupervisionDelegate):
    """US_MO implementation of the supervision delegate"""

    def supervision_types_mutually_exclusive(self) -> bool:
        """In US_MO, people on DUAL supervision are tracked as mutually exclusive from groups of people
        on PAROLE or PROBATION."""
        return True

    def supervision_period_in_supervision_population_in_non_excluded_date_range(
        self,
        supervision_period: NormalizedStateSupervisionPeriod,
    ) -> bool:
        """In US_MO, a supervision period should have an active PO to also be included to the
        supervision population.
        """

        return supervision_period.supervising_officer_staff_id is not None

    def assessment_types_to_include_for_class(
        self, assessment_class: StateAssessmentClass
    ) -> Optional[List[StateAssessmentType]]:
        """In US_MO, only Ohio Risk Assessment System (ORAS) assessment types are
        supported."""
        if assessment_class == StateAssessmentClass.RISK:
            return [
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            ]
        return None
