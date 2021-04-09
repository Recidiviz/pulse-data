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
"""State-specific utils for determining compliance with supervision standards for US_ND."""
from datetime import date
import logging
from typing import Optional, List

from dateutil.relativedelta import relativedelta

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)
from recidiviz.calculator.pipeline.utils.supervision_case_compliance_manager import (
    StateSupervisionCaseComplianceManager,
)

# Please refer to http://go/nd-risk-assessment-policy for context on these values.
# These values were last verified on 02/23/2021.
LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE: int = 30
REASSESSMENT_DEADLINE_DAYS: int = 212


class UsNdSupervisionCaseCompliance(StateSupervisionCaseComplianceManager):
    """US_ND specific calculations for supervision case compliance."""

    def _get_initial_assessment_number_of_days(self) -> int:
        """Returns the number of days that an initial assessment should take place."""
        return LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE

    def _guidelines_applicable_for_case(self) -> bool:
        """Returns whether the standard state guidelines are applicable for the given supervision case. The standard
        guidelines are not applicable for people who are not classified, who are in interstate compact, or in diversion
        programs."""
        disallowed_supervision_levels: List[StateSupervisionLevel] = [
            StateSupervisionLevel.EXTERNAL_UNKNOWN,
            StateSupervisionLevel.INTERSTATE_COMPACT,
            StateSupervisionLevel.DIVERSION,
        ]
        return (
            self.supervision_period.supervision_level
            not in disallowed_supervision_levels
        )

    def _num_days_past_required_reassessment(
        self,
        compliance_evaluation_date: date,
        most_recent_assessment_date: date,
        most_recent_assessment_score: int,
    ) -> int:
        """Returns the number of days it has been since the required reassessment deadline. Returns 0
        if the reassessment is not overdue."""
        reassessment_deadline = most_recent_assessment_date + relativedelta(
            days=REASSESSMENT_DEADLINE_DAYS
        )
        logging.debug(
            "Last assessment was taken on %s. Re-assessment due by %s, and the compliance evaluation date is %s",
            most_recent_assessment_date,
            reassessment_deadline,
            compliance_evaluation_date,
        )
        return max(0, (compliance_evaluation_date - reassessment_deadline).days)

    def _face_to_face_contact_frequency_is_sufficient(
        self, compliance_evaluation_date: date
    ) -> Optional[bool]:
        """Returns whether the frequency of face-to-face contacts between the officer and the person on supervision
        is sufficient with respect to the state standards for the level of supervision of the case."""
        # TODO(#5199): Update, once face to face contacts are ingested for US_ND.
        return None
