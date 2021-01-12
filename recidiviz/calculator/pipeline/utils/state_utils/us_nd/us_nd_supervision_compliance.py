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
from typing import Optional

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.supervision_case_compliance_manager import \
    StateSupervisionCaseComplianceManager
from recidiviz.persistence.entity.state.entities import StateAssessment

NUMBER_OF_DAYS_LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE: int = 180


class UsNdSupervisionCaseCompliance(StateSupervisionCaseComplianceManager):
    """US_ND specific calculations for supervision case compliance."""
    def _get_initial_assessment_number_of_days(self) -> int:
        """Returns the number of days that an initial assessment should take place, given a `case_type` and
        `supervision_type`."""
        return NUMBER_OF_DAYS_LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE

    def _guidelines_applicable_for_case(self) -> bool:
        """Returns whether the standard state guidelines are applicable for the given supervision case."""
        return True

    def _reassessment_requirements_are_met(self, compliance_evaluation_date: date,
                                           most_recent_assessment: StateAssessment) -> bool:
        """Returns whether the requirements for reassessment have been met."""
        return self._compliance_evaluation_date_before_reassessment_deadline(compliance_evaluation_date,
                                                                             most_recent_assessment.assessment_date)

    def _face_to_face_contact_frequency_is_sufficient(self, compliance_evaluation_date: date) -> bool:
        """Returns whether the frequency of face-to-face contacts between the officer and the person on supervision
        is sufficient with respect to the state standards for the level of supervision of the case."""
        # TODO(#5199): Update, once face to face contacts are ingested for US_ND.
        return False

    def _compliance_evaluation_date_before_reassessment_deadline(self, compliance_evaluation_date: date,
                                                                 most_recent_assessment_date: Optional[date]) \
            -> bool:
        """Returns whether the compliance evaluation date is before the risk reassessment deadline."""

        # Their assessment is up to date if the compliance_evaluation_date is within
        # NUMBER_OF_DAYS_LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE number of days since the last assessment date.
        if most_recent_assessment_date is None:
            return False
        reassessment_deadline = \
            most_recent_assessment_date + relativedelta(days=NUMBER_OF_DAYS_LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE)
        logging.debug(
            "Last assessment was taken on %s. Re-assessment due by %s, and the compliance evaluation date is %s",
            most_recent_assessment_date, reassessment_deadline, compliance_evaluation_date)
        return compliance_evaluation_date < reassessment_deadline
