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
"""[WIP] State-specific utils for determining compliance with supervision standards for US_PA."""
from datetime import date
from typing import Optional

from recidiviz.calculator.pipeline.utils.supervision_case_compliance_manager import (
    StateSupervisionCaseComplianceManager,
)


class UsPaSupervisionCaseCompliance(StateSupervisionCaseComplianceManager):
    """US_PA specific calculations for supervision case compliance."""

    def _guidelines_applicable_for_case(self) -> bool:
        # TODO(#7052) Update with appropriate policies
        return False

    def _get_initial_assessment_number_of_days(self) -> int:
        # TODO(#7052) Update with appropriate policies
        return 0

    def _num_days_past_required_reassessment(
        self,
        compliance_evaluation_date: date,
        most_recent_assessment_date: date,
        most_recent_assessment_score: int,
    ) -> int:
        # TODO(#7052) Update with appropriate policies
        return 0

    def _face_to_face_contact_frequency_is_sufficient(
        self, compliance_evaluation_date: date
    ) -> Optional[bool]:
        # TODO(#7052) Update with appropriate policies
        return None
