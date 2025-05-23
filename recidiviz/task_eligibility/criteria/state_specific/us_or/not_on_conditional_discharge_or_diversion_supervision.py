# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
# ============================================================================
"""Identifies individuals who are not on conditional-discharge or diversion supervision
in OR.
"""

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.utils.state_specific_criteria_builders import (
    state_specific_supervision_type_raw_text_is_not,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_OR_NOT_ON_CONDITIONAL_DISCHARGE_OR_DIVERSION_SUPERVISION"

VIEW_BUILDER = state_specific_supervision_type_raw_text_is_not(
    state_code=StateCode.US_OR,
    criteria_name=_CRITERIA_NAME,
    criteria_description=__doc__,
    ineligible_raw_text_supervision_type_condition="IN ('CD', 'DV')",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
