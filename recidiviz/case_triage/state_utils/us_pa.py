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
"""Contains policy requirements that are specific to Pennsylvania."""
from typing import Dict, Optional, Tuple

from recidiviz.case_triage.state_utils.types import PolicyRequirements
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_person import StateGender
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)

# Dictionary from case type -> supervision level -> tuple of number of times they must be contacted per time period.
# A tuple (x, y) should be interpreted as x home visits every y days.
SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS: Dict[
    StateSupervisionCaseType, Dict[StateSupervisionLevel, Tuple[int, int]]
] = {
    StateSupervisionCaseType.GENERAL: {
        StateSupervisionLevel.MINIMUM: (1, 90),
        StateSupervisionLevel.MEDIUM: (1, 30),
        StateSupervisionLevel.MAXIMUM: (2, 30),
        StateSupervisionLevel.HIGH: (4, 30),
    }
}

# Dictionary from supervision level -> tuple of number of times they must be contacted per time period.
# A tuple (x, y) should be interpreted as x home visits every y days.
SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS: Dict[
    StateSupervisionCaseType, Dict[StateSupervisionLevel, Tuple[int, int]]
] = {
    StateSupervisionCaseType.GENERAL: {
        StateSupervisionLevel.MEDIUM: (1, 60),
        StateSupervisionLevel.MAXIMUM: (1, 30),
        StateSupervisionLevel.HIGH: (1, 30),
    }
}

CURRENT_US_PA_ASSESSMENT_SCORE_RANGE: Dict[
    StateGender, Dict[StateSupervisionLevel, Tuple[int, Optional[int]]]
] = {
    gender: {
        StateSupervisionLevel.MINIMUM: (0, 19),
        StateSupervisionLevel.MEDIUM: (20, 27),
        StateSupervisionLevel.MAXIMUM: (28, None),
    }
    for gender in StateGender
}

US_PA_SUPERVISION_LEVEL_NAMES = {
    StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY: "Monitored",
    StateSupervisionLevel.LIMITED: "Administrative",
    StateSupervisionLevel.MINIMUM: "Minimum",
    StateSupervisionLevel.MEDIUM: "Medium",
    StateSupervisionLevel.HIGH: "Enhanced",
    StateSupervisionLevel.MAXIMUM: "Maximum",
}


def us_pa_policy_requirements() -> PolicyRequirements:
    """Returns set of policy requirements for Idaho."""
    return PolicyRequirements(
        assessment_score_cutoffs=CURRENT_US_PA_ASSESSMENT_SCORE_RANGE,
        doc_short_name="PBPP",
        oms_name="CAPTOR",
        policy_references_for_opportunities={},
        supervision_contact_frequencies=SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS,
        supervision_level_names=US_PA_SUPERVISION_LEVEL_NAMES,
        supervision_home_visit_frequencies=SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS,
        supervision_policy_reference=None,
    )
