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
"""Contains policy requirements that are specific to Idaho."""
from typing import Dict, Optional, Tuple

from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_compliance import (
    SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS,
)
from recidiviz.case_triage.state_utils.types import PolicyRequirements
from recidiviz.common.constants.person_characteristics import Gender
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)


# Note: This mapping doesn't contain details for EXTERNAL_UNKNOWN or NULL. All other types
# that we know about (as of Feb. 9, 2021) are reflected in this mapping.
# See
# http://forms.idoc.idaho.gov/WebLink/0/edoc/281944/Interim%20Standards%20to%20Probation%20and%20Parole%20Supervision%20Strategies.pdf
US_ID_ASSESSMENT_SCORE_RANGE: Dict[
    Gender, Dict[StateSupervisionLevel, Tuple[int, Optional[int]]]
] = {
    Gender.FEMALE: {
        StateSupervisionLevel.MINIMUM: (0, 22),
        StateSupervisionLevel.MEDIUM: (23, 30),
        StateSupervisionLevel.HIGH: (31, None),
    },
    Gender.TRANS_FEMALE: {
        StateSupervisionLevel.MINIMUM: (0, 22),
        StateSupervisionLevel.MEDIUM: (23, 30),
        StateSupervisionLevel.HIGH: (31, None),
    },
    Gender.MALE: {
        StateSupervisionLevel.MINIMUM: (0, 20),
        StateSupervisionLevel.MEDIUM: (21, 28),
        StateSupervisionLevel.HIGH: (29, None),
    },
    Gender.TRANS_MALE: {
        StateSupervisionLevel.MINIMUM: (0, 20),
        StateSupervisionLevel.MEDIUM: (21, 28),
        StateSupervisionLevel.HIGH: (29, None),
    },
}

US_ID_SUPERVISION_LEVEL_NAMES = {
    StateSupervisionLevel.MINIMUM: "Low",
    StateSupervisionLevel.MEDIUM: "Moderate",
    StateSupervisionLevel.HIGH: "High",
}


def us_id_policy_requirements() -> PolicyRequirements:
    """Returns set of policy requirements for Idaho."""
    return PolicyRequirements(
        assessment_score_cutoffs=US_ID_ASSESSMENT_SCORE_RANGE,
        supervision_contact_frequencies=SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS,
        supervision_level_names=US_ID_SUPERVISION_LEVEL_NAMES,
    )
