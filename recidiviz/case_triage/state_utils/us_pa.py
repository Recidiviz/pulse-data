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
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_compliance import (
    CURRENT_US_PA_ASSESSMENT_SCORE_RANGE,
    SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS,
    SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS,
)
from recidiviz.case_triage.state_utils.types import PolicyRequirements
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)

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
