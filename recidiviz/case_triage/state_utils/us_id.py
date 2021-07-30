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
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_compliance import (
    CURRENT_US_ID_ASSESSMENT_SCORE_RANGE,
    SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS,
    US_ID_SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS,
)
from recidiviz.case_triage.opportunities.types import OpportunityType
from recidiviz.case_triage.state_utils.types import PolicyRequirements
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)

US_ID_SUPERVISION_LEVEL_NAMES = {
    StateSupervisionLevel.MINIMUM: "Low",
    StateSupervisionLevel.MEDIUM: "Moderate",
    StateSupervisionLevel.HIGH: "High",
}

US_ID_POLICY_REFERENCES_FOR_OPPORTUNITIES = {
    OpportunityType.OVERDUE_DOWNGRADE: "https://forms.idoc.idaho.gov/WebLink/0/edoc/281944/Interim%20Standards%20to%20Probation%20and%20Parole%20Supervision%20Strategies.pdf",
    OpportunityType.CONTACT: "http://forms.idoc.idaho.gov/WebLink/0/edoc/281944/Interim%20Standards%20to%20Probation%20and%20Parole%20Supervision%20Strategies.pdf",
}

US_ID_SUPERVISION_POLICY_REFERENCE = "http://forms.idoc.idaho.gov/WebLink/0/edoc/281944/Interim%20Standards%20to%20Probation%20and%20Parole%20Supervision%20Strategies.pdf"


def us_id_policy_requirements() -> PolicyRequirements:
    """Returns set of policy requirements for Idaho."""
    return PolicyRequirements(
        assessment_score_cutoffs=CURRENT_US_ID_ASSESSMENT_SCORE_RANGE,
        doc_short_name="IDOC",
        oms_name="CIS",
        policy_references_for_opportunities=US_ID_POLICY_REFERENCES_FOR_OPPORTUNITIES,
        supervision_contact_frequencies=SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS,
        supervision_level_names=US_ID_SUPERVISION_LEVEL_NAMES,
        supervision_home_visit_frequencies=US_ID_SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS,
        supervision_policy_reference=US_ID_SUPERVISION_POLICY_REFERENCE,
    )
