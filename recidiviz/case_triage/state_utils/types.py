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
"""Implements shared types for state utils."""
from typing import Any, Dict, Optional, Tuple

import attr

from recidiviz.case_triage.opportunities.types import OpportunityType
from recidiviz.common.constants.person_characteristics import Gender
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)


@attr.s(auto_attribs=True)
class PolicyRequirements:
    """Implements policy requirements data class used to convey requirements for a state."""

    # Mapping from gender -> supervision level -> ranges of assessment scores for those folks.
    # If the second element of the range is None, then it is unbounded.
    assessment_score_cutoffs: Dict[
        Gender, Dict[StateSupervisionLevel, Tuple[int, Optional[int]]]
    ]

    # Abbreviated name of the state's DOC
    doc_short_name: str

    # Name of OMS used by the state
    oms_name: str

    # Map from opportunity type -> external policy document reference
    policy_references_for_opportunities: Dict[OpportunityType, str]

    # Mapping from case type -> supervision level -> (days per period, length of period for supervision contact)
    supervision_contact_frequencies: Dict[
        StateSupervisionCaseType, Dict[StateSupervisionLevel, Tuple[int, int]]
    ]

    # Mapping from state supervision level -> home visit contact frequencies
    supervision_home_visit_frequencies: Dict[StateSupervisionLevel, Tuple[int, int]]

    # Mapping from state supervision level -> how they are named by the state
    supervision_level_names: Dict[StateSupervisionLevel, str]

    # External reference to policy doc describing supervision policies
    supervision_policy_reference: Optional[str]

    def get_supervision_level_name(
        self, supervision_level: StateSupervisionLevel
    ) -> str:
        return self.supervision_level_names.get(
            supervision_level, supervision_level.value
        )

    def to_json(self) -> Dict[str, Any]:
        assessment_score_cutoff_dict = {
            gender.value: {
                level.value: score_range for level, score_range in sub_dict.items()
            }
            for gender, sub_dict in self.assessment_score_cutoffs.items()
        }

        policy_references_dict = {
            opportunity_type.value: url
            for opportunity_type, url in self.policy_references_for_opportunities.items()
        }

        supervision_contact_dict = {
            case_type.value: {level.value: period for level, period in sub_dict.items()}
            for case_type, sub_dict in self.supervision_contact_frequencies.items()
        }

        supervision_level_names = {
            level.value: text for level, text in self.supervision_level_names.items()
        }

        supervision_home_visit_dict = {
            level.value: period
            for level, period in self.supervision_home_visit_frequencies.items()
        }

        return {
            "assessmentScoreCutoffs": assessment_score_cutoff_dict,
            "docShortName": self.doc_short_name,
            "omsName": self.oms_name,
            "policyReferencesForOpportunities": policy_references_dict,
            "supervisionContactFrequencies": supervision_contact_dict,
            "supervisionLevelNames": supervision_level_names,
            "supervisionHomeVisitFrequencies": supervision_home_visit_dict,
            "supervisionPolicyReference": self.supervision_policy_reference,
        }
