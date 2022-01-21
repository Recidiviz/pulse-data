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
"""Utils for state-specific normalization logic related to violations in US_MO."""

from typing import List

from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violations_delegate import (
    LAW_CITATION_SUBTYPE_STR,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolationResponse,
    StateSupervisionViolationTypeEntry,
)

_LAW_CONDITION_STR = "LAW"


class UsMoViolationResponseNormalizationDelegate(
    StateSpecificViolationResponseNormalizationDelegate
):
    """US_MO implementation of the
    StateSpecificViolationResponseNormalizationDelegate."""

    def get_additional_violation_types_for_response(
        self,
        response: StateSupervisionViolationResponse,
    ) -> List[StateSupervisionViolationTypeEntry]:
        """Responses of type CITATION in US_MO do not have violation types on their
        violations, so the violation types and conditions violated on these
        violations are updated. If a citation has an associated violation, then we
        add TECHNICAL to the list of supervision_violation_types so that the citation
        will get classified as a technical violation."""
        if (
            response.response_type == StateSupervisionViolationResponseType.CITATION
            and response.supervision_violation
        ):
            supervision_violation = response.supervision_violation

            if not supervision_violation.supervision_violation_types:
                technical_entry = StateSupervisionViolationTypeEntry(
                    state_code=response.state_code,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text=None,
                )
                return [technical_entry]
        return []

    def update_condition(
        self,
        response: StateSupervisionViolationResponse,
        condition_entry: StateSupervisionViolatedConditionEntry,
    ) -> StateSupervisionViolatedConditionEntry:
        """If the response is of type CITATION and the condition is
        the LAW_CONDITION_STR, updates the condition to instead be the
        _LAW_CITATION_SUBTYPE_STR string so that we can track law citations
        independently from other violation reports with LAW conditions on them.

        For responses that are not of type CITATION, does nothing to the condition."""

        if response.supervision_violation:
            if (
                response.response_type == StateSupervisionViolationResponseType.CITATION
                and condition_entry.condition == _LAW_CONDITION_STR
            ):
                condition_entry.condition = LAW_CITATION_SUBTYPE_STR
        return condition_entry
