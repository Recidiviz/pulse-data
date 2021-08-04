# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utils for state-specific calculations related to violations for US_MO."""
from typing import List

# TODO(#8106): Delete these imports before closing this task
# pylint: disable=protected-access
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violations_delegate import (
    _LAW_CITATION_SUBTYPE_STR,
    _LAW_CONDITION_STR,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolationResponse,
    StateSupervisionViolationTypeEntry,
)


def us_mo_prepare_violation_responses_for_calculations(
    violation_responses: List[StateSupervisionViolationResponse],
) -> List[StateSupervisionViolationResponse]:
    """Performs required pre-processing on US_MO StateSupervisionViolationResponses."""
    return [
        _normalize_violations_on_responses(response) for response in violation_responses
    ]


def _normalize_violations_on_responses(
    response: StateSupervisionViolationResponse,
) -> StateSupervisionViolationResponse:
    """For responses that are not of type CITATION or don't have an associated violation, does nothing. Responses of
    type CITATION do not have violation types on their violations, so the violation types and conditions violated on
    these violations are updated."""
    if not response.state_code or response.state_code.upper() != "US_MO":
        raise ValueError(
            f"Calling state-specific US_MO helper function for a response from {response.state_code}"
        )

    # Citations in US_MO don't have violation types on them. However, we want to classify them as TECHNICAL.
    # If this is a CITATION and there's a violation on it, add TECHNICAL to the list of violation types.
    if (
        response.response_type == StateSupervisionViolationResponseType.CITATION
        and response.supervision_violation
    ):
        supervision_violation = response.supervision_violation

        if not supervision_violation.supervision_violation_types:
            supervision_violation.supervision_violation_types.append(
                StateSupervisionViolationTypeEntry(
                    state_code=response.state_code,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text=None,
                )
            )

        # Update citations that list the _LAW_CONDITION_STR as a condition to instead list _LAW_CITATION_SUBTYPE_STR
        # so we can track law citations independently from other violations with LAW conditions on them
        for condition_entry in supervision_violation.supervision_violated_conditions:
            if condition_entry.condition == _LAW_CONDITION_STR:
                condition_entry.condition = _LAW_CITATION_SUBTYPE_STR

    return response
