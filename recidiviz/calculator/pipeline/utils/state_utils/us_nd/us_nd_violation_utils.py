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
"""Utils for state-specific calculations related to violations for US_ND."""
import datetime
from collections import defaultdict
from typing import List, Dict, Set

import attr

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


def us_nd_filter_violation_responses(
    violation_responses: List[StateSupervisionViolationResponse],
) -> List[StateSupervisionViolationResponse]:
    """Filters out VIOLATION_REPORT responses that are not of type INI (Initial) or ITR (Inter district)."""
    filtered_responses = [
        response
        for response in violation_responses
        if response.response_type
        == StateSupervisionViolationResponseType.PERMANENT_DECISION
    ]

    return filtered_responses


def us_nd_prepare_violation_responses_for_calculations(
    violation_responses: List[StateSupervisionViolationResponse],
) -> List[StateSupervisionViolationResponse]:
    """Performs required pre-processing on US_ND StateSupervisionViolationResponses.

    We only learn about violations in US_ND when a supervision period has been
    terminated. Since there are overlapping SPs in US_ND, there may be multiple SPs
    terminated on the same day, each indicating the violation_type that caused the
    termination. To avoid over-counting violation responses for US_ND, we collapse
    all violation types listed on the violations for responses with the same
    response_date.
    """
    bucketed_responses_by_date: Dict[
        datetime.date, List[StateSupervisionViolationResponse]
    ] = defaultdict(list)
    for response in violation_responses:
        if not response.response_date:
            continue
        bucketed_responses_by_date[response.response_date].append(response)

    deduped_responses_by_date: Dict[
        datetime.date, StateSupervisionViolationResponse
    ] = {}
    for response_date, responses in bucketed_responses_by_date.items():
        updated_response = responses[0]
        seen_violation_types: Set[StateSupervisionViolationType] = set()
        deduped_violation_type_entries: List[StateSupervisionViolationTypeEntry] = []
        for response in responses:
            if response.supervision_violation is None:
                raise ValueError(
                    f"Supervision violation for response "
                    f"[{response.supervision_violation_response_id}] should not be None."
                    f"We expect all StateSupervisionViolationResponse entities in"
                    f"US_ND to have a set StateSupervisionViolation."
                )
            for (
                violation_type_entry
            ) in response.supervision_violation.supervision_violation_types:
                if violation_type_entry.violation_type is None:
                    continue
                if violation_type_entry.violation_type not in seen_violation_types:
                    deduped_violation_type_entries.append(violation_type_entry)
                    seen_violation_types.add(violation_type_entry.violation_type)

        updated_response.supervision_violation = attr.evolve(
            updated_response.supervision_violation,
            supervision_violation_types=list(deduped_violation_type_entries),
        )
        deduped_responses_by_date[response_date] = updated_response

    return list(deduped_responses_by_date.values())
