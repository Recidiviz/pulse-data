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
"""US_MO specific enum helper methods.

TODO(#8899): This file should become empty and be deleted when we have fully migrated
 this state to new ingest mappings version.
"""
from typing import Dict, List, Optional, Set

from more_itertools import one

from recidiviz.common.constants.entity_enum import EnumParsingError
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.str_field_utils import sorted_list_from_str
from recidiviz.ingest.direct.regions.us_mo.us_mo_custom_enum_parsers import (
    STR_TO_INCARCERATION_PERIOD_ADMISSION_REASON_MAPPINGS,
    rank_incarceration_period_admission_reason_status_str,
)


def incarceration_period_admission_reason_mapper(
    status_list_str: str,
) -> StateIncarcerationPeriodAdmissionReason:
    """Converts a string with a list of TAK026 MO status codes into a valid incarceration period admission reason."""
    start_statuses = sorted_list_from_str(status_list_str, " ")

    ranked_status_map: Dict[int, List[str]] = {}

    # First rank all statuses individually
    for status_str in start_statuses:
        status_rank = rank_incarceration_period_admission_reason_status_str(status_str)
        if status_rank is None:
            # If None, this is not an status code for determining the admission status
            continue
        if status_rank not in ranked_status_map:
            ranked_status_map[status_rank] = []
        ranked_status_map[status_rank].append(status_str)

    if not ranked_status_map:
        # None of the statuses can meaningfully tell us what the admission reason is (rare)
        return StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN

    # Find the highest order status(es) and use those to determine the admission reason
    highest_rank = sorted(list(ranked_status_map.keys()))[0]
    statuses_at_rank = ranked_status_map[highest_rank]

    potential_admission_reasons: Set[StateIncarcerationPeriodAdmissionReason] = set()
    for status_str in statuses_at_rank:
        if status_str not in STR_TO_INCARCERATION_PERIOD_ADMISSION_REASON_MAPPINGS:
            raise ValueError(
                f"No mapping for incarceration admission status {status_str}"
            )
        potential_admission_reasons.add(
            STR_TO_INCARCERATION_PERIOD_ADMISSION_REASON_MAPPINGS[status_str]
        )

    if len(potential_admission_reasons) > 1:
        raise EnumParsingError(
            StateIncarcerationPeriodAdmissionReason,
            f"Found status codes with conflicting information: [{statuses_at_rank}], which evaluate to "
            f"[{potential_admission_reasons}]",
        )

    return one(potential_admission_reasons)


def supervising_officer_mapper(label: str) -> Optional[StateAgentType]:
    """Maps |label|, a MO specific job title, to its corresponding StateAgentType."""
    if not label:
        return None

    if ("PROBATION" in label and "PAROLE" in label) or "P P" in label:
        return StateAgentType.SUPERVISION_OFFICER
    return StateAgentType.INTERNAL_UNKNOWN
