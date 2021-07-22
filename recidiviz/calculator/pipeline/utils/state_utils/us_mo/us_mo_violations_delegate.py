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
"""Utils for state-specific logic related to violations
in US_MO."""

from typing import Optional

from recidiviz.calculator.pipeline.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolationResponse,
)

FOLLOW_UP_RESPONSE_SUBTYPE = "SUP"


class UsMoViolationDelegate(StateSpecificViolationDelegate):
    """US_MO implementation of the StateSpecificViolationDelegate."""

    def should_include_response_in_violation_history(
        self,
        response: StateSupervisionViolationResponse,
        include_follow_up_responses: bool = False,
    ) -> bool:
        """For US_MO, we include all responses of type CITATION, and certain VIOLATION_REPORT responses depending on
        the report subtype and whether follow-up responses should be included in the violation history."""
        return (
            response.response_type == StateSupervisionViolationResponseType.CITATION
            or (
                response.response_type
                == StateSupervisionViolationResponseType.VIOLATION_REPORT
                and _get_violation_report_subtype_should_be_included_in_calculations(
                    response.response_subtype, include_follow_up_responses
                )
            )
        )


def _get_violation_report_subtype_should_be_included_in_calculations(
    response_subtype: Optional[str], include_follow_up_responses: bool
) -> bool:
    """Returns whether a VIOLATION_REPORT with the given response_subtype should be included in calculations."""
    if response_subtype in ["INI", "ITR"]:
        return True
    if response_subtype == FOLLOW_UP_RESPONSE_SUBTYPE:
        return include_follow_up_responses
    if response_subtype is None or response_subtype in ["HOF", "MOS", "ORI"]:
        return False

    raise ValueError(
        f"Unexpected violation response subtype {response_subtype} for a US_MO VIOLATION_REPORT."
    )
