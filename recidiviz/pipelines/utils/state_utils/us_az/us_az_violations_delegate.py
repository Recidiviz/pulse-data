# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Contains US_AZ implementation of the StateSpecificViolationDelegate."""


from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)


class UsAzViolationDelegate(StateSpecificViolationDelegate):
    """US_AZ implementation of the StateSpecificViolationDelegate."""

    def should_include_response_in_violation_history(
        self,
        response: NormalizedStateSupervisionViolationResponse,
        include_follow_up_responses: bool = False,
    ) -> bool:
        """For US_AZ, we include all responses of type CITATION, VIOLATION_REPORT and PERMANENT_DECISION responses to
        be included in the violation history.
        """
        return response.response_type in (
            StateSupervisionViolationResponseType.VIOLATION_REPORT,
            StateSupervisionViolationResponseType.CITATION,
            StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )
