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
"""Implements requirements fetcher for each state."""
from recidiviz.case_triage.exceptions import CaseTriageInvalidStateException
from recidiviz.case_triage.state_utils.types import PolicyRequirements
from recidiviz.case_triage.state_utils.us_id import us_id_policy_requirements
from recidiviz.case_triage.state_utils.us_pa import us_pa_policy_requirements
from recidiviz.common.constants.states import StateCode


def policy_requirements_for_state(state: StateCode) -> PolicyRequirements:
    if state == StateCode.US_ID:
        return us_id_policy_requirements()
    if state == StateCode.US_PA:
        return us_pa_policy_requirements()
    raise CaseTriageInvalidStateException(state.value)
