# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Contains US_MI implementation of the StateSpecificViolationResponseNormalizationDelegate."""
import datetime
from typing import List, Optional

from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.normalized_entities_utils import (
    update_normalized_entity_with_globally_unique_id,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionViolationResponse,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
)
from recidiviz.pipelines.utils.incarceration_period_utils import (
    standard_date_sort_for_incarceration_periods,
)


class UsMiViolationResponseNormalizationDelegate(
    StateSpecificViolationResponseNormalizationDelegate
):
    """US_MI implementation of the StateSpecificViolationResponseNormalizationDelegate."""

    def __init__(self, incarceration_periods: List[StateIncarcerationPeriod]):
        self._incarceration_periods = incarceration_periods

    def get_additional_violation_types_for_response(
        self,
        person_id: int,
        response: StateSupervisionViolationResponse,
        violation_response_index: int,
        sorted_violation_responses: Optional[List[StateSupervisionViolationResponse]],
    ) -> List[StateSupervisionViolationTypeEntry]:
        """Returns the list of additional violation types that need to be added to a
        StateSupervisionViolationResponse's list of supervision_violation_types.
        For each probation supervision violation, use the subsequent incarceration
        period with a probation revocation start reason to add a supervision_violation_type
        if such an incarceration period occurs before the next probation supervision violation
        """

        if sorted_violation_responses:
            # skip adding violation types if it's not a probation supervision violation
            if not response.external_id or "PROBATION" not in response.external_id:
                return []

            # determine the date of the next probation violation response if one exists, else set as date max
            next_response_date = datetime.date.max

            while violation_response_index < len(sorted_violation_responses) - 1:
                next_response = sorted_violation_responses[violation_response_index + 1]
                if (
                    next_response.external_id
                    and "PROBATION" in next_response.external_id
                    and next_response.response_date
                ):
                    next_response_date = next_response.response_date
                    break
                violation_response_index += 1

            # identify any incarceration periods that start between the current probation violation response date
            # and the next probation violation response date that have an admission reason of probation revocation

            sorted_incarceration_periods = standard_date_sort_for_incarceration_periods(
                self._incarceration_periods
            )

            incarceration_periods_index = 0

            while incarceration_periods_index <= len(sorted_incarceration_periods) - 1:
                incarceration_period = sorted_incarceration_periods[
                    incarceration_periods_index
                ]

                if incarceration_period.admission_date and response.response_date:
                    if (
                        (incarceration_period.admission_reason_raw_text in ["14", "15"])
                        and response.response_date
                        < incarceration_period.admission_date
                        < next_response_date
                    ):
                        if incarceration_period.admission_reason_raw_text == "15":
                            technical_entry = StateSupervisionViolationTypeEntry(
                                state_code=response.state_code,
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                                violation_type_raw_text=None,
                                supervision_violation=response.supervision_violation,
                            )

                            # Add a unique id value to the new violation type entry
                            update_normalized_entity_with_globally_unique_id(
                                person_id,
                                technical_entry,
                                state_code=StateCode(response.state_code),
                            )

                            return [technical_entry]

                        law_entry = StateSupervisionViolationTypeEntry(
                            state_code=response.state_code,
                            violation_type=StateSupervisionViolationType.LAW,
                            violation_type_raw_text=None,
                            supervision_violation=response.supervision_violation,
                        )

                        # Add a unique id value to the new violation type entry
                        update_normalized_entity_with_globally_unique_id(
                            person_id,
                            law_entry,
                            state_code=StateCode(response.state_code),
                        )

                        return [law_entry]

                incarceration_periods_index += 1

        return []
