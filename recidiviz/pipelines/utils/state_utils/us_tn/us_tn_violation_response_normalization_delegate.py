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
"""Contains US_TN implementation of the StateSpecificViolationResponseNormalizationDelegate."""
import datetime
import re
from typing import List, Optional

from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.persistence.entity.normalized_entities_utils import (
    update_entity_with_globally_unique_id,
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


class UsTnViolationResponseNormalizationDelegate(
    StateSpecificViolationResponseNormalizationDelegate
):
    """US_TN implementation of the StateSpecificViolationResponseNormalizationDelegate."""

    def __init__(self, incarceration_periods: List[StateIncarcerationPeriod]):
        self._incarceration_periods = incarceration_periods

    def get_additional_violation_types_for_response(
        self,
        person_id: int,
        response: StateSupervisionViolationResponse,
        violation_response_index: int,
        sorted_violation_responses: Optional[List[StateSupervisionViolationResponse]],
    ) -> List[StateSupervisionViolationTypeEntry]:
        """Responses to Inferred Violations in US_TN do not have a violation type associated to them at ingest
        since we have to infer that the violations ever happened based on the VRPT or VWAR being recorded in
        Contact Notes. Because of this, we can only assume that an inferred violation has a certain type by
        looking forward to incarceration period admission reasons of VIOLT (for TECHNICAL) or VIOLW
        (for LAW) to confirm what type of violation preceded the recorded response."""

        if response.external_id is None:
            return None

        is_inferred_violation = re.search(r"INFERRED", response.external_id)

        # We should not have any violations come through without a response date.
        if not response.response_date:
            return []

        # First, we check to only continue inference for INFERRED violations
        if is_inferred_violation:
            if not sorted_violation_responses:
                return []

            # determine the date of the next probation violation response if one exists, else set as date max
            next_response_date = next(
                (
                    response.response_date
                    for response in sorted_violation_responses[
                        violation_response_index + 1 :
                    ]
                    if response.external_id and response.response_date
                ),
                datetime.date.max,
            )

            # identify any incarceration periods that start between the current violation response date
            # and the next violation response date that have an admission reason of with VIOLT or VIOLW movement reason.

            sorted_incarceration_periods = standard_date_sort_for_incarceration_periods(
                self._incarceration_periods
            )

            for incarceration_period in sorted_incarceration_periods:
                if (
                    incarceration_period.admission_date
                    and incarceration_period.admission_reason_raw_text
                ):
                    if response.response_date >= incarceration_period.admission_date:
                        continue
                    if (
                        incarceration_period.admission_date >= next_response_date
                        or (
                            incarceration_period.admission_date - response.response_date
                        ).days
                        >= 180
                    ):
                        break

                    # for IPs with VIOLT movement reasons that follow a violation, add violation type of TECHNICAL
                    if "VIOLT" in incarceration_period.admission_reason_raw_text:
                        technical_entry = StateSupervisionViolationTypeEntry(
                            state_code=response.state_code,
                            violation_type=StateSupervisionViolationType.TECHNICAL,
                            violation_type_raw_text=None,
                            supervision_violation=response.supervision_violation,
                        )

                        # Add a unique id value to the new violation type entry
                        update_entity_with_globally_unique_id(
                            root_entity_id=person_id, entity=technical_entry
                        )

                        return [technical_entry]

                    # for IPs with VIOLW movement reasons that follow a violation, add violation type of LAW
                    if "VIOLW" in incarceration_period.admission_reason_raw_text:
                        law_entry = StateSupervisionViolationTypeEntry(
                            state_code=response.state_code,
                            violation_type=StateSupervisionViolationType.LAW,
                            violation_type_raw_text=None,
                            supervision_violation=response.supervision_violation,
                        )

                        # Add a unique id value to the new violation type entry
                        update_entity_with_globally_unique_id(
                            root_entity_id=person_id, entity=law_entry
                        )

                        return [law_entry]

        return []
