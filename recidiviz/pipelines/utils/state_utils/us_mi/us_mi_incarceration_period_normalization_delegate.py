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
"""Contains US_MI implementation of the StateSpecificIncarcerationNormalizationDelegate."""
from typing import List, Optional

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.pipelines.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalized_entities import (
    NormalizedStateIncarcerationSentence,
)
from recidiviz.pipelines.normalization.utils.normalized_entities_utils import (
    update_normalized_entity_with_globally_unique_id,
)


class UsMiIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_MI implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    def incarceration_admission_reason_override(
        self,
        incarceration_period: StateIncarcerationPeriod,
        incarceration_sentences: Optional[List[NormalizedStateIncarcerationSentence]],
    ) -> Optional[StateIncarcerationPeriodAdmissionReason]:
        if (
            incarceration_period.admission_reason
            == StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
        ):
            return StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION
        return incarceration_period.admission_reason

    def infer_additional_periods(
        self,
        person_id: int,
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """
        If we see an incarceration period that ends with release reason TEMPORARY_RELEASE
        and a subsequent incarceration period that begins with admission reason RETURN_FROM_TEMPORARY_RELEASE,
        let's infer an incarceration period for the time between those two periods.
        """

        new_incarceration_periods: List[StateIncarcerationPeriod] = []

        for index, incarceration_period in enumerate(incarceration_periods):
            new_incarceration_periods.append(incarceration_period)

            # If this IP ended because of a TEMPORARY_RELEASE and this is not the last IP for this person
            if (
                incarceration_period.release_reason
                == StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE
                and len(incarceration_periods) - 1 >= index + 1
            ):
                if incarceration_period.release_date is None:
                    raise ValueError(
                        "Unexpected null termination date for incarceration period with "
                        f"termination reason: {incarceration_period.incarceration_period_id}"
                    )

                next_incarceration_period = incarceration_periods[index + 1]

                # if the next IP for this person starts with a RETURN_FROM_TEMPORARY_RELEASE
                # and the admission date is after the release date of the previous period
                if (
                    next_incarceration_period.admission_reason
                    == StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE
                    and next_incarceration_period.admission_date
                    and next_incarceration_period.admission_date
                    > incarceration_period.release_date
                ):

                    # create a new incarceration period for this TEMPORARY RELEASE period
                    new_incarceration_period = StateIncarcerationPeriod(
                        state_code=StateCode.US_MI.value,
                        external_id=f"{incarceration_period.external_id}-2-INFERRED",
                        admission_date=incarceration_period.release_date,
                        release_date=next_incarceration_period.admission_date,
                        admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_RELEASE,
                        release_reason=StateIncarcerationPeriodReleaseReason.RETURN_FROM_TEMPORARY_RELEASE,
                        county_code=incarceration_period.county_code,
                        custodial_authority=StateCustodialAuthority.STATE_PRISON,
                        custodial_authority_raw_text=incarceration_period.custodial_authority_raw_text,
                        specialized_purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
                        specialized_purpose_for_incarceration_raw_text=incarceration_period.specialized_purpose_for_incarceration_raw_text,
                        custody_level=incarceration_period.custody_level,
                        custody_level_raw_text=incarceration_period.custody_level_raw_text,
                        incarceration_type=incarceration_period.incarceration_type,
                        incarceration_type_raw_text=incarceration_period.incarceration_type_raw_text,
                    )

                    # Add a unique id to the new IP
                    update_normalized_entity_with_globally_unique_id(
                        person_id=person_id,
                        entity=new_incarceration_period,
                    )

                    new_incarceration_periods.append(new_incarceration_period)

        return new_incarceration_periods
