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

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodCustodyLevel,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.normalized_entities_utils import (
    update_normalized_entity_with_globally_unique_id,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateIncarcerationSentence,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.pipelines.utils.period_utils import (
    find_last_terminated_period_on_or_before_date,
)


class UsMiIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_MI implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    _REASON_RAW_TEXT_TO_INCARCERATION_ADMISSION_VIOLATION_TYPE_MAP = {
        # Movement reasons that indicate technical revocation
        # 15 - New Commitment - Probation Technical Violator (Rec Ctr Only)
        "15": StateSupervisionViolationType.TECHNICAL,
        # 17 - Returned as Parole Technical Rule Violator
        "17": StateSupervisionViolationType.TECHNICAL,
        # Movement reasons that indicate new sentence revocation
        # 12 - New Commitment - Parole Viol. w/ New Sentence (Rec Ctr Only)
        "12": StateSupervisionViolationType.LAW,
        # 14 - New Commitment - Probationer w/ New Sentence (Rec Ctr. Only)
        "14": StateSupervisionViolationType.LAW,
    }

    def incarceration_admission_reason_override(
        self,
        incarceration_period: StateIncarcerationPeriod,
        incarceration_sentences: List[NormalizedStateIncarcerationSentence],
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
        supervision_period_index: NormalizedSupervisionPeriodIndex,
    ) -> List[StateIncarcerationPeriod]:
        """
        If we see an incarceration period that ends with release reason TEMPORARY_RELEASE
        and a subsequent incarceration period that begins with admission reason RETURN_FROM_TEMPORARY_RELEASE,
        let's infer an incarceration period for the time between those two periods.
        """

        new_incarceration_periods: List[StateIncarcerationPeriod] = []

        supervision_periods: List[NormalizedStateSupervisionPeriod] = []

        if supervision_period_index:
            # identify all supervision periods with a SUPERVISION type
            supervision_periods = [
                period
                for period in supervision_period_index.sorted_supervision_periods
                if period.supervision_type
                in (
                    StateSupervisionPeriodSupervisionType.PAROLE,
                    StateSupervisionPeriodSupervisionType.PROBATION,
                    StateSupervisionPeriodSupervisionType.DUAL,
                    StateSupervisionPeriodSupervisionType.BENCH_WARRANT,
                    StateSupervisionPeriodSupervisionType.ABSCONSION,
                )
            ]

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

                    continue

            if supervision_period_index and incarceration_period.admission_date:
                # identify the most recent supervision period with a SUPERVISION type
                most_recent_supervision_period = (
                    find_last_terminated_period_on_or_before_date(
                        upper_bound_date_inclusive=incarceration_period.admission_date,
                        periods=supervision_periods,
                        maximum_months_proximity=1,
                    )
                )

                # identify the most recent incarceration period
                most_recent_incarceration_period = (
                    incarceration_periods[index - 1] if index > 0 else None
                )

                # if there is a preceding supervision period that exists with a proper SUPERVISION type and
                # it's not directly preceding the current incarceration period
                if (
                    most_recent_supervision_period
                    and most_recent_supervision_period.termination_date
                    and most_recent_supervision_period.termination_date
                    != incarceration_period.admission_date
                ):
                    # If this IP started because of a REVOCATION or SANCTION_ADMISSION
                    if incarceration_period.admission_reason in (
                        StateIncarcerationPeriodAdmissionReason.REVOCATION,
                        StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                    ):
                        # Set the admission reason raw text for the inferred period so violation type can be added later if needed
                        admission_reason_raw_text = (
                            incarceration_period.admission_reason_raw_text
                            if incarceration_period.admission_reason_raw_text
                            in self._REASON_RAW_TEXT_TO_INCARCERATION_ADMISSION_VIOLATION_TYPE_MAP
                            else None
                        )

                        # create a new incarceration period that starts when that supervision period ended and ends when the revocation IP starts
                        # ~NOTE~ This means there might be overlapping incarceration periods if there was a preceding IP that wasn't due to revocation/sanction admission
                        new_incarceration_period = StateIncarcerationPeriod(
                            state_code=StateCode.US_MI.value,
                            external_id=f"{incarceration_period.external_id}-0-INFERRED",
                            admission_date=most_recent_supervision_period.termination_date,
                            release_date=incarceration_period.admission_date,
                            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                            admission_reason_raw_text=admission_reason_raw_text,
                            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
                            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
                            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
                            custody_level=StateIncarcerationPeriodCustodyLevel.INTERNAL_UNKNOWN,
                            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
                        )

                        # Add a unique id to the new IP
                        update_normalized_entity_with_globally_unique_id(
                            person_id=person_id,
                            entity=new_incarceration_period,
                        )

                        new_incarceration_periods.append(new_incarceration_period)

                        continue

                    # If the most recent SP ended because of a REVOCATION or ADMITTED_TO_INCARCERATION
                    # and this is first IP since the most recent SP ended
                    if most_recent_supervision_period.termination_reason in (
                        StateSupervisionPeriodTerminationReason.REVOCATION,
                        StateSupervisionPeriodTerminationReason.ADMITTED_TO_INCARCERATION,
                    ) and (
                        most_recent_incarceration_period is None
                        or (
                            most_recent_incarceration_period.release_date
                            and most_recent_supervision_period.start_date
                            and most_recent_incarceration_period.release_date
                            <= most_recent_supervision_period.termination_date
                        )
                    ):
                        # Set the admission reason raw text for the inferred period so violation type can be added later if needed
                        admission_reason_raw_text = (
                            most_recent_supervision_period.termination_reason_raw_text
                            if most_recent_supervision_period.termination_reason_raw_text
                            in self._REASON_RAW_TEXT_TO_INCARCERATION_ADMISSION_VIOLATION_TYPE_MAP
                            else None
                        )

                        # create a new incarceration period that starts when that supervision period ended and ends when the next IP starts
                        new_incarceration_period = StateIncarcerationPeriod(
                            state_code=StateCode.US_MI.value,
                            external_id=f"{incarceration_period.external_id}-0-INFERRED",
                            admission_date=most_recent_supervision_period.termination_date,
                            release_date=incarceration_period.admission_date,
                            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                            admission_reason_raw_text=admission_reason_raw_text,
                            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
                            custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
                            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
                            custody_level=StateIncarcerationPeriodCustodyLevel.INTERNAL_UNKNOWN,
                            incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
                        )

                        # Add a unique id to the new IP
                        update_normalized_entity_with_globally_unique_id(
                            person_id=person_id,
                            entity=new_incarceration_period,
                        )

                        new_incarceration_periods.append(new_incarceration_period)

                        continue

        return new_incarceration_periods

    def get_incarceration_admission_violation_type(  # pylint: disable=unused-argument
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> Optional[StateSupervisionViolationType]:
        """MI specific implementation of get_incarceration_admission_violation_type
        that returns StateSupervisionViolationType.TECHNICAL or StateSupervisionViolationType.LAW
        depending on admission reason raw text. If admission reason raw text does not indicate
        this is a revocation admission, we return None
        """

        admission_reason_raw_text = incarceration_period.admission_reason_raw_text

        if admission_reason_raw_text:
            return (
                self._REASON_RAW_TEXT_TO_INCARCERATION_ADMISSION_VIOLATION_TYPE_MAP.get(
                    admission_reason_raw_text
                )
            )

        return None
