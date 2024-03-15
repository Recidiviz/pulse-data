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
"""Contains US_TN implementation of the StateSpecificIncarcerationNormalizationDelegate."""
from datetime import timedelta
from typing import List, Optional

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import DateRange
from recidiviz.persistence.entity.normalized_entities_utils import (
    update_normalized_entity_with_globally_unique_id,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.pipelines.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)


class UsTnIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_TN implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    def normalize_period_if_commitment_from_supervision(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        original_sorted_incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: NormalizedSupervisionPeriodIndex,
    ) -> StateIncarcerationPeriod:
        return _us_tn_normalize_period_if_commitment_from_supervision(
            incarceration_period_list_index=incarceration_period_list_index,
            sorted_incarceration_periods=sorted_incarceration_periods,
            supervision_period_index=supervision_period_index,
        )

    def period_is_parole_board_hold(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
    ) -> bool:
        """There are no parole board hold incarceration periods in US_TN."""
        # TODO(#10294): It's unclear whether there are IPs in TN that represent time
        #  spent in a parole board hold. We need to get more information from US_TN,
        #  and then update this logic accordingly to classify the parole board periods
        #  if they do exist.
        return False

    def get_incarceration_admission_violation_type(
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> Optional[StateSupervisionViolationType]:
        """TN specific implementation of get_incarceration_admission_violation_type
        that returns StateSupervisionViolationType.TECHNICAL or StateSupervisionViolationType.LAW
        depending on admission reason raw text. If admission reason raw text does not indicate
        this is a VIOLT or VIOLW admission, we return None
        """

        if incarceration_period.admission_reason_raw_text is None:
            return None

        # Movement reasons that indicate technical revocation in TN use
        # MovementReason = VIOLT which is defined as VIOLATION WARRANT-TECHNICAL

        if incarceration_period.admission_reason_raw_text.endswith("VIOLT"):
            return StateSupervisionViolationType.TECHNICAL

        # Movement reasons that indicate warrant issued  in TN use
        # MovementReason = VIOLW which is defined as Warrant violation (new charge)

        if incarceration_period.admission_reason_raw_text.endswith("VIOLW"):
            return StateSupervisionViolationType.LAW

        return None

    def infer_additional_periods(
        self,
        person_id: int,
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: NormalizedSupervisionPeriodIndex,
    ) -> List[StateIncarcerationPeriod]:
        return _us_tn_infer_additional_periods(
            person_id=person_id,
            incarceration_periods=incarceration_periods,
            supervision_period_index=supervision_period_index,
        )


RELEASED_FROM_TEMPORARY_CUSTODY_RAW_TEXT_VALUES: List[str] = [
    # Revocations are logged after temporary custody due to a violation.
    "PAFA-PAVOK",  # Parole Revoked
    "PAFA-REVOK",  # Revocation
    "PRFA-PRVOK",  # Probation revoked
    "PRFA-PTVOK",  # Partial revocation
    "CCFA-REVOK",  # Revocation
    "CCFA-PTVOK",  # Partial revocation
    "PAFA-RECIS",  # Rescission
    "DVCT-PRVOK",  # Revocation
]


def _us_tn_normalize_period_if_commitment_from_supervision(
    incarceration_period_list_index: int,
    sorted_incarceration_periods: List[StateIncarcerationPeriod],
    supervision_period_index: NormalizedSupervisionPeriodIndex,
) -> StateIncarcerationPeriod:
    """Returns an updated version of the specified incarceration period if it is a
    commitment from supervision admission.

    For US_TN, commitments from supervision occur in the following circumstances:

    If the period represents an admission from XXX supervision, changes the NEW_ADMISSION admission_reason
    to be TEMPORARY CUSTODY.
    """
    incarceration_period = sorted_incarceration_periods[incarceration_period_list_index]

    if (
        incarceration_period.admission_reason
        == StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION
    ):
        if not incarceration_period.admission_date:
            raise ValueError(
                "Unexpected missing admission_date on incarceration period: "
                f"[{incarceration_period}]"
            )

        # Find most relevant pre- (or overlapping) commitment supervision period
        pre_commitment_supervision_period = (
            supervision_period_index.get_supervision_period_overlapping_with_date_range(
                date_range=DateRange(
                    incarceration_period.admission_date - timedelta(days=1),
                    incarceration_period.admission_date,
                )
            )
        )

        # Confirm that there is an overlapping or abutting supervision period
        if pre_commitment_supervision_period:
            # There is a supervision period that abuts or overlaps with this NEW_ADMISSION incarceration period
            # so this is actually a TEMPORARY CUSTODY period, not a NEW ADMISSION.
            incarceration_period.admission_reason = (
                StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY
            )
            incarceration_period.specialized_purpose_for_incarceration = (
                StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY
            )

            if (
                incarceration_period.release_reason
                == StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION
                or incarceration_period.release_reason_raw_text
                in RELEASED_FROM_TEMPORARY_CUSTODY_RAW_TEXT_VALUES
            ):
                incarceration_period.release_reason = (
                    StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
                )

    return incarceration_period


def _us_tn_infer_additional_periods(
    person_id: int,
    incarceration_periods: List[StateIncarcerationPeriod],
    supervision_period_index: NormalizedSupervisionPeriodIndex,
) -> List[StateIncarcerationPeriod]:
    """
    If we have a supervision period in TN with the supervision_level of IN_CUSTODY, we want to infer an
    incarceration_period for that time in order to begin sessions at the correct incarceration start.
    """

    # Infer a temporary custody incarceration period if supervision level is IN_CUSTODY
    if supervision_period_index:
        for sp in supervision_period_index.sorted_supervision_periods:
            if sp.supervision_level == StateSupervisionLevel.IN_CUSTODY:

                inference_reason = "IN-CUSTODY"

                if sp.termination_date:
                    # If the SP has a termination date, we set the new inferred IP with that termination date.
                    new_incarceration_period = StateIncarcerationPeriod(
                        state_code=StateCode.US_TN.value,
                        external_id=f"{sp.external_id}-{inference_reason}",
                        admission_date=sp.start_date,
                        admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                        release_date=sp.termination_date,
                        release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
                        custodial_authority=StateCustodialAuthority.COUNTY,
                        incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
                    )

                    # Add a unique id to the new IP
                    update_normalized_entity_with_globally_unique_id(
                        person_id=person_id,
                        entity=new_incarceration_period,
                        state_code=StateCode.US_TN,
                    )

                    incarceration_periods.append(new_incarceration_period)

                    continue

                # If the SP does not have a termination date (meaning it is a current open period), we set only the admission date.
                new_incarceration_period = StateIncarcerationPeriod(
                    state_code=StateCode.US_TN.value,
                    external_id=f"{sp.external_id}-{inference_reason}",
                    admission_date=sp.start_date,
                    admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                    custodial_authority=StateCustodialAuthority.COUNTY,
                    incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
                )

                # Add a unique id to the new IP
                update_normalized_entity_with_globally_unique_id(
                    person_id=person_id,
                    entity=new_incarceration_period,
                    state_code=StateCode.US_TN,
                )

                incarceration_periods.append(new_incarceration_period)

    return incarceration_periods
