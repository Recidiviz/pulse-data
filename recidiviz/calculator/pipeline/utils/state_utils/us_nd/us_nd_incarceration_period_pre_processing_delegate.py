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
"""Contains state-specific logic for certain aspects of pre-processing US_ND
StateIncarcerationPeriod entities so that they are ready to be used in pipeline
calculations."""
from typing import Dict, List, Optional

import attr

from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    StateSpecificIncarcerationPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.incarceration_period_utils import (
    incarceration_periods_with_admissions_between_dates,
    periods_are_temporally_adjacent,
)
from recidiviz.calculator.pipeline.utils.period_utils import (
    find_last_terminated_period_before_date,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateSpecializedPurposeForIncarceration,
    is_commitment_from_supervision,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)

# The number of months for the window of time prior to a new admission return to search
# for a previous probation supervision period that ended due to revocation to which we
# may attribute this commitment from supervision
_NEW_ADMISSION_PROBATION_COMMITMENT_LOOKBACK_MONTHS = 24

# In ND we use incarceration admission reason raw texts to identify an associated prior supervision type. In some
# instances, we need to update the admission reason raw text to the correct value. To facilitate identifying when a
# raw prefix was modified, prepend a prefix.
PROBATION_REVOCATION_PREPROCESSING_PREFIX = "US_ND_PREPROC_PROBATION_REVOCATION"
PAROLE_REVOCATION_PREPROCESSING_PREFIX = "US_ND_PREPROC_PAROLE_REVOCATION"


# Maps commitment from supervision admission reasons to the corresponding supervision
# type of the period that preceded the admission, as inferred from the admission reason raw text
PREVIOUS_SUPERVISION_TYPE_TO_INCARCERATION_ADMISSION_REASON_RAW_TEXT: Dict[
    str, StateSupervisionPeriodSupervisionType
] = {
    "PARL": StateSupervisionPeriodSupervisionType.PAROLE,
    "PV": StateSupervisionPeriodSupervisionType.PAROLE,
    "NPRB": StateSupervisionPeriodSupervisionType.PROBATION,
    "NPROB": StateSupervisionPeriodSupervisionType.PROBATION,
    "RPRB": StateSupervisionPeriodSupervisionType.PROBATION,
    "PRB": StateSupervisionPeriodSupervisionType.PROBATION,
    # The following are prefixes set in pre-processing. The admission reason raw texts
    # may be prefixed with one of the following, indicating which type of supervision
    # was associated with the revocation admission.
    PROBATION_REVOCATION_PREPROCESSING_PREFIX: StateSupervisionPeriodSupervisionType.PROBATION,
    PAROLE_REVOCATION_PREPROCESSING_PREFIX: StateSupervisionPeriodSupervisionType.PAROLE,
}


class UsNdIncarcerationPreProcessingDelegate(
    StateSpecificIncarcerationPreProcessingDelegate
):
    """US_ND implementation of the StateSpecificIncarcerationPreProcessingDelegate."""

    def normalize_period_if_commitment_from_supervision(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        original_sorted_incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: Optional[PreProcessedSupervisionPeriodIndex],
    ) -> StateIncarcerationPeriod:
        return _us_nd_normalize_period_if_commitment_from_supervision(
            incarceration_period_list_index=incarceration_period_list_index,
            sorted_incarceration_periods=sorted_incarceration_periods,
            original_sorted_incarceration_periods=original_sorted_incarceration_periods,
            supervision_period_index=supervision_period_index,
        )

    def handle_erroneously_set_temporary_custody_period(
        self,
        incarceration_period: StateIncarcerationPeriod,
        previous_incarceration_period: Optional[StateIncarcerationPeriod],
    ) -> StateIncarcerationPeriod:
        """Updates periods that were erroneously cast as temporary custody during
        ingest. We don't consider temporary custody holds as actual holds if they follow
        consecutively within 2 days after a period in a state prison, so we update
        the custodial authority and pfi values accordingly.
        """
        if (
            incarceration_period.specialized_purpose_for_incarceration
            != StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY
        ):
            raise ValueError(
                "Function should only be called on IPs with a pfi value "
                f"of TEMPORARY_CUSTODY. Found: {incarceration_period}."
            )

        if (
            previous_incarceration_period
            and previous_incarceration_period.incarceration_type
            == StateIncarcerationType.STATE_PRISON
            and periods_are_temporally_adjacent(
                previous_incarceration_period,
                incarceration_period,
                # In US_ND periods that start/end within 2 days of each other are still
                # considered consecutive, as we expect that data to still represent
                # one, same-day movement.
                valid_adjacency_threshold_override=2,
            )
        ):
            # We don't consider temporary custody holds as actual holds if they follow
            # consecutively within 2 days after a period in a state prison. If a
            # significant period of time (>2 days) has passed after a state prison
            # stay, then it can be considered a valid temporary custody hold.
            return attr.evolve(
                incarceration_period,
                custodial_authority=StateCustodialAuthority.STATE_PRISON,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            )

        return incarceration_period

    def period_is_parole_board_hold(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
    ) -> bool:
        """There are no parole board hold incarceration periods in US_ND."""
        if super().period_is_parole_board_hold(
            incarceration_period_list_index, sorted_incarceration_periods
        ):
            raise ValueError(
                "Unexpected "
                "StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD "
                "value in US_ND. We do not expect any parole board hold "
                "periods for this state."
            )
        return False

    def period_is_non_board_hold_temporary_custody(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
    ) -> bool:
        """Periods of temporary custody have a pfi value of TEMPORARY_CUSTODY in
        US_ND."""
        incarceration_period = sorted_incarceration_periods[
            incarceration_period_list_index
        ]

        return (
            incarceration_period.specialized_purpose_for_incarceration
            == StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY
        )

    def pre_processing_relies_on_supervision_periods(self) -> bool:
        """The apply_commitment_from_supervision_period_overrides function for US_ND
        relies on supervision period entities."""
        return True


def _us_nd_normalize_period_if_commitment_from_supervision(
    incarceration_period_list_index: int,
    sorted_incarceration_periods: List[StateIncarcerationPeriod],
    original_sorted_incarceration_periods: List[StateIncarcerationPeriod],
    supervision_period_index: Optional[PreProcessedSupervisionPeriodIndex],
) -> StateIncarcerationPeriod:
    """Returns an updated version of the specified incarceration period if it is a
    commitment from supervision admission.

    Updates the admission_reason_raw_text with a probation or parole specific identifier prefix
    Updates the admission_reason to be a REVOCATION for the following scenarios:
        - The person was admitted to a state prison due to a NEW_ADMISSION (not a period
            of TEMPORARY_CUSTODY) and was previously in a PAROLE or PROBATION
            supervision period that terminated due to a REVOCATION.

            Also note that in this case, we also ensure that there was not an
            intermediate period of incarceration in a state prison between the
            supervision REVOCATION and this incarceration period under examination,
            to make sure we do not mistakenly re-classify what is truly a NEW_ADMISSION
            as a REVOCATION.

        - The person has a TRANSFER admission into a GENERAL incarceration period
            after adjacent period(s) of TEMPORARY_CUSTODY.
    """
    if supervision_period_index is None:
        raise ValueError(
            "IP pre-processing relies on supervision periods for US_ND. "
            "Expected non-null supervision_period_index."
        )
    if len(sorted_incarceration_periods) != len(original_sorted_incarceration_periods):
        raise ValueError(
            f"Expected lengths of mid-processing and original periods to be equal. "
            f"Found [{len(sorted_incarceration_periods)}] != "
            f"[{len(original_sorted_incarceration_periods)}]."
        )

    incarceration_period = sorted_incarceration_periods[incarceration_period_list_index]

    admission_date = incarceration_period.admission_date

    if not admission_date:
        raise ValueError(f"Admission date for null for {incarceration_period}")

    admission_reason = incarceration_period.admission_reason
    if admission_reason == StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION:
        most_recent_supervision_period = find_last_terminated_period_before_date(
            upper_bound_date=admission_date,
            periods=supervision_period_index.supervision_periods,
            maximum_months_proximity=_NEW_ADMISSION_PROBATION_COMMITMENT_LOOKBACK_MONTHS,
        )

        if (
            most_recent_supervision_period
            and most_recent_supervision_period.termination_reason
            == StateSupervisionPeriodTerminationReason.REVOCATION
            and most_recent_supervision_period.supervision_type
            in (
                StateSupervisionPeriodSupervisionType.PAROLE,
                StateSupervisionPeriodSupervisionType.PROBATION,
            )
        ):
            return _updated_ip_after_revoked_sp(
                most_recent_supervision_period,
                incarceration_period,
                sorted_incarceration_periods,
            )

    if (
        admission_reason
        in (
            StateIncarcerationPeriodAdmissionReason.TRANSFER,
            StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
        )
        and incarceration_period.specialized_purpose_for_incarceration
        == StateSpecializedPurposeForIncarceration.GENERAL
    ):
        earliest_temp_custody_admission = (
            _get_earliest_adjacent_original_temp_custody_period(
                incarceration_period_list_index,
                original_sorted_incarceration_periods,
                sorted_incarceration_periods,
            )
        )

        # Update the incarceration period to have the admission reason enum value
        # associated with the raw text on the earliest adjacent temporary custody
        # period.
        if earliest_temp_custody_admission:
            return _update_ip_after_temp_custody(
                incarceration_period,
                earliest_temp_custody_admission,
            )

    # This period does not require any updated values
    return incarceration_period


def _get_earliest_adjacent_original_temp_custody_period(
    incarceration_period_list_index: int,
    original_sorted_incarceration_periods: List[StateIncarcerationPeriod],
    mid_processing_sorted_incarceration_periods: List[StateIncarcerationPeriod],
) -> Optional[StateIncarcerationPeriod]:
    """Finds the earliest temporary custody period that is temporally adjacent to this
    incarceration period (with only temporary custody periods between), if one exists.
    Returns the corresponding period from original_sorted_incarceration_periods so that
    the period will have the original admission reason, before it was overwritten to
    TEMPORARY_CUSTODY.
    """
    earliest_adjacent_temp_custody_period = None
    possible_temp_custody_idx = incarceration_period_list_index - 1
    while possible_temp_custody_idx >= 0:
        possible_temp_custody_ip = mid_processing_sorted_incarceration_periods[
            possible_temp_custody_idx
        ]
        if (
            possible_temp_custody_ip.specialized_purpose_for_incarceration
            != StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY
        ):
            break

        subsequent_ip = mid_processing_sorted_incarceration_periods[
            possible_temp_custody_idx + 1
        ]
        if not periods_are_temporally_adjacent(possible_temp_custody_ip, subsequent_ip):
            break

        earliest_adjacent_temp_custody_period = original_sorted_incarceration_periods[
            possible_temp_custody_idx
        ]
        possible_temp_custody_idx -= 1
    return earliest_adjacent_temp_custody_period


def _updated_ip_after_revoked_sp(
    most_recent_supervision_period: StateSupervisionPeriod,
    incarceration_period: StateIncarcerationPeriod,
    sorted_incarceration_periods: List[StateIncarcerationPeriod],
) -> StateIncarcerationPeriod:
    """Returns the incarceration period with updated values, if necessary,
    for an incarceration period with a NEW_ADMISSION where the most recent
    supervision period ended in a revocation.

    If there was a separate admission to a state prison after the revocation and
    before the admission to the incarceration_period, then it the admission was not
    due to a revocation and the original incarceration period is returned.
    """
    was_intermediate_state_prison_admission = _intermediate_state_prison_admission(
        most_recent_supervision_period,
        incarceration_period,
        sorted_incarceration_periods,
    )

    # If there was a separate admission to a state prison after the revocation
    # and before this admission, then it is not accurate to describe *this*
    # admission as being due to a revocation
    if was_intermediate_state_prison_admission:
        return incarceration_period

    admission_reason = StateIncarcerationPeriodAdmissionReason.REVOCATION
    # Override the admission reason raw text, if the previous supervision type is
    # probation or parole.
    if (
        most_recent_supervision_period.supervision_type
        == StateSupervisionPeriodSupervisionType.PROBATION
    ):
        admission_reason_prefix = PROBATION_REVOCATION_PREPROCESSING_PREFIX
    else:
        admission_reason_prefix = PAROLE_REVOCATION_PREPROCESSING_PREFIX

    return attr.evolve(
        incarceration_period,
        admission_reason=admission_reason,
        # If there is an existing admission reason raw text, prefix it.
        # Otherwise, leave it as empty.
        admission_reason_raw_text=f"{admission_reason_prefix}-{incarceration_period.admission_reason_raw_text}"
        if incarceration_period.admission_reason_raw_text
        else None,
    )


def _update_ip_after_temp_custody(
    incarceration_period: StateIncarcerationPeriod,
    earliest_original_temp_custody_period: StateIncarcerationPeriod,
) -> StateIncarcerationPeriod:
    """Returns the incarceration period with updated admission_reason and,
    if necessary, admission_reason_raw_text values corresponding to the information
    in the admission_reason_raw_text on the earliest adjacent temporary custody
    period that preceded the |incarceration_period|."""

    temp_custody_ip_admission_reason = (
        earliest_original_temp_custody_period.admission_reason
    )
    temp_custody_ip_admission_reason_raw_text = (
        earliest_original_temp_custody_period.admission_reason_raw_text
    )

    if (
        not temp_custody_ip_admission_reason
        or temp_custody_ip_admission_reason
        in (
            StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
        )
        or not temp_custody_ip_admission_reason_raw_text
    ):
        return incarceration_period

    admission_reason_prefix: Optional[str] = None
    if is_commitment_from_supervision(temp_custody_ip_admission_reason):
        pre_commitment_supervision_type = (
            PREVIOUS_SUPERVISION_TYPE_TO_INCARCERATION_ADMISSION_REASON_RAW_TEXT[
                temp_custody_ip_admission_reason_raw_text
            ]
        )

        if (
            pre_commitment_supervision_type
            == StateSupervisionPeriodSupervisionType.PROBATION
        ):
            admission_reason_prefix = PROBATION_REVOCATION_PREPROCESSING_PREFIX
        else:
            admission_reason_prefix = PAROLE_REVOCATION_PREPROCESSING_PREFIX

    return attr.evolve(
        incarceration_period,
        admission_reason=temp_custody_ip_admission_reason,
        # If there is a admission_reason_prefix, add it as prefix.
        # Otherwise, leave it as the existing admission_reason_raw_text.
        admission_reason_raw_text=f"{admission_reason_prefix}-{incarceration_period.admission_reason_raw_text}"
        if admission_reason_prefix
        else incarceration_period.admission_reason_raw_text,
    )


def _intermediate_state_prison_admission(
    most_recent_supervision_period: StateSupervisionPeriod,
    incarceration_period: StateIncarcerationPeriod,
    incarceration_periods: List[StateIncarcerationPeriod],
) -> bool:
    """Returns whether or not there was an admission to a state prison after the most
    recent supervision period ended and before the given incarceration period started,
    inclusive of the supervision period termination date and exclusive of the
    incarceration period admission date."""
    start_date = most_recent_supervision_period.termination_date
    end_date = incarceration_period.admission_date

    if start_date is None or end_date is None:
        raise ValueError(
            "Expected a supervision period termination date and an incarceration period admission date at this point. "
            f"Termination date: [{start_date}]. Admission date: [{end_date}]."
        )

    intermediate_incarceration_periods = (
        incarceration_periods_with_admissions_between_dates(
            incarceration_periods, start_date, end_date
        )
    )

    return any(
        ip.incarceration_type == StateIncarcerationType.STATE_PRISON
        for ip in intermediate_incarceration_periods
    )
