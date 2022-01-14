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
"""Utils for calculations regarding incarceration admissions that are commitments from
supervision."""
import datetime
from typing import Any, Dict, List, NamedTuple, Optional

from recidiviz.calculator.pipeline.utils.incarceration_period_utils import (
    periods_are_temporally_adjacent,
)
from recidiviz.calculator.pipeline.utils.period_utils import (
    find_last_terminated_period_on_or_before_date,
    sort_period_by_external_id,
)
from recidiviz.calculator.pipeline.utils.pre_processed_incarceration_period_index import (
    PreProcessedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_commitment_from_supervision_delegate import (
    StateSpecificCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    filter_out_unknown_supervision_type_periods,
    identify_most_severe_case_type,
    supervising_officer_and_location_info,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateSpecializedPurposeForIncarceration,
    is_commitment_from_supervision,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateSupervisionPeriod,
    StateSupervisionSentence,
)

# The number of months for the window of time prior to a commitment to
# from supervision in which we look for the associated terminated supervision
# period
SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT = 24

CommitmentDetails = NamedTuple(
    "CommitmentDetails",
    [
        (
            "purpose_for_incarceration",
            StateSpecializedPurposeForIncarceration,
        ),
        ("purpose_for_incarceration_subtype", Optional[str]),
        ("supervising_officer_external_id", Optional[str]),
        ("level_1_supervision_location_external_id", Optional[str]),
        ("level_2_supervision_location_external_id", Optional[str]),
        ("case_type", Optional[StateSupervisionCaseType]),
        ("supervision_type", StateSupervisionPeriodSupervisionType),
        ("supervision_level", Optional[StateSupervisionLevel]),
        ("supervision_level_raw_text", Optional[str]),
    ],
)


def get_commitment_from_supervision_details(
    incarceration_period: StateIncarcerationPeriod,
    incarceration_period_index: PreProcessedIncarcerationPeriodIndex,
    supervision_period_index: PreProcessedSupervisionPeriodIndex,
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    commitment_from_supervision_delegate: StateSpecificCommitmentFromSupervisionDelegate,
    supervision_delegate: StateSpecificSupervisionDelegate,
    supervision_period_to_agent_associations: Optional[Dict[int, Dict[Any, Any]]],
) -> CommitmentDetails:
    """Identifies various attributes of the commitment to incarceration from
    supervision.
    """
    supervising_officer_external_id = None
    level_1_supervision_location_external_id = None
    level_2_supervision_location_external_id = None

    pre_commitment_supervision_period = (
        _get_commitment_from_supervision_supervision_period(
            incarceration_period=incarceration_period,
            supervision_period_index=supervision_period_index,
            commitment_from_supervision_delegate=commitment_from_supervision_delegate,
            incarceration_period_index=incarceration_period_index,
        )
    )

    if pre_commitment_supervision_period and supervision_period_to_agent_associations:
        (
            supervising_officer_external_id,
            level_1_supervision_location_external_id,
            level_2_supervision_location_external_id,
        ) = supervising_officer_and_location_info(
            pre_commitment_supervision_period,
            supervision_period_to_agent_associations,
            supervision_delegate,
        )

    if not incarceration_period.specialized_purpose_for_incarceration:
        raise ValueError(
            "Unexpected incarceration period without an "
            f"specialized_purpose_for_incarceration: {incarceration_period}. Should "
            f"be set in IP pre-processing."
        )

    purpose_for_incarceration = (
        incarceration_period.specialized_purpose_for_incarceration
    )

    if not incarceration_period.incarceration_period_id:
        raise ValueError(
            "Unexpected incarceration period without an "
            f"incarceration_period_id: {incarceration_period}."
        )

    purpose_for_incarceration_subtype = incarceration_period_index.ip_id_to_pfi_subtype[
        incarceration_period.incarceration_period_id
    ]

    case_type = (
        identify_most_severe_case_type(pre_commitment_supervision_period)
        if pre_commitment_supervision_period
        else StateSupervisionCaseType.GENERAL
    )

    supervision_type = (
        commitment_from_supervision_delegate.get_commitment_from_supervision_supervision_type(
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences,
            incarceration_period=incarceration_period,
            previous_supervision_period=pre_commitment_supervision_period,
        )
        or StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    )

    supervision_level = (
        pre_commitment_supervision_period.supervision_level
        if pre_commitment_supervision_period
        else None
    )

    supervision_level_raw_text = (
        pre_commitment_supervision_period.supervision_level_raw_text
        if pre_commitment_supervision_period
        else None
    )

    commitment_details_result = CommitmentDetails(
        purpose_for_incarceration=purpose_for_incarceration,
        purpose_for_incarceration_subtype=purpose_for_incarceration_subtype,
        supervising_officer_external_id=supervising_officer_external_id,
        level_1_supervision_location_external_id=level_1_supervision_location_external_id,
        level_2_supervision_location_external_id=level_2_supervision_location_external_id,
        case_type=case_type,
        supervision_type=supervision_type,
        supervision_level=supervision_level,
        supervision_level_raw_text=supervision_level_raw_text,
    )

    return commitment_details_result


def period_is_commitment_from_supervision_admission_from_parole_board_hold(
    incarceration_period: StateIncarcerationPeriod,
    most_recent_board_hold: Optional[StateIncarcerationPeriod],
) -> bool:
    """Determines whether the incarceration_period represents a commitment from
    supervision admission after being held for a parole board hold."""
    if not most_recent_board_hold:
        # Commitments from board holds must follow a period of being in a parole
        # board hold
        return False

    if (
        most_recent_board_hold.specialized_purpose_for_incarceration
        != StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
    ):
        raise ValueError(
            "Found an IP that is not a board hold in the most_recent_board_hold arg."
        )

    return (
        # Admissions from a parole board hold should happen on the same day
        # as the release from the parole board hold
        periods_are_temporally_adjacent(
            first_incarceration_period=most_recent_board_hold,
            second_incarceration_period=incarceration_period,
        )
        and incarceration_period.admission_reason
        # Valid commitment from supervision admission reasons following a parole board
        # hold
        in (
            StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )
    )


def _filter_to_matching_supervision_types(
    admission_reason: StateIncarcerationPeriodAdmissionReason,
    supervision_type_for_admission_reason: Optional[
        StateSupervisionPeriodSupervisionType
    ],
    supervision_periods: List[StateSupervisionPeriod],
) -> List[StateSupervisionPeriod]:
    """Filters the given |supervision_periods| to ony the ones that have a
    supervision type that matches the supervision type implied in the
    |admission_reason| or |admission_reason_raw_text| (for example, filtering to only PAROLE periods if the
    |admission_reason_raw_text| is associated with a parole revocation)."""
    supervision_types_to_match: List[StateSupervisionPeriodSupervisionType]

    if (
        supervision_type_for_admission_reason
        == StateSupervisionPeriodSupervisionType.DUAL
    ):
        supervision_types_to_match = [
            StateSupervisionPeriodSupervisionType.PAROLE,
            StateSupervisionPeriodSupervisionType.PROBATION,
            StateSupervisionPeriodSupervisionType.DUAL,
        ]
    elif supervision_type_for_admission_reason:
        supervision_types_to_match = [supervision_type_for_admission_reason]
    else:
        raise ValueError(
            "This function should only be called using "
            "StateIncarcerationPeriodAdmissionReason values that can be "
            "used to filter to matching supervision types. Function "
            f"called with admission_reason: {admission_reason}."
        )

    return [
        period
        for period in supervision_periods
        if period.supervision_type in supervision_types_to_match
    ]


def _get_commitment_from_supervision_supervision_period(
    incarceration_period: StateIncarcerationPeriod,
    commitment_from_supervision_delegate: StateSpecificCommitmentFromSupervisionDelegate,
    supervision_period_index: PreProcessedSupervisionPeriodIndex,
    incarceration_period_index: PreProcessedIncarcerationPeriodIndex,
) -> Optional[StateSupervisionPeriod]:
    """Identifies the supervision period associated with the commitment to supervision
    admission on the given |admission_date|.

    If |prioritize_overlapping_periods| is True, prioritizes supervision periods that
    are overlapping with the |admission_date|. Else, prioritizes the period that has
    most recently terminated within SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT months of
    the |admission_date|.
    """
    if not supervision_period_index.supervision_periods:
        return None

    if not incarceration_period.admission_date:
        raise ValueError(
            "Unexpected missing admission_date on incarceration period: "
            f"[{incarceration_period}]"
        )
    if not incarceration_period.admission_reason:
        raise ValueError(
            "Unexpected missing admission_reason on incarceration period: "
            f"[{incarceration_period}]"
        )

    admission_date = incarceration_period.admission_date
    admission_reason = incarceration_period.admission_reason
    admission_reason_raw_text = incarceration_period.admission_reason_raw_text

    if not is_commitment_from_supervision(admission_reason):
        raise ValueError(
            "This function should only be called with an "
            "incarceration_period that is a commitment from supervision. "
            "Found an incarceration period with an admission_reason that "
            "is not a valid commitment from supervision admission: "
            f"{admission_reason}."
        )

    if not incarceration_period_index.transfers_are_collapsed:
        raise ValueError(
            "Must perform commitment from supervision calculations on IPs "
            "with collapsed transfers."
        )

    most_recent_board_hold = incarceration_period_index.most_recent_board_hold_in_index(
        incarceration_period
    )

    is_commitment_from_board_hold = (
        period_is_commitment_from_supervision_admission_from_parole_board_hold(
            incarceration_period=incarceration_period,
            most_recent_board_hold=most_recent_board_hold,
        )
    )

    if is_commitment_from_board_hold:
        if not most_recent_board_hold:
            raise ValueError(
                "This should never happen, since the determination of "
                "whether the commitment came from a board hold requires "
                "the existence of the most_recent_board_hold."
            )

        if not most_recent_board_hold.admission_date:
            raise ValueError(
                "Unexpected missing admission_date on incarceration period: "
                f"[{most_recent_board_hold}]"
            )

        # If this person was a commitment from supervision from a parole board hold,
        # then the date that they entered prison was the date of the preceding
        # incarceration period.
        admission_date = most_recent_board_hold.admission_date

    relevant_periods = _get_relevant_sps_for_pre_commitment_sp_search(
        admission_reason=admission_reason,
        admission_reason_raw_text=admission_reason_raw_text,
        supervision_periods=supervision_period_index.supervision_periods,
        commitment_from_supervision_delegate=commitment_from_supervision_delegate,
    )

    overlapping_periods = _supervision_periods_overlapping_with_date(
        admission_date, relevant_periods
    )

    # If there's more than one recently terminated period with the same
    # termination_date, prioritize the ones with REVOCATION or RETURN_TO_INCARCERATION
    # termination_reasons
    def _same_date_sort_override(
        period_a: StateSupervisionPeriod, period_b: StateSupervisionPeriod
    ) -> int:
        prioritized_termination_reasons = [
            StateSupervisionPeriodTerminationReason.REVOCATION,
            StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION,
        ]
        prioritize_a = period_a.termination_reason in prioritized_termination_reasons
        prioritize_b = period_b.termination_reason in prioritized_termination_reasons

        if prioritize_a and prioritize_b:
            return sort_period_by_external_id(period_a, period_b)
        return -1 if prioritize_a else 1

    most_recent_terminated_period = find_last_terminated_period_on_or_before_date(
        upper_bound_date_inclusive=admission_date,
        periods=relevant_periods,
        maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        same_date_sort_fn=_same_date_sort_override,
    )

    terminated_periods = (
        [most_recent_terminated_period] if most_recent_terminated_period else []
    )

    if (
        # We prioritize periods that overlap with the admission to parole boards
        # holds, because we do not expect supervision periods to have already
        # terminated on the date someone is admitted to a board hold
        is_commitment_from_board_hold
    ) or (
        admission_reason_raw_text
        in commitment_from_supervision_delegate.admission_reason_raw_texts_that_should_prioritize_overlaps_in_pre_commitment_sp_search()
    ):
        valid_pre_commitment_periods = (
            overlapping_periods if overlapping_periods else terminated_periods
        )
    else:
        valid_pre_commitment_periods = (
            terminated_periods if terminated_periods else overlapping_periods
        )

    if not valid_pre_commitment_periods:
        return None

    # In the case where there are multiple relevant SPs at this point, sort and return
    # the first one
    return min(
        valid_pre_commitment_periods,
        key=lambda e: (
            # Prioritize terminated periods with a termination_reason of REVOCATION
            # (False sorts before True)
            e.termination_reason != StateSupervisionPeriodTerminationReason.REVOCATION,
            # Prioritize termination_date closest to the admission_date
            abs(((e.termination_date or datetime.date.today()) - admission_date).days),
            # Deterministically sort by external_id in the case where there
            # are two REVOKED periods with the same termination_date
            e.external_id,
        ),
    )


def _supervision_periods_overlapping_with_date(
    intersection_date: datetime.date, supervision_periods: List[StateSupervisionPeriod]
) -> List[StateSupervisionPeriod]:
    """Returns the supervision periods that overlap with the intersection_date."""
    overlapping_periods = [
        supervision_period
        for supervision_period in supervision_periods
        if supervision_period.start_date is not None
        and supervision_period.start_date < intersection_date
        and (
            supervision_period.termination_date is None
            or intersection_date <= supervision_period.termination_date
        )
    ]

    return overlapping_periods


def _get_relevant_sps_for_pre_commitment_sp_search(
    admission_reason: StateIncarcerationPeriodAdmissionReason,
    admission_reason_raw_text: Optional[str],
    supervision_periods: List[StateSupervisionPeriod],
    commitment_from_supervision_delegate: StateSpecificCommitmentFromSupervisionDelegate,
) -> List[StateSupervisionPeriod]:
    """Filters the provided |supervision_periods| to the ones that should be
    considered when looking for pre-commitment supervision periods based on the filter
    configuration defined in the provided |commitment_from_supervision_delegate|."""
    relevant_sps = supervision_periods

    if (
        commitment_from_supervision_delegate.should_filter_out_unknown_supervision_type_in_pre_commitment_sp_search()
    ):
        relevant_sps = filter_out_unknown_supervision_type_periods(supervision_periods)

    if (
        commitment_from_supervision_delegate.should_filter_to_matching_supervision_types_in_pre_commitment_sp_search()
    ):
        pre_incarceration_supervision_type = commitment_from_supervision_delegate.get_pre_incarceration_supervision_type_from_ip_admission_reason(
            admission_reason, admission_reason_raw_text
        )
        if pre_incarceration_supervision_type:
            relevant_sps = _filter_to_matching_supervision_types(
                admission_reason,
                pre_incarceration_supervision_type,
                supervision_periods,
            )

    return relevant_sps
