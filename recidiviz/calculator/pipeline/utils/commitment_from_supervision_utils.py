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
import abc
import datetime
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Set, Tuple

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.incarceration_period_utils import (
    periods_are_temporally_adjacent,
)
from recidiviz.calculator.pipeline.utils.period_utils import (
    find_last_terminated_period_before_date,
    sort_period_by_external_id,
)
from recidiviz.calculator.pipeline.utils.pre_processed_incarceration_period_index import (
    PreProcessedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    filter_out_unknown_supervision_period_supervision_type_periods,
    identify_most_severe_case_type,
)
from recidiviz.calculator.pipeline.utils.supervision_type_identification import (
    get_pre_incarceration_supervision_type_from_ip_admission_reason,
    sentence_supervision_types_to_supervision_period_supervision_type,
)
from recidiviz.calculator.pipeline.utils.violation_response_utils import (
    violation_responses_in_window,
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
from recidiviz.common.date import DateRange
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
    StateSupervisionViolationResponse,
)

# The number of months for the window of time prior to a commitment to
# from supervision in which we look for the associated terminated supervision
# period
SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT = 24


class StateSpecificCommitmentFromSupervisionDelegate(abc.ABC):
    """Interface for state-specific decisions involved in categorizing various
    attributes of commitment from supervision admissions."""

    def should_filter_to_matching_supervision_types_in_pre_commitment_sp_search(
        self,
    ) -> bool:
        """Whether or not we should only look at supervision periods where the
        supervision type matches the type of supervision that ended due to the
        commitment admission as indicated by the admission_reason.

        Default behavior is look at any supervision period, regardless of type.
        Should be overridden by state-specific implementations if necessary.
        """
        return False

    def should_filter_out_unknown_supervision_type_in_pre_commitment_sp_search(
        self,
    ) -> bool:
        """Whether or not we should ignore supervision periods with unset
        supervision types when identifying the pre-commitment supervision period.

        Default behavior is to not filter out periods with unknown supervision types.
        Should be overridden by state-specific implementations if necessary.
        """
        return False

    def admission_reasons_that_should_prioritize_overlaps_in_pre_commitment_sp_search(
        self,
    ) -> Set[StateIncarcerationPeriodAdmissionReason]:
        """Returns the set of commitment from supervision admission reasons for which
        we should prioritize periods that *overlap* with the date of admission to
        incarceration, as opposed to prioritizing periods that have already terminated
        by the date of admission.

        Default behavior is always prioritizing periods that have terminated prior to
        the admission. Should be overridden by state-specific implementations if
        necessary.

        A state may want to override this if supervision periods are habitually
        terminated after commitment periods begin.
        """

        return set()

    def identify_specialized_purpose_for_incarceration_and_subtype(
        self,
        incarceration_period: StateIncarcerationPeriod,
        _violation_responses: List[StateSupervisionViolationResponse],
    ) -> Tuple[Optional[StateSpecializedPurposeForIncarceration], Optional[str]]:
        """Determines the specialized_purpose_for_incarceration and, if applicable, the
        specialized_purpose_for_incarceration_subtype of the commitment from supervision
        admission to the given incarceration_period.

        Should be overridden by state-specific implementations if necessary.
        """
        specialized_purpose_for_incarceration = (
            incarceration_period.specialized_purpose_for_incarceration
            # Default to GENERAL if no specialized_purpose_for_incarceration is set
            or StateSpecializedPurposeForIncarceration.GENERAL
        )

        # For now, all non-state-specific specialized_purpose_for_incarceration_subtypes
        # are None
        return specialized_purpose_for_incarceration, None

    def violation_history_window_pre_commitment_from_supervision(
        self,
        admission_date: datetime.date,
        sorted_and_filtered_violation_responses: List[
            StateSupervisionViolationResponse
        ],
        default_violation_history_window_months: int,
    ) -> DateRange:
        """Returns the window of time before a commitment from supervision in which we
        should consider violations for the violation history prior to the admission.

        Default behavior is to use the date of the last violation response recorded
        prior to the |admission_date| as the upper bound of the window, with a lower
        bound that is |default_violation_history_window_months| before that date.

        Should be overridden by state-specific implementations if necessary.
        """
        # We will use the date of the last response prior to the admission as the
        # window cutoff.
        responses_before_admission = violation_responses_in_window(
            violation_responses=sorted_and_filtered_violation_responses,
            upper_bound_exclusive=admission_date + relativedelta(days=1),
            lower_bound_inclusive=None,
        )

        violation_history_end_date = admission_date

        if responses_before_admission:
            # If there were violation responses leading up to the incarceration
            # admission, then we want the violation history leading up to the last
            # response_date instead of the admission_date on the
            # incarceration_period
            last_response = responses_before_admission[-1]

            if not last_response.response_date:
                # This should never happen, but is here to silence mypy warnings
                # about empty response_dates.
                raise ValueError(
                    "Not effectively filtering out responses without valid"
                    " response_dates."
                )
            violation_history_end_date = last_response.response_date

        violation_window_lower_bound_inclusive = (
            violation_history_end_date
            - relativedelta(months=default_violation_history_window_months)
        )
        violation_window_upper_bound_exclusive = (
            violation_history_end_date + relativedelta(days=1)
        )

        return DateRange(
            lower_bound_inclusive_date=violation_window_lower_bound_inclusive,
            upper_bound_exclusive_date=violation_window_upper_bound_exclusive,
        )


def get_commitment_from_supervision_supervision_period(
    incarceration_period: StateIncarcerationPeriod,
    supervision_periods: List[StateSupervisionPeriod],
    commitment_from_supervision_delegate: StateSpecificCommitmentFromSupervisionDelegate,
    incarceration_period_index: PreProcessedIncarcerationPeriodIndex,
) -> Optional[StateSupervisionPeriod]:
    """Identifies the supervision period associated with the commitment to supervision
    admission on the given |admission_date|.

    If |prioritize_overlapping_periods| is True, prioritizes supervision periods that
    are overlapping with the |admission_date|. Else, prioritizes the period that has
    most recently terminated within SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT months of
    the |admission_date|.
    """
    if not supervision_periods:
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

    if not is_commitment_from_supervision(admission_reason):
        raise ValueError(
            "This function should only be called with an "
            "incarceration_period that is a commitment from supervision. "
            "Found an incarceration period with an admission_reason that "
            "is not a valid commitment from supervision admission: "
            f"{admission_reason}."
        )

    preceding_incarceration_period = (
        incarceration_period_index.preceding_incarceration_period_in_index(
            incarceration_period
        )
    )

    if period_is_commitment_from_supervision_admission_from_parole_board_hold(
        incarceration_period=incarceration_period,
        preceding_incarceration_period=preceding_incarceration_period,
    ):
        if not preceding_incarceration_period:
            raise ValueError(
                "This should never happen, since the determination of "
                "whether the commitment came from a board hold requires "
                "the preceding_incarceration_period to be a board hold."
            )

        if not preceding_incarceration_period.admission_date:
            raise ValueError(
                "Unexpected missing admission_date on incarceration period: "
                f"[{preceding_incarceration_period}]"
            )

        # If this person was a commitment from supervision from a parole board hold,
        # then the date that they entered prison was the date of the preceding
        # incarceration period.
        admission_date = preceding_incarceration_period.admission_date

    relevant_periods = _get_relevant_sps_for_pre_commitment_sp_search(
        admission_reason=admission_reason,
        supervision_periods=supervision_periods,
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

    most_recent_terminated_period = find_last_terminated_period_before_date(
        upper_bound_date=admission_date,
        periods=relevant_periods,
        maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        same_date_sort_fn=_same_date_sort_override,
    )

    terminated_periods = (
        [most_recent_terminated_period] if most_recent_terminated_period else []
    )

    if (
        admission_reason
        in commitment_from_supervision_delegate.admission_reasons_that_should_prioritize_overlaps_in_pre_commitment_sp_search()
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


CommitmentDetails = NamedTuple(
    "CommitmentDetails",
    [
        (
            "purpose_for_incarceration",
            Optional[StateSpecializedPurposeForIncarceration],
        ),
        ("purpose_for_incarceration_subtype", Optional[str]),
        ("supervising_officer_external_id", Optional[str]),
        ("level_1_supervision_location_external_id", Optional[str]),
        ("level_2_supervision_location_external_id", Optional[str]),
        ("case_type", Optional[StateSupervisionCaseType]),
        ("supervision_level", Optional[StateSupervisionLevel]),
        ("supervision_level_raw_text", Optional[str]),
    ],
)


def get_commitment_from_supervision_details(
    incarceration_period: StateIncarcerationPeriod,
    pre_commitment_supervision_period: Optional[StateSupervisionPeriod],
    commitment_from_supervision_delegate: StateSpecificCommitmentFromSupervisionDelegate,
    violation_responses: List[StateSupervisionViolationResponse],
    supervision_period_to_agent_associations: Optional[Dict[int, Dict[Any, Any]]],
    state_specific_officer_and_location_info_from_supervision_period_fn: Callable[
        [StateSupervisionPeriod, Dict[int, Dict[str, Any]]],
        Tuple[Optional[str], Optional[str], Optional[str]],
    ],
) -> CommitmentDetails:
    """Identifies various attributes of the commitment to incarceration from
    supervision.
    """
    supervising_officer_external_id = None
    level_1_supervision_location_external_id = None
    level_2_supervision_location_external_id = None

    if pre_commitment_supervision_period and supervision_period_to_agent_associations:
        (
            supervising_officer_external_id,
            level_1_supervision_location_external_id,
            level_2_supervision_location_external_id,
        ) = state_specific_officer_and_location_info_from_supervision_period_fn(
            pre_commitment_supervision_period, supervision_period_to_agent_associations
        )

    (
        purpose_for_incarceration,
        purpose_for_incarceration_subtype,
    ) = commitment_from_supervision_delegate.identify_specialized_purpose_for_incarceration_and_subtype(
        incarceration_period,
        violation_responses,
    )

    case_type = (
        identify_most_severe_case_type(pre_commitment_supervision_period)
        if pre_commitment_supervision_period
        else StateSupervisionCaseType.GENERAL
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
        supervision_level=supervision_level,
        supervision_level_raw_text=supervision_level_raw_text,
    )

    return commitment_details_result


def period_is_commitment_from_supervision_admission_from_parole_board_hold(
    incarceration_period: StateIncarcerationPeriod,
    preceding_incarceration_period: Optional[StateIncarcerationPeriod],
) -> bool:
    """Determines whether the transition from the preceding_incarceration_period to
    the incarceration_period is a commitment from supervision admission after being
    held for a parole board hold."""
    if not preceding_incarceration_period:
        # Commitments from board holds must follow a period of being in a parole
        # board hold
        return False

    if not periods_are_temporally_adjacent(
        first_incarceration_period=preceding_incarceration_period,
        second_incarceration_period=incarceration_period,
    ):
        return False

    return (
        incarceration_period.admission_reason
        # Valid revocation admission reasons following a parole board hold
        in (
            StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION,
            StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )
        # Revocation admission from a parole board hold should happen on the same day
        # as the release from the parole board hold
        and preceding_incarceration_period.specialized_purpose_for_incarceration
        == StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
    )


def filter_to_matching_supervision_types(
    admission_reason: StateIncarcerationPeriodAdmissionReason,
    supervision_periods: List[StateSupervisionPeriod],
) -> List[StateSupervisionPeriod]:
    """Filters the given |supervision_periods| to ony the ones that have a
    supervision type that matches the supervision type implied in the
    |admission_reason| (for example, filtering to only PAROLE periods if the
    |admission_reason| is a PAROLE_REVOCATION)."""
    supervision_types_to_match: List[StateSupervisionPeriodSupervisionType]

    supervision_type_for_admission_reason = (
        get_pre_incarceration_supervision_type_from_ip_admission_reason(
            admission_reason=admission_reason
        )
    )

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
        if period.supervision_period_supervision_type in supervision_types_to_match
        # TODO(#2891): supervision_type is DEPRECATED - remove support for this field
        #  when we delete the supervision_type attribute.
        or (
            sentence_supervision_types_to_supervision_period_supervision_type(
                {period.supervision_type}
            )
            in supervision_types_to_match
        )
    ]


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
        relevant_sps = filter_out_unknown_supervision_period_supervision_type_periods(
            supervision_periods
        )

    if (
        commitment_from_supervision_delegate.should_filter_to_matching_supervision_types_in_pre_commitment_sp_search()
    ):
        relevant_sps = filter_to_matching_supervision_types(
            admission_reason, supervision_periods
        )

    return relevant_sps
