# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utils for validating and manipulating supervision periods for use in calculations."""
import datetime
import itertools
from typing import List, Optional, Set

from recidiviz.calculator.pipeline.utils.incarceration_period_utils import (
    period_is_commitment_from_supervision_admission_from_parole_board_hold,
)
from recidiviz.calculator.pipeline.utils.period_utils import (
    find_last_terminated_period_before_date,
    sort_period_by_external_id,
    sort_periods_by_set_dates_and_statuses,
)
from recidiviz.calculator.pipeline.utils.pre_processed_incarceration_period_index import (
    PreProcessedIncarcerationPeriodIndex,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateSupervisionPeriod,
    StateSupervisionSentence,
)

CASE_TYPE_SEVERITY_ORDER = [
    StateSupervisionCaseType.SEX_OFFENSE,
    StateSupervisionCaseType.DOMESTIC_VIOLENCE,
    StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS,
    # Diversionary courts
    StateSupervisionCaseType.DRUG_COURT,
    StateSupervisionCaseType.MENTAL_HEALTH_COURT,
    StateSupervisionCaseType.FAMILY_COURT,
    StateSupervisionCaseType.VETERANS_COURT,
    # End Diversionary courts
    StateSupervisionCaseType.ALCOHOL_DRUG,
    StateSupervisionCaseType.GENERAL,
]

# The number of months for the window of time prior to a commitment to
# from supervision in which we look for the associated terminated supervision
# period
SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT = 24


def _is_active_period(period: StateSupervisionPeriod) -> bool:
    return period.status == StateSupervisionPeriodStatus.UNDER_SUPERVISION


def _is_transfer_start(period: StateSupervisionPeriod) -> bool:
    return (
        period.admission_reason
        == StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE
    )


def _is_transfer_end(period: StateSupervisionPeriod) -> bool:
    return (
        period.termination_reason
        == StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE
    )


def standard_date_sort_for_supervision_periods(
    supervision_periods: List[StateSupervisionPeriod],
) -> List[StateSupervisionPeriod]:
    """Sorts supervision periods chronologically by dates and statuses."""
    sort_periods_by_set_dates_and_statuses(
        supervision_periods, _is_active_period, _is_transfer_start, _is_transfer_end
    )

    return supervision_periods


def get_commitment_from_supervision_supervision_period(
    incarceration_period: StateIncarcerationPeriod,
    supervision_periods: List[StateSupervisionPeriod],
    incarceration_period_index: PreProcessedIncarcerationPeriodIndex,
    prioritize_overlapping_periods: bool,
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

    admission_date = incarceration_period.admission_date

    preceding_incarceration_period = (
        incarceration_period_index.preceding_incarceration_period_in_index(
            incarceration_period
        )
    )

    if (
        preceding_incarceration_period
        and period_is_commitment_from_supervision_admission_from_parole_board_hold(
            incarceration_period=incarceration_period,
            preceding_incarceration_period=preceding_incarceration_period,
        )
    ):
        if not preceding_incarceration_period.admission_date:
            raise ValueError(
                "Unexpected missing admission_date on incarceration period: "
                f"[{preceding_incarceration_period}]"
            )

        # If this person was a commitment from supervision from a parole board hold,
        # then the date that they entered prison was the date of the preceding
        # incarceration period.
        admission_date = preceding_incarceration_period.admission_date

    overlapping_periods = supervision_periods_overlapping_with_date(
        admission_date, supervision_periods
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
        periods=supervision_periods,
        maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        same_date_sort_fn=_same_date_sort_override,
    )

    terminated_periods = (
        [most_recent_terminated_period] if most_recent_terminated_period else []
    )

    if prioritize_overlapping_periods:
        relevant_periods = (
            overlapping_periods if overlapping_periods else terminated_periods
        )
    else:
        relevant_periods = (
            terminated_periods if terminated_periods else overlapping_periods
        )

    if not relevant_periods:
        return None

    # In the case where there are multiple relevant SPs at this point, sort and return
    # the first one
    return min(
        relevant_periods,
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


def supervision_periods_overlapping_with_date(
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


def get_supervision_periods_from_sentences(
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
) -> List[StateSupervisionPeriod]:
    """Returns all unique supervision periods associated with any of the given
    sentences."""
    sentences = itertools.chain(supervision_sentences, incarceration_sentences)
    supervision_period_ids: Set[int] = set()
    supervision_periods: List[StateSupervisionPeriod] = []

    for sentence in sentences:
        if not isinstance(
            sentence, (StateIncarcerationSentence, StateSupervisionSentence)
        ):
            raise ValueError(f"Sentence has unexpected type {type(sentence)}")

        for supervision_period in sentence.supervision_periods:
            supervision_period_id = supervision_period.supervision_period_id

            if (
                supervision_period_id is not None
                and supervision_period_id not in supervision_period_ids
            ):
                supervision_periods.append(supervision_period)
                supervision_period_ids.add(supervision_period_id)

    return supervision_periods


def identify_most_severe_case_type(
    supervision_period: StateSupervisionPeriod,
) -> StateSupervisionCaseType:
    """Identifies the most severe supervision case type that the supervision period
    is classified as. If there are no case types on the period that are listed in the
    severity ranking, then StateSupervisionCaseType.GENERAL is returned."""
    case_type_entries = supervision_period.case_type_entries

    if case_type_entries:
        case_types = [entry.case_type for entry in case_type_entries]
    else:
        case_types = [StateSupervisionCaseType.GENERAL]

    return next(
        (
            case_type
            for case_type in CASE_TYPE_SEVERITY_ORDER
            if case_type in case_types
        ),
        StateSupervisionCaseType.GENERAL,
    )
