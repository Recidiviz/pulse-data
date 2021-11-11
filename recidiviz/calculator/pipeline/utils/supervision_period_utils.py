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
import itertools
from typing import Any, Dict, List, Optional, Set, Tuple

from recidiviz.calculator.pipeline.supervision.events import SupervisionPopulationEvent
from recidiviz.calculator.pipeline.utils.period_utils import (
    sort_periods_by_set_dates_and_statuses,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
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
        supervision_periods,
        has_active_status_function=None,
        is_transfer_start_function=_is_transfer_start,
        is_transfer_end_function=_is_transfer_end,
    )

    return supervision_periods


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


def filter_out_unknown_supervision_type_periods(
    supervision_periods: List[StateSupervisionPeriod],
) -> List[StateSupervisionPeriod]:
    """Filters the list of supervision periods to only include ones with a set
    supervision_type."""
    # Drop any supervision periods that don't have a set
    # supervision_type (this could signify a bench warrant,
    # for example).
    return [
        period
        for period in supervision_periods
        if period.supervision_type is not None
        and period.supervision_type
        != StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    ]


def supervising_officer_and_location_info(
    supervision_period: StateSupervisionPeriod,
    supervision_period_to_agent_associations: Dict[int, Dict[str, Any]],
    supervision_delegate: StateSpecificSupervisionDelegate,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extracts supervising officer and location information associated with a
    supervision_period in the given state.

    Returns a tuple of supervising_officer_external_id and level 1/2 location information.
    """
    supervising_officer_external_id = None
    (
        level_1_supervision_location,
        level_2_supervision_location,
    ) = supervision_delegate.supervision_location_from_supervision_site(
        supervision_period.supervision_site
    )

    supervising_officer_external_id = (
        supervision_delegate.get_supervising_officer_external_id_for_supervision_period(
            supervision_period, supervision_period_to_agent_associations
        )
    )

    return (
        supervising_officer_external_id,
        level_1_supervision_location,
        level_2_supervision_location,
    )


def supervision_period_is_out_of_state(
    supervision_population_event: SupervisionPopulationEvent,
    supervision_delegate: StateSpecificSupervisionDelegate,
) -> bool:
    """Returns whether the given day on supervision was served out of state."""
    return (
        supervision_population_event.is_out_of_state_custodial_authority
        or supervision_delegate.is_supervision_location_out_of_state(
            supervision_population_event
        )
    )
