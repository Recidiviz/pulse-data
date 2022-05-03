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
import sys
from typing import Any, Dict, List, Optional, Tuple, TypeVar

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.metrics.supervision.events import (
    SupervisionPopulationEvent,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
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
from recidiviz.common.date import DateRange, DateRangeDiff
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
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
    # Unknown values
    StateSupervisionCaseType.INTERNAL_UNKNOWN,
    StateSupervisionCaseType.EXTERNAL_UNKNOWN,
]

POST_RELEASE_LOOKFORWARD_DAYS = 30

StateSupervisionPeriodT = TypeVar(
    "StateSupervisionPeriodT", bound=StateSupervisionPeriod
)


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
    supervision_periods: List[StateSupervisionPeriodT],
) -> List[StateSupervisionPeriodT]:
    """Sorts supervision periods chronologically by dates and statuses."""
    sort_periods_by_set_dates_and_statuses(
        supervision_periods,
        is_transfer_start_function=_is_transfer_start,
        is_transfer_end_function=_is_transfer_end,
    )

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


def filter_out_supervision_period_types_excluded_from_pre_admission_search(
    supervision_periods: List[StateSupervisionPeriodT],
) -> List[StateSupervisionPeriodT]:
    """Filters the list of supervision periods to only include ones with a
    supervision_type that should be included when looking for SPs that preceded an
    admission to prison."""
    included_in_search: List[StateSupervisionPeriodSupervisionType] = [
        StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT,
        StateSupervisionPeriodSupervisionType.DUAL,
        StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION,
        StateSupervisionPeriodSupervisionType.INVESTIGATION,
        StateSupervisionPeriodSupervisionType.PAROLE,
        StateSupervisionPeriodSupervisionType.PROBATION,
    ]

    # The following supervision types are excluded when identifying the type of
    # supervision that preceded incarceration as these are not associated with an
    # explicit *type* of supervision
    not_included_in_search: List[StateSupervisionPeriodSupervisionType] = [
        StateSupervisionPeriodSupervisionType.ABSCONSION,
        StateSupervisionPeriodSupervisionType.BENCH_WARRANT,
        StateSupervisionPeriodSupervisionType.EXTERNAL_UNKNOWN,
        StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
    ]

    filtered_periods: List[StateSupervisionPeriodT] = []

    for sp in supervision_periods:
        if not sp.supervision_type:
            continue

        if sp.supervision_type in included_in_search:
            filtered_periods.append(sp)
        elif sp.supervision_type in not_included_in_search:
            continue
        else:
            raise ValueError(
                "StateSupervisionPeriodSupervisionType value not "
                f"handled: {sp.supervision_type}."
            )

    return filtered_periods


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


def should_produce_supervision_event_for_period(
    supervision_period: StateSupervisionPeriod,
) -> bool:
    """Whether or not any SupervisionEvents should be created using the
    supervision_period. In some cases, supervision period normalization will not drop
    periods entirely because we need them for context in some of the calculations,
    but we do not want to create metrics using the periods.

    If this returns True, it does not necessarily mean they should be counted towards
    the supervision population for any part of this period. It just means that a
    person was actively assigned to supervision at this time and various
    characteristics of this period may be relevant for generating metrics (such as the
    termination reason / date) even if we may not count this person towards the
    supervision population during the period time span (e.g. if they are incarcerated
    the whole time).

    Default behavior is to not produce any supervision events associated with
    investigation or pre-confinement supervision. Currently, there should be no
    supervision events associated with investigation or pre-confinement supervision.
    """
    return (
        supervision_period.supervision_type
        != StateSupervisionPeriodSupervisionType.INVESTIGATION
    )


def get_post_incarceration_supervision_type(
    incarceration_period: StateIncarcerationPeriod,
    supervision_period_index: NormalizedSupervisionPeriodIndex,
    supervision_delegate: StateSpecificSupervisionDelegate,
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """If the person was released from incarceration onto some form of supervision,
    returns the type of supervision they were released to."""

    if not incarceration_period.release_date:
        raise ValueError(
            f"No release date for incarceration period {incarceration_period.incarceration_period_id}"
        )

    release_date_lookforward_date_range = DateRange(
        incarceration_period.release_date,
        incarceration_period.release_date
        + relativedelta(days=POST_RELEASE_LOOKFORWARD_DAYS),
    )

    overlapping_sps = [
        supervision_period
        for supervision_period in supervision_period_index.sorted_supervision_periods
        if DateRangeDiff(
            supervision_period.duration, release_date_lookforward_date_range
        ).overlapping_range
    ]

    if overlapping_sps:
        relevant_sp = sorted(
            overlapping_sps,
            key=lambda sp: _sort_supervision_periods_for_release_type(
                sp, incarceration_period, supervision_delegate
            ),
        )[0]
        return relevant_sp.supervision_type

    return None


def _sort_supervision_periods_for_release_type(
    supervision_period: StateSupervisionPeriod,
    incarceration_period: StateIncarcerationPeriod,
    supervision_delegate: StateSpecificSupervisionDelegate,
) -> Tuple[int, int, int]:
    """To determine the most relevant supervision period for post incarceration release,
    sort on three criteria:
        1. The proximity of the supervision period's start date to the incarceration
        period's release date. Shorter is better so less positive.
        2. Whether or not the supervision period's supervision type matches the
        release criteria for the incarceration period (i.e. looking at ND release notes).
        3. The duration of the supervision period. Longer is better so more negative."""

    proximity = sys.maxsize
    matches_supervision_type = 0

    if supervision_period.start_date and incarceration_period.release_date:
        proximity = abs(
            (supervision_period.start_date - incarceration_period.release_date).days
        )

    supervision_type_at_release = (
        supervision_delegate.get_incarceration_period_supervision_type_at_release(
            incarceration_period
        )
    )
    if supervision_type_at_release:
        if supervision_type_at_release == supervision_period.supervision_type:
            matches_supervision_type = -1
        else:
            matches_supervision_type = 1

    duration = (
        supervision_period.duration.lower_bound_inclusive_date
        - supervision_period.duration.upper_bound_exclusive_date
    ).days

    return (proximity, matches_supervision_type, duration)


def supervision_periods_overlapping_with_date(
    intersection_date: datetime.date, supervision_periods: List[StateSupervisionPeriodT]
) -> List[StateSupervisionPeriodT]:
    """Identifies supervision_periods where the |intersection_date| falls between the
    start and end of the supervision period, inclusive of the start_date and
    exclusive of the termination_date."""
    return [
        sp
        for sp in supervision_periods
        if sp.start_date is not None
        and sp.start_date <= intersection_date
        and (sp.termination_date is None or intersection_date < sp.termination_date)
    ]
