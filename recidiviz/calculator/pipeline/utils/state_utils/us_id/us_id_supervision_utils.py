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
"""US_ID-specific implementations of functions related to supervision."""
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional, Set, Tuple

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.supervision.events import (
    NonRevocationReturnSupervisionTimeBucket,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    get_supervision_periods_from_sentences,
)
from recidiviz.common.common_utils import date_spans_overlap_exclusive
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    get_most_relevant_supervision_type,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateSupervisionPeriod,
    StateSupervisionSentence,
)

# We expect transition dates from supervision to incarceration to be fairly close together for the data in US_ID. We
# limit the search for a pre or post-incarceration supervision type to 30 days prior to admission or 30 days following
# release.
SUPERVISION_TYPE_LOOKBACK_DAYS_LIMIT = 30

_OUT_OF_STATE_EXTERNAL_ID_IDENTIFIERS: List[str] = [
    "INTERSTATE PROBATION",
    "PAROLE COMMISSION OFFICE",
]


def us_id_get_pre_incarceration_supervision_type(
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_period: StateIncarcerationPeriod,
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Calculates the pre-incarceration supervision type for US_ID people by calculating the most recent type of
    supervision a given person was on within INCARCERATION_SUPERVISION_TYPE_DAYS_LIMIT days of the
    incarceration admission. If no supervision type is found, returns None.
    """
    admission_date = incarceration_period.admission_date

    if not admission_date:
        raise ValueError(
            f"No admission date for incarceration period {incarceration_period.incarceration_period_id}"
        )

    supervision_periods = get_supervision_periods_from_sentences(
        incarceration_sentences, supervision_sentences
    )

    return us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
        upper_bound_exclusive_date=admission_date,
        lower_bound_inclusive_date=admission_date
        - relativedelta(days=SUPERVISION_TYPE_LOOKBACK_DAYS_LIMIT),
        supervision_periods=supervision_periods,
    )


def us_id_get_post_incarceration_supervision_type(
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_period: StateIncarcerationPeriod,
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Calculates the post-incarceration supervision type for US_ID people by calculating the type of supervision the
    person was on directly after their release from incarceration.
    """
    if not incarceration_period.release_date:
        raise ValueError(
            f"No release date for incarceration period {incarceration_period.incarceration_period_id}"
        )

    if (
        incarceration_period.release_reason
        != StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE
    ):
        return None

    supervision_periods = get_supervision_periods_from_sentences(
        incarceration_sentences, supervision_sentences
    )

    return us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
        upper_bound_exclusive_date=incarceration_period.release_date
        + relativedelta(days=SUPERVISION_TYPE_LOOKBACK_DAYS_LIMIT),
        lower_bound_inclusive_date=incarceration_period.release_date,
        supervision_periods=supervision_periods,
    )


def us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
    upper_bound_exclusive_date: date,
    lower_bound_inclusive_date: Optional[date],
    supervision_periods: List[StateSupervisionPeriod],
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Finds the most recent nonnull supervision period supervision type on the supervision periods, preceding or
    overlapping the provided date. An optional lower bound may be provided to limit the lookback window.

    Returns the most recent StateSupervisionPeriodSupervisionType. If there is no valid supervision
    type found (e.g. the person has only been incarcerated for the time window), returns None. In the case where
    multiple SupervisionPeriodSupervisionTypes end on the same day, this returns only the most relevant
    SupervisionPeriodSupervisionType based on our own ranking.
    """
    supervision_types_by_end_date: Dict[
        date, Set[StateSupervisionPeriodSupervisionType]
    ] = defaultdict(set)

    lower_bound_exclusive_date = (
        lower_bound_inclusive_date - relativedelta(days=1)
        if lower_bound_inclusive_date
        else date.min
    )

    for supervision_period in supervision_periods:
        start_date = supervision_period.start_date

        if not start_date:
            continue

        termination_date = (
            supervision_period.termination_date
            if supervision_period.termination_date
            else date.today()
        )

        supervision_period_supervision_type = (
            supervision_period.supervision_period_supervision_type
        )

        if not supervision_period_supervision_type:
            continue

        if not date_spans_overlap_exclusive(
            start_1=lower_bound_exclusive_date,
            end_1=upper_bound_exclusive_date,
            start_2=start_date,
            end_2=termination_date,
        ):
            continue

        supervision_types_by_end_date[termination_date].add(
            supervision_period_supervision_type
        )

    if not supervision_types_by_end_date:
        return None

    max_end_date = max(supervision_types_by_end_date.keys())

    return get_most_relevant_supervision_type(
        supervision_types_by_end_date[max_end_date]
    )


def us_id_supervision_period_is_out_of_state(
    supervision_time_bucket: NonRevocationReturnSupervisionTimeBucket,
) -> bool:
    """Returns whether the given supervision time bucket should be considered a supervision period that is being
    served out of state.

    This is true if either:
    - The supervision district identifier indicates a non-Idaho entity/jurisdiction
    - The supervision period custodial authority indicates a non-Idaho entity
    """
    # TODO(#4713): Rely on level_2_supervising_district_external_id, once it is populated.
    external_id = supervision_time_bucket.supervising_district_external_id
    out_of_state_identifier = external_id is not None and external_id.startswith(
        tuple(_OUT_OF_STATE_EXTERNAL_ID_IDENTIFIERS)
    )

    custodial_authority = supervision_time_bucket.custodial_authority
    out_of_state_authority = (
        custodial_authority is not None
        and custodial_authority
        in (
            StateCustodialAuthority.FEDERAL,
            StateCustodialAuthority.OTHER_COUNTRY,
            StateCustodialAuthority.OTHER_STATE,
        )
    )

    return out_of_state_identifier or out_of_state_authority


def us_id_get_supervision_period_admission_override(
    supervision_period: StateSupervisionPeriod,
    supervision_period_index: PreProcessedSupervisionPeriodIndex,
) -> Optional[StateSupervisionPeriodAdmissionReason]:
    """Looks at the provided |supervision_period| and all supervision periods for this person via the
    |supervision_period_index| and returns the (potentially updated) admission reason for this |supervision period|.
    This is necessary because ID periods that occur after investigation should be counted as newly court sentenced, as
    opposed to transfers.

    In order to determine if an override should occur, this method looks at the sorted list of supervision periods
    from |supervision_period_index| to find the most recently non-null supervision period supervision type preceding
    the given |supervision_period|.
    """
    if not supervision_period.start_date:
        raise ValueError(
            "Found null supervision_period.start_date while getting admission reason override."
        )

    lower_bound_date = supervision_period.start_date - relativedelta(
        days=SUPERVISION_TYPE_LOOKBACK_DAYS_LIMIT
    )

    # Get the most recent, non-null previous supervision type.
    previous_supervision_type = None
    previous_index = (
        supervision_period_index.supervision_periods.index(supervision_period) - 1
    )
    while previous_index >= 0:
        previous_sp = supervision_period_index.supervision_periods[previous_index]

        # Stop looking if we go too far back
        if (
            previous_sp.termination_date
            and previous_sp.termination_date < lower_bound_date
        ):
            break

        # Stop looking if we found a non-null previous supervision type
        previous_supervision_type = previous_sp.supervision_period_supervision_type
        if previous_supervision_type:
            break

        previous_index = previous_index - 1

    if (
        previous_supervision_type == StateSupervisionPeriodSupervisionType.INVESTIGATION
        and supervision_period.admission_reason
        == StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE
    ):
        return StateSupervisionPeriodAdmissionReason.COURT_SENTENCE
    return supervision_period.admission_reason


def us_id_get_supervising_officer_and_location_info_from_supervision_period(
    supervision_period: StateSupervisionPeriod,
    supervision_period_to_agent_associations: Dict[int, Dict[str, Any]],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    supervising_officer_external_id = None
    level_1_supervision_location = None
    level_2_supervision_location = None

    if not supervision_period.supervision_period_id:
        raise ValueError("Unexpected null supervision_period_id")

    if supervision_period.supervision_site:
        # In ID, supervision_site follows format
        # "{supervision district}|{location/office within district}"
        (
            level_2_supervision_location,
            level_1_supervision_location,
        ) = supervision_period.supervision_site.split("|")

    agent_info = supervision_period_to_agent_associations.get(
        supervision_period.supervision_period_id
    )

    if agent_info is not None:
        supervising_officer_external_id = agent_info["agent_external_id"]

    return (
        supervising_officer_external_id,
        level_1_supervision_location or None,
        level_2_supervision_location or None,
    )
