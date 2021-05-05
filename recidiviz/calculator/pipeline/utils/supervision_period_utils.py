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
import logging
from datetime import date
from typing import List, Optional, Set

from recidiviz.calculator.pipeline.utils.period_utils import (
    sort_periods_by_set_dates_and_statuses,
    find_last_terminated_period_before_date,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodAdmissionReason as AdmissionReason,
    StateSupervisionPeriodTerminationReason as TerminationReason,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionPeriod,
    StateIncarcerationSentence,
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


def prepare_supervision_periods_for_calculations(
    supervision_periods: List[StateSupervisionPeriod],
    drop_federal_and_other_country_supervision_periods: bool,
    earliest_death_date: Optional[date] = None,
) -> List[StateSupervisionPeriod]:
    supervision_periods = _drop_placeholder_periods(supervision_periods)

    if drop_federal_and_other_country_supervision_periods:
        supervision_periods = _drop_other_country_and_federal_supervision_periods(
            supervision_periods
        )

    supervision_periods = _infer_missing_dates_and_statuses(supervision_periods)

    if earliest_death_date:
        supervision_periods = _drop_and_close_open_supervision_periods_for_deceased(
            supervision_periods, earliest_death_date
        )

    return supervision_periods


def _drop_placeholder_periods(
    supervision_periods: List[StateSupervisionPeriod],
) -> List[StateSupervisionPeriod]:
    """Drops all placeholder supervision periods."""
    return [period for period in supervision_periods if not is_placeholder(period)]


def _is_active_period(period: StateSupervisionPeriod) -> bool:
    return period.status == StateSupervisionPeriodStatus.UNDER_SUPERVISION


def _infer_missing_dates_and_statuses(
    supervision_periods: List[StateSupervisionPeriod],
) -> List[StateSupervisionPeriod]:
    """First, sorts the supervision_periods in chronological order of the start and termination dates. Then, for any
    periods missing dates and statuses, infers this information given the other supervision periods.
    """
    sort_periods_by_set_dates_and_statuses(supervision_periods, _is_active_period)

    updated_periods: List[StateSupervisionPeriod] = []

    for sp in supervision_periods:
        if sp.termination_date is None:
            if sp.status != StateSupervisionPeriodStatus.UNDER_SUPERVISION:
                # If the person is not under supervision on this period, set the termination date to the start date.
                sp.termination_date = sp.start_date
                sp.termination_reason = TerminationReason.INTERNAL_UNKNOWN
            elif sp.termination_reason or sp.termination_reason_raw_text:
                # There is no termination date on this period, but the set termination_reason indicates that the person
                # is no longer in custody. Set the termination date to the start date.
                sp.termination_date = sp.start_date
                sp.status = StateSupervisionPeriodStatus.TERMINATED

                logging.warning(
                    "No termination_date for supervision period (%d) with nonnull termination_reason (%s) "
                    "or termination_reason_raw_text (%s)",
                    sp.supervision_period_id,
                    sp.termination_reason,
                    sp.termination_reason_raw_text,
                )

        elif sp.termination_date > date.today():
            # This is an erroneous termination_date in the future. For the purpose of calculations, clear the
            # termination_date and the termination_reason.
            sp.termination_date = None
            sp.termination_reason = None
            sp.status = StateSupervisionPeriodStatus.UNDER_SUPERVISION

        if sp.start_date is None:
            logging.info("Dropping supervision period without start_date: [%s]", sp)
            continue
        if sp.start_date > date.today():
            logging.info(
                "Dropping supervision period with start_date in the future: [%s]", sp
            )
            continue

        if sp.admission_reason is None:
            # We have no idea what this admission reason was. Set as INTERNAL_UNKNOWN.
            sp.admission_reason = AdmissionReason.INTERNAL_UNKNOWN
        if sp.termination_date is not None and sp.termination_reason is None:
            # We have no idea what this termination reason was. Set as INTERNAL_UNKNOWN.
            sp.termination_reason = TerminationReason.INTERNAL_UNKNOWN

        if sp.start_date and sp.termination_date:
            if sp.termination_date < sp.start_date:
                logging.info(
                    "Dropping supervision period with termination before admission: [%s]",
                    sp,
                )
                continue

        updated_periods.append(sp)

    return updated_periods


def _drop_other_country_and_federal_supervision_periods(
    supervision_periods: List[StateSupervisionPeriod],
) -> List[StateSupervisionPeriod]:
    """Drop all supervision periods whose custodial authority excludes it from the state's supervision metrics."""
    return [
        period
        for period in supervision_periods
        if period.custodial_authority
        not in (StateCustodialAuthority.FEDERAL, StateCustodialAuthority.OTHER_COUNTRY)
    ]


def get_relevant_supervision_periods_before_admission_date(
    admission_date: Optional[date], supervision_periods: List[StateSupervisionPeriod]
) -> List[StateSupervisionPeriod]:
    """Returns the relevant supervision periods preceding an admission to prison that can be used to determine
    supervision type prior to admission.
    """

    if not admission_date:
        logging.warning("Unexpectedly no admission date provided")
        return []

    relevant_periods = _supervision_periods_overlapping_with_date(
        admission_date, supervision_periods
    )

    if not relevant_periods:
        # If there are no overlapping supervision periods, but they had a prison admission, then they
        # may have been on supervision at some time before this admission. If present within the
        # |SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT|, find the most recently terminated supervision period.
        most_recent_supervision_period = find_last_terminated_period_before_date(
            admission_date,
            supervision_periods,
            SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        if most_recent_supervision_period:
            relevant_periods.append(most_recent_supervision_period)

    return relevant_periods


def _supervision_periods_overlapping_with_date(
    intersection_date: date, supervision_periods: List[StateSupervisionPeriod]
) -> List[StateSupervisionPeriod]:
    """Returns the supervision periods that overlap with the intersection_date."""
    overlapping_periods = [
        supervision_period
        for supervision_period in supervision_periods
        if supervision_period.start_date is not None
        and supervision_period.start_date <= intersection_date
        and (
            supervision_period.termination_date is None
            or intersection_date <= supervision_period.termination_date
        )
    ]

    return overlapping_periods


def _drop_and_close_open_supervision_periods_for_deceased(
    supervision_periods: List[StateSupervisionPeriod],
    earliest_death_date: date,
) -> List[StateSupervisionPeriod]:
    """Updates supervision periods for people who are deceased by
    - Dropping open supervision periods that start after the |earliest_death_date|
    - Setting supervision periods with termination dates after the |earliest_death_date| to have a termination
      date of |earliest_death_date| and a termination reason of DEATH
    - Closing any open supervision period that start before the |earliest_death_date| to have a termination date
      of |earliest_death_date| and a termination reason of DEATH"""

    updated_periods: List[StateSupervisionPeriod] = []

    for sp in supervision_periods:
        if not sp.start_date:
            raise ValueError(f"Period cannot have unset start dates: {sp}")

        if sp.start_date >= earliest_death_date:
            # Drop open supervision periods that start after the person's death
            continue

        if (sp.termination_date is None) or (earliest_death_date < sp.termination_date):
            # If the supervision period is open, or if the termination_date is after
            # the earliest_death_date, set the termination_date to the
            # earliest_death_date and update the termination_reason and status
            sp.termination_date = earliest_death_date
            sp.termination_reason = StateSupervisionPeriodTerminationReason.DEATH
            sp.status = StateSupervisionPeriodStatus.TERMINATED

        updated_periods.append(sp)

    return updated_periods


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
