# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Identifies months of supervision and classifies them as either instances of
revocation or not."""
import logging
from collections import defaultdict
from datetime import date
from typing import List, Dict, Set, Tuple, Optional

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.supervision.supervision_month import \
    SupervisionMonth, RevocationReturnSupervisionMonth, \
    NonRevocationReturnSupervisionMonth
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason
from recidiviz.calculator.pipeline.utils.incarceration_period_utils import \
    drop_placeholder_temporary_and_missing_status_periods, \
    validate_admission_data, validate_release_data
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod, StateSupervisionPeriod


def find_supervision_months(
        supervision_periods: List[StateSupervisionPeriod],
        incarceration_periods: List[StateIncarcerationPeriod]) \
        -> List[SupervisionMonth]:
    """Finds instances of months that a person was on supervision and determines
    if they resulted in revocation return.

    Transforms each StateSupervisionPeriod into months where the person spent
    any number of days on supervision. Excludes any months that the person was
    incarcerated for the entire month by looking through all of the person's
    StateIncarcerationPeriods. For each month that someone was on supervision,
    a SupervisionMonth object is created. If the person was admitted to prison
    in that month for a revocation that matches the type of supervision the
    SupervisionMonth describes, then that object is a
    RevocationSupervisionMonth. If no applicable revocation occurs, then the
    object is of type NonRevocationSupervisionMonth.

    If someone serving both probation and parole simultaneously,
    there will be one SupervisionMonth for the probation type and one for the
    parole type. If someone has multiple overlapping supervision periods, there
    will be one SupervisionMonth for each month on each supervision period.

    If a revocation return to prison occurs in a month where there is no
    recorded supervision period, we count the individual as having been on
    supervision that month so that they can be included in the
    revocation return count and rate metrics for that month. In these cases,
    we add supplemental RevocationReturnSupervisionMonths to the list of
    SupervisionMonths.

    Args:
        - supervision_periods: list of StateSupervisionPeriods for a person
        - incarceration_periods: list of StateIncarcerationPeriods for a person

    Returns:
        A list of SupervisionMonths for the person.
    """

    supervision_months: List[SupervisionMonth] = []

    incarceration_periods = \
        validate_incarceration_periods(incarceration_periods)

    incarceration_periods.sort(key=lambda b: b.admission_date)

    indexed_incarceration_periods = \
        index_incarceration_periods_by_admission_month(incarceration_periods)

    months_of_incarceration = identify_months_of_incarceration(
        incarceration_periods)

    for supervision_period in supervision_periods:
        supervision_months = supervision_months + \
            find_months_for_supervision_period(
                supervision_period,
                indexed_incarceration_periods,
                months_of_incarceration)

    # TODO(2680): Update revocation logic to not rely on adding months for
    #  missing returns
    supervision_months = add_missing_revocation_return_months(
        incarceration_periods, supervision_months)

    return supervision_months


def validate_incarceration_periods(
        incarceration_periods: List[StateIncarcerationPeriod]) -> \
        List[StateIncarcerationPeriod]:
    """Validates the incarceration period inputs.

    Ensures the necessary dates and fields are set on each incarceration period.
    If an incarceration period is found with missing data, drops the
    incarceration period from the calculations. Then, returns the list of valid
    StateIncarcerationPeriods.
    """
    incarceration_periods_no_placeholders = \
        drop_placeholder_temporary_and_missing_status_periods(
            incarceration_periods
        )

    incarceration_periods_valid_admissions = \
        validate_admission_data(incarceration_periods_no_placeholders)

    incarceration_periods_valid_releases = \
        validate_release_data(incarceration_periods_valid_admissions)

    validated_incarceration_periods = incarceration_periods_valid_releases

    return validated_incarceration_periods


def find_months_for_supervision_period(
        supervision_period: StateSupervisionPeriod,
        indexed_incarceration_periods:
        Dict[int, Dict[int, List[StateIncarcerationPeriod]]],
        months_of_incarceration: Set[Tuple[int, int]]) -> \
        List[SupervisionMonth]:
    """Finds months that this person was on supervision for the given
    StateSupervisionPeriod, where the person was not incarcerated for the full
    month. Classifies each month on supervision as either an instance of
    revocation or not.

    Args:
        - supervision_period: The supervision period the person was on
        - indexed_incarceration_periods: A dictionary mapping years and months
            of admissions to prison to the StateIncarcerationPeriods that
            started in that month.
        - months_of_incarceration: A set of tuples in the format (year, month)
            for each month of which this person has been incarcerated for the
            full month.

    Returns
        - A set of unique SupervisionMonths for the person for the given
            StateSupervisionPeriod.
    """

    supervision_months: List[SupervisionMonth] = []

    start_date = supervision_period.start_date
    termination_date = supervision_period.termination_date

    if termination_date is None:
        termination_date = date.today()

    if start_date is None:
        return supervision_months

    month_bucket = date(start_date.year, start_date.month, 1)

    while month_bucket <= termination_date:
        if month_bucket.year in indexed_incarceration_periods.keys() and \
            month_bucket.month in \
                indexed_incarceration_periods[month_bucket.year].keys():
            # An admission to prison happened during this month
            incarceration_periods = \
                indexed_incarceration_periods[month_bucket.year][
                    month_bucket.month]

            for incarceration_period in incarceration_periods:
                if incarceration_period.admission_date and \
                        incarceration_period.admission_date.month == \
                        month_bucket.month:

                    # Add the supervision month for this month
                    supervision_month = _get_supervision_month(
                        supervision_period, incarceration_period,
                        month_bucket, months_of_incarceration)
                    if supervision_month is not None:
                        supervision_months.append(supervision_month)

                    release_date = incarceration_period.release_date
                    if release_date is None or \
                            termination_date < release_date:
                        # Person is either still in custody or this
                        # supervision period ended before the incarceration
                        # period was over. Stop counting supervision months
                        # for this supervision period.
                        return supervision_months

                    # This person continued on supervision after their
                    # release. Start on the month of the release and
                    #  continue to count supervision months.
                    month_bucket = date(
                        release_date.year, release_date.month, 1)
                    continue

        if (month_bucket.year, month_bucket.month) not in \
                months_of_incarceration:
            supervision_months.append(NonRevocationReturnSupervisionMonth(
                supervision_period.state_code, month_bucket.year,
                month_bucket.month,
                supervision_period.supervision_type))

        month_bucket = month_bucket + relativedelta(months=1)

    return supervision_months


def _revocation_occurred(supervision_period: StateSupervisionPeriod,
                         incarceration_period: StateIncarcerationPeriod) -> \
        bool:
    """For a given pair of a StateSupervisionPeriod and a
    StateIncarcerationPeriod, returns true if the supervision type matches the
    type of supervision revocation in the admission reason."""
    if incarceration_period.admission_reason == \
            AdmissionReason.PAROLE_REVOCATION and \
            supervision_period.supervision_type == \
            StateSupervisionType.PROBATION or \
            incarceration_period.admission_reason == \
            AdmissionReason.PROBATION_REVOCATION and \
            supervision_period.supervision_type == \
            StateSupervisionType.PAROLE:
        logging.info("Revocation type %s does not match the supervision type"
                     "%s.", incarceration_period.admission_reason,
                     supervision_period.supervision_type)

    return incarceration_period.admission_reason == \
        AdmissionReason.PAROLE_REVOCATION and \
        supervision_period.supervision_type == \
        StateSupervisionType.PAROLE or \
        incarceration_period.admission_reason == \
        AdmissionReason.PROBATION_REVOCATION and \
        supervision_period.supervision_type == \
        StateSupervisionType.PROBATION


def _get_supervision_month(supervision_period: StateSupervisionPeriod,
                           incarceration_period: StateIncarcerationPeriod,
                           month_bucket: date,
                           months_of_incarceration: Set[Tuple[int, int]]) \
        -> Optional[SupervisionMonth]:
    """Returns a SupervisionMonth if one should be recorded given the
    supervision period and the months of incarceration.

    If a revocation occurred during the month, returns a
    RevocationSupervisionMonth. If a revocation did not occur, and the person
    was not incarcerated for the whole month, then returns a
    NonRevocationSupervisionMonth. If the person was incarcerated for the whole
    month, then returns None.
    """
    if _revocation_occurred(supervision_period,
                            incarceration_period):
        # A revocation occurred this month
        source_violation_response = \
            incarceration_period. \
            source_supervision_violation_response
        revocation_type = None
        violation_type = None
        if source_violation_response:
            revocation_type = \
                source_violation_response.revocation_type
            source_violation = \
                source_violation_response.supervision_violation
            if source_violation:
                violation_type = source_violation.violation_type

        return RevocationReturnSupervisionMonth(
            supervision_period.state_code,
            month_bucket.year, month_bucket.month,
            supervision_period.supervision_type,
            revocation_type, violation_type)

    if (month_bucket.year, month_bucket.month) not in \
            months_of_incarceration:
        # They weren't incarcerated for this month and there
        # was no revocation
        return NonRevocationReturnSupervisionMonth(
            supervision_period.state_code,
            month_bucket.year, month_bucket.month,
            supervision_period.supervision_type)

    return None


def index_incarceration_periods_by_admission_month(
        incarceration_periods: List[StateIncarcerationPeriod]) -> \
        Dict[int, Dict[int, List[StateIncarcerationPeriod]]]:
    """Organizes the list of StateIncarcerationPeriods by the year and month
     of the admission_date on the period."""

    indexed_incarceration_periods: \
        Dict[int, Dict[int, List[StateIncarcerationPeriod]]] = defaultdict()

    for incarceration_period in incarceration_periods:
        if incarceration_period.admission_date:
            year = incarceration_period.admission_date.year
            month = incarceration_period.admission_date.month

            if year not in indexed_incarceration_periods.keys():
                indexed_incarceration_periods[year] = {
                    month: [incarceration_period]
                }
            elif month not in indexed_incarceration_periods[year].keys():
                indexed_incarceration_periods[year][month] = \
                    [incarceration_period]
            else:
                indexed_incarceration_periods[year][month].append(
                    incarceration_period)

    return indexed_incarceration_periods


def identify_months_of_incarceration(
        incarceration_periods: List[StateIncarcerationPeriod]) -> \
        Set[Tuple[int, int]]:
    """For each StateIncarcerationPeriod, identifies months where the
    person was incarcerated for every day during that month. Returns a set of
    months in the format (year, month) for which the person spent the entire
    month in a prison."""

    months_incarcerated: Set[Tuple[int, int]] = set()

    for incarceration_period in incarceration_periods:
        admission_date = incarceration_period.admission_date
        release_date = incarceration_period.release_date

        if admission_date is None:
            return months_incarcerated

        if release_date is None:
            release_date = date.today()

        if admission_date.day == 1:
            month_date = admission_date
        else:
            month_date = date(admission_date.year, admission_date.month, 1) + \
                relativedelta(months=1)

        while month_date + relativedelta(months=1) <= \
                release_date + relativedelta(days=1):
            months_incarcerated.add((month_date.year, month_date.month))
            month_date = month_date + relativedelta(months=1)

    return months_incarcerated


def add_missing_revocation_return_months(
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_months: List[SupervisionMonth]) -> List[SupervisionMonth]:
    """Looks at all incarceration periods to see if they were revocation
    returns. If the list of supervision months does not have a recorded
    revocation return corresponding to the incarceration admission, then a
    RevocationReturnSupervisionMonth is added to supervision_months.
    """

    for incarceration_period in incarceration_periods:
        # This check is here to silence mypy warnings. We have already validated
        # that this incarceration period has an admission date.
        if incarceration_period.admission_date:
            source_violation_response = \
                incarceration_period. \
                source_supervision_violation_response
            revocation_type = None
            violation_type = None
            if source_violation_response:
                revocation_type = \
                    source_violation_response.revocation_type
                source_violation = \
                    source_violation_response.supervision_violation
                if source_violation:
                    violation_type = source_violation.violation_type

            year = incarceration_period.admission_date.year
            month = incarceration_period.admission_date.month

            if incarceration_period.admission_reason == \
                    AdmissionReason.PROBATION_REVOCATION:
                supervision_month = RevocationReturnSupervisionMonth(
                    incarceration_period.state_code,
                    year, month,
                    StateSupervisionType.PROBATION,
                    revocation_type, violation_type)

                if supervision_month not in supervision_months:
                    supervision_months.append(supervision_month)
            elif incarceration_period.admission_reason == \
                    AdmissionReason.PAROLE_REVOCATION:
                supervision_month = RevocationReturnSupervisionMonth(
                    incarceration_period.state_code,
                    year, month,
                    StateSupervisionType.PAROLE,
                    revocation_type, violation_type)

                if supervision_month not in supervision_months:
                    supervision_months.append(supervision_month)

    return supervision_months
