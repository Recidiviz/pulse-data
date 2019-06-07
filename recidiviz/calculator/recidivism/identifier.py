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

"""Identifies instances of recidivism and non-recidivism for calculation.

This contains the core logic for identifying recidivism events on a
person-by-person basis, transforming incarceration periods for a given person
into instances of recidivism or non-recidivism as appropriate.


"""

from datetime import date
import logging
from typing import Dict, List, Optional

from collections import defaultdict
from recidiviz.calculator.recidivism.release_event import \
    IncarcerationReturnType, ReleaseEvent, RecidivismReleaseEvent, \
    NonRecidivismReleaseEvent
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodReleaseReason, \
    StateIncarcerationPeriodAdmissionReason
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


def find_release_events_by_cohort_year(
        incarceration_periods: List[StateIncarcerationPeriod],
        classify_revocation_return_as_recidivism: bool = True) \
        -> Dict[int, List[ReleaseEvent]]:
    """Finds instances of release and determines if they resulted in recidivism.

    Transforms each StateIncarcerationPeriod from which the person has been
    released into a mapping from its release cohort to the details of the event.
    The release cohort is an integer for the year, e.g. 2006. The event details
    are a ReleaseEvent object, which can represent events of both recidivism and
    non-recidivism. That is, each StateIncarcerationPeriod is transformed into a
    recidivism event unless it is the most recent period of incarceration and
    they are still incarcerated, or it is connected to a subsequent
    StateIncarcerationPeriod by a transfer.

    Example output for someone who went to prison in 2006, was released in 2008,
    went back in 2010, was released in 2012, and never returned:
    {
      2008: [RecidivismReleaseEvent(original_admission_date="2006-04-05", ...)],
      2012: [NonRecidivismReleaseEvent(original_admission_date="2010-09-17",
                ...)]
    }

    Args:
        incarceration_periods: list of StateIncarcerationPeriods for a person
        classify_revocation_return_as_recidivism: a boolean indicating whether
            or not to classify revocations of supervision as instances of
            recidivism.

    Returns:
        A dictionary mapping release cohorts to a list of ReleaseEvents
            for the given person in that cohort.
    """

    # If there is more than one StateIncarcerationPeriod, sort them and then
    # collapse the ones that are linked by transfer
    if incarceration_periods and len(incarceration_periods) > 1:
        incarceration_periods.sort(key=lambda b: b.admission_date)

        incarceration_periods = \
            collapse_incarceration_periods(incarceration_periods)

    release_events: Dict[int, List[ReleaseEvent]] = defaultdict(list)

    for index, incarceration_period in enumerate(incarceration_periods):
        if incarceration_period.admission_date is None:
            # If there is no admission date,
            # there is nothing we can process. Skip it.
            continue

        if incarceration_period.status != \
                StateIncarcerationPeriodStatus.IN_CUSTODY and not \
                incarceration_period.release_date:
            # If the StateIncarcerationPeriod is marked as not in custody but
            #  there is no release date, there is nothing we can process.
            #  Skip it.
            continue

        if incarceration_period.status != \
                StateIncarcerationPeriodStatus.IN_CUSTODY and not \
                incarceration_period.release_reason:
            # If the StateIncarcerationPeriod is marked as not in custody but
            # there is no release reason, there is nothing we can process.
            # Skip it.
            continue

        admission_date = incarceration_period.admission_date
        release_facility = incarceration_period.facility
        release_date = incarceration_period.release_date
        release_cohort = release_date.year if release_date else None

        if len(incarceration_periods) - index == 1:
            event = for_last_incarceration_period(incarceration_period,
                                                  admission_date,
                                                  release_date,
                                                  release_facility)
            if event and release_cohort:
                release_events[release_cohort].append(event)
        else:
            # If there is a StateIncarcerationPeriod after this one and
            # they have been released, then they recidivated.
            # Capture the details.
            event = for_intermediate_incarceration_period(
                incarceration_period, incarceration_periods[index + 1],
                admission_date, release_date, release_facility,
                classify_revocation_return_as_recidivism)

            if event and release_cohort:
                release_events[release_cohort].append(event)

    return release_events


def collapse_incarceration_periods(incarceration_periods:
                                   List[StateIncarcerationPeriod]) -> \
        List[StateIncarcerationPeriod]:
    """Collapses any incarceration periods that are connected by transfers.

    Loops through all of the StateIncarcerationPeriods and combines adjacent
    periods that are connected by a transfer. Only connects two periods if the
    release reason of the first is `TRANSFER` and the admission reason for the
    second is also `TRANSFER`.

    Args:
        incarceration_periods: list of StateIncarcerationPeriods for a
            StatePerson

    Returns:
        A list of collapsed StateIncarcerationPeriods.
    """

    new_incarceration_periods: List[StateIncarcerationPeriod] = []
    open_transfer = False

    # TODO(1782): Check to see if back to back incarceration periods are related
    #  to the same StateIncarcerationSentence or SentenceGroup to be sure we
    #  aren't counting stacked sentences or related periods as recidivism.
    for incarceration_period in incarceration_periods:
        if open_transfer:
            if incarceration_period.admission_reason == \
                    StateIncarcerationPeriodAdmissionReason.TRANSFER:
                # If there is an open transfer period and they were transferred
                # into this incarceration period, then combine this period with
                # the open transfer period.
                start_period = new_incarceration_periods.pop(-1)
                combined_period = \
                    combine_incarceration_periods(start_period,
                                                  incarceration_period)
                new_incarceration_periods.append(combined_period)
            else:
                # They weren't transferred here. Add this as a new
                # incarceration period.
                # TODO(1790): Analyze how often a transfer out is followed by an
                #  admission type that isn't a transfer to ensure we aren't
                #  making bad assumptions with this transfer logic.
                new_incarceration_periods.append(incarceration_period)
        else:
            # TODO(1790): Analyze how often an incarceration period that starts
            #  with a transfer in is not preceded by a transfer out of a
            #  different facility.
            new_incarceration_periods.append(incarceration_period)

        # If this incarceration period ended in a transfer, then flag
        # that there's an open transfer period.
        open_transfer = (incarceration_period.release_reason ==
                         StateIncarcerationPeriodReleaseReason.TRANSFER)

    return new_incarceration_periods


def combine_incarceration_periods(start: StateIncarcerationPeriod,
                                  end: StateIncarcerationPeriod) -> \
        StateIncarcerationPeriod:
    """Combines two StateIncarcerationPeriods.

    Brings together two StateIncarcerationPeriods by setting the following
    fields on the |start| StateIncarcerationPeriod to the values on the |end|
    StateIncarcerationPeriod:

        [status, release_date, facility, housing_unit, facility_security_level,
        facility_security_level_raw_text, projected_release_reason,
        projected_release_reason_raw_text, release_reason,
        release_reason_raw_text]

        Args:
            start: The starting StateIncarcerationPeriod.
            end: The ending StateIncarcerationPeriod.
    """

    start.status = end.status
    start.release_date = end.release_date
    start.facility = end.facility
    start.housing_unit = end.housing_unit
    start.facility_security_level = end.facility_security_level
    start.facility_security_level_raw_text = \
        end.facility_security_level_raw_text
    start.projected_release_reason = end.projected_release_reason
    start.projected_release_reason_raw_text = \
        end.projected_release_reason_raw_text
    start.release_reason = end.release_reason
    start.release_reason_raw_text = end.release_reason_raw_text

    return start


def for_last_incarceration_period(
        incarceration_period: StateIncarcerationPeriod, admission_date: date,
        release_date: Optional[date],
        release_facility: Optional[str]) -> Optional[ReleaseEvent]:
    """Looks at the last StateIncarcerationPeriod for a non-recidivism event.

    If the person has been released from their last StateIncarcerationPeriod,
    there is an instance of non-recidivism to count. If they are still
    incarcerated, or they were released because they died, there is nothing to
    count. Returns any non-recidivism event relevant to the person's last
    StateIncarcerationPeriod.


        Args:
            incarceration_period: a StateIncarcerationPeriod for some person
            admission_date: when this StateIncarcerationPeriod started
            release_date: when they were released from this
                 StateIncarcerationPeriod
            release_facility: the facility they were released from on this
                StateIncarcerationPeriod

        Returns:
            A non-recidivism event if released legitimately from this
                StateIncarcerationPeriod. None otherwise.
        """

    # If the person is still in custody, there is nothing to track.
    if incarceration_period.status == StateIncarcerationPeriodStatus.IN_CUSTODY:
        logging.debug('Person is still in custody on last or only '
                      'incarceration period %s. Nothing to track',
                      incarceration_period.incarceration_period_id)
        return None

    # If the person was released from this incarceration period because they
    # died, do not include them in the recidivism calculations.
    if incarceration_period.release_reason == \
            StateIncarcerationPeriodReleaseReason.DEATH:
        return None

    # If the person was released from this incarceration period because they
    # escaped, do not include them in the recidivism calculations.
    if incarceration_period.release_reason == \
            StateIncarcerationPeriodReleaseReason.ESCAPE:
        return None

    if not release_date:
        # If for some reason there is no release date, don't return a
        # ReleaseEvent
        return None

    logging.debug('Person was released from last or only '
                  'incarceration period %s. No recidivism.',
                  incarceration_period.incarceration_period_id)

    return NonRecidivismReleaseEvent(admission_date, release_date,
                                     release_facility)


def for_intermediate_incarceration_period(
        incarceration_period: StateIncarcerationPeriod,
        next_incarceration_period: StateIncarcerationPeriod,
        admission_date: Optional[date], release_date: Optional[date],
        release_facility: Optional[str],
        classify_revocation_return_as_recidivism: bool) -> \
        Optional[ReleaseEvent]:
    """Returns the ReleaseEvent relevant to an intermediate
    StateIncarcerationPeriod.

    If this is not the person's last StateIncarcerationPeriod and they have been
    released, there is probably an instance of recidivism to count. There is not
    an instance of recidivism if the person escaped or if the next period of
    incarceration was due to a supervision revocation and the identifier has
    been told not to classify revocation returns as recidivism.

    Args:
        incarceration_period: a StateIncarcerationPeriod for some person
        next_incarceration_period: the next StateIncarcerationPeriod after this,
            through which recidivism has occurred
        admission_date: when this StateIncarcerationPeriod started
        release_date: when they were released from this StateIncarcerationPeriod
        release_facility: the facility they were released from on this
            StateIncarcerationPeriod
        classify_revocation_return_as_recidivism: a boolean indicating whether
            or not to classify revocations of supervision as instances of
            recidivism.

    Returns:
        A ReleaseEvent.
    """

    # If the person escaped, do not include them in the recidivism calculations.
    if incarceration_period.release_reason == \
            StateIncarcerationPeriodReleaseReason.ESCAPE:
        return None

    logging.debug('Person was released from incarceration period %s and went '
                  'back again. Yes recidivism.',
                  incarceration_period.incarceration_period_id)

    reincarceration_date = next_incarceration_period.admission_date
    reincarceration_facility = next_incarceration_period.facility

    # Capture what kind of return this was
    if next_incarceration_period.admission_reason == \
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION:
        return_type = IncarcerationReturnType.PROBATION_REVOCATION
    elif next_incarceration_period.admission_reason == \
            StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION:
        return_type = IncarcerationReturnType.PAROLE_REVOCATION
    else:
        return_type = IncarcerationReturnType.RECONVICTION

    if return_type != IncarcerationReturnType.RECONVICTION and not \
            classify_revocation_return_as_recidivism:
        # If this return was due to a revocation of supervision, and
        # supervision revocations should not be classified as recidivism,
        # then return None.
        return None

    if not admission_date or not release_date or not reincarceration_date:
        # If any of these required ReleaseEvent fields are empty, return
        # None
        return None

    return RecidivismReleaseEvent(admission_date, release_date,
                                  release_facility, reincarceration_date,
                                  reincarceration_facility, return_type)
