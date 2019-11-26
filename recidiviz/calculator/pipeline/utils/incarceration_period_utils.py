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
"""Utils for validating and manipulating incarceration periods for use in
calculations."""

import logging
from datetime import date
from typing import List

from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.calculator.pipeline.utils import us_nd_utils
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason, \
    StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodReleaseReason as ReleaseReason
from recidiviz.persistence.entity.entity_utils import is_placeholder


def drop_placeholder_temporary_and_missing_status_periods(
        incarceration_periods: List[StateIncarcerationPeriod]) -> \
        List[StateIncarcerationPeriod]:
    """Removes any incarceration periods that are placeholders, are periods of
    temporary custody, or do not have a valid status field set.

    Returns the valid incarceration periods.
    """
    validated_incarceration_periods: List[StateIncarcerationPeriod] = []

    for incarceration_period in incarceration_periods:
        if is_placeholder(incarceration_period):
            # Drop any placeholder incarceration periods from the calculations
            continue
        if incarceration_period.admission_reason == \
                AdmissionReason.TEMPORARY_CUSTODY:
            # Drop any temporary incarceration periods from the calculations
            continue
        if not incarceration_period.status:
            logging.info("No status on incarceration period with id: %d",
                         incarceration_period.incarceration_period_id)
            continue

        validated_incarceration_periods.append(incarceration_period)

    return validated_incarceration_periods


def validate_admission_data(
        incarceration_periods: List[StateIncarcerationPeriod]) -> \
        List[StateIncarcerationPeriod]:
    """Removes any incarceration periods that don't have admission dates
    or admission reasons.

    Returns the valid incarceration periods.
    """

    if incarceration_periods and \
            incarceration_periods[0].state_code == 'US_ND':
        # If these are North Dakota incarceration periods, send to the
        # state-specific ND data validation function
        incarceration_periods = \
            us_nd_utils.set_missing_admission_data(incarceration_periods)

    validated_incarceration_periods: List[StateIncarcerationPeriod] = []

    for incarceration_period in incarceration_periods:
        if is_placeholder(incarceration_period):
            # Drop any placeholder incarceration periods from the calculations
            continue
        if not incarceration_period.admission_date:
            logging.info("No admission_date on incarceration period with"
                         " id: %d",
                         incarceration_period.incarceration_period_id)
            continue
        if not incarceration_period.admission_reason:
            logging.info("No admission_reason on incarceration period with"
                         " id: %d",
                         incarceration_period.incarceration_period_id)
            continue

        validated_incarceration_periods.append(incarceration_period)

    return validated_incarceration_periods


def validate_release_data(
        incarceration_periods: List[StateIncarcerationPeriod]) -> \
        List[StateIncarcerationPeriod]:
    """Removes any incarceration periods that don't have release dates
    or release reasons. Removes release data if the release date is in the
    future.

    Returns the valid incarceration periods.
    """
    validated_incarceration_periods: List[StateIncarcerationPeriod] = []

    for incarceration_period in incarceration_periods:
        if not incarceration_period.release_date and \
                incarceration_period.status != \
                StateIncarcerationPeriodStatus.IN_CUSTODY:
            logging.info("No release_date on intermediate incarceration "
                         "period with id: %d",
                         incarceration_period.incarceration_period_id)
            continue
        if not incarceration_period.release_reason and \
                incarceration_period.status != \
                StateIncarcerationPeriodStatus.IN_CUSTODY:
            logging.info("No release_reason on intermediate incarceration "
                         "period with id: %d",
                         incarceration_period.incarceration_period_id)
            continue
        if incarceration_period.release_date is not None and \
                incarceration_period.release_date > date.today():
            # If the person has not been released yet, remove the release
            # date and release reason, and set the status to be in custody
            incarceration_period.release_date = None
            incarceration_period.release_reason = None
            incarceration_period.status = \
                StateIncarcerationPeriodStatus.IN_CUSTODY

        validated_incarceration_periods.append(incarceration_period)

    return validated_incarceration_periods


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
                    AdmissionReason.TRANSFER:
                # If there is an open transfer period and they were
                # transferred into this incarceration period, then combine this
                # period with the open transfer period.
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
                         ReleaseReason.TRANSFER)

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
