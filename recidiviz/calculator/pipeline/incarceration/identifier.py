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
"""Identifies instances of admission and release from incarceration."""
from typing import List, Optional, Any, Dict, Set

from pydot import frozendict

from recidiviz.calculator.pipeline.incarceration.incarceration_event import \
    IncarcerationEvent, IncarcerationAdmissionEvent, IncarcerationReleaseEvent
from recidiviz.calculator.pipeline.utils.incarceration_period_utils import \
    prepare_incarceration_periods_for_calculations
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


def find_incarceration_events(
        incarceration_periods: List[StateIncarcerationPeriod]) -> \
        List[IncarcerationEvent]:
    """Finds instances of admission or release from incarceration.

    Transforms StateIncarcerationPeriods into IncarcerationAdmissionEvents and
    IncarcerationReleaseEvents, representing admission and release from
    incarceration.

    Args:
        - incarceration_periods: All of the person's StateIncarcerationPeriods

    Returns:
        A list of IncarcerationEvents for the person.
    """

    incarceration_events: List[IncarcerationEvent] = []

    incarceration_periods = \
        prepare_incarceration_periods_for_calculations(incarceration_periods)

    de_duplicated_incarceration_admissions = de_duplicated_admissions(
        incarceration_periods
    )

    for incarceration_period in de_duplicated_incarceration_admissions:
        admission_event = admission_event_for_period(incarceration_period)

        if admission_event:
            incarceration_events.append(admission_event)

    de_duplicated_incarceration_releases = de_duplicated_releases(
        incarceration_periods
    )

    for incarceration_period in de_duplicated_incarceration_releases:
        release_event = release_event_for_period(incarceration_period)

        if release_event:
            incarceration_events.append(release_event)

    return incarceration_events


def de_duplicated_admissions(incarceration_periods:
                             List[StateIncarcerationPeriod]) -> \
        List[StateIncarcerationPeriod]:
    """Returns a list of incarceration periods that are de-duplicated
    for any incarceration periods that share state_code,
    admission_date, admission_reason, and facility."""

    unique_admission_dicts: Set[Dict[str, Any]] = set()

    unique_incarceration_admissions: List[StateIncarcerationPeriod] = []

    for incarceration_period in incarceration_periods:
        admission_dict = frozendict({
            'state_code': incarceration_period.state_code,
            'admission_date': incarceration_period.admission_date,
            'admission_reason': incarceration_period.admission_reason,
            'facility': incarceration_period.facility
        })

        if admission_dict not in unique_admission_dicts:
            unique_incarceration_admissions.append(incarceration_period)

        unique_admission_dicts.add(admission_dict)

    return unique_incarceration_admissions


def de_duplicated_releases(incarceration_periods:
                           List[StateIncarcerationPeriod]) -> \
        List[StateIncarcerationPeriod]:
    """Returns a list of incarceration periods that are de-duplicated
    for any incarceration periods that share state_code,
    release_date, release_reason, and facility."""

    unique_release_dicts: Set[Dict[str, Any]] = set()

    unique_incarceration_releases: List[StateIncarcerationPeriod] = []

    for incarceration_period in incarceration_periods:
        release_dict = frozendict({
            'state_code': incarceration_period.state_code,
            'release_date': incarceration_period.release_date,
            'release_reason': incarceration_period.release_reason,
            'facility': incarceration_period.facility
        })

        if release_dict not in unique_release_dicts:
            unique_incarceration_releases.append(incarceration_period)

        unique_release_dicts.add(release_dict)

    return unique_incarceration_releases


def admission_event_for_period(incarceration_period:
                               StateIncarcerationPeriod) \
        -> Optional[IncarcerationAdmissionEvent]:
    """Returns an IncarcerationAdmissionEvent if this incarceration period
    represents an admission to incarceration."""

    admission_date = incarceration_period.admission_date
    admission_reason = incarceration_period.admission_reason

    if admission_date and admission_reason:
        year = admission_date.year
        month = admission_date.month

        return IncarcerationAdmissionEvent(
            state_code=incarceration_period.state_code,
            year=year,
            month=month,
            facility=incarceration_period.facility,
            admission_reason=admission_reason
        )

    return None


def release_event_for_period(incarceration_period:
                             StateIncarcerationPeriod) \
        -> Optional[IncarcerationReleaseEvent]:
    """Returns an IncarcerationReleaseEvent if this incarceration period
    represents an release from incarceration."""

    release_date = incarceration_period.release_date
    release_reason = incarceration_period.release_reason

    if release_date and release_reason:
        year = release_date.year
        month = release_date.month

        return IncarcerationReleaseEvent(
            state_code=incarceration_period.state_code,
            year=year,
            month=month,
            facility=incarceration_period.facility,
            release_reason=release_reason
        )

    return None
