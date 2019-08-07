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

"""Tests for recidivism/recidivism_event.py."""


from datetime import datetime
from recidiviz.calculator.recidivism.release_event import \
    RecidivismReleaseEvent, NonRecidivismReleaseEvent, ReincarcerationReturnType


def test_recidivism_event():
    state_code = 'CA'
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    return_type = ReincarcerationReturnType.NEW_ADMISSION

    event = RecidivismReleaseEvent(
        state_code, original_admission_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, return_type)

    assert state_code == event.state_code
    assert original_admission_date == event.original_admission_date
    assert release_date == event.release_date
    assert release_facility == event.release_facility
    assert reincarceration_date == event.reincarceration_date
    assert reincarceration_facility == event.reincarceration_facility
    assert return_type == event.return_type


def test_recidivism_event_parole_revocation():
    state_code = 'CA'
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    return_type = ReincarcerationReturnType.REVOCATION

    event = RecidivismReleaseEvent(
        state_code, original_admission_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, return_type)

    assert state_code == event.state_code
    assert original_admission_date == event.original_admission_date
    assert release_date == event.release_date
    assert release_facility == event.release_facility
    assert reincarceration_date == event.reincarceration_date
    assert reincarceration_facility == event.reincarceration_facility
    assert return_type == event.return_type


def test_recidivism_event_probation_revocation():
    state_code = 'MT'
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    return_type = ReincarcerationReturnType.REVOCATION

    event = RecidivismReleaseEvent(
        state_code, original_admission_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, return_type)

    assert state_code == event.state_code
    assert original_admission_date == event.original_admission_date
    assert release_date == event.release_date
    assert release_facility == event.release_facility
    assert reincarceration_date == event.reincarceration_date
    assert reincarceration_facility == event.reincarceration_facility
    assert return_type == event.return_type


def test_non_recidivism_event():
    state_code = 'UT'
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'

    event = NonRecidivismReleaseEvent(state_code, original_admission_date,
                                      release_date, release_facility)

    assert state_code == event.state_code
    assert original_admission_date == event.original_admission_date
    assert release_date == event.release_date
    assert release_facility == event.release_facility
    assert not isinstance(event, RecidivismReleaseEvent)


def test_eq_different_field():
    state_code = 'NV'
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    return_type = ReincarcerationReturnType.NEW_ADMISSION

    first = RecidivismReleaseEvent(
        state_code, original_admission_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, return_type)

    second = RecidivismReleaseEvent(
        state_code, original_admission_date, release_date, release_facility,
        reincarceration_date, 'A beautiful place out in the country',
        return_type)

    assert first != second


def test_eq_different_events():
    state_code = 'FL'
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    return_type = ReincarcerationReturnType.NEW_ADMISSION

    first = RecidivismReleaseEvent(
        state_code, original_admission_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, return_type)

    second = NonRecidivismReleaseEvent(
        state_code, original_admission_date, release_date, release_facility)

    assert first != second


def test_eq_different_types():
    state_code = 'GA'
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    return_type = ReincarcerationReturnType.NEW_ADMISSION

    event = RecidivismReleaseEvent(
        state_code, original_admission_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, return_type)

    different = "Everything you do is a balloon"

    assert event != different
