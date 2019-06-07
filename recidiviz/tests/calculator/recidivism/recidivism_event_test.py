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
from recidiviz.calculator.recidivism import recidivism_event
from recidiviz.calculator.recidivism.recidivism_event import \
    IncarcerationReturnType


def test_recidivism_event():
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    return_type = IncarcerationReturnType.RECONVICTION

    event = recidivism_event.RecidivismEvent.recidivism_event(
        original_admission_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, return_type)

    assert event.recidivated
    assert original_admission_date == event.original_admission_date
    assert release_date == event.release_date
    assert release_facility == event.release_facility
    assert reincarceration_date == event.reincarceration_date
    assert reincarceration_facility == event.reincarceration_facility
    assert return_type == event.return_type


def test_recidivism_event_parole_revocation():
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    return_type = IncarcerationReturnType.PAROLE_REVOCATION

    event = recidivism_event.RecidivismEvent.recidivism_event(
        original_admission_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, return_type)

    assert event.recidivated
    assert original_admission_date == event.original_admission_date
    assert release_date == event.release_date
    assert release_facility == event.release_facility
    assert reincarceration_date == event.reincarceration_date
    assert reincarceration_facility == event.reincarceration_facility
    assert return_type == event.return_type


def test_recidivism_event_probation_revocation():
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    return_type = IncarcerationReturnType.PROBATION_REVOCATION

    event = recidivism_event.RecidivismEvent.recidivism_event(
        original_admission_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, return_type)

    assert event.recidivated
    assert original_admission_date == event.original_admission_date
    assert release_date == event.release_date
    assert release_facility == event.release_facility
    assert reincarceration_date == event.reincarceration_date
    assert reincarceration_facility == event.reincarceration_facility
    assert return_type == event.return_type


def test_non_recidivism_event():
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'

    event = recidivism_event.RecidivismEvent.non_recidivism_event(
        original_admission_date, release_date, release_facility)

    assert not event.recidivated
    assert original_admission_date == event.original_admission_date
    assert release_date == event.release_date
    assert release_facility == event.release_facility
    assert not event.reincarceration_date
    assert not event.reincarceration_facility
    assert not event.return_type


def test_eq_different_field():
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    return_type = IncarcerationReturnType.RECONVICTION

    first = recidivism_event.RecidivismEvent.recidivism_event(
        original_admission_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, return_type)

    second = recidivism_event.RecidivismEvent.recidivism_event(
        original_admission_date, release_date, release_facility,
        reincarceration_date, 'A beautiful place out in the country',
        return_type)

    assert first != second


def test_eq_different_events():
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    return_type = IncarcerationReturnType.RECONVICTION

    first = recidivism_event.RecidivismEvent.recidivism_event(
        original_admission_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, return_type)

    second = recidivism_event.RecidivismEvent.non_recidivism_event(
        original_admission_date, release_date, release_facility)

    assert first != second


def test_eq_different_types():
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    return_type = IncarcerationReturnType.RECONVICTION

    event = recidivism_event.RecidivismEvent.recidivism_event(
        original_admission_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, return_type)

    different = "Everything you do is a balloon"

    assert not event.__eq__(different)


def test_repr_recidivism_event():
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    return_type = IncarcerationReturnType.RECONVICTION

    event = recidivism_event.RecidivismEvent.recidivism_event(
        original_admission_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, return_type)

    representation = event.__repr__()

    assert representation == "<RecidivismEvent recidivated: True, " \
                             "original_admission_date: 2009-06-17 00:00:00, " \
                             "release_date: 2012-06-17 00:00:00, " \
                             "release_facility: Hexagon Sun, " \
                             "reincarceration_date: 2014-06-17 00:00:00, " \
                             "reincarceration_facility: Hexagon Sun, " \
                             "return_type: IncarcerationReturnType." \
                             "RECONVICTION>"


def test_repr_non_recidivism_event():
    original_admission_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'

    event = recidivism_event.RecidivismEvent.non_recidivism_event(
        original_admission_date, release_date, release_facility)

    representation = event.__repr__()

    assert representation == "<RecidivismEvent recidivated: False, " \
                             "original_admission_date: 2009-06-17 00:00:00, " \
                             "release_date: 2012-06-17 00:00:00, " \
                             "release_facility: Hexagon Sun, " \
                             "reincarceration_date: None, " \
                             "reincarceration_facility: None, " \
                             "return_type: None>"
