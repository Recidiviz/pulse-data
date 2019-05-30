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


def test_recidivism_event():
    original_entry_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    was_conditional = False

    event = recidivism_event.RecidivismEvent.recidivism_event(
        original_entry_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, was_conditional)

    assert event.recidivated
    assert original_entry_date == event.original_entry_date
    assert release_date == event.release_date
    assert release_facility == event.release_facility
    assert reincarceration_date == event.reincarceration_date
    assert reincarceration_facility == event.reincarceration_facility
    assert was_conditional == event.was_conditional


def test_non_recidivism_event():
    original_entry_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'

    event = recidivism_event.RecidivismEvent.non_recidivism_event(
        original_entry_date, release_date, release_facility)

    assert not event.recidivated
    assert original_entry_date == event.original_entry_date
    assert release_date == event.release_date
    assert release_facility == event.release_facility
    assert not event.reincarceration_date
    assert not event.reincarceration_facility
    assert not event.was_conditional


def test_eq_different_field():
    original_entry_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    was_conditional = False

    first = recidivism_event.RecidivismEvent.recidivism_event(
        original_entry_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, was_conditional)

    second = recidivism_event.RecidivismEvent.recidivism_event(
        original_entry_date, release_date, release_facility,
        reincarceration_date, 'A beautiful place out in the country',
        was_conditional)

    assert first != second


def test_eq_different_events():
    original_entry_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    was_conditional = False

    first = recidivism_event.RecidivismEvent.recidivism_event(
        original_entry_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, was_conditional)

    second = recidivism_event.RecidivismEvent.non_recidivism_event(
        original_entry_date, release_date, release_facility)

    assert first != second


def test_eq_different_types():
    original_entry_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    was_conditional = False

    event = recidivism_event.RecidivismEvent.recidivism_event(
        original_entry_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, was_conditional)

    different = "Everything you do is a balloon"

    assert not event.__eq__(different)


def test_repr_recidivism_event():
    original_entry_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'
    reincarceration_date = datetime(2014, 6, 17)
    reincarceration_facility = 'Hexagon Sun'
    was_conditional = False

    event = recidivism_event.RecidivismEvent.recidivism_event(
        original_entry_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, was_conditional)

    representation = event.__repr__()

    assert representation == "<RecidivismEvent recidivated: True, " \
                             "original_entry_date: 2009-06-17 00:00:00, " \
                             "release_date: 2012-06-17 00:00:00, " \
                             "release_facility: Hexagon Sun, " \
                             "reincarceration_date: 2014-06-17 00:00:00, " \
                             "reincarceration_facility: Hexagon Sun, " \
                             "was_conditional: False>"


def test_repr_non_recidivism_event():
    original_entry_date = datetime(2009, 6, 17)
    release_date = datetime(2012, 6, 17)
    release_facility = 'Hexagon Sun'

    event = recidivism_event.RecidivismEvent.non_recidivism_event(
        original_entry_date, release_date, release_facility)

    representation = event.__repr__()

    assert representation == "<RecidivismEvent recidivated: False, " \
                             "original_entry_date: 2009-06-17 00:00:00, " \
                             "release_date: 2012-06-17 00:00:00, " \
                             "release_facility: Hexagon Sun, " \
                             "reincarceration_date: None, " \
                             "reincarceration_facility: None, " \
                             "was_conditional: False>"
