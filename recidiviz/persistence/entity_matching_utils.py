# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
"""Contains utils for match database entities with ingested entities."""

import copy
import datetime
from typing import Callable, Optional

from recidiviz.persistence import entities


# '*' catches positional arguments, making our arguments named and required.
def is_person_match(
        *, db_entity: entities.Person, ingested_entity: entities.Person) \
        -> bool:
    """
    Given a database person and an ingested person, determine if they should be
    considered the same person.
    Args:
        db_entity: (entities.Person)
        ingested_entity: (entities.Person)
    Returns: (bool)
    """
    if db_entity.external_id or ingested_entity.external_id:
        return db_entity.external_id == ingested_entity.external_id

    if not all([db_entity.full_name, ingested_entity.full_name]):
        return False

    return db_entity.full_name == ingested_entity.full_name \
           and _is_birthdate_match(db_entity, ingested_entity)


def _is_birthdate_match(a: entities.Person, b: entities.Person) -> bool:
    if a.birthdate_inferred_from_age and b.birthdate_inferred_from_age:
        return _is_inferred_birthdate_match(a.birthdate, b.birthdate)

    if not a.birthdate_inferred_from_age \
            and not b.birthdate_inferred_from_age:
        return a.birthdate == b.birthdate

    return False


def _is_inferred_birthdate_match(
        a: Optional[datetime.date],
        b: Optional[datetime.date]) -> bool:
    if not a or not b:
        return False
    return abs(a.year - b.year) <= 1


# '*' catches positional arguments, making our arguments named and required.
def is_booking_match(
        *, db_entity: entities.Booking, ingested_entity: entities.Booking) \
        -> bool:
    """
    Given a database booking and an ingested booking, determine if they should
    be considered the same booking. Should only be used to compare bookings for
    the same person.
    Args:
        db_entity: (entities.Booking)
        ingested_entity: (entities.Booking)
    Returns: (bool)
    """
    if db_entity.external_id or ingested_entity.external_id:
        return db_entity.external_id == ingested_entity.external_id

    # If the db booking's admission date was scraped (not inferred), the
    # ingested booking must have the same admission date to be a match.
    if not db_entity.admission_date_inferred:
        return db_entity.admission_date == ingested_entity.admission_date

    # TODO(612): Determine if we need to match a newly released ingested booking
    # with an open db booking
    return _is_active(db_entity) and _is_active(ingested_entity)


def _is_active(booking: entities.Booking) -> bool:
    """
    Returns True if a booking is active/open. This is true when the booking
    does not have a release date.
    Args:
        booking: (entities.Booking)
    Returns: (bool)
    """
    return not booking.release_date


# '*' catches positional arguments, making our arguments named and required.
def is_hold_match(
        *, db_entity: entities.Hold, ingested_entity: entities.Hold) -> bool:
    """
    Given a database hold and an ingested hold, determine if they should
    be considered the same hold. Should only be used to compare holds for
    the same booking.
    Args:
        db_entity: (entities.Hold)
        ingested_entity: (entities.Hold)
    Returns: (bool)
    """
    return _is_match(db_entity, ingested_entity, _sanitize_hold)


def _sanitize_hold(hold: entities.Hold) -> entities.Hold:
    sanitized = copy.deepcopy(hold)
    sanitized.hold_id = None

    return sanitized


# '*' catches positional arguments, making our arguments named and required.
def is_charge_match(
        *, db_entity: entities.Charge, ingested_entity: entities.Charge) \
        -> bool:
    """
    Given a database charge and an ingested charge, determine if they should be
    considered the same charge. Should only be used to compare charges for the
    same booking.
    Args:
        db_entity: (entities.Charge)
        ingested_entity: (entities.Charge)
    Returns: (bool)
    """
    return _is_match(db_entity, ingested_entity, _sanitize_charge)


def _sanitize_charge(charge: entities.Charge) -> entities.Charge:
    sanitized = copy.deepcopy(charge)
    sanitized.charge_id = None
    sanitized.bond = None
    sanitized.sentence = None
    sanitized.next_court_date = None
    sanitized.judge_name = None

    if charge.bond:
        sanitized.bond = _sanitize_bond(charge.bond)
    if charge.sentence:
        sanitized.sentence = _sanitize_sentence(charge.sentence)
    return sanitized


# '*' catches positional arguments, making our arguments named and required.
def is_bond_match(
        *, db_entity: entities.Bond, ingested_entity: entities.Bond) -> bool:
    """
    Given a database bond and an ingested bond, determine if they should be
    considered the same bond. Should only be used to compare bonds for the same
    charges.
    Args:
        db_entity: (entities.Bond)
        ingested_entity: (entities.Bond)
    Returns: (bool)
    """
    return _is_match(db_entity, ingested_entity, _sanitize_bond)


def _sanitize_bond(bond: entities.Bond) -> entities.Bond:
    sanitized = copy.deepcopy(bond)
    sanitized.bond_id = None
    sanitized.status = None
    return sanitized


# '*' catches positional arguments, making our arguments named and required.
def is_sentence_match(
        *, db_entity: entities.Sentence, ingested_entity: entities.Sentence) \
        -> bool:
    """
    Given a database sentence and an ingested sentence, determine if they
    should be considered the same sentence. Should only be used to compare
    sentences for the same charge.
    Args:
        db_entity: (entities.Sentence)
        ingested_entity: (entities.Sentence)
    Returns: (bool)
    """
    return _is_match(db_entity, ingested_entity, _sanitize_sentence)


def _sanitize_sentence(sentence: entities.Sentence) -> entities.Sentence:
    # TODO(350): decide which fields cannot change
    # TODO(400): update with new incarceration / supervision objects
    sanitized = copy.deepcopy(sentence)
    sanitized.sentence_id = None
    sanitized.related_sentences = []
    return sanitized


def _is_match(db_entity: entities.Entity, ingested_entity: entities.Entity,
              sanitize_fn: Callable) -> bool:
    if db_entity.external_id or ingested_entity.external_id:
        return db_entity.external_id == ingested_entity.external_id

    return sanitize_fn(db_entity) == sanitize_fn(ingested_entity)
