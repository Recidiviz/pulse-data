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
"""Contains utils for match database entities with ingested entities."""

import copy
import datetime
import logging
from typing import Callable, Optional

import deepdiff

from recidiviz.common.constants.bond import BondStatus
from recidiviz.persistence import entities
from recidiviz.persistence.errors import PersistenceError
from recidiviz.persistence.persistence_utils import is_booking_active


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

    return (db_entity.full_name == ingested_entity.full_name
            and _is_birthdate_match(db_entity, ingested_entity)
            and _db_open_booking_matches_ingested_booking(
                db_entity=db_entity, ingested_entity=ingested_entity))


def diff_count(entity_a: entities.Entity, entity_b: entities.Entity) -> int:
    """Counts the number of differences between two entities, including
    their descendants."""
    ddiff = deepdiff.DeepDiff(entity_a, entity_b,
                              ignore_order=True, report_repetition=True)
    DIFF_TYPES = ('values_changed', 'type_changes', 'iterable_item_added',
                  'iterable_item_removed', 'repetition_change')
    if not any(diff_type in ddiff for diff_type in DIFF_TYPES):
        logging.warning("DeepDiff did not return any of the expected diff "
                        "report fields. Maybe the API changed?\nDiff output:%s",
                        ddiff)

    return sum(len(diffs) for diff_type, diffs in ddiff.items()
               if diff_type in DIFF_TYPES)


def _is_birthdate_match(a: entities.Person, b: entities.Person) -> bool:
    if a.birthdate_inferred_from_age and b.birthdate_inferred_from_age:
        return _is_inferred_birthdate_match(a.birthdate, b.birthdate)

    if not a.birthdate_inferred_from_age \
            and not b.birthdate_inferred_from_age:
        return a.birthdate == b.birthdate

    return False


def _db_open_booking_matches_ingested_booking(
        *, db_entity: entities.Person,
        ingested_entity: entities.Person) -> bool:
    """Returns True if the external id on the open booking in the database
    matches any of the external ids of the bookings on the ingested person.
    If there is no open booking in the db, return True as well.

    Note: if the same person has been rebooked on subsequent scrapes, and the
    ingested person doesn't have historical bookings, we will not match the
    person entities. This is the same behavior as if the person is rebooked on
    non-consecutive days.
    """
    db_open_bookings = [b for b in db_entity.bookings if is_booking_active(b)]
    if not db_open_bookings:
        return True
    if len(db_open_bookings) > 1:
        raise PersistenceError(
            "db person {} has more than one open booking".format(
                db_entity.person_id))
    return any(db_open_bookings[0].external_id == ingested_booking.external_id
               for ingested_booking in ingested_entity.bookings)


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
    return is_booking_active(db_entity) and is_booking_active(ingested_entity)


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


def is_charge_match_with_children(
        *, db_entity: entities.Charge, ingested_entity: entities.Charge) \
        -> bool:
    sentences_match = is_sentence_match(
        db_entity=db_entity.sentence, ingested_entity=ingested_entity.sentence)
    bonds_match = is_bond_match(
        db_entity=db_entity.bond, ingested_entity=ingested_entity.bond)

    return sentences_match and bonds_match and is_charge_match(
        db_entity=db_entity, ingested_entity=ingested_entity)


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

    if db_entity.external_id or ingested_entity.external_id:
        return db_entity.external_id == ingested_entity.external_id

    return _sanitize_charge(db_entity) == _sanitize_charge(ingested_entity)


def _sanitize_charge(charge: entities.Charge) -> entities.Charge:
    sanitized = copy.deepcopy(charge)
    sanitized.charge_id = None
    sanitized.bond = None
    sanitized.sentence = None
    sanitized.next_court_date = None
    sanitized.judge_name = None
    sanitized.charge_notes = None
    return sanitized


# '*' catches positional arguments, making our arguments named and required.
def is_bond_match(
        *, db_entity: Optional[entities.Bond],
        ingested_entity: Optional[entities.Bond]) -> bool:
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
    sanitized.status = BondStatus.REMOVED_WITHOUT_INFO

    # booking_id is added just before writing to the DB. Because this method is
    # used to compare ingested to db entities, we will ignore it for now.
    # TODO(1488): Remove once we use whitelist for equality instead of blacklist
    sanitized.booking_id = None
    return sanitized


# '*' catches positional arguments, making our arguments named and required.
def is_sentence_match(
        *, db_entity: Optional[entities.Sentence],
        ingested_entity: Optional[entities.Sentence]) -> bool:
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
    # TODO(400): update with new incarceration / supervision objects
    sanitized = copy.deepcopy(sentence)
    sanitized.sentence_id = None
    sanitized.related_sentences = []
    # booking_id is added just before writing to the DB. Because this method is
    # used to compare ingested to db entities, we will ignore it for now.
    # TODO(1488): Remove once we use whitelist for equality instead of blacklist
    sanitized.booking_id = None
    return sanitized


def _is_match(
        db_entity: Optional[entities.Entity],
        ingested_entity: Optional[entities.Entity],
        sanitize_fn: Callable) -> bool:
    if not db_entity or not ingested_entity:
        return db_entity == ingested_entity

    if db_entity.external_id or ingested_entity.external_id:
        return db_entity.external_id == ingested_entity.external_id

    return sanitize_fn(db_entity) == sanitize_fn(ingested_entity)
