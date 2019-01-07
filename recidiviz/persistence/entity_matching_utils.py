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


# '*' catches positional arguments, making our arguments named and required.
def is_person_match(*, db_entity, ingested_entity):
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

    # TODO(351): consider not matching if birthdate_inferred_from_age

    if not all([db_entity.given_names, ingested_entity.given_names,
                db_entity.surname, ingested_entity.surname,
                db_entity.birthdate, ingested_entity.birthdate]):
        return False

    return db_entity.given_names == ingested_entity.given_names and \
           db_entity.surname == ingested_entity.surname and \
           db_entity.birthdate == ingested_entity.birthdate


# '*' catches positional arguments, making our arguments named and required.
def is_booking_match(*, db_entity, ingested_entity):
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

    # TODO(348): we assume that dates are inferred after entity matching. If
    # they're inferred before we can simplify the following two cases.

    # If the db booking's admission date was scraped (not inferred), the
    # ingested booking must have the same admission date to be a match.
    if not db_entity.admission_date_inferred:
        return db_entity.admission_date == ingested_entity.admission_date

    return _is_active(db_entity) and _is_active(ingested_entity)


def _is_active(booking):
    """
    Returns True if a booking is active/open. This is true when the booking
    does not have a release date.
    Args:
        booking: (entities.Booking)
    Returns: (bool)
    """
    return not booking.release_date


# '*' catches positional arguments, making our arguments named and required.
def is_charge_match(*, db_entity, ingested_entity):
    """
    Given a database charge and an ingested charge, determine if they should be
    considered the same charge. Should only be used to compare charges for the
    same booking.
    Args:
        db_entity: (entities.Charge)
        ingested_entity: (entities.Charge)
    Returns: (bool)
    """
    # TODO(350): decide which fields cannot change
    return _sanitize_charge(db_entity) == _sanitize_charge(ingested_entity)


def _sanitize_charge(charge):
    sanitized = copy.deepcopy(charge)
    sanitized.charge_id = None
    return sanitized


# '*' catches positional arguments, making our arguments named and required.
def is_bond_match(*, db_entity, ingested_entity):
    """
    Given a database bond and an ingested bond, determine if they should be
    considered the same bond. Should only be used to compare bonds for the same
    charges.
    Args:
        db_entity: (entities.Bond)
        ingested_entity: (entities.Bond)
    Returns: (bool)
    """
    # TODO(350): decide which fields cannot change
    return _sanitize_bond(db_entity) == _sanitize_bond(ingested_entity)


def _sanitize_bond(bond):
    sanitized = copy.deepcopy(bond)
    sanitized.bond_id = None
    return sanitized


# '*' catches positional arguments, making our arguments named and required.
def is_sentence_match(*, db_entity, ingested_entity):
    """
    Given a database sentence and an ingested sentence, determine if they
    should be considered the same sentence. Should only be used to compare
    sentences for the same charge.
    Args:
        db_entity: (entities.Sentence)
        ingested_charge: (entities.Charge)
    Returns: (bool)
    """
    # TODO(350): decide which fields cannot change
    return _sanitize_sentence(db_entity) == \
           _sanitize_sentence(ingested_entity)


def _sanitize_sentence(sentence):
    sanitized = copy.deepcopy(sentence)
    sanitized.sentence_id = None
    return sanitized
