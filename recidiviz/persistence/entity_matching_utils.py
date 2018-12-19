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


def is_person_match(person_a, person_b):
    """
    Given a database person and an ingested person, determine if they should be
    considered the same person.
    Args:
        person_a: (entities.Person)
        person_b: (entities.Person)
    Returns: (bool)
    """
    if person_a.external_id or person_b.external_id:
        return person_a.external_id == person_b.external_id

    # TODO(351): consider not matching if birthdate_inferred_from_age

    if not all([person_a.given_names, person_b.given_names,
                person_a.surname, person_b.surname,
                person_a.birthdate, person_b.birthdate]):
        return False

    return person_a.given_names == person_b.given_names and \
           person_a.surname == person_b.surname and \
           person_a.birthdate == person_b.birthdate


# TODO(176): in Python 3, make these fields named and required
def is_booking_match(db_booking, ingested_booking):
    """
    Given a database booking and an ingested booking, determine if they should
    be considered the same booking. Should only be used to compare bookings for
    the same person.
    Args:
        db_booking: (entities.Booking)
        ingested_booking: (entities.Booking)
    Returns: (bool)
    """
    if db_booking.external_id or ingested_booking.external_id:
        return db_booking.external_id == ingested_booking.external_id

    # TODO(348): we assume that dates are inferred after entity matching. If
    # they're inferred before we can simplify the following two cases.

    # If the db booking's admission date was scraped (not inferred), the
    # ingested booking must have the same admission date to be a match.
    if not db_booking.admission_date_inferred:
        return db_booking.admission_date == ingested_booking.admission_date

    return _is_active(db_booking) and _is_active(ingested_booking)


def _is_active(booking):
    """
    Returns True if a booking is active/open. This is true when the booking
    does not have a release date.
    Args:
        booking: (entities.Booking)
    Returns: (bool)
    """
    return not booking.release_date

def is_charge_match(charge_a, charge_b):
    """
    Given a database charge and an ingested charge, determine if they should be
    considered the same charge. Should only be used to compare charges for the
    same booking.
    Args:
        db_charge: (entities.Charge)
        ingested_charge: (entities.Charge)
    Returns: (bool)
    """
    # TODO(350): decide which fields cannot change
    return _sanitize_charge(charge_a) == _sanitize_charge(charge_b)


def _sanitize_charge(charge):
    sanitized = copy.deepcopy(charge)
    sanitized.charge_id = None
    return sanitized


def is_bond_match(bond_a, bond_b):
    """
    Given a database bond and an ingested bond, determine if they should be
    considered the same bond. Should only be used to compare bonds for the same
    charges.
    Args:
        db_bond: (entities.Bond)
        ingested_bond: (entities.Bond)
    Returns: (bool)
    """
    # TODO(350): decide which fields cannot change
    return _sanitize_bond(bond_a) == _sanitize_bond(bond_b)


def _sanitize_bond(bond):
    sanitized = copy.deepcopy(bond)
    sanitized.bond_id = None
    return sanitized


def is_sentence_match(sentence_a, sentence_b):
    """
    Given a database sentence and an ingested sentence, determine if they
    should be considered the same charge. Should only be used to compare
    sentences for the same charge.
    Args:
        db_charge: (entities.Charge)
        ingested_charge: (entities.Charge)
    Returns: (bool)
    """
    # TODO(350): decide which fields cannot change
    return _sanitize_sentence(sentence_a) == \
           _sanitize_sentence(sentence_b)


def _sanitize_sentence(sentence):
    sanitized = copy.deepcopy(sentence)
    sanitized.sentence_id = None
    return sanitized
