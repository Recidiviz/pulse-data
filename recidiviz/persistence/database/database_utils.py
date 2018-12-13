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
"""Contains helpers to interact with the database."""

from recidiviz.persistence.database import schema


def convert_person_to_database(person):
    """Converts the given person to a database person."""
    person_db = schema.Person()
    for k, v in vars(person).items():
        if k == 'bookings':
            person_db.bookings = [_convert_booking_to_database(b) for b in
                                  person.bookings]
        else:
            setattr(person_db, k, v)
    return person_db


def _convert_booking_to_database(booking):
    """Converts the given person to a database booking."""
    booking_db = schema.Booking()
    for k, v in vars(booking).items():
        if k == 'holds':
            booking_db.holds = [_convert_object_to_database(h, schema.Hold())
                                for h in booking.holds]
        elif k == 'arrest':
            booking_db.arrest = _convert_object_to_database(
                booking.arrest, schema.Arrest())
        elif k == 'charges':
            booking_db.charges = [_convert_charge_to_database(c) for c in
                                  booking.charges]
        else:
            setattr(booking_db, k, v)
    return booking_db


def _convert_charge_to_database(charge):
    """Converts the given charge to a database charge."""
    charge_db = schema.Charge()
    for k, v in vars(charge).items():
        if k == 'bond':
            charge_db.bond = _convert_object_to_database(
                charge.bond, schema.Bond())
        elif k == 'sentence':
            charge_db.sentence = _convert_object_to_database(
                charge.sentence, schema.Sentence())

        else:
            setattr(charge_db, k, v)
    return charge_db


def _convert_object_to_database(entity_object, db_object):
    """Converts the given person to a database person."""
    for k, v in vars(entity_object).items():
        setattr(db_object, k, v)
    return db_object
