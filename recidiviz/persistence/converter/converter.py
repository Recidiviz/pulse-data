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
# ============================================================================
"""Converts scraped IngestInfo data to the persistence layer entity."""
from copy import deepcopy

from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.persistence import entities
from recidiviz.persistence.converter import arrest, sentence, \
    charge, bond, booking, person
from recidiviz.persistence.converter.converter_utils import fn, parse_dollars


def convert(ingest_info):
    """Convert an IngestInfo proto into a persistence layer entity.

    Returns:
        A list of entities.Person
    """
    return _Converter(ingest_info).convert()


class _Converter:
    """Converts between ingest_info objects and persistence layer entity."""

    def __init__(self, ingest_info):
        self.ingest_info = ingest_info

        self.bookings = {b.booking_id: b for b in ingest_info.bookings}
        self.arrests = {a.arrest_id: a for a in ingest_info.arrests}
        self.charges = {c.charge_id: c for c in ingest_info.charges}
        self.bonds = {b.bond_id: b for b in ingest_info.bonds}
        self.sentences = {s.sentence_id: s for s in ingest_info.sentences}

    def convert(self):
        return [self._convert_person(p) for p in self.ingest_info.people]

    def _convert_person(self, ingest_person):
        """Converts an ingest_info proto Person to a persistence entity."""
        person_builder = entities.Person.builder()

        person.copy_fields_to_builder(ingest_person, person_builder)
        person_builder.bookings = [
            self._convert_booking(self.bookings[booking_id])
            for booking_id in ingest_person.booking_ids
        ]

        return person_builder.build()

    def _convert_booking(self, ingest_booking):
        """Converts an ingest_info proto Booking to a persistence entity."""
        new_booking = booking.convert(ingest_booking)

        new_booking.arrest = \
            fn(lambda i: arrest.convert(self.arrests[i]),
               'arrest_id', ingest_booking)
        new_booking.charges = [self._convert_charge(self.charges[charge_id]) for
                               charge_id in ingest_booking.charge_ids]

        # TODO: Populate hold when the proto is updated to contain a hold table

        bond_amount = fn(parse_dollars, 'total_bond_amount', ingest_booking)
        if bond_amount is not None:
            new_booking.charges = \
                _charges_pointing_to_total_bond(bond_amount, new_booking)

        return new_booking

    def _convert_charge(self, ingest_charge):
        """Converts an ingest_info proto Charge to a persistence entity."""
        new = charge.convert(ingest_charge)

        new.bond = \
            fn(lambda i: bond.convert(self.bonds[i]), 'bond_id', ingest_charge)
        new.sentence = \
            fn(lambda i: sentence.convert(self.sentences[i]),
               'sentence_id', ingest_charge)

        return new


def _charges_pointing_to_total_bond(bond_amount, new_booking):
    """Infers a bond from the total_bond field and points all charges to the
    inferred bond. If no charges exist, then also infer a charge."""
    inferred_bond = entities.Bond(amount_dollars=bond_amount,
                                  status=BondStatus.POSTED)

    if not new_booking.charges:
        inferred_charge = entities.Charge(bond=inferred_bond,
                                          status=ChargeStatus.PENDING)
        return [inferred_charge]

    if any(c.bond is not None for c in new_booking.charges):
        raise ValueError("Can't use total_bond and create a bond on a charge")

    charges_pointing_to_inferred_bond = deepcopy(new_booking.charges)
    for c in charges_pointing_to_inferred_bond:
        c.bond = inferred_bond

    return charges_pointing_to_inferred_bond
