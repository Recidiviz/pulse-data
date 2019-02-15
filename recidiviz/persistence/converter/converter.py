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
from typing import List

import more_itertools

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence import entities
from recidiviz.persistence.converter import arrest, sentence, \
    charge, bond, booking, person, hold
from recidiviz.persistence.converter.converter_utils import fn, \
    parse_bond_amount_and_check_for_type_and_status_info, parse_int


def convert(ingest_info, metadata):
    """Convert an IngestInfo proto into a persistence layer entity.

    Returns:
        A list of entities.Person
    """
    return _Converter(ingest_info, metadata).convert()


class _Converter:
    """Converts between ingest_info objects and persistence layer entity."""

    def __init__(self, ingest_info, metadata):
        self.ingest_info = ingest_info
        self.metadata = metadata

        self.bookings = {b.booking_id: b for b in ingest_info.bookings}
        self.arrests = {a.arrest_id: a for a in ingest_info.arrests}
        self.charges = {c.charge_id: c for c in ingest_info.charges}
        self.holds = {h.hold_id: h for h in ingest_info.holds}
        self.bonds = {b.bond_id: b for b in ingest_info.bonds}
        self.sentences = {s.sentence_id: s for s in ingest_info.sentences}

    def convert(self):
        return [self._convert_person(p) for p in self.ingest_info.people]

    def _convert_person(self, ingest_person):
        """Converts an ingest_info proto Person to a persistence entity."""
        person_builder = entities.Person.builder()

        person.copy_fields_to_builder(
            person_builder, ingest_person, self.metadata)

        converted_bookings = [self._convert_booking(self.bookings[booking_id])
                              for booking_id in ingest_person.booking_ids]

        # If no bookings were ingested, create booking to house inferred data.
        if not converted_bookings:
            inferred_booking = self._convert_booking(ingest_info_pb2.Booking())
            converted_bookings = [inferred_booking]

        person_builder.bookings = converted_bookings

        return person_builder.build()

    def _convert_booking(self, ingest_booking):
        """Converts an ingest_info proto Booking to a persistence entity."""
        booking_builder = entities.Booking.builder()

        booking.copy_fields_to_builder(booking_builder, ingest_booking,
                                       self.metadata)

        booking_builder.arrest = \
            fn(lambda i: arrest.convert(self.arrests[i]),
               'arrest_id', ingest_booking)

        converted_holds = [
            hold.convert(self.holds[hold_id], self.metadata) for hold_id in
            ingest_booking.hold_ids]
        booking_builder.holds = list(
            more_itertools.unique_everseen(converted_holds))

        ingest_charges = [self.charges[c] for c in ingest_booking.charge_ids]
        charges = self._convert_charges(ingest_charges)
        booking_builder.charges = charges

        bond_info_tuple = fn(
            parse_bond_amount_and_check_for_type_and_status_info,
            'total_bond_amount',
            ingest_booking)
        if bond_info_tuple is not None:
            bond_amount, bond_type, bond_status = bond_info_tuple
            booking_builder.charges = \
                _charges_pointing_to_total_bond(
                    bond_amount, bond_type, bond_status, charges)

        return booking_builder.build()

    def _convert_charges(self, ingest_charges) -> List[entities.Charge]:
        """Converts all ingest_info proto Charges to persistence entity Charges.

        When charges.number_of_counts is set, create duplicate charges for the
        persistence entity.
        """
        charges: List[entities.Charge] = []
        for ingest_charge in ingest_charges:
            new_charge = self._convert_charge(ingest_charge)
            number_of_counts = parse_int(ingest_charge.number_of_counts) if \
                ingest_charge.HasField('number_of_counts') else 1
            charges.extend(number_of_counts * [new_charge])

        return charges

    def _convert_charge(self, ingest_charge):
        """Converts an ingest_info proto Charge to a persistence entity."""
        charge_builder = entities.Charge.builder()

        charge.copy_fields_to_builder(charge_builder, ingest_charge,
                                      self.metadata)

        charge_builder.bond = \
            fn(lambda i: bond.convert(self.bonds[i], self.metadata),
               'bond_id',
               ingest_charge)
        charge_builder.sentence = \
            fn(lambda i: sentence.convert(self.sentences[i], self.metadata),
               'sentence_id', ingest_charge)

        return charge_builder.build()


def _charges_pointing_to_total_bond(
        bond_amount, bond_type, bond_status, charges):
    """Infers a bond from the total_bond field and creates a copy of all charges
    updated to point to the inferred bond. If no charges exist, then also infer
    a charge."""
    inferred_bond = entities.Bond(
        external_id=None,
        amount_dollars=bond_amount,
        bond_type=bond_type,
        bond_type_raw_text=None,
        status=bond_status,
        status_raw_text=None,
        bond_agent=None,
    )

    if not charges:
        inferred_charge = entities.Charge.new_with_defaults(
            bond=inferred_bond,
            status=ChargeStatus.UNKNOWN_FOUND_IN_SOURCE
        )
        return [inferred_charge]

    if any(c.bond is not None for c in charges):
        raise ValueError("Can't use total_bond and create a bond on a charge")

    charges_pointing_to_inferred_bond = deepcopy(charges)
    for c in charges_pointing_to_inferred_bond:
        c.bond = inferred_bond

    return charges_pointing_to_inferred_bond
