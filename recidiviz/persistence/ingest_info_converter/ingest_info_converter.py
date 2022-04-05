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
# ============================================================================
"""Converts scraped IngestInfo data to the persistence layer entity."""
import copy
import logging
from typing import List

import attr
import more_itertools

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.entity_enum import EnumParsingError
from recidiviz.common.constants.person import PROTECTED_CLASSES
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo
from recidiviz.persistence import entities, persistence_utils
from recidiviz.persistence.ingest_info_converter.entity_helpers import (
    arrest,
    person,
    hold,
    charge,
    sentence,
    bond,
    booking
)
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_bond_amount_type_and_status
)
from recidiviz.common.str_field_utils import parse_int


@attr.s(frozen=True)
class IngestInfoConversionResult:
    enum_parsing_errors: int = attr.ib()
    general_parsing_errors: int = attr.ib()
    protected_class_errors: int = attr.ib()
    people: List[entities.Person] = attr.ib(factory=list)


def convert_to_persistence_entities(
        ingest_info: IngestInfo, metadata: IngestMetadata
) -> IngestInfoConversionResult:
    return _IngestInfoConverter(ingest_info, metadata).run_convert()


class _IngestInfoConverter:
    """Converts between ingest_info objects and persistence layer entity."""

    def __init__(self,
                 ingest_info: IngestInfo,
                 metadata: IngestMetadata) -> None:

        self.ingest_info = copy.deepcopy(ingest_info)
        self.metadata = metadata

        self.bookings = {b.booking_id: b for b in ingest_info.bookings}
        self.arrests = {a.arrest_id: a for a in ingest_info.arrests}
        self.charges = {c.charge_id: c for c in ingest_info.charges}
        self.holds = {h.hold_id: h for h in ingest_info.holds}
        self.bonds = {b.bond_id: b for b in ingest_info.bonds}
        self.sentences = {s.sentence_id: s for s in ingest_info.sentences}

    def run_convert(self):
        people: List[entities.Person] = []
        protected_class_errors = 0
        enum_parsing_errors = 0
        general_parsing_errors = 0
        while not self._is_complete():
            try:
                people.append(self._convert_and_pop())
            except EnumParsingError as e:
                logging.error(str(e))
                if e.entity_type in PROTECTED_CLASSES:
                    protected_class_errors += 1
                else:
                    enum_parsing_errors += 1
            except Exception as e:
                logging.error(str(e))
                general_parsing_errors += 1
        return IngestInfoConversionResult(
            people=people,
            enum_parsing_errors=enum_parsing_errors,
            general_parsing_errors=general_parsing_errors,
            protected_class_errors=protected_class_errors)

    def _convert_and_pop(self):
        return self._convert_person(self.ingest_info.people.pop())

    def _is_complete(self):
        if self.ingest_info.people:
            return False
        return True

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

        converted_person = person_builder.build()

        # Scrub PII if the person either has an external id or has no open
        # bookings.
        if converted_person.external_id \
                or not persistence_utils.has_active_booking(converted_person):
            persistence_utils.remove_pii_for_person(converted_person)

        return converted_person

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
            parse_bond_amount_type_and_status,
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
            charges.extend(
                _duplicate_charge_with_counts(new_charge, number_of_counts))

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


def _duplicate_charge_with_counts(converted_charge: entities.Charge,
                                  counts: int) -> List[entities.Charge]:
    if counts < 1:
        raise ValueError("Cannot convert charge with fewer than 1 count; "
                         "charge {} has"
                         "{} counts".format(converted_charge, counts))
    duplicated_charges = []
    for i in range(1, counts + 1):
        # Perform a shallow copy so that bonds and sentences are shared rather
        # than duplicated.
        duplicated_charge = copy.copy(converted_charge)
        if duplicated_charge.external_id:
            new_external_id = '{}_COUNT_{}'.format(
                converted_charge.external_id, i)
            duplicated_charge.external_id = new_external_id
        duplicated_charges.append(duplicated_charge)
    return duplicated_charges


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
            status=ChargeStatus.PRESENT_WITHOUT_INFO
        )
        return [inferred_charge]

    if any(c.bond is not None for c in charges):
        raise ValueError("Can't use total_bond and create a bond on a charge")

    charges_pointing_to_inferred_bond = copy.deepcopy(charges)
    for c in charges_pointing_to_inferred_bond:
        c.bond = inferred_bond

    return charges_pointing_to_inferred_bond
