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

"""Tests for ingest_info"""

import unittest

from recidiviz.ingest.models.ingest_info import IngestInfo, _Person, \
    _Booking, _Charge, _Arrest, _Sentence, _Bond
from recidiviz.ingest.models.ingest_info_pb2 import Person, Booking, Charge, \
    Arrest, Sentence, Bond


class FieldsDontMatchError(Exception):
    pass


class TestIngestInfo(unittest.TestCase):
    """Tests for ingest_info"""

    def test_proto_fields_match(self):
        def _verify_fields(proto, ingest_info_source, ignore=None):
            ignore = ignore or []
            proto_fields = [field.name for field in proto.DESCRIPTOR.fields]
            source_fields = vars(ingest_info_source)
            for field in proto_fields:
                if field not in source_fields and field not in ignore:
                    raise FieldsDontMatchError(
                        "Field '%s' exists in '%s' proto"
                        " but not in the IngestInfo object" % (
                            field, proto.__name__))

            for field in source_fields:
                if field not in proto_fields and field not in ignore:
                    raise FieldsDontMatchError(
                        "Field '%s' exists in '%s'"
                        " IngestInfo object but not in the proto object" % (
                            field, proto.__name__))

        person_fields_ignore = ['booking_ids', 'booking']
        booking_fields_ignore = ['arrest_id', 'charge_ids', 'arrest', 'charge']
        charge_fields_ignore = ['bond_id', 'sentence_id', 'bond', 'sentence']

        person = IngestInfo().create_person()
        booking = person.create_booking()
        charge = booking.create_charge()
        arrest = booking.create_arrest()
        sentence = charge.create_sentence()
        bond = charge.create_bond()
        _verify_fields(Person, person, person_fields_ignore)
        _verify_fields(Booking, booking, booking_fields_ignore)
        _verify_fields(Charge, charge, charge_fields_ignore)
        _verify_fields(Arrest, arrest)
        _verify_fields(Sentence, sentence)
        _verify_fields(Bond, bond)
        return True

    def test_bool_falsy(self):
        ii = IngestInfo()
        person = ii.create_person()
        person.create_booking().create_arrest()
        person.create_booking()
        self.assertFalse(ii)

    def test_bool_truthy(self):
        ii = IngestInfo()
        person = ii.create_person()
        person.create_booking().create_arrest(date='1/2/3')
        person.create_booking()
        self.assertTrue(ii)

    def test_prune(self):
        ii = IngestInfo(people=[
            _Person(),
            _Person(bookings=[
                _Booking(),
                _Booking(arrest=_Arrest(), charges=[
                    _Charge(),
                    _Charge(bond=_Bond(), sentence=_Sentence()),
                    _Charge(bond=_Bond(), sentence=_Sentence(is_life='False'))
                ])
            ])
        ])

        expected = IngestInfo(people=[
            _Person(bookings=[
                _Booking(charges=[
                    _Charge(
                        sentence=_Sentence(is_life='False'))])])])
        self.assertEqual(ii.prune(), expected)
