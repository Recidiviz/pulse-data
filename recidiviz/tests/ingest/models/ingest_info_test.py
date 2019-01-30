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

from recidiviz.ingest.models import ingest_info
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.models.ingest_info_pb2 import Person, Booking, Charge, \
    Hold, Arrest, Sentence, Bond


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

        person_fields_ignore = ['booking_ids', 'bookings']
        booking_fields_ignore = ['arrest_id', 'charge_ids', 'hold_ids',
                                 'arrest', 'charges', 'holds']
        charge_fields_ignore = ['bond_id', 'sentence_id', 'bond', 'sentence']

        _verify_fields(Person, ingest_info.Person(), person_fields_ignore)
        _verify_fields(Booking, ingest_info.Booking(), booking_fields_ignore)
        _verify_fields(Charge, ingest_info.Charge(), charge_fields_ignore)
        _verify_fields(Hold, ingest_info.Hold())
        _verify_fields(Arrest, ingest_info.Arrest())
        _verify_fields(Sentence, ingest_info.Sentence())
        _verify_fields(Bond, ingest_info.Bond())
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
            ingest_info.Person(),
            ingest_info.Person(bookings=[
                ingest_info.Booking(),
                ingest_info.Booking(arrest=ingest_info.Arrest(), charges=[
                    ingest_info.Charge(),
                    ingest_info.Charge(bond=ingest_info.Bond(),
                                       sentence=ingest_info.Sentence()),
                    ingest_info.Charge(bond=ingest_info.Bond(),
                                       sentence=ingest_info.Sentence(
                                           is_life='False'))
                ], holds=[
                    ingest_info.Hold(), ingest_info.Hold(hold_id=1)
                ])
            ])
        ])

        expected = IngestInfo(people=[
            ingest_info.Person(bookings=[
                ingest_info.Booking(
                    charges=[ingest_info.Charge(
                        sentence=ingest_info.Sentence(is_life='False'))],
                    holds=[ingest_info.Hold(hold_id=1)])])])
        self.assertEqual(ii.prune(), expected)
