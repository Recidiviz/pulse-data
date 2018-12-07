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

"""Tests for ingest_info_utils"""

from recidiviz.ingest import ingest_info_utils
from recidiviz.ingest.models import ingest_info
from recidiviz.ingest.models import ingest_info_pb2


def test_id_is_generated():
    info = ingest_info.IngestInfo()
    person = info.create_person()
    person.surname = 'testname'
    booking = person.create_booking()

    expected_proto = ingest_info_pb2.IngestInfo()
    proto_person = expected_proto.people.add()
    proto_person.surname = 'testname'
    proto_person.person_id = str(id(person)) + '_generate'
    proto_booking = expected_proto.bookings.add()
    proto_booking.booking_id = str(id(booking)) + '_generate'
    proto_person.booking_ids.append(proto_booking.booking_id)

    proto = ingest_info_utils.convert_ingest_info_to_proto(info)
    assert proto == expected_proto

def test_id_is_not_generated():
    info = ingest_info.IngestInfo()
    person = info.create_person()
    person.person_id = 'id1'
    person.surname = 'testname'
    booking = person.create_booking()
    booking.booking_id = 'id2'
    booking.admission_date = 'testdate'

    expected_proto = ingest_info_pb2.IngestInfo()
    person = expected_proto.people.add()
    person.person_id = 'id1'
    person.surname = 'testname'
    person.booking_ids.append('id2')
    booking = expected_proto.bookings.add()
    booking.booking_id = 'id2'
    booking.admission_date = 'testdate'

    proto = ingest_info_utils.convert_ingest_info_to_proto(info)
    assert expected_proto == proto


def test_one_charge_to_one_bond():
    info = ingest_info.IngestInfo()
    person = info.create_person()
    person.person_id = 'id1'

    booking = person.create_booking()
    booking.booking_id = 'id1'
    charge = booking.create_charge()
    charge.charge_id = 'id1'
    bond1 = charge.create_bond()
    bond1.amount = '$1'
    charge = booking.create_charge()
    charge.charge_id = 'id2'
    bond2 = charge.create_bond()
    bond2.amount = '$1'

    expected_proto = ingest_info_pb2.IngestInfo()
    person = expected_proto.people.add()
    person.person_id = 'id1'
    person.booking_ids.append('id1')
    booking = expected_proto.bookings.add()
    booking.booking_id = 'id1'
    booking.charge_ids.extend(['id1', 'id2'])
    charge = expected_proto.charges.add()
    charge.charge_id = 'id1'
    proto_bond1 = expected_proto.bonds.add()
    proto_bond1.amount = '$1'
    proto_bond1.bond_id = str(id(bond1)) + '_generate'
    charge.bond_id = proto_bond1.bond_id
    charge = expected_proto.charges.add()
    charge.charge_id = 'id2'
    proto_bond2 = expected_proto.bonds.add()
    proto_bond2.amount = '$1'
    proto_bond2.bond_id = str(id(bond2)) + '_generate'
    charge.bond_id = proto_bond2.bond_id

    proto = ingest_info_utils.convert_ingest_info_to_proto(info)
    assert expected_proto == proto

def test_many_charge_to_one_bond():
    info = ingest_info.IngestInfo()
    person = info.create_person()
    person.person_id = 'id1'

    booking = person.create_booking()
    booking.booking_id = 'id1'
    charge = booking.create_charge()
    charge.charge_id = 'id1'
    bond1 = charge.create_bond()
    bond1.amount = '$1'
    charge = booking.create_charge()
    charge.charge_id = 'id2'
    charge.bond = bond1

    expected_proto = ingest_info_pb2.IngestInfo()
    person = expected_proto.people.add()
    person.person_id = 'id1'
    person.booking_ids.append('id1')
    booking = expected_proto.bookings.add()
    booking.booking_id = 'id1'
    booking.charge_ids.extend(['id1', 'id2'])
    charge = expected_proto.charges.add()
    charge.charge_id = 'id1'
    proto_bond = expected_proto.bonds.add()
    proto_bond.amount = '$1'
    proto_bond.bond_id = str(id(bond1)) + '_generate'
    charge.bond_id = proto_bond.bond_id
    charge = expected_proto.charges.add()
    charge.charge_id = 'id2'
    charge.bond_id = proto_bond.bond_id

    proto = ingest_info_utils.convert_ingest_info_to_proto(info)
    assert expected_proto == proto
