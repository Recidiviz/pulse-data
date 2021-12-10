# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for ingest/models/serialization.py"""
import unittest

from mock import Mock, patch

from recidiviz.common import common_utils
from recidiviz.ingest.models import ingest_info, ingest_info_pb2, serialization


class TestSerialization(unittest.TestCase):
    """Tests for serialization"""

    def _create_generated_id(self) -> str:
        self.counter += 1
        return str(self.counter) + common_utils.GENERATED_ID_SUFFIX

    def setUp(self) -> None:
        self.counter = 0

    @patch("recidiviz.common.common_utils.create_generated_id")
    def test_convert_ingest_info_id_is_generated(self, mock_create: Mock) -> None:
        mock_create.side_effect = self._create_generated_id
        info = ingest_info.IngestInfo()
        person = info.create_person()
        person.surname = "testname"
        person.create_booking()

        expected_proto = ingest_info_pb2.IngestInfo()
        proto_person = expected_proto.people.add()
        proto_person.surname = "testname"
        proto_person.person_id = "1_GENERATE"
        proto_booking = expected_proto.bookings.add()
        proto_booking.booking_id = "2_GENERATE"
        proto_person.booking_ids.append(proto_booking.booking_id)

        proto = serialization.convert_ingest_info_to_proto(info)
        assert proto == expected_proto

        info_back = serialization.convert_proto_to_ingest_info(proto)
        assert info_back == info

    def test_convert_ingest_info_id_is_not_generated(self) -> None:
        info = ingest_info.IngestInfo()
        person = info.create_person()
        person.person_id = "id1"
        person.surname = "testname"
        booking = person.create_booking()
        booking.booking_id = "id2"
        booking.admission_date = "testdate"

        expected_proto = ingest_info_pb2.IngestInfo()
        proto_person = expected_proto.people.add()
        proto_person.person_id = "id1"
        proto_person.surname = "testname"
        proto_person.booking_ids.append("id2")
        proto_booking = expected_proto.bookings.add()
        proto_booking.booking_id = "id2"
        proto_booking.admission_date = "testdate"

        proto = serialization.convert_ingest_info_to_proto(info)
        assert expected_proto == proto

        info_back = serialization.convert_proto_to_ingest_info(proto)
        assert info_back == info

    @patch("recidiviz.common.common_utils.create_generated_id")
    def test_convert_ingest_info_one_charge_to_one_bond(
        self, mock_create: Mock
    ) -> None:
        mock_create.side_effect = self._create_generated_id
        info = ingest_info.IngestInfo()
        person = info.create_person()
        person.person_id = "id1"

        booking = person.create_booking()
        booking.booking_id = "id1"
        charge = booking.create_charge()
        charge.charge_id = "id1"
        bond1 = charge.create_bond()
        bond1.amount = "$1"
        charge = booking.create_charge()
        charge.charge_id = "id2"
        bond2 = charge.create_bond()
        bond2.amount = "$1"

        expected_proto = ingest_info_pb2.IngestInfo()
        proto_person = expected_proto.people.add()
        proto_person.person_id = "id1"
        proto_person.booking_ids.append("id1")
        proto_booking = expected_proto.bookings.add()
        proto_booking.booking_id = "id1"
        proto_booking.charge_ids.extend(["id1", "id2"])
        proto_charge = expected_proto.charges.add()
        proto_charge.charge_id = "id1"
        proto_bond1 = expected_proto.bonds.add()
        proto_bond1.amount = "$1"
        proto_bond1.bond_id = "1_GENERATE"
        proto_charge.bond_id = proto_bond1.bond_id
        proto_charge = expected_proto.charges.add()
        proto_charge.charge_id = "id2"
        proto_bond2 = expected_proto.bonds.add()
        proto_bond2.amount = "$1"
        proto_bond2.bond_id = "2_GENERATE"
        proto_charge.bond_id = proto_bond2.bond_id

        proto = serialization.convert_ingest_info_to_proto(info)
        assert expected_proto == proto

        info_back = serialization.convert_proto_to_ingest_info(proto)
        assert info_back == info

    @patch("recidiviz.common.common_utils.create_generated_id")
    def test_convert_ingest_info_many_charge_to_one_bond(
        self, mock_create: Mock
    ) -> None:
        mock_create.side_effect = self._create_generated_id
        info = ingest_info.IngestInfo()
        person = info.create_person()
        person.person_id = "id1"

        booking = person.create_booking()
        booking.booking_id = "id1"
        charge = booking.create_charge()
        charge.charge_id = "id1"
        bond1 = charge.create_bond()
        bond1.amount = "$1"
        charge = booking.create_charge()
        charge.charge_id = "id2"
        charge.bond = bond1

        expected_proto = ingest_info_pb2.IngestInfo()
        proto_person = expected_proto.people.add()
        proto_person.person_id = "id1"
        proto_person.booking_ids.append("id1")
        proto_booking = expected_proto.bookings.add()
        proto_booking.booking_id = "id1"
        proto_booking.charge_ids.extend(["id1", "id2"])
        proto_charge = expected_proto.charges.add()
        proto_charge.charge_id = "id1"
        proto_bond = expected_proto.bonds.add()
        proto_bond.amount = "$1"
        proto_bond.bond_id = "1_GENERATE"
        proto_charge.bond_id = proto_bond.bond_id
        proto_charge = expected_proto.charges.add()
        proto_charge.charge_id = "id2"
        proto_charge.bond_id = proto_bond.bond_id

        proto = serialization.convert_ingest_info_to_proto(info)
        assert len(proto.bonds) == 1
        assert expected_proto == proto

        info_back = serialization.convert_proto_to_ingest_info(proto)
        assert info_back == info

    def test_serializable(self) -> None:
        info = ingest_info.IngestInfo()
        person = info.create_person()
        person.person_id = "id1"

        booking = person.create_booking()
        booking.booking_id = "id1"
        charge = booking.create_charge()
        charge.charge_id = "id1"
        bond1 = charge.create_bond()
        bond1.amount = "$1"
        charge = booking.create_charge()
        charge.charge_id = "id2"
        bond2 = charge.create_bond()
        bond2.amount = "$1"

        converted_info = serialization.ingest_info_from_serializable(
            serialization.ingest_info_to_serializable(info)
        )

        assert converted_info == info
