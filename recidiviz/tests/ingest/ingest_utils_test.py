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

"""Tests for ingest/ingest_utils.py."""

from recidiviz.ingest import ingest_utils, constants
from recidiviz.ingest.models import ingest_info_pb2, ingest_info


def test_validate_regions_one_ok():
    assert ingest_utils.validate_regions(["us_ny"]) == ["us_ny"]


def test_validate_regions_one_all():
    assert set(ingest_utils.validate_regions(["all"])) == {
        "us_ar_van_buren",
        "us_co_mesa",
        "us_fl_martin",
        "us_fl_osceola",
        "us_mo_stone",
        "us_mt_gallatin",
        "us_fl_nassau",
        "us_nc_guilford",
        "us_ny",
        "us_pa",
        "us_pa_dauphin",
        "us_pa_greene",
        "us_vt",
    }


def test_validate_regions_one_invalid():
    assert not ingest_utils.validate_regions(["ca_bc"])


def test_validate_regions_multiple_ok():
    assert ingest_utils.validate_regions(["us_pa", "us_ny"]) == ["us_pa",
                                                                 "us_ny"]


def test_validate_regions_multiple_invalid():
    assert not ingest_utils.validate_regions(["us_pa", "invalid"])


def test_validate_regions_multiple_all():
    assert set(ingest_utils.validate_regions(["us_pa", "all"])) == {
        "us_ar_van_buren",
        "us_co_mesa",
        "us_fl_martin",
        "us_fl_osceola",
        "us_mo_stone",
        "us_mt_gallatin",
        "us_fl_nassau",
        "us_nc_guilford",
        "us_ny",
        "us_pa",
        "us_pa_dauphin",
        "us_pa_greene",
        "us_vt",
    }


def test_validate_regions_multiple_all_invalid():
    assert not ingest_utils.validate_regions(["all", "invalid"])


def test_validate_regions_empty():
    assert ingest_utils.validate_regions([]) == []


def test_validate_scrape_types_one_ok():
    assert ingest_utils.validate_scrape_types(
        [constants.SNAPSHOT_SCRAPE]) == [constants.SNAPSHOT_SCRAPE]


def test_validate_scrape_types_one_all():
    assert ingest_utils.validate_scrape_types(["all"]) == [
        constants.BACKGROUND_SCRAPE, constants.SNAPSHOT_SCRAPE]


def test_validate_scrape_types_one_invalid():
    assert not ingest_utils.validate_scrape_types(["When You Were Young"])


def test_validate_scrape_types_multiple_ok():
    assert ingest_utils.validate_scrape_types(
        [constants.BACKGROUND_SCRAPE, constants.SNAPSHOT_SCRAPE]) == \
           [constants.BACKGROUND_SCRAPE, constants.SNAPSHOT_SCRAPE]


def test_validate_scrape_types_multiple_invalid():
    assert not ingest_utils.validate_scrape_types(
        [constants.BACKGROUND_SCRAPE, "invalid"])


def test_validate_scrape_types_multiple_all():
    assert ingest_utils.validate_scrape_types(
        [constants.BACKGROUND_SCRAPE, "all"]) == \
           [constants.BACKGROUND_SCRAPE, constants.SNAPSHOT_SCRAPE]


def test_validate_scrape_types_multiple_all_invalid():
    assert not ingest_utils.validate_scrape_types(["all", "invalid"])


def test_validate_scrape_types_empty():
    assert ingest_utils.validate_scrape_types(
        []) == [constants.BACKGROUND_SCRAPE]


def test_convert_ingest_info_id_is_generated():
    info = ingest_info.IngestInfo()
    person = info.create_person()
    person.surname = 'testname'
    booking = person.create_booking()

    expected_proto = ingest_info_pb2.IngestInfo()
    proto_person = expected_proto.people.add()
    proto_person.surname = 'testname'
    proto_person.person_id = str(id(person)) + '_GENERATE'
    proto_booking = expected_proto.bookings.add()
    proto_booking.booking_id = str(id(booking)) + '_GENERATE'
    proto_person.booking_ids.append(proto_booking.booking_id)

    proto = ingest_utils.convert_ingest_info_to_proto(info)
    assert proto == expected_proto


def test_convert_ingest_info_id_is_not_generated():
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

    proto = ingest_utils.convert_ingest_info_to_proto(info)
    assert expected_proto == proto


def test_convert_ingest_info_one_charge_to_one_bond():
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
    proto_bond1.bond_id = str(id(bond1)) + '_GENERATE'
    charge.bond_id = proto_bond1.bond_id
    charge = expected_proto.charges.add()
    charge.charge_id = 'id2'
    proto_bond2 = expected_proto.bonds.add()
    proto_bond2.amount = '$1'
    proto_bond2.bond_id = str(id(bond2)) + '_GENERATE'
    charge.bond_id = proto_bond2.bond_id

    proto = ingest_utils.convert_ingest_info_to_proto(info)
    assert expected_proto == proto

def test_convert_ingest_info_many_charge_to_one_bond():
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
    proto_bond.bond_id = str(id(bond1)) + '_GENERATE'
    charge.bond_id = proto_bond.bond_id
    charge = expected_proto.charges.add()
    charge.charge_id = 'id2'
    charge.bond_id = proto_bond.bond_id

    proto = ingest_utils.convert_ingest_info_to_proto(info)
    assert expected_proto == proto
