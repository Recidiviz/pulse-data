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

from recidiviz.ingest import ingest_utils
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.models import ingest_info_pb2, ingest_info


def test_validate_regions_one_ok():
    assert ingest_utils.validate_regions(["us_ny"]) == ["us_ny"]


def test_validate_regions_one_all():
    assert set(ingest_utils.validate_regions(["all"])) == {
        'us_al_autauga',
        'us_al_cherokee',
        'us_al_dale',
        'us_al_dekalb',
        'us_al_fayette',
        'us_al_franklin',
        'us_al_jackson',
        'us_al_marion',
        'us_al_pike',
        'us_ar_boone',
        'us_ar_craighead',
        'us_ar_faulkner',
        "us_ar_garland",
        'us_ar_hempstead',
        'us_ar_jefferson',
        'us_ar_johnson',
        'us_ar_lonoke',
        'us_ar_marion',
        'us_ar_monroe',
        'us_ar_nevada',
        'us_ar_poinsett',
        'us_ar_saline',
        'us_ar_stone',
        "us_ar_van_buren",
        "us_co_mesa",
        "us_fl_bradford",
        "us_fl_columbia",
        "us_fl_hendry",
        "us_fl_martin",
        "us_fl_osceola",
        "us_ga_berrien",
        "us_ga_douglas",
        "us_ga_floyd",
        "us_ga_gwinnett",
        "us_ga_lumpkin",
        "us_in_jackson",
        "us_in_scott",
        "us_in_vigo",
        "us_ks_cherokee",
        "us_ks_jefferson",
        "us_ks_pratt",
        "us_mo_barry",
        "us_mo_cape_girardeau",
        "us_mo_johnson",
        "us_mo_lawrence",
        "us_mo_livingston",
        "us_mo_morgan",
        "us_mo_stone",
        "us_ms_clay",
        "us_ms_desoto",
        "us_ms_kemper",
        "us_ms_tunica",
        "us_mt_gallatin",
        "us_fl_nassau",
        "us_nc_alamance",
        "us_nc_buncombe",
        "us_nc_guilford",
        "us_nj_bergen",
        "us_ny",
        "us_ok_rogers",
        "us_pa",
        "us_pa_dauphin",
        "us_pa_greene",
        "us_tn_mcminn",
        "us_tx_brown",
        "us_tx_cochran",
        "us_tx_coleman",
        "us_tx_cooke",
        "us_tx_erath",
        "us_tx_freestone",
        "us_tx_hockley",
        "us_tx_hopkins",
        "us_tx_liberty",
        "us_tx_ochiltree",
        "us_tx_red_river",
        "us_tx_rusk",
        "us_tx_titus",
        "us_tx_upshur",
        "us_tx_van_zandt",
        "us_tx_wilson",
        "us_tx_wichita",
        "us_tx_young",
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
        'us_al_autauga',
        'us_al_cherokee',
        'us_al_dale',
        'us_al_dekalb',
        'us_al_fayette',
        'us_al_franklin',
        'us_al_jackson',
        'us_al_marion',
        'us_al_pike',
        'us_ar_boone',
        'us_ar_craighead',
        'us_ar_faulkner',
        "us_ar_garland",
        'us_ar_hempstead',
        'us_ar_jefferson',
        'us_ar_johnson',
        'us_ar_lonoke',
        'us_ar_marion',
        'us_ar_monroe',
        'us_ar_nevada',
        'us_ar_poinsett',
        'us_ar_saline',
        'us_ar_stone',
        "us_ar_van_buren",
        "us_co_mesa",
        "us_fl_bradford",
        "us_fl_columbia",
        "us_fl_hendry",
        "us_fl_martin",
        "us_fl_osceola",
        "us_ga_berrien",
        "us_ga_douglas",
        "us_ga_floyd",
        "us_ga_gwinnett",
        "us_ga_lumpkin",
        "us_in_jackson",
        "us_in_scott",
        "us_in_vigo",
        "us_ks_cherokee",
        "us_ks_jefferson",
        "us_ks_pratt",
        "us_mo_barry",
        "us_mo_cape_girardeau",
        "us_mo_johnson",
        "us_mo_lawrence",
        "us_mo_livingston",
        "us_mo_morgan",
        "us_mo_stone",
        "us_ms_clay",
        "us_ms_desoto",
        "us_ms_kemper",
        "us_ms_tunica",
        "us_mt_gallatin",
        "us_fl_nassau",
        "us_nc_alamance",
        "us_nc_buncombe",
        "us_nc_guilford",
        "us_nj_bergen",
        "us_ny",
        "us_ok_rogers",
        "us_pa",
        "us_pa_dauphin",
        "us_pa_greene",
        "us_tn_mcminn",
        "us_tx_brown",
        "us_tx_cochran",
        "us_tx_coleman",
        "us_tx_cooke",
        "us_tx_erath",
        "us_tx_freestone",
        "us_tx_hockley",
        "us_tx_hopkins",
        "us_tx_liberty",
        "us_tx_ochiltree",
        "us_tx_red_river",
        "us_tx_rusk",
        "us_tx_titus",
        "us_tx_upshur",
        "us_tx_van_zandt",
        "us_tx_wilson",
        "us_tx_wichita",
        "us_tx_young",
        "us_vt",
    }


def test_validate_regions_multiple_all_invalid():
    assert not ingest_utils.validate_regions(["all", "invalid"])


def test_validate_regions_empty():
    assert ingest_utils.validate_regions([]) == []


def test_validate_scrape_types_one_ok():
    assert ingest_utils.validate_scrape_types(
        [constants.ScrapeType.SNAPSHOT.value]) == \
           [constants.ScrapeType.SNAPSHOT]


def test_validate_scrape_types_one_all():
    assert ingest_utils.validate_scrape_types(["all"]) == [
        constants.ScrapeType.BACKGROUND, constants.ScrapeType.SNAPSHOT]


def test_validate_scrape_types_one_invalid():
    assert not ingest_utils.validate_scrape_types(["When You Were Young"])


def test_validate_scrape_types_multiple_ok():
    assert ingest_utils.validate_scrape_types(
        [constants.ScrapeType.BACKGROUND.value,
         constants.ScrapeType.SNAPSHOT.value]) == \
           [constants.ScrapeType.BACKGROUND, constants.ScrapeType.SNAPSHOT]


def test_validate_scrape_types_multiple_invalid():
    assert not ingest_utils.validate_scrape_types(
        [constants.ScrapeType.BACKGROUND.value, "invalid"])


def test_validate_scrape_types_multiple_all():
    assert ingest_utils.validate_scrape_types(
        [constants.ScrapeType.BACKGROUND.value, "all"]) == \
           [constants.ScrapeType.BACKGROUND, constants.ScrapeType.SNAPSHOT]


def test_validate_scrape_types_multiple_all_invalid():
    assert not ingest_utils.validate_scrape_types(["all", "invalid"])


def test_validate_scrape_types_empty():
    assert ingest_utils.validate_scrape_types(
        []) == [constants.ScrapeType.BACKGROUND]


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
