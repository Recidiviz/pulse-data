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

from mock import patch, mock_open

from recidiviz.ingest.models import ingest_info, ingest_info_pb2
from recidiviz.ingest.scrape import constants, ingest_utils
from recidiviz.utils import regions

REGIONS = """
    regions:
        us_ny: 1
        us_pa: 2
        us_vt: 3
        us_pa_greene: 4
"""

class TestIngestUtils:
    """Tests for regions.py."""

    def setup_method(self, _test_method):
        regions.MANIFEST = None

    def teardown_method(self, _test_method):
        regions.MANIFEST = None

    @patch('builtins.open', mock_open(read_data=REGIONS))
    def test_validate_regions_one_ok(self):
        assert ingest_utils.validate_regions(['us_ny']) == ['us_ny']

    @patch('builtins.open', mock_open(read_data=REGIONS))
    def test_validate_regions_one_all(self):
        assert set(ingest_utils.validate_regions(['all'])) == {
            'us_ny',
            'us_pa',
            'us_vt',
            'us_pa_greene',
        }

    @patch('builtins.open', mock_open(read_data=REGIONS))
    def test_validate_regions_one_invalid(self):
        assert not ingest_utils.validate_regions(['ca_bc'])

    @patch('builtins.open', mock_open(read_data=REGIONS))
    def test_validate_regions_multiple_ok(self):
        assert ingest_utils.validate_regions(['us_pa', 'us_ny']) == ['us_pa',
                                                                     'us_ny']

    @patch('builtins.open', mock_open(read_data=REGIONS))
    def test_validate_regions_multiple_invalid(self):
        assert not ingest_utils.validate_regions(['us_pa', 'invalid'])

    @patch('builtins.open', mock_open(read_data=REGIONS))
    def test_validate_regions_multiple_all(self):
        assert set(ingest_utils.validate_regions(['us_pa', 'all'])) == {
            'us_ny',
            'us_pa',
            'us_vt',
            'us_pa_greene',
        }

    @patch('builtins.open', mock_open(read_data=REGIONS))
    def test_validate_regions_multiple_all_invalid(self):
        assert not ingest_utils.validate_regions(['all', 'invalid'])

    @patch('builtins.open', mock_open(read_data=REGIONS))
    def test_validate_regions_empty(self):
        assert ingest_utils.validate_regions([]) == []

    def test_validate_scrape_types_one_ok(self):
        assert ingest_utils.validate_scrape_types(
            [constants.ScrapeType.SNAPSHOT.value]) == \
            [constants.ScrapeType.SNAPSHOT]

    def test_validate_scrape_types_one_all(self):
        assert ingest_utils.validate_scrape_types(['all']) == [
            constants.ScrapeType.BACKGROUND, constants.ScrapeType.SNAPSHOT]

    def test_validate_scrape_types_one_invalid(self):
        assert not ingest_utils.validate_scrape_types(['When You Were Young'])

    def test_validate_scrape_types_multiple_ok(self):
        assert ingest_utils.validate_scrape_types(
            [constants.ScrapeType.BACKGROUND.value,
             constants.ScrapeType.SNAPSHOT.value]) == \
            [constants.ScrapeType.BACKGROUND, constants.ScrapeType.SNAPSHOT]

    def test_validate_scrape_types_multiple_invalid(self):
        assert not ingest_utils.validate_scrape_types(
            [constants.ScrapeType.BACKGROUND.value, 'invalid'])

    def test_validate_scrape_types_multiple_all(self):
        assert ingest_utils.validate_scrape_types(
            [constants.ScrapeType.BACKGROUND.value, 'all']) == \
            [constants.ScrapeType.BACKGROUND, constants.ScrapeType.SNAPSHOT]

    def test_validate_scrape_types_multiple_all_invalid(self):
        assert not ingest_utils.validate_scrape_types(['all', 'invalid'])

    def test_validate_scrape_types_empty(self):
        assert ingest_utils.validate_scrape_types(
            []) == [constants.ScrapeType.BACKGROUND]

    def test_convert_ingest_info_id_is_generated(self):
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

    def test_convert_ingest_info_id_is_not_generated(self):
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

    def test_convert_ingest_info_one_charge_to_one_bond(self):
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

    def test_convert_ingest_info_many_charge_to_one_bond(self):
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
