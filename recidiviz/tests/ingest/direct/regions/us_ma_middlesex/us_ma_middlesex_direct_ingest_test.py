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
# =============================================================================
"""Tests for the direct ingest parser.py."""
import datetime
from unittest import TestCase

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.direct.regions.us_ma_middlesex.us_ma_middlesex_parser \
    import UsMaMiddlesexParser
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.individual_ingest_test import IndividualIngestTest
from recidiviz.utils import regions

_ROSTER_JSON = fixtures.as_dict('direct/regions/us_ma_middlesex',
                                'roster.json')
_FAKE_START_TIME = datetime.datetime(year=2019, month=1, day=2)


class UsMaMiddlesexDirectIngestParser(IndividualIngestTest, TestCase):
    def testParse(self):
        region = regions.get_region('us_ma_middlesex', is_direct_ingest=True)
        controller = region.get_ingestor()

        metadata = IngestMetadata(
            region.region_code,
            region.jurisdiction_id,
            _FAKE_START_TIME,
            controller.get_enum_overrides())

        ingest_info = UsMaMiddlesexParser().parse(_ROSTER_JSON)

        expected_info = IngestInfo()
        p1 = expected_info.create_person(
            person_id='12345       ', birthdate='1111-01-01 00:00:00.000',
            gender='M', ethnicity='HISPANIC',
            place_of_residence='123 ST DORCHESTER MA 01234     ')

        b1 = p1.create_booking(
            booking_id='1.0',
            admission_date='2017-01-01 00:00:00.000',
            admission_reason='BAIL MITTIMUS',
            facility='MAIN      ')
        b1.create_charge(
            charge_id='1245.0', statute='90/24/K',
            name='OUI-LIQUOR, 2ND OFFENSE c90 ss24',
            case_number='111.0', court_type='Middlesex SC (81)',
            charge_notes='Other')
        b1.create_charge(
            charge_id='1502.0',
            offense_date='2017-01-28 00:00:00',
            statute='90/23/J',
            name='OUI while license suspended for OUI',
            case_number='222.0', court_type='Middlesex SC (81)',
            charge_notes='Drug or Alcohol', status='DISMISSED').create_bond(
            bond_id='12345.0')
        b1.create_hold(hold_id='00000.0', jurisdiction_name='Middlesex SC (81)')

        p2 = expected_info.create_person(
            person_id='10472       ', birthdate='1111-02-02 00:00:00.000',
            gender='M', race='BLACK or AFRICAN AMERICAN',
            place_of_residence='456 ST MALDEN MA 98765      ')
        b2 = p2.create_booking(
            booking_id='333.0',
            admission_date='2018-02-02 00:00:00.000',
            admission_reason='SENTENCE MITTIMUS',
            facility='MAIN      ')
        b2.create_arrest(agency='Cambridge PD')
        b2.create_charge(charge_id='12341234.0', statute='269/10/J',
                         name='FIREARM, CARRY WITHOUT LICENSE c269 ss10',
                         case_number='555.0', charge_notes='Other',
                         court_type='Cambridge DC (52)')

        self.validate_ingest(ingest_info, expected_info, metadata)
