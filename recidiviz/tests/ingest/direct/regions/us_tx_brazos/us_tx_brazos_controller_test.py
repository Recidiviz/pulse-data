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

from mock import patch, Mock

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.direct.regions.us_tx_brazos.us_tx_brazos_controller \
    import UsTxBrazosController
from recidiviz.ingest.models.ingest_info import Arrest, Bond, Booking, Charge, \
    Hold, Person, IngestInfo
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.individual_ingest_test import IndividualIngestTest
from recidiviz.tests.ingest.direct.direct_ingest_util import \
    build_gcsfs_controller_for_tests, ingest_args_for_fixture_file
from recidiviz.utils import regions


FIXTURE_PATH_PREFIX = 'direct/regions/us_tx_brazos'
_ROSTER_PATH_CONTENTS = fixtures.as_string(FIXTURE_PATH_PREFIX,
                                           'daily_data.csv')
_FAKE_START_TIME = datetime.datetime(year=2019, month=1, day=2)


@patch('recidiviz.utils.metadata.project_id',
       Mock(return_value='recidiviz-staging'))
class UsTxBrazosControllerTest(IndividualIngestTest, TestCase):
    """Test Brazos direct ingest.
    """

    def testParse(self):
        controller = build_gcsfs_controller_for_tests(UsTxBrazosController,
                                                      FIXTURE_PATH_PREFIX,
                                                      run_async=False)

        args = ingest_args_for_fixture_file(controller, 'daily_data.csv')

        # pylint:disable=protected-access
        ingest_info = controller._parse(args, _ROSTER_PATH_CONTENTS)
        expected_info = IngestInfo(
            people=[
                Person(
                    person_id='12345',
                    birthdate='8/10/1987 12:00:00 AM',
                    gender='M ',
                    race='White',
                    place_of_residence='Brazos',
                    bookings=[
                        Booking(
                            booking_id='321',
                            admission_date='3/31/2019 12:00:00 AM',
                            custody_status=
                            'L. Pretrial State Jail Felons (SJF)',
                            facility='BCDC',
                            arrest=Arrest(
                                agency="Brazos County Sheriff's Office"),
                            charges=[
                                Charge(
                                    charge_id='1',
                                    offense_date='3/25/2019 12:00:00 AM',
                                    statute='481.121(B)(1) HSC',
                                    name='17.16 DET PEND ORD/POSS MARIJ <2OZ',
                                    degree='Class B Misdemeanor',
                                    charge_class='MIS',
                                    status='Intake',
                                    bond=Bond(
                                        bond_id='1',
                                        amount='2000.0000',
                                        bond_type='Surety Bond',
                                        status='Posted')),
                                Charge(
                                    charge_id='2',
                                    offense_date='3/25/2019 12:00:00 AM',
                                    statute='483.041 HSC',
                                    name=
                                    '17.16 DET PEND ORD/POSS DANGEROUS DRUG',
                                    degree='Class A Misdemeanor',
                                    charge_class='MIS',
                                    status='Intake',
                                    bond=Bond(
                                        bond_id='2',
                                        amount='4000.0000',
                                        bond_type='Surety Bond',
                                        status='Posted')),
                                Charge(
                                    charge_id='3',
                                    offense_date='1/20/2019 12:00:00 AM',
                                    statute='31.03(e)(2)(A)',
                                    name='THEFT PROP >=$100<$750',
                                    degree='Class B Misdemeanor',
                                    charge_class='MIS',
                                    status='Accepted'),
                                Charge(
                                    charge_id='4',
                                    offense_date='3/30/2019 12:00:00 AM',
                                    statute='30.02(C)(1) PC',
                                    name='BURGLARY OF BUILDING',
                                    degree='State Jail Felony',
                                    charge_class='FEL',
                                    status='Warrant Issued'),
                                Charge(
                                    charge_id='5',
                                    offense_date='3/25/2019 12:00:00 AM',
                                    statute='481.116(B) HSC',
                                    name='POSS CS PG 2 LESS THAN ONE GRAM',
                                    degree='State Jail Felony',
                                    charge_class='FEL',
                                    status='Warrant Issued'),
                                Charge(
                                    charge_id='6',
                                    offense_date='3/23/2019 12:00:00 AM',
                                    statute='30.02(C)(1) PC',
                                    name='BURGLARY OF BUILDING',
                                    degree='State Jail Felony',
                                    charge_class='FEL',
                                    status='Warrant Issued'),
                                Charge(
                                    charge_id='7',
                                    offense_date='3/24/2019 12:00:00 AM',
                                    statute='30.02(C)(1) PC',
                                    name='BURGLARY OF BUILDING',
                                    degree='State Jail Felony',
                                    charge_class='FEL',
                                    status='Warrant Issued')])]),
                Person(
                    person_id='23456',
                    birthdate='2/21/1982 12:00:00 AM',
                    gender='M ',
                    race='Black',
                    place_of_residence='Brazos',
                    bookings=[
                        Booking(
                            booking_id='432',
                            admission_date='5/14/2018 12:00:00 AM',
                            custody_status=
                            'L. Pretrial State Jail Felons (SJF)',
                            facility='BCDC',
                            arrest=Arrest(
                                agency="Brazos County Sheriff's Office"),
                            charges=[
                                Charge(
                                    charge_id='8',
                                    statute='NA',
                                    name='Parole Violation/Bl/Wrnt/tty',
                                    degree='Felony Unassigned',
                                    charge_class='FEL'),
                                Charge(
                                    charge_id='9',
                                    offense_date='5/14/2018 12:00:00 AM',
                                    statute='38.04(B)(1) PC',
                                    name='EVADING ARREST DET W/PREV CONVICTION',
                                    degree='State Jail Felony',
                                    charge_class='FEL',
                                    status='Defendant Indicted'),
                                Charge(
                                    charge_id='10',
                                    statute='NON REPORTABLE',
                                    name='Awaiting Trans To Dept State '
                                    'Health Services',
                                    degree='Not Applicable')],
                            holds=[
                                Hold(
                                    jurisdiction_name='TDC Hold')])])])

        region = regions.get_region('us_tx_brazos', is_direct_ingest=True)
        metadata = IngestMetadata(
            region.region_code,
            region.jurisdiction_id,
            _FAKE_START_TIME,
            controller.get_enum_overrides(),
        )

        self.validate_ingest(ingest_info, expected_info, metadata)
