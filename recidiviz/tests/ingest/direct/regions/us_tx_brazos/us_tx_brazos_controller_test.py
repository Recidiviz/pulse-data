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
from typing import Type

from mock import patch, Mock
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.regions.us_tx_brazos.us_tx_brazos_controller \
    import UsTxBrazosController
from recidiviz.ingest.models.ingest_info import Arrest, Bond, Booking, Charge, \
    Hold, Person, IngestInfo
from recidiviz.persistence.database.base_schema import JailsBase
from recidiviz.persistence.persistence import OVERALL_THRESHOLD, ENUM_THRESHOLD, ENTITY_MATCHING_THRESHOLD, \
    DATABASE_INVARIANT_THRESHOLD
from recidiviz.tests.ingest.direct.direct_ingest_util import \
    path_for_fixture_file, process_task_queues
from recidiviz.tests.ingest.direct import fixture_util
from recidiviz.tests.ingest.direct.regions.base_direct_ingest_controller_tests \
    import BaseDirectIngestControllerTests
from recidiviz.tests.utils.individual_ingest_test import IndividualIngestTest
from recidiviz.utils import regions

_FAKE_START_TIME = datetime.datetime(year=2019, month=1, day=2)


@patch('recidiviz.utils.metadata.project_id',
       Mock(return_value='recidiviz-staging'))
class UsTxBrazosControllerTest(IndividualIngestTest,
                               BaseDirectIngestControllerTests):
    """Test Brazos direct ingest.
    """

    @classmethod
    def region_code(cls) -> str:
        return 'us_tx_brazos'

    @classmethod
    def controller_cls(cls) -> Type[GcsfsDirectIngestController]:
        return UsTxBrazosController

    @classmethod
    def schema_base(cls) -> DeclarativeMeta:
        return JailsBase

    def setUp(self) -> None:
        super().setUp()

        # Set entity matching error threshold to a diminishingly small number
        # for tests. We cannot set it to 0 because we throw when errors *equal*
        # the error threshold.
        self.entity_matching_error_threshold_patcher = patch.dict(
            'recidiviz.persistence.persistence.SYSTEM_TYPE_TO_ERROR_THRESHOLD',
            {SystemLevel.COUNTY: {
                OVERALL_THRESHOLD: 0,
                ENUM_THRESHOLD: 0,
                ENTITY_MATCHING_THRESHOLD: 0,
                DATABASE_INVARIANT_THRESHOLD: 0,
            }})
        self.entity_matching_error_threshold_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()

        self.entity_matching_error_threshold_patcher.stop()

    def testParse(self):
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
                            booking_id='321 (Individual ID: 12345)',
                            admission_date='3/31/2019 12:00:00 AM',
                            custody_status=
                            'L. Pretrial State Jail Felons (SJF)',
                            facility='BCDC',
                            arrest=Arrest(
                                agency="College Station Police Department"),
                            charges=[
                                Charge(
                                    charge_id='1',
                                    offense_date='3/25/2019 12:00:00 AM',
                                    statute='481.121(B)(1) HSC',
                                    name='POSS MARIJ <2OZ',
                                    charge_notes='17.16 DET PEND ORD',
                                    level='B',
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
                                    name='POSS DANGEROUS DRUG',
                                    charge_notes='17.16 DET PEND ORD',
                                    level='A',
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
                                    level='B',
                                    charge_class='MIS',
                                    status='Accepted'),
                                Charge(
                                    charge_id='4',
                                    offense_date='3/30/2019 12:00:00 AM',
                                    statute='30.02(C)(1) PC',
                                    name='BURGLARY OF BUILDING',
                                    level='State Jail Felony',
                                    charge_class='FEL',
                                    status='Warrant Issued'),
                                Charge(
                                    charge_id='5',
                                    offense_date='3/25/2019 12:00:00 AM',
                                    statute='481.116(B) HSC',
                                    name='POSS CS PG 2 LESS THAN ONE GRAM',
                                    level='State Jail Felony',
                                    charge_class='FEL',
                                    status='Warrant Issued'),
                                Charge(
                                    charge_id='6',
                                    offense_date='3/23/2019 12:00:00 AM',
                                    statute='30.02(C)(1) PC',
                                    name='BURGLARY OF BUILDING',
                                    level='State Jail Felony',
                                    charge_class='FEL',
                                    status='Warrant Issued'),
                                Charge(
                                    charge_id='7',
                                    offense_date='3/24/2019 12:00:00 AM',
                                    statute='30.02(C)(1) PC',
                                    name='BURGLARY OF BUILDING',
                                    level='State Jail Felony',
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
                            booking_id='432 (Individual ID: 23456)',
                            admission_date='5/14/2018 12:00:00 AM',
                            custody_status=
                            'L. Pretrial State Jail Felons (SJF)',
                            facility='BCDC',
                            arrest=Arrest(
                                agency="Unlisted"),
                            charges=[
                                Charge(
                                    charge_id='8',
                                    name='Parole Violation/Bl/Wrnt/tty',
                                    degree='Felony Unassigned',
                                    charge_class='FEL'),
                                Charge(
                                    charge_id='9',
                                    offense_date='5/14/2018 12:00:00 AM',
                                    statute='38.04(B)(1) PC',
                                    name='EVADING ARREST DET W/PREV CONVICTION',
                                    level='State Jail Felony',
                                    charge_class='FEL',
                                    status='Defendant Indicted'),
                                Charge(
                                    charge_id='10',
                                    statute='NON REPORTABLE',
                                    name='Awaiting Trans To Dept State '
                                         'Health Services',
                                    degree='Not Applicable',
                                    bond=Bond(bond_id='2', amount='20'))],
                            holds=[
                                Hold(
                                    jurisdiction_name='TDC Hold')])])])

        ingest_info = self.run_parse_file_test(
            expected_info, 'VERABrazosJailData_01012019_115703')

        region = regions.get_region(self.region_code(), is_direct_ingest=True)
        metadata = IngestMetadata(
            region.region_code,
            region.jurisdiction_id,
            _FAKE_START_TIME,
            self.controller.get_enum_overrides(),
        )

        self.validate_ingest(ingest_info, expected_info, metadata)

    def test_run_full_ingest_all_files(self):
        file_tags = sorted(self.controller.get_file_tag_rank_list())
        file_path = path_for_fixture_file(
            self.controller, 'VERABrazosJailData_01012019_115703.csv', False)
        fixture_util.add_direct_ingest_path(self.controller.fs.gcs_file_system, file_path)
        process_task_queues(self, self.controller, file_tags)
