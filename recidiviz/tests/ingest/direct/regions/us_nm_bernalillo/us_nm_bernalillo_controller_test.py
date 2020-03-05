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

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.regions.us_nm_bernalillo.\
    us_nm_bernalillo_controller import UsNmBernalilloController
from recidiviz.ingest.models.ingest_info import Arrest, Bond, Booking, Charge, \
    Hold, Person, IngestInfo
from recidiviz.persistence.database.base_schema import JailsBase
from recidiviz.tests.ingest.direct.direct_ingest_util import \
    path_for_fixture_file, process_task_queues
from recidiviz.tests.ingest.direct.regions.base_direct_ingest_controller_tests \
    import BaseDirectIngestControllerTests
from recidiviz.tests.utils.individual_ingest_test import IndividualIngestTest
from recidiviz.utils import regions

_FAKE_START_TIME = datetime.datetime(year=2019, month=1, day=2)


@patch('recidiviz.utils.metadata.project_id',
       Mock(return_value='recidiviz-staging'))
class UsNmBernalilloControllerTest(IndividualIngestTest,
                                   BaseDirectIngestControllerTests):
    """Test Bernco direct ingest.
    """

    @classmethod
    def region_code(cls) -> str:
        return 'us_nm_bernalillo'

    @classmethod
    def controller_cls(cls) -> Type[GcsfsDirectIngestController]:
        return UsNmBernalilloController

    @classmethod
    def schema_base(cls) -> DeclarativeMeta:
        return JailsBase

    def testParse(self):
        expected_info = IngestInfo(
            people=[
                Person(
                    person_id='100041685',
                    gender='M',
                    age='41',
                    race='AMERICAN INDIAN',
                    bookings=[Booking(
                        booking_id='130877687',
                        admission_date='02/27/2020 14:51',
                        custody_status='IN CUSTODY',
                        facility='BERNALILLO COUNTY METRO DETENTION CENTER',
                        arrest=Arrest(
                            agency='/BSO',
                        ),
                        charges=[
                            Charge(
                                offense_date='02/27/2020',
                                name='FAIL TO COMPLY',
                                case_number='D202CR201802134',
                            ),
                            Charge(
                                offense_date='02/27/2020',
                                name='AGGRAVATED DWI-3',
                                case_number='D202CR201802134',
                            ),
                        ]
                    )]
                ),
                Person(
                    person_id='100044962',
                    gender='M',
                    age='42',
                    race='HISPANIC',
                    bookings=[Booking(
                        booking_id='130847497',
                        admission_date='12/18/2018 01:21',
                        custody_status='IN CUSTODY',
                        facility='BERNALILLO COUNTY METRO DETENTION CENTER',
                        arrest=Arrest(
                            agency='/APD/BCDC',
                        ),
                        charges=[
                            Charge(
                                offense_date='01/07/2019',
                                name='GRAND JURY INDICTMENT',
                                case_number='D202CR2019000032',
                            ),
                            Charge(
                                offense_date='01/07/2019',
                                name='AGG BATTERY W/DEADLY WEAPON',
                                case_number='D202CR2019000032',
                            ),
                        ]
                    )]
                ),
                Person(
                    person_id='100265415',
                    gender='M',
                    age='35',
                    race='WHITE',
                    bookings=[Booking(
                        booking_id='130877954',
                        admission_date='03/02/2020 18:54',
                        custody_status='IN CUSTODY',
                        facility='BERNALILLO COUNTY METRO DETENTION CENTER',
                        arrest=Arrest(
                            agency='/APD',
                        ),
                        charges=[
                            Charge(
                                bond=Bond(amount='0',
                                          bond_type='To be set by Judge')
                            ),
                        ]
                    )]
                )
            ]
        )

        ingest_info = self.run_parse_file_test(expected_info,
                                               'MDC_VERA_20200303_01')

        region = regions.get_region(self.region_code(), is_direct_ingest=True)
        metadata = IngestMetadata(
            region.region_code,
            region.jurisdiction_id,
            _FAKE_START_TIME,
            self.controller.get_enum_overrides(),
        )

        self.validate_ingest(ingest_info, expected_info, metadata)

    def test_run_full_ingest_all_files(self):
        # pylint:disable=protected-access
        file_tags = sorted(self.controller._get_file_tag_rank_list())
        file_path = path_for_fixture_file(
            self.controller, 'MDC_VERA_20200303_01.csv', False)
        self.controller.fs.test_add_path(file_path)
        process_task_queues(self, self.controller, file_tags)
