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
"""Tests the TN `OffenderMovementIncarcerationPeriod` view logic."""
from typing import Any, List, Tuple

import pandas as pd
from mock import Mock, patch
from more_itertools import one
from pandas.testing import assert_frame_equal

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.ingest.direct.query_utils import get_region_raw_file_config
from recidiviz.tests.big_query.view_test_util import BaseViewTest
from recidiviz.utils.regions import get_region

STATE_CODE = StateCode.US_TN.value


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class OffenderMovementIncarcerationPeriodTest(BaseViewTest):
    """Tests the TN `OffenderMovementIncarcerationPeriod` query functionality."""

    def setUp(self) -> None:
        super().setUp()
        view_builders = DirectIngestPreProcessedIngestViewCollector(
            get_region(STATE_CODE, is_direct_ingest=True), []
        ).collect_view_builders()
        self.view_builder = one(
            view
            for view in view_builders
            if view.file_tag == "OffenderMovementIncarcerationPeriod"
        )

        # All columns need to be in lowercase.
        self.expected_result_columns = [
            "offenderid",
            "startdatetime",
            "enddatetime",
            "site",
            "sitetype",
            "startmovementtype",
            "startmovementreason",
            "endmovementtype",
            "endmovementreason",
            "incarcerationperiodsequencenumber",
        ]

    def run_test(
        self,
        offender_name: List[Tuple[Any, ...]],
        offender_attribute: List[Tuple[Any, ...]],
        offender_movement: List[Tuple[Any, ...]],
        site: List[Tuple[Any, ...]],
        expected_output: List[List[Any]],
    ) -> None:
        """Runs a test that executes the OffenderMovementIncarcerationPeriod query given the provided
        input rows.
        """

        # Arrange
        raw_file_configs = get_region_raw_file_config(STATE_CODE).raw_file_configs

        self.create_mock_raw_file(
            region_code=STATE_CODE,
            file_config=raw_file_configs["OffenderName"],
            mock_data=offender_name,
        )

        self.create_mock_raw_file(
            region_code=STATE_CODE,
            file_config=raw_file_configs["OffenderAttributes"],
            mock_data=offender_attribute,
        )

        self.create_mock_raw_file(
            region_code=STATE_CODE,
            file_config=raw_file_configs["OffenderMovement"],
            mock_data=offender_movement,
        )

        self.create_mock_raw_file(
            region_code=STATE_CODE,
            file_config=raw_file_configs["Site"],
            mock_data=site,
        )

        # Act
        results = self.query_raw_data_view_for_builder(
            self.view_builder, dimensions=self.expected_result_columns
        )

        # Assert
        expected = pd.DataFrame(expected_output, columns=self.expected_result_columns)
        expected = expected.set_index(self.expected_result_columns)
        print(expected)
        print(results)
        assert_frame_equal(expected, results)

    def test_offender_movement_incarceration_period_simple(self) -> None:
        self.run_test(
            offender_name=[
                (
                    "12345678",  # OffenderID
                    "0",  # SequenceNumber
                    "FIRST",  # FirstName
                    "MIDDLE",  # MiddleName
                    "LAST",  # LastName
                    "",  # Suffix
                    "O",  # NameType
                    "INAC",  # OffenderStatus
                    "",  # ActualSiteID
                    "W",  # Race
                    "M",  # Sex
                    "1998-12-04 00:00:00",  # BirthDate
                    "",  # SocialSecurityNumber
                    "",  # STGNicknameFLag
                    "AB12345",  # LastUpdateUserID
                    "2019-08-02 08:49:21.161200",  # LastUpdateDate
                )
            ],
            offender_attribute=[
                (
                    "12345678",  # OffenderID
                    "0",  # Weight
                    "0",  # HeightFeet
                    "0",  # HeightInches
                    "",  # EyeColor
                    "",  # HairColor
                    "",  # Complexion
                    "M",  # MaritalStatus
                    "TX",  # DriverLicenseState
                    "1234567",  # DriverLicenseNumber
                    "",  # ScarsMarksTattoos
                    "52",  # Religion
                    "",  # BirthCounty
                    "IN",  # BirthState
                    "",  # NCICFingerprintID
                    "",  # TennesseeFingerprintID
                    "",  # DeathType
                    None,  # DeathDate
                    "",  # DeathLocation
                    "",  # DisposalType
                    "",  # DisposalDate
                    "",  # DisposalLocation
                    "",  # DeathPostedByStaffID
                    "ABCDEF12",  # OBSCISID
                    None,  # RestoreDate
                    "",  # DeathSiteID
                    "",  # DeathCertificateFlag
                    "Y",  # OldFBINumber
                    "",  # OldSTGAffiliation
                    "",  # OldSTG
                    "",  # OldSTGComments
                    "",  # CitizenshipCountry
                    "",  # BirthPlace
                    "",  # AlienID
                    "",  # FBINumber
                    "",  # SIDNumber
                    "29301",  # LastUpdateUserID
                    "2020-02-04 12:43:43.823350",  # LastUpdateDate
                )
            ],
            offender_movement=[
                # New Admission to jail.
                (
                    "12345678",  # OffenderID
                    "2020-12-30 03:30:00.000000",  # MovementDateTime
                    "CTFA",  # MovementType
                    "NEWAD",  # MovementReason
                    '"019 "',  # FromLocationID
                    '"024   "',  # ToLocationID
                    "D",  # ArrivalDepartureFlag
                    '"AB1234 "',  # LastUpdateUserID
                    "2020-12-30 15:22:04.323930",  # LastUpdateDate
                ),
                # Transfer from one jail facility to another.
                (
                    "12345678",  # OffenderID
                    "2021-05-15 10:30:00.000000",  # MovementDateTime
                    "FAFA",  # MovementType
                    "INCIB",  # MovementReason
                    '"024   "',  # FromLocationID
                    '"088   "',  # ToLocationID
                    "D",  # ArrivalDepartureFlag
                    '"AB1234 "',  # LastUpdateUserID
                    "2020-05-15 09:20:05.212820",  # LastUpdateDate
                ),
            ],
            site=[
                (
                    "088",  # SiteID
                    "VAN BUREN COUNTY JAIL",  # SiteName
                    "JA",  # SiteType
                    "A",  # Status,
                    None,  # StatusDate
                    None,  # AddressLine1
                    None,  # AddressLine2
                    None,  # AddressCity
                    None,  # AddressState
                    None,  # AddressZip
                    None,  # PhoneNumber
                    None,  # SecurityLevel
                    "V",  # Region
                    None,  # Division
                    None,  # District
                    "TDOC",  # Agency
                    None,  # NCICAgencyID
                    None,  # InChargeStaffID
                    "B",  # SexTypesAllowed
                    None,  # ContactPerson
                    None,  # LastUpdateUserID
                    None,  # LastUpdateDate
                ),
                (
                    "024",  # SiteID
                    "FAYETTE COUNTY JAIL",  # SiteName
                    "JA",  # SiteType
                    "A",  # Status,
                    None,  # StatusDate
                    None,  # AddressLine1
                    None,  # AddressLine2
                    None,  # AddressCity
                    None,  # AddressState
                    None,  # AddressZip
                    None,  # PhoneNumber
                    None,  # SecurityLevel
                    "R",  # Region
                    None,  # Division
                    None,  # District
                    "TDOC",  # Agency
                    None,  # NCICAgencyID
                    None,  # InChargeStaffID
                    "B",  # SexTypesAllowed
                    None,  # ContactPerson
                    None,  # LastUpdateUserID
                    None,  # LastUpdateDate
                ),
            ],
            expected_output=[
                [
                    "12345678",  # OffenderID
                    "2020-12-30 03:30:00.000000",  # StartDateTime
                    "2021-05-15 10:30:00.000000",  # EndDateTime
                    "024",  # Site
                    "JA",  # SiteType
                    "CTFA",  # StartMovementType
                    "NEWAD",  # StartMovementReason
                    "FAFA",  # EndMovementInfo
                    "INCIB",  # EndMovementReaso
                    1,  # IncarcerationPeriodSequenceNumber
                ],
                [
                    "12345678",  # OffenderID
                    "2021-05-15 10:30:00.000000",  # StartDateTime
                    "",  # EndDateTime
                    "088",  # Site
                    "JA",  # SiteType
                    "FAFA",  # StartMovementType
                    "INCIB",  # StartMovementReason
                    "",  # EndMovementType
                    "",  # EndMovementReason
                    2,  # IncarcerationPeriodSequenceNumber
                ],
            ],
        )

    def test_offender_movement_incarceration_period_with_death_date(self) -> None:
        self.run_test(
            offender_name=[
                (
                    "12345678",  # OffenderID
                    "0",  # SequenceNumber
                    "FIRST",  # FirstName
                    "MIDDLE",  # MiddleName
                    "LAST",  # LastName
                    "",  # Suffix
                    "O",  # NameType
                    "INAC",  # OffenderStatus
                    "",  # ActualSiteID
                    "W",  # Race
                    "M",  # Sex
                    "1998-12-04 00:00:00",  # BirthDate
                    "",  # SocialSecurityNumber
                    "",  # STGNicknameFLag
                    "AB12345",  # LastUpdateUserID
                    "2019-08-02 08:49:21.161200",  # LastUpdateDate
                )
            ],
            offender_attribute=[
                (
                    "12345678",  # OffenderID
                    "0",  # Weight
                    "0",  # HeightFeet
                    "0",  # HeightInches
                    "",  # EyeColor
                    "",  # HairColor
                    "",  # Complexion
                    "M",  # MaritalStatus
                    "TX",  # DriverLicenseState
                    "1234567",  # DriverLicenseNumber
                    "",  # ScarsMarksTattoos
                    "52",  # Religion
                    "",  # BirthCounty
                    "IN",  # BirthState
                    "",  # NCICFingerprintID
                    "",  # TennesseeFingerprintID
                    "",  # DeathType
                    "2021-06-01 10:30:00.000000",  # DeathDate
                    "",  # DeathLocation
                    "",  # DisposalType
                    "",  # DisposalDate
                    "",  # DisposalLocation
                    "",  # DeathPostedByStaffID
                    "ABCDEF12",  # OBSCISID
                    "",  # RestoreDate
                    "",  # DeathSiteID
                    "",  # DeathCertificateFlag
                    "Y",  # OldFBINumber
                    "",  # OldSTGAffiliation
                    "",  # OldSTG
                    "",  # OldSTGComments
                    "",  # CitizenshipCountry
                    "",  # BirthPlace
                    "",  # AlienID
                    "",  # FBINumber
                    "",  # SIDNumber
                    "29301",  # LastUpdateUserID
                    "2020-05-20 12:43:43.823350",  # LastUpdateDate
                )
            ],
            offender_movement=[
                # New Admission to jail.
                (
                    "12345678",  # OffenderID
                    "2020-12-30 03:30:00.000000",  # MovementDateTime
                    "CTFA",  # MovementType
                    "NEWAD",  # MovementReason
                    '"019 "',  # FromLocationID
                    '"024   "',  # ToLocationID
                    "D",  # ArrivalDepartureFlag
                    '"AB1234 "',  # LastUpdateUserID
                    "2020-12-30 15:22:04.323930",  # LastUpdateDate
                ),
                # Transfer from one jail facility to another.
                (
                    "12345678",  # OffenderID
                    "2021-05-15 10:30:00.000000",  # MovementDateTime
                    "FAFA",  # MovementType
                    "INCIB",  # MovementReason
                    '"024   "',  # FromLocationID
                    '"088   "',  # ToLocationID
                    "D",  # ArrivalDepartureFlag
                    '"AB1234 "',  # LastUpdateUserID
                    "2020-05-15 09:20:05.212820",  # LastUpdateDate
                ),
            ],
            site=[
                (
                    "088",  # SiteID
                    "VAN BUREN COUNTY JAIL",  # SiteName
                    "JA",  # SiteType
                    "A",  # Status,
                    None,  # StatusDate
                    None,  # AddressLine1
                    None,  # AddressLine2
                    None,  # AddressCity
                    None,  # AddressState
                    None,  # AddressZip
                    None,  # PhoneNumber
                    None,  # SecurityLevel
                    "V",  # Region
                    None,  # Division
                    None,  # District
                    "TDOC",  # Agency
                    None,  # NCICAgencyID
                    None,  # InChargeStaffID
                    "B",  # SexTypesAllowed
                    None,  # ContactPerson
                    None,  # LastUpdateUserID
                    None,  # LastUpdateDate
                ),
                (
                    "024",  # SiteID
                    "FAYETTE COUNTY JAIL",  # SiteName
                    "JA",  # SiteType
                    "A",  # Status,
                    None,  # StatusDate
                    None,  # AddressLine1
                    None,  # AddressLine2
                    None,  # AddressCity
                    None,  # AddressState
                    None,  # AddressZip
                    None,  # PhoneNumber
                    None,  # SecurityLevel
                    "R",  # Region
                    None,  # Division
                    None,  # District
                    "TDOC",  # Agency
                    None,  # NCICAgencyID
                    None,  # InChargeStaffID
                    "B",  # SexTypesAllowed
                    None,  # ContactPerson
                    None,  # LastUpdateUserID
                    None,  # LastUpdateDate
                ),
            ],
            expected_output=[
                [
                    "12345678",  # OffenderID
                    "2020-12-30 03:30:00.000000",  # StartDateTime
                    "2021-05-15 10:30:00.000000",  # EndDateTime
                    "024",  # Site
                    "JA",  # SiteType
                    "CTFA",  # StartMovementInfo
                    "NEWAD",  # StartMovementReason
                    "FAFA",  # EndMovementType
                    "INCIB",  # EndMovementReason
                    1,  # IncarcerationPeriodSequenceNumber
                ],
                [
                    "12345678",  # OffenderID
                    "2021-05-15 10:30:00.000000",  # StartDateTime
                    "2021-06-01 10:30:00",  # EndDateTime (decimals get truncated in postgres version of query)
                    "088",  # Site
                    "JA",  # SiteType
                    "FAFA",  # StartMovementType
                    "INCIB",  # StartMovementReason
                    "",  # EndMovementType
                    "",  # EndMovementReason
                    2,  # IncarcerationPeriodSequenceNumber
                ],
            ],
        )

    def test_offender_movement_incarceration_period_with_death_date_before_next_period_start_date(
        self,
    ) -> None:
        self.run_test(
            offender_name=[
                (
                    "12345678",  # OffenderID
                    "0",  # SequenceNumber
                    "FIRST",  # FirstName
                    "MIDDLE",  # MiddleName
                    "LAST",  # LastName
                    "",  # Suffix
                    "O",  # NameType
                    "INAC",  # OffenderStatus
                    "",  # ActualSiteID
                    "W",  # Race
                    "M",  # Sex
                    "1998-12-04 00:00:00",  # BirthDate
                    "",  # SocialSecurityNumber
                    "",  # STGNicknameFLag
                    "AB12345",  # LastUpdateUserID
                    "2019-08-02 08:49:21.161200",  # LastUpdateDate
                )
            ],
            offender_attribute=[
                (
                    "12345678",  # OffenderID
                    "0",  # Weight
                    "0",  # HeightFeet
                    "0",  # HeightInches
                    "",  # EyeColor
                    "",  # HairColor
                    "",  # Complexion
                    "M",  # MaritalStatus
                    "TX",  # DriverLicenseState
                    "1234567",  # DriverLicenseNumber
                    "",  # ScarsMarksTattoos
                    "52",  # Religion
                    "",  # BirthCounty
                    "IN",  # BirthState
                    "",  # NCICFingerprintID
                    "",  # TennesseeFingerprintID
                    "",  # DeathType
                    "2021-06-01 10:30:00.000000",  # DeathDate
                    "",  # DeathLocation
                    "",  # DisposalType
                    "",  # DisposalDate
                    "",  # DisposalLocation
                    "",  # DeathPostedByStaffID
                    "ABCDEF12",  # OBSCISID
                    "",  # RestoreDate
                    "",  # DeathSiteID
                    "",  # DeathCertificateFlag
                    "Y",  # OldFBINumber
                    "",  # OldSTGAffiliation
                    "",  # OldSTG
                    "",  # OldSTGComments
                    "",  # CitizenshipCountry
                    "",  # BirthPlace
                    "",  # AlienID
                    "",  # FBINumber
                    "",  # SIDNumber
                    "29301",  # LastUpdateUserID
                    "2020-05-20 12:43:43.823350",  # LastUpdateDate
                )
            ],
            offender_movement=[
                # New Admission to jail.
                (
                    "12345678",  # OffenderID
                    "2020-12-30 03:30:00.000000",  # MovementDateTime
                    "CTFA",  # MovementType
                    "NEWAD",  # MovementReason
                    '"019 "',  # FromLocationID
                    '"024   "',  # ToLocationID
                    "D",  # ArrivalDepartureFlag
                    '"AB1234 "',  # LastUpdateUserID
                    "2020-12-30 15:22:04.323930",  # LastUpdateDate
                ),
                # Transfer from one jail facility to another after death date.
                (
                    "12345678",  # OffenderID
                    "2021-08-15 10:30:00.000000",  # MovementDateTime
                    "FAFA",  # MovementType
                    "INCIB",  # MovementReason
                    '"024   "',  # FromLocationID
                    '"088   "',  # ToLocationID
                    "D",  # ArrivalDepartureFlag
                    '"AB1234 "',  # LastUpdateUserID
                    "2020-08-15 09:20:05.212820",  # LastUpdateDate
                ),
            ],
            site=[
                # Division	District	Agency	NCICAgencyID	InChargeStaffID	SexTypesAllowed	ContactPerson	LastUpdateUserID	LastUpdateDate
                (
                    "088",  # SiteID
                    "VAN BUREN COUNTY JAIL",  # SiteName
                    "JA",  # SiteType
                    "A",  # Status,
                    None,  # StatusDate
                    None,  # AddressLine1
                    None,  # AddressLine2
                    None,  # AddressCity
                    None,  # AddressState
                    None,  # AddressZip
                    None,  # PhoneNumber
                    None,  # SecurityLevel
                    "V",  # Region
                    None,  # Division
                    None,  # District
                    "TDOC",  # Agency
                    None,  # NCICAgencyID
                    None,  # InChargeStaffID
                    "B",  # SexTypesAllowed
                    None,  # ContactPerson
                    None,  # LastUpdateUserID
                    None,  # LastUpdateDate
                ),
                (
                    "024",  # SiteID
                    "FAYETTE COUNTY JAIL",  # SiteName
                    "JA",  # SiteType
                    "A",  # Status,
                    None,  # StatusDate
                    None,  # AddressLine1
                    None,  # AddressLine2
                    None,  # AddressCity
                    None,  # AddressState
                    None,  # AddressZip
                    None,  # PhoneNumber
                    None,  # SecurityLevel
                    "R",  # Region
                    None,  # Division
                    None,  # District
                    "TDOC",  # Agency
                    None,  # NCICAgencyID
                    None,  # InChargeStaffID
                    "B",  # SexTypesAllowed
                    None,  # ContactPerson
                    None,  # LastUpdateUserID
                    None,  # LastUpdateDate
                ),
            ],
            expected_output=[
                [
                    "12345678",  # OffenderID
                    "2020-12-30 03:30:00.000000",  # StartDateTime
                    "2021-06-01 10:30:00",  # EndDateTime (= death date, truncated without decimals)
                    "024",  # Site
                    "JA",  # SiteType
                    "CTFA",  # StartMovementType
                    "NEWAD",  # StartMovementReason
                    "",  # EndMovementType
                    "",  # EndMovementReason
                    1,  # IncarcerationPeriodSequenceNumber
                ],
            ],
        )

    # TODO(#9191): Add test to validate that important periods aren't getting dropped.
