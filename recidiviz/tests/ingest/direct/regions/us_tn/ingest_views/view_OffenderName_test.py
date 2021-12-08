#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests the TN `OffenderName` view logic."""
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
class OffenderNameTest(BaseViewTest):
    """Tests the TN `OffenderName` query functionality."""

    def setUp(self) -> None:
        super().setUp()
        view_builders = DirectIngestPreProcessedIngestViewCollector(
            get_region(STATE_CODE, is_direct_ingest=True), []
        ).collect_view_builders()
        self.view_builder = one(
            view for view in view_builders if view.file_tag == "OffenderName"
        )

        # All columns need to be in lowercase.
        self.expected_result_columns = [
            "offenderid",
            "firstname",
            "middlename",
            "lastname",
            "race",
            "ethnicity",
            "sex",
            "birthdate",
        ]

    def run_test(
        self,
        offender_name: List[Tuple[Any, ...]],
        expected_output: List[List[Any]],
    ) -> None:
        """Runs a test that executes the OffenderName query given the provided
        input rows.
        """

        # Arrange
        raw_file_configs = get_region_raw_file_config(STATE_CODE).raw_file_configs

        self.create_mock_raw_file(
            region_code=STATE_CODE,
            file_config=raw_file_configs["OffenderName"],
            mock_data=offender_name,
        )

        # Act
        results = self.query_raw_data_view_for_builder(
            self.view_builder,
            dimensions=self.expected_result_columns,
        )

        # Assert
        expected = pd.DataFrame(expected_output, columns=self.expected_result_columns)
        expected = expected.set_index(self.expected_result_columns)
        print(expected)
        print(results)
        assert_frame_equal(expected, results)

    def test_offender_name_simple(self) -> None:
        self.run_test(
            offender_name=[
                (
                    "12345678",  # OffenderID
                    "1",  # SequenceNumber
                    "FIRST",  # FirstName
                    "MIDDLE",  # MiddleName
                    "LAST",  # LastName
                    None,  # Suffix
                    "A",  # NameType
                    None,  # OffenderStatus
                    "ABC",  # ActualSiteID
                    "W",  # Race
                    "M",  # Sex
                    "1998-12-04 00:00:00",  # BirthDate
                    None,  # SocialSecurityNumber
                    None,  # STGNicknameFlag
                    "ABCDE",  # LastUpdateUserID
                    "2020-05-15 09:20:05.212820",  # LastUpdateDate
                )
            ],
            expected_output=[
                [
                    "12345678",  # OffenderID
                    "FIRST",  # FirstName
                    "MIDDLE",  # MiddleName
                    "LAST",  # LastName
                    "W",  # Race
                    "NOT_HISPANIC",  # Ethnicity
                    "M",  # Sex
                    "1998-12-04 00:00:00",  # BirthDate
                ]
            ],
        )

    def test_offender_name_multiple_rows_for_same_id_collapsed(self) -> None:
        self.run_test(
            offender_name=[
                (
                    "12345678",  # OffenderID
                    "1",  # SequenceNumber
                    "FIRST",  # FirstName
                    "MIDDLE",  # MiddleName
                    "LAST",  # LastName
                    None,  # Suffix
                    "O",  # NameType
                    None,  # OffenderStatus
                    "ABC",  # ActualSiteID
                    "W",  # Race
                    "M",  # Sex
                    "1998-12-04 00:00:00",  # BirthDate
                    None,  # SocialSecurityNumber
                    None,  # STGNicknameFlag
                    "ABCDE",  # LastUpdateUserID
                    "2020-10-15 09:20:05.212820",  # LastUpdateDate
                ),
                (
                    # Same OffenderID, different SequenceNumber and Name values.
                    "12345678",  # OffenderID
                    "10",  # SequenceNumber
                    "FIRST2",  # FirstName
                    "MIDDLE2",  # MiddleName
                    "LAST2",  # LastName
                    None,  # Suffix
                    "O",  # NameType
                    "XYZ",  # OffenderStatus
                    "ABC",  # ActualSiteID
                    "W",  # Race
                    "M",  # Sex
                    "1998-12-04 00:00:00",  # BirthDate
                    None,  # SocialSecurityNumber
                    None,  # STGNicknameFlag
                    "ABCDE",  # LastUpdateUserID
                    "2020-10-15 09:20:05.212820",  # LastUpdateDate
                ),
            ],
            expected_output=[
                [
                    # Pick contents of the second offender_name because its sequence number is higher,
                    # even with same LastUpdateDate.
                    "12345678",  # OffenderID
                    "FIRST2",  # FirstName
                    "MIDDLE2",  # MiddleName
                    "LAST2",  # LastName
                    "W",  # Race
                    "NOT_HISPANIC",  # Ethnicity
                    "M",  # Sex
                    "1998-12-04 00:00:00",  # BirthDate
                ]
            ],
        )

    def test_offender_name_multiple_rows_for_same_id_collapsed_filter_nickname(
        self,
    ) -> None:
        self.run_test(
            offender_name=[
                (
                    "12345678",  # OffenderID
                    "1",  # SequenceNumber
                    "FIRST",  # FirstName
                    "MIDDLE",  # MiddleName
                    "LAST",  # LastName
                    None,  # Suffix
                    "O",  # NameType
                    None,  # OffenderStatus
                    "ABC",  # ActualSiteID
                    "W",  # Race
                    "M",  # Sex
                    "1998-12-04 00:00:00",  # BirthDate
                    None,  # SocialSecurityNumber
                    None,  # STGNicknameFlag
                    "ABCDE",  # LastUpdateUserID
                    "2020-10-15 09:20:05.212820",  # LastUpdateDate
                ),
                (
                    # Same OffenderID, different name type (nickname).
                    "12345678",  # OffenderID
                    "25",  # SequenceNumber
                    "NICKNAME",  # FirstName
                    None,  # MiddleName
                    None,  # LastName
                    None,  # Suffix
                    "N",  # NameType
                    None,  # OffenderStatus
                    "ABC",  # ActualSiteID
                    "W",  # Race
                    "M",  # Sex
                    "1998-12-04 00:00:00",  # BirthDate
                    None,  # SocialSecurityNumber
                    None,  # STGNicknameFlag
                    "ABCDE",  # LastUpdateUserID
                    "2020-10-15 09:20:05.212820",  # LastUpdateDate
                ),
            ],
            expected_output=[
                [
                    # Pick contents of the first offender_name because its sequence number is higher and
                    # it is not a nickname.
                    "12345678",  # OffenderID
                    "FIRST",  # FirstName
                    "MIDDLE",  # MiddleName
                    "LAST",  # LastName
                    "W",  # Race
                    "NOT_HISPANIC",  # Ethnicity
                    "M",  # Sex
                    "1998-12-04 00:00:00",  # BirthDate
                ]
            ],
        )
