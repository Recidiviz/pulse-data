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
"""Tests the PA external ids query functionality"""
import datetime
from typing import Any, List, Optional

import attr
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

STATE_CODE = StateCode.US_PA.value


@attr.s(kw_only=True, frozen=True)
class ParoleCountIds:
    ParoleNumber: Optional[str] = attr.ib()
    ParoleInstNumber: Optional[str] = attr.ib()


@attr.s(kw_only=True, frozen=True)
class TblSearchInmateInfoIds:
    inmate_number: Optional[str] = attr.ib()
    control_number: Optional[str] = attr.ib()


@attr.s(kw_only=True, frozen=True)
class RecidivizReferenceLinkingIds:
    pseudo_linking_id: str = attr.ib()
    control_number: str = attr.ib()


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class ViewPersonExternalIdsTest(BaseViewTest):
    """Tests the PA external ids query functionality"""

    def setUp(self) -> None:
        super().setUp()
        view_builders = DirectIngestPreProcessedIngestViewCollector(
            get_region(STATE_CODE, is_direct_ingest=True), []
        ).collect_view_builders()
        self.view_builder = one(
            view for view in view_builders if view.file_tag == "person_external_ids_v2"
        )

        self.expected_result_columns = [
            "recidiviz_master_person_id",
            "control_numbers",
            "inmate_numbers",
            "parole_numbers",
        ]

    def run_test(
        self,
        dbo_parole_count_ids: List[ParoleCountIds],
        dbo_tbl_search_inmate_info_ids: List[TblSearchInmateInfoIds],
        recidiviz_reference_linking_ids: List[RecidivizReferenceLinkingIds],
        expected_output: List[List[Any]],
    ) -> None:
        """Runs a test that executes the person_external_ids_v2 query given the provided
        input rows.
        """
        run_time = datetime.datetime.now()
        file_upload_time = run_time - datetime.timedelta(days=1)

        # Arrange
        raw_file_configs = get_region_raw_file_config(STATE_CODE).raw_file_configs

        self.create_mock_raw_file(
            STATE_CODE,
            raw_file_configs["dbo_ParoleCount"],
            [(ids.ParoleNumber, ids.ParoleInstNumber) for ids in dbo_parole_count_ids],
            update_datetime=file_upload_time,
        )
        self.create_mock_raw_file(
            STATE_CODE,
            raw_file_configs["dbo_tblSearchInmateInfo"],
            [
                tuple([ids.inmate_number, ids.control_number] + [None] * 83)
                for ids in dbo_tbl_search_inmate_info_ids
            ],
            update_datetime=file_upload_time,
        )
        self.create_mock_raw_file(
            STATE_CODE,
            raw_file_configs["RECIDIVIZ_REFERENCE_control_number_linking_ids"],
            [
                (ids.control_number, ids.pseudo_linking_id)
                for ids in recidiviz_reference_linking_ids
            ],
            update_datetime=file_upload_time,
        )

        # Act
        results = self.query_raw_data_view_for_builder(
            self.view_builder,
            dimensions=self.expected_result_columns,
            query_run_dt=run_time,
        )

        # Assert
        expected = pd.DataFrame(expected_output, columns=self.expected_result_columns)
        expected = expected.set_index(self.expected_result_columns)
        print(expected)
        print(results)
        assert_frame_equal(expected, results)

    def test_view_person_external_ids_v2_parses(self) -> None:
        self.run_test(
            dbo_parole_count_ids=[],
            dbo_tbl_search_inmate_info_ids=[],
            recidiviz_reference_linking_ids=[],
            expected_output=[],
        )

    def test_view_person_external_ids_v2_simple(self) -> None:
        self.run_test(
            dbo_parole_count_ids=[
                ParoleCountIds(ParoleNumber="0420X", ParoleInstNumber="AB1234")
            ],
            dbo_tbl_search_inmate_info_ids=[
                TblSearchInmateInfoIds(
                    inmate_number="AB1234", control_number="12345678"
                )
            ],
            recidiviz_reference_linking_ids=[],
            expected_output=[
                [
                    "RECIDIVIZ_MASTER_CONTROL_NUMBER_12345678",
                    "12345678",  # control_numbers
                    "AB1234",  # inmate_numbers
                    "0420X",  # pa
                    # role_numbers
                ]
            ],
        )

    def test_view_person_external_ids_v2_multiple_inmate(self) -> None:
        self.run_test(
            dbo_parole_count_ids=[
                ParoleCountIds(ParoleNumber="0420X", ParoleInstNumber="AB1234")
            ],
            dbo_tbl_search_inmate_info_ids=[
                TblSearchInmateInfoIds(
                    inmate_number="AB1234", control_number="12345678"
                ),
                TblSearchInmateInfoIds(
                    inmate_number="CD4567", control_number="12345678"
                ),
            ],
            recidiviz_reference_linking_ids=[],
            expected_output=[
                [
                    "RECIDIVIZ_MASTER_CONTROL_NUMBER_12345678",
                    "12345678",  # control_numbers
                    "AB1234,CD4567",  # inmate_numbers
                    "0420X",  # parole_numbers
                ]
            ],
        )

    def test_view_person_external_ids_v2_missing_parole(self) -> None:
        self.run_test(
            dbo_parole_count_ids=[],
            dbo_tbl_search_inmate_info_ids=[
                TblSearchInmateInfoIds(
                    inmate_number="AB1234", control_number="12345678"
                ),
            ],
            recidiviz_reference_linking_ids=[],
            expected_output=[
                [
                    "RECIDIVIZ_MASTER_CONTROL_NUMBER_12345678",
                    "12345678",  # control_numbers
                    "AB1234",  # inmate_numbers
                    None,  # parole_numbers
                ]
            ],
        )

    def test_view_person_external_ids_v2_missing_control(self) -> None:
        self.run_test(
            dbo_parole_count_ids=[
                ParoleCountIds(ParoleNumber="0420X", ParoleInstNumber="AB1234"),
            ],
            dbo_tbl_search_inmate_info_ids=[],
            recidiviz_reference_linking_ids=[],
            expected_output=[
                [
                    "RECIDIVIZ_MASTER_PAROLE_NUMBER_0420X",
                    None,  # control_numbers
                    "AB1234",  # inmate_numbers
                    "0420X",  # parole_numbers
                ]
            ],
        )

    def test_view_person_external_ids_v2_null_inmate_numbers(self) -> None:
        self.run_test(
            dbo_parole_count_ids=[
                ParoleCountIds(ParoleNumber="0420X", ParoleInstNumber=None)
            ],
            dbo_tbl_search_inmate_info_ids=[
                TblSearchInmateInfoIds(inmate_number=None, control_number="12345678")
            ],
            recidiviz_reference_linking_ids=[],
            expected_output=[
                [
                    "RECIDIVIZ_MASTER_CONTROL_NUMBER_12345678",
                    "12345678",  # control_numbers
                    None,  # inmate_numbers
                    None,  # parole_numbers
                ],
                [
                    "RECIDIVIZ_MASTER_PAROLE_NUMBER_0420X",
                    None,  # control_numbers
                    None,  # inmate_numbers
                    "0420X",  # parole_numbers
                ],
            ],
        )

    def test_view_person_external_ids_v2_clean_bad_inmate_numbers(self) -> None:
        self.run_test(
            dbo_parole_count_ids=[
                ParoleCountIds(ParoleNumber="0420X", ParoleInstNumber=None),
                ParoleCountIds(ParoleNumber="0420X", ParoleInstNumber="AB1234"),
                ParoleCountIds(ParoleNumber="0420X", ParoleInstNumber="FG7899"),
                ParoleCountIds(ParoleNumber="0420X", ParoleInstNumber="XBADX"),
                ParoleCountIds(ParoleNumber="3141Y", ParoleInstNumber="CD4567"),
                ParoleCountIds(ParoleNumber="3141Y", ParoleInstNumber="YBADY"),
                ParoleCountIds(ParoleNumber="2171K", ParoleInstNumber="JBADJ"),
            ],
            dbo_tbl_search_inmate_info_ids=[],
            recidiviz_reference_linking_ids=[],
            expected_output=[
                [
                    "RECIDIVIZ_MASTER_PAROLE_NUMBER_0420X",
                    None,  # control_numbers
                    "AB1234,FG7899",  # inmate_numbers
                    "0420X",  # parole_numbers
                ],
                [
                    "RECIDIVIZ_MASTER_PAROLE_NUMBER_2171K",
                    None,  # control_numbers
                    None,  # inmate_numbers
                    "2171K",  # parole_numbers
                ],
                [
                    "RECIDIVIZ_MASTER_PAROLE_NUMBER_3141Y",
                    None,  # control_numbers
                    "CD4567",  # inmate_numbers
                    "3141Y",  # parole_numbers
                ],
            ],
        )

    def test_view_person_external_ids_v2_complex(self) -> None:
        self.run_test(
            dbo_parole_count_ids=[
                ParoleCountIds(ParoleNumber="0420X", ParoleInstNumber="AB1234"),
                ParoleCountIds(ParoleNumber="0420X", ParoleInstNumber="CD4567"),
            ],
            dbo_tbl_search_inmate_info_ids=[
                TblSearchInmateInfoIds(
                    inmate_number="AB1234", control_number="12345678"
                ),
                TblSearchInmateInfoIds(
                    inmate_number="CD4567", control_number="12345678"
                ),
                TblSearchInmateInfoIds(
                    inmate_number="FG6789", control_number="12345678"
                ),
            ],
            recidiviz_reference_linking_ids=[],
            expected_output=[
                [
                    "RECIDIVIZ_MASTER_CONTROL_NUMBER_12345678",
                    "12345678",  # control_numbers
                    "AB1234,CD4567,FG6789",  # inmate_numbers
                    "0420X",  # parole_numbers
                ]
            ],
        )

    def test_view_person_external_ids_v2_complex2(self) -> None:
        self.run_test(
            dbo_parole_count_ids=[
                ParoleCountIds(ParoleNumber="1111P", ParoleInstNumber="II1111"),
                ParoleCountIds(ParoleNumber="2222P", ParoleInstNumber="II2222"),
                ParoleCountIds(ParoleNumber="2222P", ParoleInstNumber="II3333"),
                ParoleCountIds(ParoleNumber="3333P", ParoleInstNumber="II3333"),
            ],
            dbo_tbl_search_inmate_info_ids=[
                TblSearchInmateInfoIds(
                    inmate_number="II1111", control_number="10000000"
                ),
                TblSearchInmateInfoIds(
                    inmate_number="II2222", control_number="10000000"
                ),
                TblSearchInmateInfoIds(
                    inmate_number="II3333", control_number="20000000"
                ),
                TblSearchInmateInfoIds(
                    inmate_number="II4444", control_number="20000000"
                ),
            ],
            recidiviz_reference_linking_ids=[],
            expected_output=[
                [
                    "RECIDIVIZ_MASTER_CONTROL_NUMBER_10000000",
                    "10000000,20000000",  # control_numbers
                    "II1111,II2222,II3333,II4444",  # inmate_numbers
                    "1111P,2222P,3333P",  # parole_numbers
                ]
            ],
        )

    def test_view_person_external_ids_v2_join_on_parole(self) -> None:
        self.run_test(
            dbo_parole_count_ids=[
                ParoleCountIds(ParoleNumber="0420X", ParoleInstNumber="AB1234"),
                ParoleCountIds(ParoleNumber="0420X", ParoleInstNumber="CC4567"),
            ],
            dbo_tbl_search_inmate_info_ids=[
                TblSearchInmateInfoIds(
                    inmate_number="AB1234", control_number="12345678"
                ),
            ],
            recidiviz_reference_linking_ids=[],
            expected_output=[
                [
                    "RECIDIVIZ_MASTER_CONTROL_NUMBER_12345678",
                    "12345678",  # control_numbers
                    "AB1234,CC4567",  # inmate_numbers
                    "0420X",  # parole_numbers
                ]
            ],
        )

    def test_view_person_external_ids_v2_join_on_parole_2(self) -> None:
        self.run_test(
            dbo_parole_count_ids=[
                ParoleCountIds(ParoleNumber="1234R", ParoleInstNumber=None),
                ParoleCountIds(ParoleNumber="444GS", ParoleInstNumber="KS0000"),
                ParoleCountIds(ParoleNumber="444GS", ParoleInstNumber=None),
            ],
            dbo_tbl_search_inmate_info_ids=[
                TblSearchInmateInfoIds(inmate_number="KS0000", control_number="280123"),
            ],
            recidiviz_reference_linking_ids=[],
            expected_output=[
                [
                    "RECIDIVIZ_MASTER_CONTROL_NUMBER_280123",
                    "280123",  # control_numbers
                    "KS0000",  # inmate_numbers
                    "444GS",  # parole_numbers
                ],
                [
                    "RECIDIVIZ_MASTER_PAROLE_NUMBER_1234R",
                    None,  # control_numbers
                    None,  # inmate_numbers
                    "1234R",  # parole_numbers
                ],
            ],
        )

    def test_view_person_external_ids_v2_join_on_parole_mismatch_casing(self) -> None:
        self.run_test(
            dbo_parole_count_ids=[
                ParoleCountIds(ParoleNumber="0420x", ParoleInstNumber="AB1234"),
                ParoleCountIds(ParoleNumber="0420X", ParoleInstNumber="CC4567"),
            ],
            dbo_tbl_search_inmate_info_ids=[
                TblSearchInmateInfoIds(
                    inmate_number="Ab1234", control_number="12345678"
                ),
            ],
            recidiviz_reference_linking_ids=[],
            expected_output=[
                [
                    "RECIDIVIZ_MASTER_CONTROL_NUMBER_12345678",
                    "12345678",  # control_numbers
                    "AB1234,CC4567",  # inmate_numbers
                    "0420X",  # parole_numbers
                ]
            ],
        )

    def test_view_person_external_ids_v2_multiple_control_linked_via_parole(
        self,
    ) -> None:
        self.run_test(
            dbo_parole_count_ids=[
                ParoleCountIds(ParoleNumber="7890S", ParoleInstNumber="BB9876"),
                ParoleCountIds(ParoleNumber="7890S", ParoleInstNumber="BT7654"),
                ParoleCountIds(ParoleNumber="7890S", ParoleInstNumber="Z0000"),
            ],
            dbo_tbl_search_inmate_info_ids=[
                TblSearchInmateInfoIds(inmate_number="BT7654", control_number="090909"),
                TblSearchInmateInfoIds(inmate_number="BB9876", control_number="080808"),
            ],
            recidiviz_reference_linking_ids=[],
            expected_output=[
                [
                    "RECIDIVIZ_MASTER_CONTROL_NUMBER_080808",
                    "080808,090909",  # control_numbers
                    "BB9876,BT7654",  # inmate_numbers
                    "7890S",  # parole_numbers
                ]
            ],
        )

    def test_view_person_external_ids_v2_link_via_pseudo_id(self) -> None:
        self.run_test(
            dbo_parole_count_ids=[],
            dbo_tbl_search_inmate_info_ids=[
                TblSearchInmateInfoIds(inmate_number="OA3333", control_number="080808"),
                TblSearchInmateInfoIds(inmate_number="OD6666", control_number="121212"),
            ],
            recidiviz_reference_linking_ids=[
                RecidivizReferenceLinkingIds(
                    pseudo_linking_id="74ebaf23-ed14-4d76-8d29-c86140e5ac40",
                    control_number="080808",
                ),
                RecidivizReferenceLinkingIds(
                    pseudo_linking_id="74ebaf23-ed14-4d76-8d29-c86140e5ac40",
                    control_number="121212",
                ),
            ],
            expected_output=[
                [
                    "RECIDIVIZ_MASTER_CONTROL_NUMBER_080808",
                    "080808,121212",  # control_numbers
                    "OA3333,OD6666",  # inmate_numbers
                    None,  # parole_numbers
                ]
            ],
        )

    def test_view_person_external_ids_v2_link_via_pseudo_id_one_parole_link(
        self,
    ) -> None:
        self.run_test(
            dbo_parole_count_ids=[
                ParoleCountIds(ParoleNumber="0420X", ParoleInstNumber="OD6666"),
            ],
            dbo_tbl_search_inmate_info_ids=[
                TblSearchInmateInfoIds(inmate_number="OA3333", control_number="080808"),
                TblSearchInmateInfoIds(inmate_number="OD6666", control_number="121212"),
            ],
            recidiviz_reference_linking_ids=[
                RecidivizReferenceLinkingIds(
                    pseudo_linking_id="74ebaf23-ed14-4d76-8d29-c86140e5ac40",
                    control_number="080808",
                ),
                RecidivizReferenceLinkingIds(
                    pseudo_linking_id="74ebaf23-ed14-4d76-8d29-c86140e5ac40",
                    control_number="121212",
                ),
            ],
            expected_output=[
                [
                    "RECIDIVIZ_MASTER_CONTROL_NUMBER_080808",
                    "080808,121212",  # control_numbers
                    "OA3333,OD6666",  # inmate_numbers
                    "0420X",  # parole_numbers
                ]
            ],
        )
