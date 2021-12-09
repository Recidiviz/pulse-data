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
"""Tests the PA sci incarceration period logic"""

import datetime
from typing import Any, List

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
from recidiviz.tests.ingest.direct.regions.us_pa.ingest_views.test_util import (
    Movrec,
    PersonExternalIds,
    Senrec,
    create_id_tables_from_external_ids,
)
from recidiviz.utils.regions import get_region

STATE_CODE = StateCode.US_PA.value


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="t"))
class ViewPersonExternalIdsTest(BaseViewTest):
    """Tests the PA external ids query functionality"""

    def setUp(self) -> None:
        super().setUp()
        view_builders = DirectIngestPreProcessedIngestViewCollector(
            get_region(STATE_CODE, is_direct_ingest=True), []
        ).collect_view_builders()
        self.view_builder = one(
            view
            for view in view_builders
            if view.file_tag == "sci_incarceration_period"
        )

        self.expected_result_columns = [
            "control_number",
            "inmate_number",
            "sequence_number",
            "start_movement_date",
            "end_movement_date",
            "location",
            "start_sentence_status_code",
            "end_sentence_status_code",
            "start_parole_status_code",
            "end_parole_status_code",
            "start_movement_code",
            "end_movement_code",
            "start_is_new_revocation",
            "start_is_admin_edge",
            "end_is_admin_edge",
            "sentence_type",
        ]

    def run_test(
        self,
        external_ids: List[PersonExternalIds],
        movrecs: List[Movrec],
        senrecs: List[Senrec],
        expected_output: List[List[Any]],
    ) -> None:
        """Runs a test that executes the person_external_ids query given the provided
        input rows.
        """
        run_time = datetime.datetime.now()
        file_upload_time = run_time - datetime.timedelta(days=1)

        # Arrange
        raw_file_configs = get_region_raw_file_config(STATE_CODE).raw_file_configs

        create_id_tables_from_external_ids(
            self,
            external_ids,
            raw_file_configs,
            file_upload_time,
        )

        self.create_mock_raw_file(
            STATE_CODE,
            raw_file_configs["dbo_Movrec"],
            [attr.astuple(movrec) for movrec in movrecs],
            update_datetime=file_upload_time,
        )

        self.create_mock_raw_file(
            STATE_CODE,
            raw_file_configs["dbo_Senrec"],
            [attr.astuple(senrec) for senrec in senrecs],
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

    def test_move_facilities(self) -> None:
        self.run_test(
            external_ids=[
                PersonExternalIds(
                    recidiviz_primary_person_id="RECIDIVIZ_PRIMARY_CONTROL_NUMBER_12345678",
                    control_numbers=["12345678"],
                    inmate_numbers=["AB1234", "CD4567"],
                    parole_numbers=["0420X"],
                )
            ],
            # Movements ordered by most recent
            movrecs=[
                # Move to 140
                Movrec(
                    mov_cnt_num="12345678",
                    mov_seq_num="00005",
                    mov_chg_num="000",
                    mov_cur_inmt_num="AB1234",
                    mov_sig_date=None,
                    mov_term_id="",
                    mov_sig_time="",
                    mov_user_id="",
                    mov_del_year="",
                    mov_del_month="",
                    mov_del_day="",
                    last_chg_num_used="",
                    mov_move_code="TRN",
                    mov_move_date="20200215",
                    mov_move_time="0700",
                    mov_move_to_loc="140",
                    parole_stat_cd="NA",
                    mov_sent_stat_cd="AS",
                    mov_rec_del_flag="N",
                    mov_sent_group="",
                    mov_move_to_location_type="C",
                    mov_move_from_location="120",
                    mov_move_from_location_type="C",
                    mov_permanent_institution="MUN",
                    mov_to_institution="",
                ),
                # Stay in 120
                Movrec(
                    mov_cnt_num="12345678",
                    mov_seq_num="00004",
                    mov_chg_num="000",
                    mov_cur_inmt_num="AB1234",
                    mov_sig_date=None,
                    mov_term_id="",
                    mov_sig_time="",
                    mov_user_id="",
                    mov_del_year="",
                    mov_del_month="",
                    mov_del_day="",
                    last_chg_num_used="",
                    mov_move_code="TRN",
                    mov_move_date="20200201",
                    mov_move_time="0800",
                    mov_move_to_loc="120",
                    parole_stat_cd="NA",
                    mov_sent_stat_cd="AS",
                    mov_rec_del_flag="N",
                    mov_sent_group="",
                    mov_move_to_location_type="C",
                    mov_move_from_location="120",
                    mov_move_from_location_type="C",
                    mov_permanent_institution="FRA",
                    mov_to_institution="",
                ),
                # Move to 120
                Movrec(
                    mov_cnt_num="12345678",
                    mov_seq_num="00003",
                    mov_chg_num="000",
                    mov_cur_inmt_num="AB1234",
                    mov_sig_date=None,
                    mov_term_id="",
                    mov_sig_time="",
                    mov_user_id="",
                    mov_del_year="",
                    mov_del_month="",
                    mov_del_day="",
                    last_chg_num_used="",
                    mov_move_code="TRN",
                    mov_move_date="20200115",
                    mov_move_time="0800",
                    mov_move_to_loc="120",
                    parole_stat_cd="NA",
                    mov_sent_stat_cd="AS",
                    mov_rec_del_flag="N",
                    mov_sent_group="",
                    mov_move_to_location_type="C",
                    mov_move_from_location="QUE",
                    mov_move_from_location_type="I",
                    mov_permanent_institution="FRA",
                    mov_to_institution="",
                ),
                # Move to QUE
                Movrec(
                    mov_cnt_num="12345678",
                    mov_seq_num="00002",
                    mov_chg_num="000",
                    mov_cur_inmt_num="AB1234",
                    mov_sig_date=None,
                    mov_term_id="",
                    mov_sig_time="",
                    mov_user_id="",
                    mov_del_year="",
                    mov_del_month="",
                    mov_del_day="",
                    last_chg_num_used="",
                    mov_move_code="TRN",
                    mov_move_date="20200101",
                    mov_move_time="0600",
                    mov_move_to_loc="QUE",
                    parole_stat_cd="NA",
                    mov_sent_stat_cd="AS",
                    mov_rec_del_flag="N",
                    mov_sent_group="00",
                    mov_move_to_location_type="I",
                    mov_move_from_location="BUS",
                    mov_move_from_location_type="B",
                    mov_permanent_institution="BEN",
                    mov_to_institution="QUE",
                ),
            ],
            senrecs=[
                Senrec(
                    curr_inmate_num="AB1234",
                    type_number="",
                    addit_sent_detnr="",
                    bail_yrs="",
                    bail_mths="",
                    bail_days="",
                    class_of_sent="",
                    commit_crdit_yrs="",
                    commit_crdit_mths="",
                    commit_crdit_days="",
                    max_cort_sent_yrs="",
                    max_cort_sent_mths="",
                    max_cort_sent_days="",
                    max_cort_sent_l_da="",
                    min_cort_sent_yrs="",
                    min_cort_sent_mths="",
                    min_cort_sent_days="",
                    min_cort_sent_l_da="",
                    effective_date="",
                    escape_yrs="",
                    escape_mths="",
                    escape_days="",
                    max_expir_date=None,
                    min_expir_date=None,
                    max_fac_sent_yrs="",
                    max_fac_sent_mths="",
                    max_fac_sent_days="",
                    min_fac_sent_yrs="",
                    min_fac_sent_mths="",
                    min_fac_sent_days="",
                    gbmi="",
                    indictment_num="",
                    judge="",
                    offense_code="",
                    offense_track_num="",
                    parole_status_cde="",
                    parole_status_dt=None,
                    sent_date=None,
                    sent_start_date=None,
                    sent_status_code="",
                    sent_status_date=None,
                    sent_stop_date=None,
                    sentcing_cnty="",
                    st_to_frm_compact="",
                    term_of_cort="",
                    type_of_sent="P",
                    crime_facts_ind="",
                    megans_law_ind="",
                    sig_date=None,
                    sig_time="",
                    user_id="",
                    cntinued_frm_doc_n="",
                )
            ],
            # Expect one per facility (collapsing the 120 movements)
            expected_output=[
                [
                    "12345678",
                    "AB1234",
                    1,
                    "2020-01-01 00:00:00",
                    "2020-01-15 00:00:00",
                    "QUE",
                    "AS",
                    "AS",
                    "NA",
                    "NA",
                    "TRN",
                    "TRN",
                    False,
                    "",
                    False,
                    "P",
                ],
                [
                    "12345678",
                    "AB1234",
                    2,
                    "2020-01-15 00:00:00",
                    "2020-02-15 00:00:00",
                    "120",
                    "AS",
                    "AS",
                    "NA",
                    "NA",
                    "TRN",
                    "TRN",
                    False,
                    False,
                    False,
                    "P",
                ],
                [
                    "12345678",
                    "AB1234",
                    3,
                    "2020-02-15 00:00:00",
                    "",
                    "140",
                    "AS",
                    "",
                    "NA",
                    "",
                    "TRN",
                    "",
                    False,
                    False,
                    "",
                    "P",
                ],
            ],
        )
