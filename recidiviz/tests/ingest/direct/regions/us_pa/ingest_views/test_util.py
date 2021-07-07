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
"""Utilities for testing PA ingest views"""

import datetime
import uuid
from typing import Dict, List, Optional

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
)
from recidiviz.tests.big_query.view_test_util import BaseViewTest

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


@attr.s(kw_only=True, frozen=True)
class PersonExternalIds:
    recidiviz_master_person_id: str = attr.ib()

    control_numbers: List[str] = attr.ib()
    inmate_numbers: List[str] = attr.ib()
    parole_numbers: List[str] = attr.ib()


def create_id_tables_from_external_ids(
    test: BaseViewTest,
    ids: List[PersonExternalIds],
    raw_file_configs: Dict[str, DirectIngestRawFileConfig],
    file_upload_time: datetime.datetime,
) -> None:
    """Takes a set of desired external ids and creates the raw tables necessary to build them."""
    dbo_parole_count_ids: List[ParoleCountIds] = []
    dbo_tbl_search_inmate_info_ids: List[TblSearchInmateInfoIds] = []
    recidiviz_reference_linking_ids: List[RecidivizReferenceLinkingIds] = []

    for external_id in ids:
        # Generate all combinations
        dbo_parole_count_ids.extend(
            [
                ParoleCountIds(
                    ParoleNumber=parole_number, ParoleInstNumber=inmate_number
                )
                for parole_number in external_id.parole_numbers
                for inmate_number in external_id.inmate_numbers
            ]
        )

        # Generate all combinations
        dbo_tbl_search_inmate_info_ids.extend(
            [
                TblSearchInmateInfoIds(
                    inmate_number=inmate_number, control_number=control_number
                )
                for inmate_number in external_id.inmate_numbers
                for control_number in external_id.control_numbers
            ]
        )

        pseudo_id = str(uuid.uuid4())
        recidiviz_reference_linking_ids.extend(
            [
                RecidivizReferenceLinkingIds(
                    pseudo_linking_id=pseudo_id, control_number=control_number
                )
                for control_number in external_id.control_numbers
            ]
        )

    create_id_tables(
        test,
        dbo_parole_count_ids,
        dbo_tbl_search_inmate_info_ids,
        recidiviz_reference_linking_ids,
        raw_file_configs,
        file_upload_time,
    )


def create_id_tables(
    test: BaseViewTest,
    dbo_parole_count_ids: List[ParoleCountIds],
    dbo_tbl_search_inmate_info_ids: List[TblSearchInmateInfoIds],
    recidiviz_reference_linking_ids: List[RecidivizReferenceLinkingIds],
    raw_file_configs: Dict[str, DirectIngestRawFileConfig],
    file_upload_time: datetime.datetime,
) -> None:
    test.create_mock_raw_file(
        STATE_CODE,
        raw_file_configs["dbo_ParoleCount"],
        [(ids.ParoleNumber, ids.ParoleInstNumber) for ids in dbo_parole_count_ids],
        update_datetime=file_upload_time,
    )
    test.create_mock_raw_file(
        STATE_CODE,
        raw_file_configs["dbo_tblSearchInmateInfo"],
        [
            tuple([ids.inmate_number, ids.control_number] + [None] * 83)
            for ids in dbo_tbl_search_inmate_info_ids
        ],
        update_datetime=file_upload_time,
    )
    test.create_mock_raw_file(
        STATE_CODE,
        raw_file_configs["RECIDIVIZ_REFERENCE_control_number_linking_ids"],
        [
            (ids.control_number, ids.pseudo_linking_id)
            for ids in recidiviz_reference_linking_ids
        ],
        update_datetime=file_upload_time,
    )


@attr.s(kw_only=True, frozen=True)
class Movrec:
    mov_cnt_num: Optional[str] = attr.ib()
    mov_seq_num: Optional[str] = attr.ib()
    mov_chg_num: Optional[str] = attr.ib()
    mov_cur_inmt_num: Optional[str] = attr.ib()
    mov_sig_date: Optional[str] = attr.ib()
    mov_term_id: Optional[str] = attr.ib()
    mov_sig_time: Optional[str] = attr.ib()
    mov_user_id: Optional[str] = attr.ib()
    mov_del_year: Optional[str] = attr.ib()
    mov_del_month: Optional[str] = attr.ib()
    mov_del_day: Optional[str] = attr.ib()
    last_chg_num_used: Optional[str] = attr.ib()
    mov_move_code: Optional[str] = attr.ib()
    mov_move_date: Optional[str] = attr.ib()
    mov_move_time: Optional[str] = attr.ib()
    mov_move_to_loc: Optional[str] = attr.ib()
    parole_stat_cd: Optional[str] = attr.ib()
    mov_sent_stat_cd: Optional[str] = attr.ib()
    mov_rec_del_flag: Optional[str] = attr.ib()
    mov_sent_group: Optional[str] = attr.ib()
    mov_move_to_location_type: Optional[str] = attr.ib()
    mov_move_from_location: Optional[str] = attr.ib()
    mov_move_from_location_type: Optional[str] = attr.ib()
    mov_permanent_institution: Optional[str] = attr.ib()
    mov_to_institution: Optional[str] = attr.ib()


@attr.s(kw_only=True, frozen=True)
class Senrec:
    """Class to represent a row in dbo_Senrec"""

    curr_inmate_num: Optional[str] = attr.ib()
    type_number: Optional[str] = attr.ib()
    addit_sent_detnr: Optional[str] = attr.ib()
    bail_yrs: Optional[str] = attr.ib()
    bail_mths: Optional[str] = attr.ib()
    bail_days: Optional[str] = attr.ib()
    class_of_sent: Optional[str] = attr.ib()
    commit_crdit_yrs: Optional[str] = attr.ib()
    commit_crdit_mths: Optional[str] = attr.ib()
    commit_crdit_days: Optional[str] = attr.ib()
    max_cort_sent_yrs: Optional[str] = attr.ib()
    max_cort_sent_mths: Optional[str] = attr.ib()
    max_cort_sent_days: Optional[str] = attr.ib()
    max_cort_sent_l_da: Optional[str] = attr.ib()
    min_cort_sent_yrs: Optional[str] = attr.ib()
    min_cort_sent_mths: Optional[str] = attr.ib()
    min_cort_sent_days: Optional[str] = attr.ib()
    min_cort_sent_l_da: Optional[str] = attr.ib()
    effective_date: Optional[str] = attr.ib()
    escape_yrs: Optional[str] = attr.ib()
    escape_mths: Optional[str] = attr.ib()
    escape_days: Optional[str] = attr.ib()
    max_expir_date: Optional[str] = attr.ib()
    min_expir_date: Optional[str] = attr.ib()
    max_fac_sent_yrs: Optional[str] = attr.ib()
    max_fac_sent_mths: Optional[str] = attr.ib()
    max_fac_sent_days: Optional[str] = attr.ib()
    min_fac_sent_yrs: Optional[str] = attr.ib()
    min_fac_sent_mths: Optional[str] = attr.ib()
    min_fac_sent_days: Optional[str] = attr.ib()
    gbmi: Optional[str] = attr.ib()
    indictment_num: Optional[str] = attr.ib()
    judge: Optional[str] = attr.ib()
    offense_code: Optional[str] = attr.ib()
    offense_track_num: Optional[str] = attr.ib()
    parole_status_cde: Optional[str] = attr.ib()
    parole_status_dt: Optional[str] = attr.ib()
    sent_date: Optional[str] = attr.ib()
    sent_start_date: Optional[str] = attr.ib()
    sent_status_code: Optional[str] = attr.ib()
    sent_status_date: Optional[str] = attr.ib()
    sent_stop_date: Optional[str] = attr.ib()
    sentcing_cnty: Optional[str] = attr.ib()
    st_to_frm_compact: Optional[str] = attr.ib()
    term_of_cort: Optional[str] = attr.ib()
    type_of_sent: Optional[str] = attr.ib()
    crime_facts_ind: Optional[str] = attr.ib()
    megans_law_ind: Optional[str] = attr.ib()
    sig_date: Optional[str] = attr.ib()
    sig_time: Optional[str] = attr.ib()
    user_id: Optional[str] = attr.ib()
    cntinued_frm_doc_n: Optional[str] = attr.ib()
