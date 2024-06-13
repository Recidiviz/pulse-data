# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Provides agent case update notes for people on supervision in US_IX from
OffenderNote, OffenderNoteInfo.
"""
from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_query_builder import BigQueryQueryBuilder
from recidiviz.big_query.big_query_query_provider import (
    BigQueryQueryProvider,
    SimpleBigQueryQueryProvider,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

# TODO(#16661) Rename US_IX -> US_ID in this file/code when we are ready to migrate the
# new ATLAS pipeline to run for US_ID
US_IX_CASE_UPDATE_INFO_QUERY_NAME = "us_ix_case_update_info"

US_IX_CASE_UPDATE_INFO_QUERY_TEMPLATE = """
    WITH person_id_with_external_ids AS (
        SELECT
            person_id,
            external_id AS person_external_id,
            state_code
        FROM `{project_id}.{normalized_state_dataset}.state_person_external_id`
        WHERE state_code = 'US_IX' AND id_type = 'US_IX_DOC'
    ),
    offender_notes AS (
        SELECT
            OffenderNoteId,
            OffenderId,
            NoteDate,
            e.StaffId,
            Details,
        FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ind_OffenderNote_latest` note
        LEFT JOIN `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ind_OffenderNoteInfo_latest` info
            USING (OffenderNoteInfoId)
        LEFT JOIN `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ref_Employee_latest` e
            ON note.InsertUserId = e.EmployeeId
        WHERE NoteTypeId = '80' -- Case Update
        AND OffenderNoteStatusId = '2' -- Posted
        -- TODO(#18319) Undo this length condition when the downstream pipeline can handle
        -- these longer notes
        AND LENGTH(Details) < 100000
    )
    SELECT * EXCEPT(OffenderId) 
    FROM person_id_with_external_ids 
    JOIN offender_notes
    ON person_id_with_external_ids.person_external_id = offender_notes.OffenderId
"""


def get_us_ix_case_update_info_query_provider(
    project_id: str,
    address_overrides: BigQueryAddressOverrides | None,
) -> BigQueryQueryProvider:
    query_builder = BigQueryQueryBuilder(address_overrides=address_overrides)

    formatted_query = query_builder.build_query(
        project_id=project_id,
        query_template=US_IX_CASE_UPDATE_INFO_QUERY_TEMPLATE,
        query_format_kwargs={
            # TODO(#29518): Update to normalized_state_dataset_for_state_code() once
            #  the ingest/normalization pipeline outputs all entities to the
            #  us_xx_normalized_state dataset, whether or not they are normalized.
            "normalized_state_dataset": dataset_config.NORMALIZED_STATE_DATASET,
            "us_ix_raw_data_up_to_date_dataset": raw_latest_views_dataset_for_region(
                state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
            ),
        },
    )

    return SimpleBigQueryQueryProvider(query=formatted_query)
