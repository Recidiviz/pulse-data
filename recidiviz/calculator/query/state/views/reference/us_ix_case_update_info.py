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
"""BQ View containing US_IX case notes from OffenderNote, OffenderNoteInfo."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#16661) Rename US_IX -> US_ID in this file/code when we are ready to migrate the
# new ATLAS pipeline to run for US_ID
US_IX_CASE_UPDATE_INFO_VIEW_NAME = "us_ix_case_update_info"

US_IX_CASE_UPDATE_INFO_DESCRIPTION = """Provides agent case update notes for people on supervision in US_IX
    """

US_IX_CASE_UPDATE_INFO_QUERY_TEMPLATE = """
    WITH person_id_with_external_ids AS (
        SELECT
            person_id,
            external_id AS person_external_id,
            state_code
        FROM `{project_id}.{base_dataset}.state_person_external_id`
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

US_IX_CASE_UPDATE_INFO_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_IX_CASE_UPDATE_INFO_VIEW_NAME,
    view_query_template=US_IX_CASE_UPDATE_INFO_QUERY_TEMPLATE,
    description=US_IX_CASE_UPDATE_INFO_DESCRIPTION,
    base_dataset=dataset_config.STATE_BASE_DATASET,
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_CASE_UPDATE_INFO_VIEW_BUILDER.build_and_print()
