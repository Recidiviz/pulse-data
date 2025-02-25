# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""View of all US_IX case notes"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import CASE_NOTES_PROTOTYPE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_CASE_NOTES_VIEW_NAME = "us_ix_case_notes"

US_IX_CASE_NOTES_VIEW_DESCRIPTION = """All US_IX case notes"""

US_IX_CASE_NOTES_QUERY_TEMPLATE = """
    SELECT
        'US_IX' as state_code,
        OffenderId as external_id,
        OffenderNoteId as note_id,
        min(Details) as note_body,
        min('') as note_title,
        min(NoteDate) as note_date,
        min(NoteTypeDesc) as note_type,
        STRING_AGG(ContactModeDesc, ",") as note_mode
    FROM `{project_id}.us_ix_raw_data_up_to_date_views.ind_OffenderNote_latest` notes
    LEFT JOIN `{project_id}.us_ix_raw_data_up_to_date_views.ind_OffenderNoteInfo_latest` noteInfo
     USING(OffenderNoteInfoId)
    LEFT JOIN `{project_id}.us_ix_raw_data_up_to_date_views.ind_OffenderNoteInfo_ContactMode_latest`
     USING (OffenderNoteInfoId)
    LEFT JOIN `{project_id}.us_ix_raw_data_up_to_date_views.ind_ContactMode_latest`
     USING(ContactModeId)
    LEFT JOIN `{project_id}.us_ix_raw_data_up_to_date_views.ind_OffenderNoteStatus_latest`
     USING (OffenderNoteStatusId)
    LEFT JOIN `{project_id}.us_ix_raw_data_up_to_date_views.ref_NoteType_latest`
     USING(NoteTypeId)
    GROUP BY 1, 2, 3
"""

US_IX_CASE_NOTES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=CASE_NOTES_PROTOTYPE_DATASET,
    view_id=US_IX_CASE_NOTES_VIEW_NAME,
    view_query_template=US_IX_CASE_NOTES_QUERY_TEMPLATE,
    description=US_IX_CASE_NOTES_VIEW_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_CASE_NOTES_VIEW_BUILDER.build_and_print()
