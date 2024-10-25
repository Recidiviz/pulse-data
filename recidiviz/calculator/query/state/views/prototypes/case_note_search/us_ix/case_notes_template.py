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
"""View logic to prepare US_IX case notes for search"""

US_IX_CASE_NOTES_TEMPLATE = """
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
