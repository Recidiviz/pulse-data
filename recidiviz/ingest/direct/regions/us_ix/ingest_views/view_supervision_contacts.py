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
"""Query that generates the state supervision contacts entity using the following tables: """
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH legacy_contacts AS (
    SELECT
        OffenderId, #external_id
        OffenderNoteInfoId, #external_id
        NoteDate,
        ContactModeDesc AS contact_type, #contact_type
        TRIM(REGEXP_EXTRACT(Details, r'Contact Title: ([a-zA-Z0-9_ ]+).* Next Appointment Date & Time:')) AS contact_title, 
        TRIM(REGEXP_EXTRACT(Details, r'Contact Location: ([a-zA-Z0-9_ ]+).* Contact Result')) AS contact_location, 
        TRIM(REGEXP_EXTRACT(Details, r'Contact Result: ([a-zA-Z0-9_ ]+).* Contact Subtype')) AS contact_result , 
        TRIM(REGEXP_EXTRACT(Details, r'Employment Verification: ([a-zA-Z0-9_ ]+).* Drug Related ')) AS verified_employment,
        TRIM(REGEXP_EXTRACT(Details, r'Contact Result: ARREST')) AS resulted_in_arrest, #resulted_in_arrest,
        e.StaffId #contact_agent_id
    FROM {ind_OffenderNote}
    LEFT JOIN {ind_OffenderNoteInfo} oni
        USING (OffenderNoteInfoId)
    LEFT JOIN {ind_OffenderNoteInfo_ContactMode}
        USING (OffenderNoteInfoId)
    LEFT JOIN {ind_OffenderNoteStatus}
        USING (OffenderNoteStatusId)
    LEFT JOIN {ind_ContactMode}
        USING (ContactModeId)
    LEFT JOIN {ref_Employee} e
        ON oni.StaffId = e.EmployeeId
    WHERE NoteTypeId = '3' # Filter for only Id = 3 to get "Supervision Notes"
    AND Details LIKE 'Contact Location:%'
    AND CAST(NoteDate AS DATETIME) < '2022-11-14' # We only want to see legacy contacts from before Atlas goes live on this date
),
atlas_group_contact_modes AS (
    # TODO(#16038) - NOTE: This is a estimated query based on how we anticipate new Atlas contacts to be collected in data and 
    # subsequently ingested. For now, this query works, but should be revisited once Atlas data starts coming in to either filter 
    # for other ContactModeIds or change logic to match how date is actively coming in. 
    SELECT
        OffenderId, #external_id
        # Note: For one "contact", there may be 1 to many NoteIds with the same NoteInfoId that show N different "methods" 
        # that occurred during the same "contact session". For this reason, we keep OffenderNoteInfoId in the external_id 
        # and string_agg across that to get all modes aggregated to parse for various contact fields.
        OffenderNoteInfoId, 
        NoteDate,
        NoteTypeId, # filter for only Id = 3 to get "Supervision Notes"
        STRING_AGG(ContactModeId,',') as ContactModes, #contact_type
        e.StaffId #contact_agent_id
    FROM {ind_OffenderNote}
    LEFT JOIN {ind_OffenderNoteInfo} oni
        USING (OffenderNoteInfoId)
    LEFT JOIN {ind_OffenderNoteInfo_ContactMode}
        USING (OffenderNoteInfoId)
    LEFT JOIN {ind_OffenderNoteStatus}
        USING (OffenderNoteStatusId)
    LEFT JOIN {ind_ContactMode}
        USING (ContactModeId)
    LEFT JOIN {ref_Employee} e
        ON oni.StaffId = e.EmployeeId
    WHERE NoteTypeId = '3'  # Filter for only Id = 3 to get "Supervision Notes"
    AND Details NOT LIKE 'Contact Location%' # Filters for only non-legacy Atlas contacts
    AND CAST(NoteDate AS DATETIME) >= '2022-11-14' # We don't want any Atlas data before it is being used live which happens on this date
    GROUP BY 1, 2, 3, 4,6
),
atlas_contacts AS (
    SELECT 
        OffenderId, #external_id
        OffenderNoteInfoId, #external_id
        NoteDate,
        REGEXP_EXTRACT(ContactModes, r'905|447|35|42|526|484|474|907|486|591|429|475|581') AS contact_type,
        REGEXP_EXTRACT(ContactModes, r'585|873|915|453|874|908|221|222') AS contact_title, #reason_raw_text
        REGEXP_EXTRACT(ContactModes, r'592|28|471|966|21|14|568|962|964|18|16|474|486|107|524|553|908') AS contact_location, #location_raw_text and concat for method
        REGEXP_EXTRACT(ContactModes, r'1|2|4|5|447|539|540|543|544|534|535|910') AS contact_result,
        REGEXP_EXTRACT(ContactModes, r'17') AS verified_employment,  
        '0' AS resulted_in_arrest, #Currently, no way in new Atlas data to identify this in ContactModes  - Will update after discussion with ID about if there is a way to ID it and how
        StaffId #contact_agent_id
    FROM atlas_group_contact_modes
)
SELECT 
    OffenderId,
    OffenderNoteInfoId,
    NoteDate,
    contact_type,
    contact_title, 
    contact_location,
    contact_result,
    verified_employment, 
    resulted_in_arrest,
    StaffId
FROM legacy_contacts
UNION ALL
SELECT 
    OffenderId,
    OffenderNoteInfoId,
    NoteDate,
    contact_type,
    contact_title, 
    contact_location,
    contact_result,
    verified_employment, 
    resulted_in_arrest,
    StaffId
FROM atlas_contacts
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_ix",
    ingest_view_name="supervision_contacts",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderId",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
