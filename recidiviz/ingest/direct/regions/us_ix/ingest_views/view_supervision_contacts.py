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
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH contact_modes as (
    SELECT
        OffenderNoteInfoId,
        ARRAY_AGG(DISTINCT ContactModeId) as ContactModeIds,
        STRING_AGG(DISTINCT contact_method, ',' ORDER BY contact_method) as contact_method,
        STRING_AGG(DISTINCT contact_type, ',' ORDER BY contact_type) as contact_type,
        STRING_AGG(DISTINCT contact_reason, ',' ORDER BY contact_reason) as contact_reason,
        STRING_AGG(DISTINCT contact_location, ',' ORDER BY contact_location) as contact_location,
        STRING_AGG(DISTINCT contact_status, ',' ORDER BY contact_status) as contact_status
    FROM {ind_OffenderNoteInfo_ContactMode} modes 
    INNER JOIN {RECIDIVIZ_REFERENCE_contact_modes_mapping} map USING(ContactModeId)
    GROUP BY OffenderNoteInfoId
),
legacy_contacts AS (
    SELECT
        OffenderId, -- external_id
        OffenderNoteInfoId, -- external_id
        NoteDate,
        contact_type, -- contact_type
        contact_method,
        TRIM(REGEXP_EXTRACT(Details, r'Contact Title: ([a-zA-Z0-9_ ]+).* Next Appointment Date & Time:')) AS contact_title,
        TRIM(REGEXP_EXTRACT(Details, r'Contact Location: ([a-zA-Z0-9_ ]+).* Contact Result')) AS contact_location,
        TRIM(REGEXP_EXTRACT(Details, r'Contact Result: ([a-zA-Z0-9_ ]+).* Contact Subtype')) AS contact_result,
        REGEXP_CONTAINS(Details, r'Employment Verification: 1') AS verified_employment,
        REGEXP_CONTAINS(Details, r'Contact Result: ARREST') AS resulted_in_arrest,
        -- Some of the converted notes have a placeholder employee in `StaffId`, but the
        -- actual agent that performed the contact is in `InsertUserId`.
        IFNULL(staff_employee.EmployeeId, insert_employee.EmployeeId) AS EmployeeId,
        IFNULL(staff_employee.FirstName, insert_employee.FirstName) AS FirstName,
        IFNULL(staff_employee.MiddleName, insert_employee.MiddleName) AS MiddleName,
        IFNULL(staff_employee.LastName, insert_employee.LastName) AS LastName,
    FROM {ind_OffenderNote}
    LEFT JOIN {ind_OffenderNoteInfo} oni
        USING (OffenderNoteInfoId)
    LEFT JOIN contact_modes cms
        USING (OffenderNoteInfoId)
    LEFT JOIN {ind_OffenderNoteStatus}
        USING (OffenderNoteStatusId)
    LEFT JOIN {ref_Employee} staff_employee
        ON oni.StaffId = staff_employee.EmployeeId
    LEFT JOIN {ref_Employee} insert_employee
        ON oni.InsertUserId = insert_employee.EmployeeId
    WHERE NoteTypeId = '3' -- Filter for only Id = 3 to get "Supervision Notes"
    AND Details LIKE 'Contact Location:%'
    AND (DATE(NoteDate)) <= '2022-11-10' -- We only want to see legacy contacts from when legacy contacts were still getting converted for Atlas which seems to be 11-10-2022
),
atlas_contacts AS (
    -- TODO(#16038) - NOTE: This is a estimated query based on how we anticipate new Atlas contacts to be collected in data and
    -- subsequently ingested. For now, this query works, but should be revisited once Atlas data starts coming in to either filter
    -- for other ContactModeIds or change logic to match how date is actively coming in.
    SELECT
        OffenderId, -- external_id
        OffenderNoteInfoId, -- external_id
        NoteDate,
        contact_type,
        contact_method,
        contact_reason as contact_title,
        CASE WHEN contact_location like '%$IGNORE%' then '$IGNORE'
             ELSE contact_location
             END AS contact_location,
        contact_status as contact_result,
        (SELECT mode FROM UNNEST(ContactModeIds) AS mode WHERE mode IN ('17')) IS NOT NULL AS verified_employment,
        (SELECT mode FROM UNNEST(ContactModeIds) AS mode WHERE mode IN ('2053')) IS NOT NULL AS resulted_in_arrest,
        e.EmployeeId,
        e.FirstName,
        e.MiddleName,
        e.LastName,
    FROM {ind_OffenderNoteInfo} oni
    LEFT JOIN contact_modes
        USING (OffenderNoteInfoId)
    LEFT JOIN {ind_OffenderNoteStatus}
        USING (OffenderNoteStatusId)
    LEFT JOIN {ref_Employee} e
        ON oni.StaffId = e.EmployeeId
    WHERE NoteTypeId in ('3', '78')  -- Filter to Id = 3 to get "Supervision Notes" and Id = 78 to get "Laserfische Notes"
    AND (DATE(NoteDate)) >= '2022-11-10' -- We want Atlas data from when it started getting used, which seems to be 11-10-2022
    -- Since there's one day of overlap in the date range for legacy and new contacts, make sure we aren't grabbing legacy contacts
    AND OffenderNoteInfoId not in (select OffenderNoteInfoId from legacy_contacts)
)
SELECT
    OffenderId,
    OffenderNoteInfoId,
    NoteDate,
    contact_type,
    contact_method,
    UPPER(contact_title) as contact_title,
    UPPER(contact_location) as contact_location,
    UPPER(contact_result) as contact_result,
    verified_employment,
    resulted_in_arrest,
    EmployeeId,
    FirstName,
    MiddleName,
    LastName
FROM legacy_contacts
UNION ALL
SELECT
    OffenderId,
    OffenderNoteInfoId,
    NoteDate,
    contact_type,
    contact_method,
    contact_title,
    contact_location,
    contact_result,
    verified_employment,
    resulted_in_arrest,
    EmployeeId,
    FirstName,
    MiddleName,
    LastName
FROM atlas_contacts;
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="supervision_contacts",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
