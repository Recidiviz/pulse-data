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
WITH contact_modes AS (
    SELECT
        OffenderNoteInfoId,
        ARRAY_AGG(ContactModeId ORDER BY ContactModeId) as ContactModeIds
    FROM {ind_OffenderNoteInfo_ContactMode}
    GROUP BY 1
), legacy_contacts AS (
    SELECT
        OffenderId, -- external_id
        OffenderNoteInfoId, -- external_id
        NoteDate,
        ContactModeDesc AS contact_type, -- contact_type
        TRIM(REGEXP_EXTRACT(Details, r'Contact Title: ([a-zA-Z0-9_ ]+).* Next Appointment Date & Time:')) AS contact_title,
        TRIM(REGEXP_EXTRACT(Details, r'Contact Location: ([a-zA-Z0-9_ ]+).* Contact Result')) AS contact_location,
        TRIM(REGEXP_EXTRACT(Details, r'Contact Result: ([a-zA-Z0-9_ ]+).* Contact Subtype')) AS contact_result,
        TRIM(REGEXP_EXTRACT(Details, r'Employment Verification: ([a-zA-Z0-9_ ]+).* Drug Related ')) AS verified_employment,
        TRIM(REGEXP_EXTRACT(Details, r'Contact Result: ARREST')) AS resulted_in_arrest,  -- resulted_in_arrest,
        -- Some of the converted notes have a placeholder employee in `StaffId`, but the
        -- actual agent that performed the contact is in `InsertUserId`.
        IFNULL(staff_employee.StaffId, insert_employee.StaffId) AS StaffId,
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
    LEFT JOIN {ind_ContactMode} cm
        -- Rarely (2 instances so far), there are multiple contact modes listed for a
        -- single legacy contact. Instead of aggregating like we do for Atlas contacts,
        -- we just pick one.
        ON cms.ContactModeIds[ORDINAL(1)] = cm.ContactModeId
    LEFT JOIN {ref_Employee} staff_employee
        ON oni.StaffId = staff_employee.EmployeeId
    LEFT JOIN {ref_Employee} insert_employee
        ON oni.InsertUserId = insert_employee.EmployeeId
    WHERE NoteTypeId = '3' -- Filter for only Id = 3 to get "Supervision Notes"
    AND Details LIKE 'Contact Location:%'
    AND CAST(NoteDate AS DATETIME) < '2022-11-14' -- We only want to see legacy contacts from before Atlas goes live on this date
),
atlas_group_contact_modes AS (
    -- TODO(#16038) - NOTE: This is a estimated query based on how we anticipate new Atlas contacts to be collected in data and
    -- subsequently ingested. For now, this query works, but should be revisited once Atlas data starts coming in to either filter
    -- for other ContactModeIds or change logic to match how date is actively coming in.
    SELECT
        OffenderId, -- external_id
        OffenderNoteInfoId, -- external_id
        NoteDate,
        NoteTypeId, -- filter for only Id = 3 to get "Supervision Notes"
        e.StaffId,  -- contact_agent_id
        e.FirstName,
        e.MiddleName,
        e.LastName,
        ContactModeIds -- contact_type
    FROM {ind_OffenderNoteInfo} oni
    LEFT JOIN contact_modes
        USING (OffenderNoteInfoId)
    LEFT JOIN {ind_OffenderNoteStatus}
        USING (OffenderNoteStatusId)
    LEFT JOIN {ref_Employee} e
        ON oni.StaffId = e.EmployeeId
    WHERE NoteTypeId = '3'  -- Filter for only Id = 3 to get "Supervision Notes"
    AND CAST(NoteDate AS DATETIME) >= '2022-11-14' -- We don't want any Atlas data before it is being used live which happens on this date
),
atlas_contacts AS (
    SELECT
        OffenderId, -- external_id
        OffenderNoteInfoId, -- external_id
        NoteDate,
        (SELECT mode
         FROM UNNEST(ContactModeIds) AS mode
         WHERE mode IN (
            '35',  -- Personal Telephone Contact
            '42',  -- Telephone
            '429', -- Collateral
            '447', -- Face To Face
            '474', -- Law Enforcement
            '475', -- Mental Health Collateral
            '484', -- Negative Contact
            '486', -- Parole Commission
            '526', -- Written Correspondence
            '581', -- Support System Collateral
            '591', -- Mail
            '905', -- Virtual
            '907'  -- APP Smart Phone
         ) ORDER BY mode LIMIT 1
        ) AS contact_type,
        (SELECT mode
         FROM UNNEST(ContactModeIds) AS mode
         WHERE mode IN (
            '221', -- EPICS Reinforcement
            '222', -- EPICS Disapproval
            '453', -- General Contact
            '585', -- 72 Hour Initial Contact
            '873', -- Monthly Report: Critical
            '874', -- Monthly Report: General
            '908', -- Conversion
            '915'  -- Critical Incident Case Review
         ) ORDER BY mode LIMIT 1
        ) AS contact_title, -- reason_raw_text
        (SELECT mode
         FROM UNNEST(ContactModeIds) AS mode
         WHERE mode IN (
            '14',  -- Court Action
            '16',  -- Employment Site Check
            '18',  -- Field Visit
            '21',  -- Home Contact
            '28',  -- Office Contact
            '107', -- Interstate Compact
            '471', -- Interstate
            '474', -- Law Enforcement
            '486', -- Parole Commission
            '524', -- WBOR System
            '553', -- Community Service
            '568', -- LSU Review (Semi-Annual)
            '592', -- Office
            '908', -- Conversion
            '962', -- Change in Employment
            '964', -- Field Contact
            '966'  -- Residence Verification
         ) ORDER BY mode LIMIT 1
        ) AS contact_location, -- location_raw_text and concat for method
        (SELECT mode
         FROM UNNEST(ContactModeIds) AS mode
         WHERE mode IN (
            '1',   -- Attempted Employment Verification
            '2',   -- Attempted Home Contact
            '4',   -- Attempted Telephone Contact
            '5',   -- Attempted Urine Screen
            '447', -- Face To Face
            '534', -- GPS/EM Successful Completion
            '535', -- GPS/EM Unsuccessful Completion
            '539', -- 60/60 Completion - Successful
            '540', -- 60/60 Completion - Unsuccessful
            '543', -- End GPS Pilot Successful (D1 D4 D6)
            '544', -- End GPS Pilot Unsuccessful (D1 D4 D6)
            '910'  -- ISI Dosage Progress Review
         ) ORDER BY mode LIMIT 1
        ) AS contact_result,
        (SELECT mode
         FROM UNNEST(ContactModeIds) AS mode
         WHERE mode IN (
            '17'
         ) ORDER BY mode LIMIT 1
        ) AS verified_employment,
        '0' AS resulted_in_arrest, -- Currently, no way in new Atlas data to identify this in ContactModes  - Will update after discussion with ID about if there is a way to ID it and how
        StaffId, -- contact_agent_id
        FirstName,
        MiddleName,
        LastName
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
    StaffId,
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
    contact_title,
    contact_location,
    contact_result,
    verified_employment,
    resulted_in_arrest,
    StaffId,
    FirstName,
    MiddleName,
    LastName
FROM atlas_contacts
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="supervision_contacts",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderId",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
