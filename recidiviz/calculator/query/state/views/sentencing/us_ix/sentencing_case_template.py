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
"""View logic to prepare US_IX Sentencing case data for PSI tools"""

US_IX_SENTENCING_CASE_TEMPLATE = """
WITH
    -- this CTE gathers all the basic info on a PSI case
    case_info_cte AS (
    SELECT DISTINCT
        OffenderId AS client_id,
        "US_IX" AS state_code,
        AssignedToUserId AS staff_id,
        DueDate AS due_date,
        CompletedDate AS completion_date,
        SentenceDate AS sentence_date,
        AssignedDate AS assigned_date,
        loc.LocationName AS county,
        PSIReportId AS external_id,
        id.person_id
    FROM 
    `{project_id}.{us_ix_raw_data_up_to_date_dataset}.com_PSIReport_latest` psi
    LEFT JOIN `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ref_Location_latest` loc 
        USING(LocationId)
    LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` id 
        on psi.OffenderId = id.external_id and id_type = 'US_IX_DOC'
    ),
    -- this CTE uses the OffenderNote table to infer the type of PSI report requested
    report_type_cte AS (
        SELECT 
            psi.PSIReportId AS external_id,
            ContactModeDesc AS report_type,
            ROW_NUMBER() OVER (PARTITION BY PSIReportId ORDER BY NoteDate) AS recency_rank
        FROM
        `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ind_OffenderNote_latest`
        LEFT JOIN `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ind_OffenderNoteInfo_latest`
        USING(OffenderNoteInfoId)
        LEFT JOIN `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ind_OffenderNoteInfo_ContactMode_latest`
        USING (OffenderNoteInfoId)
        LEFT JOIN `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ind_ContactMode_latest`
        USING(ContactModeId)
        LEFT JOIN `{project_id}.{us_ix_raw_data_up_to_date_dataset}.com_PSIReport_latest` psi
        USING(OffenderId)
        WHERE NoteDate >= OrderDate and NoteDate < CompletedDate
        AND ContactModeDesc IN 
        ("PSI File Review Assigned","PSI Assigned Full",
       "PSI File Review w/LSI Assigned")
    ),
    -- this CTE connects the report type back with the general case info.
    case_info_with_report_type AS (
        SELECT
            ci.*,
           report_type
        FROM case_info_cte ci
        LEFT JOIN report_type_cte rt
            USING(external_id)
        WHERE recency_rank = 1
    ),
    -- this CTE grabs the most recent assessment score and level
    most_recent_score_cte AS
    (
       SELECT 
            *
        FROM (
            SELECT 
                        lsir.person_id,
                        lsir.assessment_score,
                        lsir.assessment_level,
                        lsir.assessment_date,
                        ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY assessment_date DESC, external_id DESC) AS recency_rank
            FROM `{project_id}.{normalized_state_dataset}.state_assessment` lsir
        )
        WHERE recency_rank = 1
    ),
    -- This CTE assigns the most recent LSIR score/level to the case;
    -- Typically old assessments are used when a full PSI isn't assigned.
    -- However, when the report_type is "PSI Assigned Full" and there is not an 
    -- assessment that falls into the span of the investigation, it's assigned NULL 
    -- since an assessment should be done. 
    case_info_with_report_type_and_assessment AS (
    SELECT 
        cirt.*,
        CASE
            WHEN report_type = "PSI Assigned Full" OR 
            report_type = "PSI File Review w/LSI Assigned"
                    AND assessment_date >= date(assigned_date) 
                    AND assessment_date <= date(completion_date)
                THEN CAST(assessment_score AS INT64)
            WHEN report_type = "PSI File Review Assigned" 
                THEN CAST(assessment_score AS INT64)
            ELSE NULL
        END AS lsir_score,
         CASE
            WHEN report_type =  "PSI Assigned Full" OR 
            report_type = "PSI File Review w/LSI Assigned" 
                    AND assessment_date >= date(assigned_date) 
                    AND assessment_date <= date(completion_date)
                THEN assessment_level
            WHEN report_type = "PSI File Review Assigned%"
                THEN assessment_level
            ELSE NULL
        END AS lsir_level
    FROM case_info_with_report_type cirt
    LEFT JOIN most_recent_score_cte mrs
        ON mrs.person_id = cirt.person_id)
    SELECT
        *
    FROM case_info_with_report_type_and_assessment
"""
