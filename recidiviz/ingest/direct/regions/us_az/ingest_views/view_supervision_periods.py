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
"""Query containing supervision period information.

The data sources in this view are the same as those in the 960 report generated in ACIS,
which is the most reliable source of information about the current supervision population
available to ADCRR. This ingest view query is an adaptation of the query used to generate
that report, so that the report can be historical and contain more detailed start and end
reasons.

In the DPP_EPISODE raw data table, the logical definition of "Active DPP Period" is:
- STATUS_ID = '1747' (Active), 
- SUPERVISION_LEVEL_ENDDATE IS NULL or 'NULL', 
- ACTIVE_FLAG = 'Y'

However, there are many cases where a period is inactive but still meets some or all of those criteria.
This view uses the following methods to identify and close periods in those circumstances. Each of these
approaches has been approved by ACIS administrators at ADCRR. 
1) Periods have a SUPERVISION_LEVEL_ENDDATE in DPPE but no case closure tracked in the movements table.
   - Use the SUPERVISION_LEVEL_ENDDATE as the end_date for the period.
   - Only do this where the start_reason of the final period is not 'Releasee Abscond', since DPPE considers an absconsion a period-closing event.
2) DPPE.ACTIVE_FLAG = 'N' and STATUS_ID = '1748'(Inactive), even if there is no SUPERVISION_LEVEL_ENDDATE. 
   - Use previous start date as the end date (create only zero-day periods). 
3) Periods that fulfill all of the logical "Active DPP Period" criteria listed above, but either: 
   a) Had intake completed at an office for which DPP_OFFICE_LOCATION.ACTIVE_FLAG = 'N' (the office no longer exists)
   b) Had intake completed > 5 years ago by an officer who is no longer active in MEA_PROFILES, and nothing has been tracked since then. 
   - Use previous start date as the end date (create only zero-day periods). 
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- Get start date and supervision level assigned on that date 
get_start_date AS (
    SELECT DISTINCT
        DPPE.PERSON_ID,
        DPPE.DPP_ID,
        SUBSTR(UPPER(COALESCE(LLEVEL.CODE, 'UNK')), 1, 3) AS SUPV_LEVEL,
        CAST(NULL AS STRING) AS OFFICE_NAME, 
        CAST(NULL AS STRING) AS OFFICER,
        CASE 
            WHEN NULLIF(LRELTYPE.DESCRIPTION, 'NULL') IS NOT NULL THEN CONCAT('START - ', LRELTYPE.DESCRIPTION)
            ELSE 'DPPE START'
        END AS MOVEMENT_DESCRIPTION,
        CAST(DPPE.SUPERVISION_LEVEL_STARTDATE AS DATETIME) AS BEGAN_DATE,
        CAST(NULL AS DATETIME) as INTAKE_DATE,
        CAST(NULL AS DATETIME) AS DATE_ASSESSMENT,
        CAST(NULL AS DATETIME) AS CRITICAL_MOVEMENT_DATE,
    FROM {DPP_EPISODE} DPPE 
    LEFT JOIN {LOOKUPS} LLEVEL ON DPPE.SUPERVISION_LEVEL_ID = LLEVEL.LOOKUP_ID
    LEFT JOIN {LOOKUPS} LRELTYPE ON DPPE.RELEASE_TYPE_ID = LRELTYPE.LOOKUP_ID
),
-- Get dates that supervising officer changed
get_officer_change_dates AS (
    SELECT DISTINCT
        DIA.PERSON_ID,
        DIA.DPP_ID,
        CAST(NULL AS STRING) AS SUPV_LEVEL,
        -- Sometimes two officers or locations are assigned at the exact same time. This chooses one randomly but deterministically.
        FIRST_VALUE(DOL.LOCATION_NAME) OVER location_agent_window AS OFFICE_NAME, 
        FIRST_VALUE(DIA.AGENT_ID) OVER location_agent_window AS OFFICER,
        'OFFICER CHANGE' AS MOVEMENT_DESCRIPTION,
        CAST(NULL AS DATETIME) AS BEGAN_DATE,
        CAST(DIA.ASSIGNED_FROM AS DATETIME) AS INTAKE_DATE,
        CAST(NULL AS DATETIME) AS DATE_ASSESSMENT,
        CAST(NULL AS DATETIME) AS CRITICAL_MOVEMENT_DATE,
    FROM {DPP_INTAKE_ASSIGNMENT} DIA
    JOIN {DPP_EPISODE} DPPE ON(DIA.DPP_ID = DPPE.DPP_ID)
    LEFT JOIN {DPP_OFFICE_LOCATION} DOL ON DIA.OFFICE_LOCATION_ID = DOL.OFFICE_LOCATION_ID
    WINDOW location_agent_window AS (PARTITION BY DIA.PERSON_ID, DIA.DPP_ID, DIA.ASSIGNED_FROM 
    ORDER BY INTAKE_ASSIGNMENT_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
),
-- Get dates that supervision level changed
get_level_change_dates AS (
    SELECT DISTINCT
        DPPE.PERSON_ID,
        ACCAT.DPP_ID,
        SUBSTR(UPPER(COALESCE(LLEVEL.CODE, 'UNK')), 1, 3) AS SUPV_LEVEL,
        CAST(NULL AS STRING) AS OFFICE_NAME, 
        CAST(NULL AS STRING) AS OFFICER,
        'SUPV LEVEL ASSESSMENT' AS MOVEMENT_DESCRIPTION,
        CAST(NULL AS DATETIME) AS BEGAN_DATE,
        CAST(NULL AS DATETIME) AS INTAKE_DATE,
        CAST(DATE_ASSESSMENT AS DATETIME) AS DATE_ASSESSMENT,
        CAST(NULL AS DATETIME) AS CRITICAL_MOVEMENT_DATE,
    FROM {AZ_CS_OMS_ACCAT} ACCAT
    JOIN {DPP_EPISODE} DPPE ON DPPE.DPP_ID = ACCAT.DPP_ID
    LEFT JOIN {LOOKUPS} LLEVEL ON ACCAT.LEVEL_ID = LLEVEL.LOOKUP_ID
    WHERE ACCAT.DATE_ASSESSMENT IS NOT NULL
),
-- Get dates that particular period-ending movements happened and their descriptions, 
-- This is the first method by which we close periods. The remaining methods are included
-- in the "infer_ends" CTE.
get_dates_from_movements AS (
    SELECT DISTINCT
        COALESCE(DPPE.PERSON_ID, DOCE.PERSON_ID) AS PERSON_ID, 
        COALESCE(NULLIF(ADITH.DPP_ID,'NULL'), DOCE.DPP_ID) AS DPP_ID,
        CAST(NULL AS STRING) AS SUPV_LEVEL,
        CAST(NULL AS STRING) AS OFFICE_NAME, 
        CAST(NULL AS STRING) AS OFFICER,
        CASE WHEN
            UPPER(MV.MOVEMENT_DESCRIPTION) IN('RELEASEE ABSCOND', 'WARRANT QUASHED') THEN MV.MOVEMENT_DESCRIPTION
            ELSE CONCAT('END - ', MV.MOVEMENT_DESCRIPTION) 
        END AS MOVEMENT_DESCRIPTION,
        CAST(NULL AS DATETIME) AS BEGAN_DATE,
        CAST(NULL AS DATETIME) AS INTAKE_DATE,
        CAST(NULL AS DATETIME) AS DATE_ASSESSMENT,
        CAST(MOVEMENT_DATE AS DATETIME) AS CRITICAL_MOVEMENT_DATE,
    FROM {AZ_DOC_INMATE_TRAFFIC_HISTORY} ADITH
    LEFT JOIN {DPP_EPISODE} DPPE 
    USING (DPP_ID)
    LEFT JOIN {DOC_EPISODE} DOCE
    USING(DOC_ID)
    LEFT JOIN {AZ_DOC_MOVEMENT_CODES} MV
    USING(MOVEMENT_CODE_ID)
    LEFT JOIN {LOOKUPS} LOGIC_LOOKUP
    ON(MV.PRSN_CMM_SUPV_EPSD_LOGIC_ID = LOGIC_LOOKUP.LOOKUP_ID)
    -- a period ended (includes absconsions)
    WHERE LOGIC_LOOKUP.OTHER_2 = 'Close' 
    -- the final three of these can all signify a return from absconsion
    OR UPPER(MOVEMENT_DESCRIPTION) IN (
        'RELEASEE ABSCOND', 
        'ADMIN ACTION CASE CLOSED', 
        'RETURN FROM ABSCOND', 
        'TEMPORARY PLACEMENT', 
        'IN CUSTODY - OTHER', 
        'WARRANT QUASHED'
        )
),
-- Collect all critical dates 
all_dates AS (
SELECT * FROM get_start_date
UNION ALL 
SELECT * FROM get_officer_change_dates
UNION ALL
SELECT * FROM get_level_change_dates
UNION ALL
SELECT * FROM get_dates_from_movements
), 
-- Since each row in this CTE is associated with a change to either the assigned office,
-- officer, or supervision level, we assume that whatever attributes are not changed in 
-- a given row are the same as they were the last time they were assigned. This CTE 
-- carries forward the last assigned values of unaltered attributes to rows tracking a 
-- change to another attribute.
carry_forward_attributes AS (
SELECT 
    PERSON_ID,
    DPP_ID,
    -- When a supervision officer / office was assigned before a person was released from 
    -- prison, we want to carry that officer's information forward. We also want to attach
    -- this information to supervision level changes that do not have an officer associated, 
    -- because the officer has not changed.
    LAST_VALUE(OFFICE_NAME IGNORE NULLS) OVER person_window AS OFFICE_NAME,
    LAST_VALUE(OFFICER IGNORE NULLS) OVER person_window AS OFFICER,
    LAST_VALUE(SUPV_LEVEL IGNORE NULLS) OVER person_window AS SUPV_LEVEL,
    COALESCE(BEGAN_DATE, INTAKE_DATE, DATE_ASSESSMENT, CRITICAL_MOVEMENT_DATE) AS period_start_date,
    LAST_VALUE(BEGAN_DATE IGNORE NULLS) OVER person_window AS BEGAN_DATE,
    INTAKE_DATE,
    DATE_ASSESSMENT,
    CRITICAL_MOVEMENT_DATE,
    MOVEMENT_DESCRIPTION
FROM all_dates
WINDOW person_window AS (PARTITION BY PERSON_ID, DPP_ID ORDER BY COALESCE(BEGAN_DATE, INTAKE_DATE, DATE_ASSESSMENT, CRITICAL_MOVEMENT_DATE), 
CASE 
    WHEN MOVEMENT_DESCRIPTION LIKE 'START - %' OR MOVEMENT_DESCRIPTION = 'DPPE START' THEN 1
    WHEN MOVEMENT_DESCRIPTION IN ('OFFICER CHANGE', 'SUPV LEVEL ASSESSMENT') THEN 2
    WHEN MOVEMENT_DESCRIPTION IN ('Releasee Abscond', 'Warrant Quashed') THEN 3
    WHEN MOVEMENT_DESCRIPTION LIKE 'END - %' OR MOVEMENT_DESCRIPTION = 'DPPE END' THEN 4
END
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
), 
-- Create periods based on the critical dates with all attributes carried forward as appropriate.
periods AS (
SELECT
    PERSON_ID, 
    DPP_ID,
    OFFICE_NAME,
    OFFICER,
    SUPV_LEVEL,
    period_start_date AS start_date,
    LEAD(period_start_date) OVER person_window AS end_date,
    MOVEMENT_DESCRIPTION AS start_reason,
    LEAD(MOVEMENT_DESCRIPTION) OVER person_window AS end_reason,
FROM carry_forward_attributes
WHERE period_start_date >= BEGAN_DATE
WINDOW person_window AS (PARTITION BY PERSON_ID, DPP_ID ORDER BY period_start_date, 
CASE 
    WHEN MOVEMENT_DESCRIPTION LIKE 'START - %' OR MOVEMENT_DESCRIPTION = 'DPPE START' THEN 1
    WHEN MOVEMENT_DESCRIPTION IN ('OFFICER CHANGE', 'SUPV LEVEL ASSESSMENT') THEN 2
    WHEN MOVEMENT_DESCRIPTION = 'Releasee Abscond' THEN 3
    WHEN MOVEMENT_DESCRIPTION LIKE 'END - %' OR MOVEMENT_DESCRIPTION = 'DPPE END' THEN 4
END)
),
-- This CTE takes the logical definition of "Active DPP Period" (DPPE.STATUS_ID = '1747' (Active), 
-- SUPERVISION_LEVEL_ENDDATE IS NULL or 'NULL', DPPE.ACTIVE_FLAG = 'Y') and closes periods
-- that fulfill all of those criteria. It also infers ends to periods. There are a few ways
-- to infer that a period is actually not active:
-- 1) Periods have a SUPERVISION_LEVEL_ENDDATE in DPPE but no end_date to account for missing case closures in the movements table.
--    - Only do this where the start_reason of the final period is not 'Releasee Abscond', since DPPE considers an absconsion a period-closing event.
-- 2) DPPE.ACTIVE_FLAG = 'N' and STATUS_ID = '1748'(Inactive), even if there is no SUPERVISION_LEVEL_ENDDATE. 
--    - Use previous start date as the end date (create only zero-day periods). 
-- 3) Periods that fulfill all of the logical "Active DPP Period" criteria listed above, but either: 
--    a) Had intake completed at an office for which DPP_OFFICE_LOCATION.ACTIVE_FLAG = 'N' (the office no longer exists)
--    b) Had intake completed > 5 years ago by an officer who is no longer active in MEA_PROFILES, and nothing has been tracked since then. 
infer_ends AS (
SELECT
 periods.* EXCEPT(end_date, start_reason, end_reason),
 CASE
   -- case has no end date but is marked as inactive
  WHEN end_date IS NULL AND start_reason != 'Releasee Abscond' AND NULLIF(DPPE.SUPERVISION_LEVEL_ENDDATE,'NULL') IS NULL AND DPPE.STATUS_ID = '1748' THEN start_date
  -- intake happened in an office that no longer exists
  WHEN end_date IS NULL AND DOL.ACTIVE_FLAG = 'N' THEN start_date
  -- officer is not active in MEA_PROFILES and the period started > 5 years ago
  WHEN end_date IS NULL AND MEA.IS_ACTIVE = 'N' AND DATE_DIFF(CURRENT_DATE, start_date, YEAR) > 5 THEN start_date
  -- actual period closures, as expected - use SUPERVISION_LEVEL_ENDDATE, where there is one that is not the start of a period of absconsion
  WHEN end_date IS NULL AND start_reason != 'Releasee Abscond' THEN CAST(NULLIF(DPPE.SUPERVISION_LEVEL_ENDDATE,'NULL') AS DATETIME)
  -- always NULL until this point
   ELSE end_date
 END AS end_date,
 start_reason,
 CASE
  -- actual period closures, as expected
  WHEN end_date IS NULL AND start_reason != 'Releasee Abscond' AND NULLIF(DPPE.SUPERVISION_LEVEL_ENDDATE,'NULL') IS NOT NULL THEN 'DPPE END'
  -- case has no end date but is marked as inactive
  WHEN end_date IS NULL AND start_reason != 'Releasee Abscond' AND NULLIF(DPPE.SUPERVISION_LEVEL_ENDDATE,'NULL') IS NULL AND DPPE.STATUS_ID = '1748' THEN 'END - INFERRED MIGRATION ERROR'
  -- intake happened in an office that no longer exists
  WHEN end_date IS NULL AND DOL.ACTIVE_FLAG = 'N' THEN 'END - INFERRED MIGRATION ERROR'
  -- officer is not active in MEA_PROFILES and the period started > 5 years ago
  WHEN end_date IS NULL AND MEA.IS_ACTIVE = 'N' AND DATE_DIFF(CURRENT_DATE, start_date, YEAR) > 5 THEN 'END - INFERRED MIGRATION ERROR'
  -- always NULL until this point
   ELSE end_reason
 END AS end_reason,
FROM periods
LEFT JOIN {DPP_EPISODE} DPPE USING(DPP_ID)
LEFT JOIN {DPP_OFFICE_LOCATION} DOL ON(periods.OFFICE_NAME = DOL.LOCATION_NAME)
LEFT JOIN {MEA_PROFILES} MEA ON (periods.OFFICER = MEA.USERID)
)
SELECT * FROM (
SELECT 
    PERSON_ID, 
    DPP_ID,
    OFFICE_NAME,
    OFFICER,
    SUPV_LEVEL,
    -- in the DPP_EPISODE table, 0001-01-01 is used in place of NULL. 
    -- When there are legitimate episodes associated with those dates based on data in other tables,
    -- we want to update that date to be the closest real date we know to be a part of the period. 
    -- Otherwise, we need to exclude the row. 
    CASE 
        WHEN end_date IS NOT NULL AND start_date = '0001-01-01' THEN end_date
        ELSE start_date
    END AS start_date,
    CASE 
        WHEN start_date != '0001-01-01' AND end_date = '0001-01-01' THEN start_date
        ELSE end_date
    END AS end_date,
    REGEXP_REPLACE(start_reason, r'START - ', '') AS start_reason,
    REGEXP_REPLACE(end_reason, r'^END - ', '') AS end_reason,
    ROW_NUMBER() OVER (PARTITION BY PERSON_ID, DPP_ID ORDER BY START_DATE, END_DATE NULLS LAST, 
    CASE 
        WHEN start_reason LIKE 'START - %' OR start_reason = 'DPPE START' THEN 1
        WHEN start_reason IN ('OFFICER CHANGE', 'SUPV LEVEL ASSESSMENT') THEN 2
        WHEN start_reason = 'Releasee Abscond' THEN 3
        WHEN end_reason LIKE 'END - %' OR end_reason = 'DPPE END' THEN 4
    END) AS period_seq
FROM infer_ends
WHERE start_reason NOT LIKE "END - %"
AND start_reason NOT LIKE '%DPPE END'
)
WHERE start_date != '0001-01-01'
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="supervision_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
