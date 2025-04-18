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
"""Query containing supervision period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- getting intial information from Parolee table and cleaning out string NULLs
parole_periods AS (
  SELECT 
    paroleeInformationId,
    inmateNumber,
    CASE 
        WHEN inmateNumber IN (SELECT DISTINCT inmateNumber FROM {PIMSParoleeInformation_Aud}) 
        THEN NULL
        ELSE fk_paroleOfficerId
    END AS original_paroleOfficerID,
    paroleType, 
    currentStatusCode,
    countyParoledToCode,
    inOutStateIndicator1Code,
    inOutStateIndicator2Code,
    inOutStateIndicator3Code,
    dischargeType AS endReason,
    revocationDate,
    dateOfParole AS startDate, 
    dischargeDate AS endDate,
  FROM {PIMSParoleeInformation}
  WHERE dateOfParole IS NOT NULL
), 
-- decode table for county and inourstateindicators
codevalues AS (
  SELECT 
    codeId,
    CodeValue
  FROM {CodeValue}
),
-- NE supervision officers use supervisionLevelOverrideCode for how they supervise, and only riskLevel if this is null - so they have asked if we create a tool to display the override, I will add a CTE to coalesce the fields, but this may come up in later discussions
all_supervision_levels AS (
  SELECT 
    inmateNumber,
    riskLevel,
    assessmentDate,
    supervisionLevelOverrideCode, 
  FROM {ORASClientRiskLevelAndNeeds}
  WHERE assessmentDate IS NOT NULL
  -- TODO(#36975): re-check non-determinsm once we receive data daily 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY inmateNumber, assessmentDate ORDER BY createdDate DESC) = 1
), 
-- NE uses the override field to determine what level someone should be supervised at. For example if someone scores LOW on the ORAS Risk assessment but is a sex offender they have to be supervised at a high level for the first 6 months, so the override field captures the actual supervised level. 
clean_supervision_levels AS (
  SELECT 
    inmateNumber,
    assessmentDate,
    COALESCE(codevalue, risklevel) AS supervisedLevel
  FROM all_supervision_levels
  LEFT JOIN codevalues 
    ON supervisionLevelOverrideCode = codeid
), 
-- looking for previous supervision levels, since we only want to create new periods when the supervision level changes
previous_supervised_level AS (
  SELECT
    inmateNumber, 
    assessmentDate,
    supervisedLevel, 
    IFNULL(LAG(supervisedLevel) OVER(PARTITION BY inmateNumber ORDER BY assessmentDate), 'X') AS previousLevel
  FROM clean_supervision_levels
  WHERE supervisedLevel IS NOT NULL
),
-- only selecting changes in supervisedLevel if the level actually changes
supervision_changes AS (
   SELECT
    inmateNumber, 
    assessmentDate,
    supervisedLevel, 
  FROM previous_supervised_level
  WHERE supervisedLevel != previousLevel
),
-- adding additional information into supervision periods before splitting and recreated with supervision changes
adding_codes_to_periods AS (
  SELECT 
    paroleeInformationId,
    inmateNumber,
    paroleType, 
    original_paroleOfficerID,
    currentStatusCode,
    county.codevalue AS countyParoledTo,
    code1.codeValue AS inOutStateIndicator1Code,
    code2.codeValue AS inOutStateIndicator2Code,
    code3.codeValue AS inOutStateIndicator3Code,
    endReason,
    startDate, 
    -- NE uses a different date field for revocation vs dishcarge of parole, but they also designate why parole ended in dischargeType, so combining these columns
    IFNULL(endDate, revocationDate) AS endDate,
  FROM parole_periods
  LEFT JOIN codevalues code1
  ON inOutStateIndicator1Code = code1.CodeId
  LEFT JOIN codevalues code2
  ON inOutStateIndicator2Code = code2.CodeId
  LEFT JOIN codevalues code3
  ON inOutStateIndicator3Code = code3.CodeId
  LEFT JOIN codevalues county
  ON countyParoledToCode = county.CodeId 
), 
-- joining in supervision level changes
adding_supervision_levels AS (
  SELECT
    paroleeInformationId,
    p.inmateNumber,
    sc.supervisedLevel, 
    sc.assessmentDate,
    paroleType, 
    original_paroleOfficerID,
    currentStatusCode,
    countyParoledTo,
    inOutStateIndicator1Code,
    inOutStateIndicator2Code,
    inOutStateIndicator3Code,
    endReason,
    startDate, 
    endDate,
  FROM adding_codes_to_periods p
  LEFT JOIN supervision_changes sc
  ON p.inmateNumber = sc.inmateNumber
  AND sc.assessmentDate BETWEEN startDate AND COALESCE(endDate, '9999-01-01')
), 
-- setting up to reorder periods with new dates for supervision level changes
setup_split_periods_for_supervision_changes AS (
  SELECT 
    paroleeInformationId AS OLD_PERIOD,
    LEAD(paroleeInformationId) OVER(PARTITION BY inmateNumber ORDER BY paroleeInformationId, startDate) AS NEXT_PERIOD,
    LAG(paroleeInformationId) OVER(PARTITION BY inmateNumber ORDER BY paroleeInformationId, startDate) AS LAST_PERIOD,
    inmateNumber,
    supervisedLevel,
    assessmentDate,
    paroleType, 
    original_paroleOfficerID,
    currentStatusCode,
    countyParoledTo,
    inOutStateIndicator1Code,
    inOutStateIndicator2Code,
    inOutStateIndicator3Code,
    endReason,
    startDate, 
    endDate,
  FROM adding_supervision_levels
), 
-- Breaking up duplicate periods with different supervision levels by using startDate to end one and begin the next, 
-- also adding "supervision level change" as admission/release reason for these instances.
split_periods_with_multiple_supervision_levels AS (
  SELECT 
    OLD_PERIOD,
    NEXT_PERIOD,
    LAST_PERIOD,
    inmateNumber,
    assessmentDate,
    supervisedLevel,
    paroleType, 
    original_paroleOfficerID,
    currentStatusCode,
    countyParoledTo,
    inOutStateIndicator1Code,
    inOutStateIndicator2Code,
    inOutStateIndicator3Code,
    IF(OLD_PERIOD = LAST_PERIOD, 'Supervision Level Change', paroleType) AS startReason,
    IF(OLD_PERIOD = NEXT_PERIOD, 'Supervision Level Change', endReason) AS endReason,
    IF(OLD_PERIOD = LAST_PERIOD, LAG(assessmentDate) OVER(PARTITION BY inmateNumber ORDER BY OLD_PERIOD, startDate), startDate) AS startDate, 
    IF(OLD_PERIOD = NEXT_PERIOD, assessmentDate, endDate) AS endDate, 
  FROM setup_split_periods_for_supervision_changes
), 
-- re ordering the periods for accurate period id
renumber_periods AS (
  SELECT 
    ROW_NUMBER() OVER (PARTITION BY inmateNumber ORDER BY startDate, endDate NULLS LAST) AS periodID,
    inmateNumber,
    supervisedLevel,
    paroleType, 
    original_paroleOfficerID,
    currentStatusCode,
    countyParoledTo,
    inOutStateIndicator1Code,
    inOutStateIndicator2Code,
    inOutStateIndicator3Code,
    startReason,
    endReason,
    startDate, 
    endDate, 
  FROM split_periods_with_multiple_supervision_levels
),
-- The PIMSParolleInformation table was first implemented in 2010, but the Audit table 
-- was incorporated in 2020. Any changes to prior records resulted in MODIFIED, but new 
-- records would have CREATED. There will be people with no records in the audit table.
clean_audit_table AS (
  SELECT 
    paroleeInformationAudId, 
    paroleeInformationId,
    inmateNumber, 
    fk_paroleOfficerID,
    operation, 
    NULLIF(IF(modifiedDateTime IS NULL AND operation = "CREATED", createdDateTime, modifiedDateTime), "NULL") AS modifiedDateTime,
  FROM {PIMSParoleeInformation_Aud}
  WHERE fk_paroleOfficerID IS NOT NULL
),
-- getting parole officer changes from the audit table
audit_table_officer_changes AS (
  SELECT 
    paroleeInformationAudId,
    paroleeInformationId, 
    inmateNumber, 
    fk_paroleOfficerID,
    IFNULL(LAG(fk_paroleOfficerID) OVER(PARTITION BY inmateNumber ORDER BY modifiedDateTime), 'X') AS previous_paroleOfficerID,
    modifiedDateTime,
  FROM clean_audit_table
  WHERE modifiedDateTime IS NOT NULL
),
-- only taking records where a new supervision level is seen
parole_officer_changes AS (
  SELECT 
    inmateNumber,
    fk_paroleOfficerID,
    modifiedDateTime,
  FROM audit_table_officer_changes
  WHERE fk_paroleOfficerID != previous_paroleOfficerID
), 
-- joining in officer changes from the audit table
adding_officer_changes AS (
  SELECT
    periodID, 
    p.inmateNumber,
    supervisedLevel,
    paroleType, 
    pc.fk_paroleOfficerID,
    original_paroleOfficerID,
    currentStatusCode,
    countyParoledTo,
    inOutStateIndicator1Code,
    inOutStateIndicator2Code,
    inOutStateIndicator3Code,
    startReason,
    endReason,
    startDate, 
    endDate, 
    modifiedDateTime,
  FROM renumber_periods p
  LEFT JOIN parole_officer_changes pc
  ON p.inmateNumber = pc.inmateNumber
  AND pc.modifiedDateTime BETWEEN startDate AND COALESCE(endDate, '9999-01-01')
), 
-- tracking previous and next periods where split by caseload changes
setup_split_periods_for_caseloads AS (
  SELECT
    periodID,
    -- TODO(#36975): re-check non-determinsm once we receive data daily 
    LEAD(periodID) OVER(PARTITION BY inmateNumber ORDER BY periodID, modifiedDateTime) AS NEXT_PERIOD,
    LAG(periodID) OVER(PARTITION BY inmateNumber ORDER BY periodID, modifiedDateTime) AS LAST_PERIOD,
    inmateNumber,
    supervisedLevel,
    paroleType, 
    fk_paroleOfficerID,
    original_paroleOfficerID,
    currentStatusCode,
    countyParoledTo,
    inOutStateIndicator1Code,
    inOutStateIndicator2Code,
    inOutStateIndicator3Code,
    startReason,
    endReason,
    startDate, 
    endDate, 
    modifiedDateTime,
  FROM adding_officer_changes
),
 -- adding start and end reasons for caseload change and fixing dates
split_periods_for_caseloads AS (
  SELECT
    periodID AS OLD_PERIOD,
    NEXT_PERIOD,
    LAST_PERIOD,
    inmateNumber,
    supervisedLevel,
    paroleType, 
    fk_paroleOfficerID,
    original_paroleOfficerID,
    currentStatusCode,
    countyParoledTo,
    inOutStateIndicator1Code,
    inOutStateIndicator2Code,
    inOutStateIndicator3Code,
    IF(periodID = LAST_PERIOD, 'Parole Officer Change', startReason) AS startReason,
    IF(periodID = NEXT_PERIOD, 'Parole Officer Change', endReason) AS endReason,
    IF(periodID = LAST_PERIOD, LAG(modifiedDateTime) OVER(PARTITION BY inmateNumber ORDER BY periodID, startDate), startDate) AS startDate, 
    IF(periodID = NEXT_PERIOD, modifiedDateTime, endDate) AS endDate, 
  FROM setup_split_periods_for_caseloads
), 
-- renumbering periods based on new caseload periods
recreate_periods AS (
  SELECT 
    ROW_NUMBER() OVER (PARTITION BY inmateNumber ORDER BY startDate,endDate NULLS LAST) AS periodId,
    inmateNumber,
    LAST_VALUE(supervisedLevel ignore nulls) OVER (PARTITION BY inmateNumber ORDER BY startDate, endDate range between UNBOUNDED preceding and current row) AS supervisedLevel,
    paroleType, 
    COALESCE(fk_paroleOfficerID, original_paroleOfficerID) AS paroleOfficerID,
    currentStatusCode,
    countyParoledTo,
    inOutStateIndicator1Code,
    inOutStateIndicator2Code,
    inOutStateIndicator3Code,
    startReason,
    endReason,
    startDate, 
    endDate, 
  FROM split_periods_for_caseloads
),
--  Adding specialconditionID so there is a way to connect downstream if needed
-- to determine compliance with special conditions
supervision_conditions AS (
  SELECT 
    inmateNumber,
    beginDate,
    endDate,
    specialConditionId || ' - '||codeValue AS specialConditions,
  FROM  {PIMSSpecialCondition}
  LEFT JOIN codevalues c1
    ON specialConditionType = c1.codeId
),
-- some people started Parole before the Audit table was created in 2020 and their information has not changed since, so for those we join back to the original ParoleeInformation table to get their caseload
populate_caseloads AS (
  SELECT 
    periodId,
    inmateNumber,
    supervisedLevel,
    LAST_VALUE(paroleOfficerID ignore nulls) OVER (PARTITION BY inmateNumber ORDER BY periodId range between UNBOUNDED preceding and current row) AS paroleOfficerID,
    countyParoledTo,
    inOutStateIndicator1Code,
    inOutStateIndicator2Code,
    inOutStateIndicator3Code,
    startReason,
    endReason,
    startDate, 
    endDate
  FROM recreate_periods
),
-- making sure all IDs are in the parole staff view so normalization won't fail.
-- adding in supervision special conditions for those within a specific supervision period
adding_conditions AS (
SELECT 
    periodId,
    rp.inmateNumber,
    supervisedLevel,
    CASE 
        WHEN paroleOfficerID IN (SELECT DISTINCT paroleOfficerId FROM {PIMSParoleOfficer})
        THEN paroleOfficerID
        ELSE null
    END AS paroleOfficerID,
    countyParoledTo,
    inOutStateIndicator1Code,
    inOutStateIndicator2Code,
    inOutStateIndicator3Code,
    startReason,
    endReason,
    startDate, 
    rp.endDate, 
    specialConditions
 FROM populate_caseloads rp
  LEFT JOIN supervision_conditions sc
   ON rp.inmateNumber = sc.inmateNumber
  AND (sc.beginDate BETWEEN rp.startDate AND rp.endDate OR sc.endDate = rp.endDate)
)
SELECT 
  periodId,
  inmateNumber,
  supervisedLevel,
  paroleOfficerID,
  countyParoledTo,
  inOutStateIndicator1Code,
  inOutStateIndicator2Code,
  inOutStateIndicator3Code,
  startReason,
  endReason,
  startDate, 
  endDate, 
  STRING_AGG(specialConditions, ', ' ORDER BY specialConditions) AS specialConditions
FROM adding_conditions 
GROUP BY periodId,inmateNumber,supervisedLevel,paroleOfficerID,countyParoledTo,inOutStateIndicator1Code, inOutStateIndicator2Code,inOutStateIndicator3Code, startReason,endReason, startDate, endDate
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ne",
    ingest_view_name="supervision_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
