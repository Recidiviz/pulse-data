# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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

"""Shared helper fragments for the US_TX ingest view queries."""

PERIOD_EXCLUSIONS_FRAGMENT = """
    (
            "Death reported",
            "Deported OOC",
            "Death",
            "Discharge",
            "Erroneous Release Returned to ID",
            "Full pardon",
            "Sentence Reversed",
            "Removed from Review Process",
            "UNKNOWN OFFENDER STATUS",
            "Returned ICC offender to sending state",
            "Other Agency Detainer",
            "Supervised out of state"
        )
"""

PHASES_FRAGMENT = """
-- This CTE addresses a bug where the PLS_STATUS_DATE and the PLM_TYPE_ID columns
-- were switched after the initial historical transfer. Ticket to fix this here: TODO(#41592)
fixed_phases AS (
SELECT
  CASE
    WHEN update_datetime = "2025-02-12T00:00:00"
    THEN PLM_TYPE_ID
    ELSE PLS_STATUS_DATE
    END AS PLM_TYPE_ID,
  CASE
    WHEN update_datetime = "2025-02-12T00:00:00"
    THEN PLS_STATUS_DATE
    ELSE PLM_TYPE_ID
    END AS PLS_STATUS_DATE,
    PLM_SID_NO,
    PLM_ID,
    PLM_STATUS,
    OFFC_REGION,
    OFFC_DISTRICT,
    OFFC_START_DATE,
    PLM_EFF_DATE
  FROM `{Phases@ALL}`
),
-- CTE to prepare OfficeDescription file for use later on. Pads numbers with a 0 if a 
-- single digit.
office_codes as (
  SELECT 
        CASE 
        WHEN LENGTH(OFFC_REGION) < 2 AND LENGTH(OFFC_DISTRICT) < 2 THEN
            CONCAT('0', OFFC_REGION, '-', '0', OFFC_DISTRICT)
        WHEN LENGTH(OFFC_REGION) < 2 THEN
            CONCAT('0', OFFC_REGION, '-', OFFC_DISTRICT)
        WHEN LENGTH(OFFC_DISTRICT) < 2 THEN
            CONCAT(OFFC_REGION, '-', '0', OFFC_DISTRICT)
        ELSE
            CONCAT(OFFC_REGION, '-', OFFC_DISTRICT)
        END AS code,
        OFFC_TYPE, 
    FROM {OfficeDescription}
),
-- Select the most recent in patient records. We have to use a qualify to get the most
-- recent record. 
filtered_phases AS
(
  SELECT DISTINCT 
    PLM_SID_NO,
    PLM_TYPE_ID,
    PLM_STATUS,
    PLM_EFF_DATE,
    OFFC_REGION,
    OFFC_DISTRICT, 
  FROM fixed_phases 
  LEFT JOIN office_codes
    ON code = concat(OFFC_REGION,"-",OFFC_DISTRICT)
  -- Only look at in patient facility types
  WHERE PLM_TYPE_ID = "7" AND OFFC_TYPE IN ("E", "F") 
  -- Look at the most recent record
  QUALIFY ROW_NUMBER() OVER (PARTITION BY PLM_SID_NO ORDER BY PLM_EFF_DATE DESC) = 1
  ),
-- Selects all of the clients that are currently housed in a treatment facility.
inpatient_clients AS (
  SELECT DISTINCT 
    PLM_SID_NO AS SID_Number
  FROM filtered_phases
  -- Active statuses only
  WHERE PLM_STATUS = "7" 
),
-- Collects all of the active program referrals
active_referrals AS 
(
  SELECT
    PREF_SID_NO AS SID_Number
  FROM `{ProgramReferral}`
  WHERE 
    -- 5 - SUBSTANCE ABUSE
    PREF_PGM_CTGRY_ID = "5"
    -- 226 = RESIDENTIAL; 227 = RELAPSE RESIDENTIAL
    -- 228 = SUPPORTIVE OUTPATIENT; 229 = RELAPSE OUTPATIENT
    AND PREF_PROG_TYPE_ID IN ("226", "227", "228", "229")
    AND PREF_PGM_STATUS IN ("1", "4", "5", "6")
),
-- Collects all of the peer support program referrals 
peer_support_referral AS (
  SELECT DISTINCT 
    PREF_SID_NO AS SID_Number
  FROM `{ProgramReferral}`
  WHERE
    PREF_PROG_TYPE_ID = "1927"
    AND PREF_PGM_STATUS IN ("1", "4", "5", "6")
),
-- Collects all of the past program referrals
past_referral AS (
  SELECT DISTINCT 
    PREF_SID_NO AS SID_Number
  FROM `{ProgramReferral}`
  WHERE
    -- 226 = RESIDENTIAL; 227 = RELAPSE RESIDENTIAL
    -- 228 = SUPPORTIVE OUTPATIENT; 229 = RELAPSE OUTPATIENT
    PREF_PROG_TYPE_ID IN ("226", "227", "228", "229")
    -- Completed
    AND PREF_PGM_STATUS = "3"
),
-- Looks for a possible phase transition
-- in the team meetings (else chooses the "current" phase)
latest_ttm AS (
  SELECT DISTINCT 
    PTMT_SID_NO,
    COALESCE(NULLIF(PTMT_PHASE_TRANS, "0"), PTMT_CRNT_SUB_PHSE) AS phase_code,
    PTMT_PREF_ID
  FROM `{ProgramPhaseTransitions}`
  WHERE
  -- Ignore 0's since those are NULLs and ignore whenever the phase is changed to 1
  -- since we will know based on in patient facility data.
    COALESCE(NULLIF(PTMT_PHASE_TRANS, "0"), PTMT_CRNT_SUB_PHSE) NOT IN ("0","1")
  QUALIFY ROW_NUMBER() OVER (PARTITION BY PTMT_SID_NO ORDER BY PTMT_UPDATE_DATE DESC) = 1

),
-- Associates each team meeting code with a phase
latest_ttm_phase_uncoded AS (
    SELECT
        PTMT_SID_NO AS SID_Number,
        LKUP_VALUE AS phase
    FROM latest_ttm
    LEFT JOIN  `{PhasesDescription}`
        ON phase_code = LKUP_CODE
),
-- Selects all clients who have an open period of substance abuse. 
all_clients AS (
  SELECT DISTINCT 
    SID_Number
  FROM fix_end_date
  WHERE Case_Type = 'Substance abuse' AND end_date IS NULL
),
-- Determines the current phase of each client based on program engagement logic
phase_logic AS (
  SELECT
    CASE 
      -- Client is currently in an inpatient facility
      WHEN ic.SID_Number IS NOT NULL THEN 'PHASE 1'
      -- Client has an active referral and a corresponding team meeting phase
      WHEN ar.SID_Number IS NOT NULL AND ltpu.SID_Number IS NOT NULL THEN ltpu.phase
      -- No active referral but has a peer support referral
      WHEN psr.SID_Number IS NOT NULL THEN 'PHASE 3'
      -- Has past referral and matching team meeting phase
      WHEN pr.SID_Number IS NOT NULL AND ltpu.SID_Number IS NOT NULL THEN ltpu.phase 
      -- Default case
      ELSE 'PHASE 2'
    END AS phase,
    ac.SID_Number
  FROM all_clients ac
  LEFT JOIN inpatient_clients ic USING (SID_Number)
  LEFT JOIN active_referrals ar USING (SID_Number)
  LEFT JOIN peer_support_referral psr USING (SID_Number)
  LEFT JOIN past_referral pr USING (SID_Number)
  LEFT JOIN latest_ttm_phase_uncoded ltpu USING (SID_Number)
),
-- Ensures consistent formatting and filtering out null phases
client_phases AS (
  SELECT DISTINCT
    SID_Number,
    CASE
      WHEN phase = '1' THEN 'PHASE 1'
      WHEN phase = '2' THEN 'PHASE 2'
      WHEN phase = '3' THEN 'PHASE 3'
      WHEN phase = '1B' THEN 'PHASE 1B'
      ELSE UPPER(phase)
    END AS phase
  FROM phase_logic
  WHERE phase IS NOT NULL
)
"""
