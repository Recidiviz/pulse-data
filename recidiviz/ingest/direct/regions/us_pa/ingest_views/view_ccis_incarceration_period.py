# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Query containing incarceration period information extracted from multiple PADOC files, where the period data
originates from CCIS (Community Corrections Information System) tables."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# The ordering here advantage of the fact that 'ADM' sorts alphabetically before 'REL', so we are placing 'REL'
# statuses first when both 'ADM' and 'REL' appear for the same movement_sequence number (e.g. when it's a transfer).
PARTITION_CLAUSE = "OVER (PARTITION BY inmate_number ORDER BY movement_sequence, movement_type DESC)"

VIEW_QUERY_TEMPLATE = f"""
WITH inmate_number_with_control_numbers AS (
  SELECT
    inmate_number,
    control_number,
    ROW_NUMBER() OVER (PARTITION BY inmate_number ORDER BY control_number ASC) as control_number_order
  FROM
    {{dbo_tblSearchInmateInfo}}
), movements_with_single_control_number AS (
  SELECT
    m.*,
    i.control_number
  FROM
    {{dbo_vwCCISAllMvmt}} m
  -- As of writing, there are no movements with control numbers that don't exist in dbo_tblSearchInmateInfo, but we
  -- do an inner JOIN to make reasoning about entity matching issues and raw data migrations easier.
  JOIN
    inmate_number_with_control_numbers i
  USING (inmate_number)
  WHERE control_number_order = 1
), movements_base AS (
  SELECT
    control_number,
    Inmate_Number AS inmate_number,
    CCISMvmt_ID AS movement_id,
    SAFE_CAST(Mvmt_SeqNum AS INT64) AS movement_sequence,
    Status_Cd as movement_status_code,
    Status_Dt AS movement_date,
    REPLACE(desc_line, '/', ': ') AS location,
  FROM movements_with_single_control_number
  LEFT JOIN
    {{dbo_tblCCISStatus}}
  USING (Status_Id)
  LEFT JOIN
   {{dbo_tblCCISAllCCC}}
  ON (TRIM(LocationFrom_Cd) = TRIM(computer_code))
  -- Statuses that do not represent an actual movement --
  WHERE Status_Cd NOT IN (
    -- ERROR
    'ERR',
    -- Awaiting Transfer - Withdrawn: This person was awaiting a transfer, and the 
    -- transfer never happened
    'AWDN',
    -- Awaiting Transfer: This person hasn't moved yet
    'AWTR',
    -- Detained By Other Authority: This is a bookkeeping status change
    'DBOA',
    -- Pending: This person may arrive at a facility, but have not arrived yet
    'PEND',
    -- Pending Rejected: The facility has rejected the admission the person
    'PREJ',
    -- Parole to Center: Status Change when any reentrant switches to a new DOC# or if a SIP participant is paroled to
    -- the center they reside in (very rare). CCIS will automatically generate an “In Residence” code after this entry.
    'PTCE',
    -- Pending Withdrawn: The person no longer needs to be admitted to the facility
    'PWTH',
    -- Awaiting Transfer - Detainer: This person hasn't moved yet
    'AWDT'
  )
), admission_movements AS (
  SELECT
    *, 'ADM' AS movement_type 
  FROM movements_base
  -- Admission statuses --
  WHERE movement_status_code IN (
    -- In Residence: Admitted to facility
    'INRS',
    -- Return to Residence: Returned to facility from elsewhere
    'RTRS',
    -- Transfer Received: Transferred to facility
    'TRRC',
    -- Return from DPW: Returned to facility from Department of Public Works
    'DPWF'
  )
), transfer_movements AS (
  SELECT
    *
  FROM movements_base,
  UNNEST(['ADM', 'REL']) AS movement_type
  -- Transfer statuses --
  WHERE movement_status_code IN (
    -- Program Change: Transfer between programs
    'PRCH'
  )
), release_movements AS (
    SELECT
    *, 'REL' AS movement_type 
  FROM movements_base
  -- Release statuses -- 
  WHERE movement_status_code IN (
    -- Parole Absconder: They have left the facility
    'ABSC',
    -- Authorized Temporary Absence
    'ATA',
    -- Awaiting Transfer - Non Report: This person was released from a facility and never showed up at the next facility
    'AWNR',
    -- Deceased - Assault
    'DECA',
    -- Deceased - Natural
    'DECN',
    -- Deceased - Suicide
    'DECS',
    -- Deceased - Accident
    'DECX',
    -- Escape
    'ESCP',
    -- Parole to Street: Released from a facility to a PBPP approved home plan
    'PTST',
    -- Sentence Completed
    'SENC',
    -- Transfer from Group Home: Although this sounds like an admission, this is a transfer out to another kind of
    -- facility
    'TRGH',
    -- Transfer to SCI: Transfer from community facility to SCI
    'TRSC',
    -- TODO(#2002): Count people on temporary medical transfers in the DOC population
    -- Temporary Transfer - Medical: Transferred to non-DOC funded medical/psychiatric treatment facility
    'TTRN',
    -- Unsuccessful Discharge: Removed from parole for rule violations or significant incidents. Sent to an SCI.  
    'UDSC',
    -- Hospital: Temporary medical transfer
    'HOSP',
    -- Unauthorized temporary Absence: Didn't return after being temporarily released
    'AWOL',
    -- Transfer to DPW: Transferred to Department of Public Works
    'DPWT',
    -- Transfer to County: Transferred to county jail
    'TRTC',
    -- Discharge to Parole: Released from a facility, still on supervision
    'DC2P'
  )
), all_movements AS (
  SELECT * FROM admission_movements
  UNION ALL
  SELECT * FROM transfer_movements
  UNION ALL
  SELECT * FROM release_movements
), program_movements AS (
  SELECT
    CCISMvmt_Id AS movement_id,
    Program_Id AS program_id,
    -- It's rare but technically possible for a movement to be associated with more than one program,
    -- so we deterministically select one program_id per movement, with a priority order of 46, 26, then 51  
    ROW_NUMBER() OVER (PARTITION BY CCISMvmt_Id
                        ORDER BY (Program_Id IN ('26', '46', '51')) DESC, 
                                  Program_Id = '46' DESC,
                                  Program_Id = '26' DESC) AS priority_ranking
  FROM {{dbo_vwCCISAllProgDtls}}
), program_base AS (
  SELECT
    * EXCEPT (priority_ranking)
  FROM program_movements
  WHERE priority_ranking = 1
), full_periods AS (
  SELECT
    control_number,
    inmate_number,
    movement_id AS start_movement_id,
    movement_sequence,
    movement_status_code AS start_status_code,
    movement_date AS start_date,
    location,
    program_id,
    movement_type,
    LEAD(movement_status_code) {PARTITION_CLAUSE} AS end_status_code,
    LEAD(movement_date) {PARTITION_CLAUSE} as end_date,
  FROM
    all_movements
  LEFT JOIN
    program_base
  USING (movement_id)
), valid_periods AS (
  SELECT
    * EXCEPT(movement_type, movement_sequence),
    -- For program change statuses, we need to know the previous program_id to determine if a parole revocation occurred
    (IF(start_status_code = 'PRCH', LAG(program_id) {PARTITION_CLAUSE}, NULL)) AS previous_program_id
  FROM full_periods
  WHERE movement_type = 'ADM'
), periods AS (
  SELECT 
    * EXCEPT (previous_program_id), 
    -- Moving from a non-revocation program to a revocation program_id is a revocation
    (start_status_code = 'PRCH'  AND program_id IN ('26', '46', '51')
        AND previous_program_id NOT IN ('26', '46', '51')) AS start_is_new_revocation
  FROM valid_periods
  -- Program IDs that signify being included in the ACT 122 population -- 
  WHERE Program_Id IN (
    -- Parole Violator: Revocation to Parole Violator Center:
    '26',
    -- Technical Parole Violator: 6-9-12 Month Revocation
    '46',
    -- Detox: Treatment Revocation
    '51'
  )
)

SELECT *
FROM periods
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_pa',
    ingest_view_name='ccis_incarceration_period',
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols='control_number, inmate_number, start_date',
    materialize_raw_data_table_views=False
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
