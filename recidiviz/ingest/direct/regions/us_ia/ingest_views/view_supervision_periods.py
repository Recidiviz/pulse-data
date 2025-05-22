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
"""
Query containing supervision periods information.  

Because a person can have multiple supervision statuses
going on at the same time, we create periods at the OffenderCd-SupervisionStatusInformationId level and will
let sessions do the deduping later on.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH

  -- This CTE returns spans of time where supervision status is parole or probation related
  -- Notes:
  --   - Supervision statuses can be overlapping
  --   - This is a PrimarySupervisionStatus flag in IA_DOC_SupervisionStatuses if we want to do anything with it
  --   - Interstate Compact Parole and Interstate Compact Probation are both IC-in
  --   - Special Sentences are a special type of parole for the sex offender population
  --   - Community Supervision 902.3A is an old status distinction that isn't used anymore
  supervision_status_spans AS (
    SELECT DISTINCT
      OffenderCd,
      SupervisionStatusInformationId,
      SupervisionStatus,
      SupervisionStatusReasonForChange AS SupervisionStatusReasonForChange,
      DATE(SupervisionStatusStartDt) AS start_date,
      DATE(SupervisionStatusEndDt) AS end_date,
      CAST(EnteredDt AS DATETIME) AS update_datetime
    FROM {IA_DOC_SupervisionStatuses}
    WHERE SupervisionStatus IN (
      "Parole",
      "Probation",
      "Interstate Compact Parole",
      "Interstate Compact Probation",
      "Special Sentence",
      "Community Supervision 902.3A",
      "Pretrial Release Without Supervision",
      "Pretrial Release With Supervision"
    )
  ),

  -- This CTE returns spans of time for each supervision level
  -- Notes:
  --   - Supervision levels are not expected to be overlapping. Except they are and sometimes the overlapping SupervisionStatusInformationId have conflicting SupervisionLevel.
  --   - Supervision levels are at the OffenderCd level, but since we are creating periods at the OffenderCd-SupervisionStatusInformationId level, 
  --     we'll merge on all distinct SupervisionStatusInformationIds per OffenderCd in this CTE, and then rely on the date merging later to only apply
  --     each supervision level span to the relevant time periodså
  supervision_level_spans AS (
    SELECT DISTINCT
      OffenderCd,
      SupervisionStatusInformationId,
      SupervisionLevel, 
      DATE(SupervisionLevelStartDt) AS start_date,
      DATE(SupervisionLevelEndDt) AS end_date,
      CAST(EnteredDt AS DATETIME) AS update_datetime
    FROM {IA_DOC_SupervisionLevels}
    LEFT JOIN supervision_status_spans USING(OffenderCd)
  ),

  -- This CTE returns spans of time for each supervision modifier
  -- Notes:
  --   - Supervision modifiers are only overlapping in old data (pre 2003, ICON migration).
  --      - However 2024-5 each have 100's of unique supervisions with overlapping modifiers on the same day.
  --   - There is additional information in the IA_DOC_Supervision_Modifiers table for confinment reason, confinement dates, hold dates, mittimus dates, etc.
  supervision_modifier_spans AS (
    SELECT DISTINCT
      OffenderCd,
      SlSupervisionModifierId,
      SupervisionStatusInformationId,
      SupervisionModifier, 
      DATE(SupervisionModifierStartDt) AS start_date,
      DATE(SupervisionModifierEndDt) AS end_date,
      CAST(EnteredDt AS DATETIME) AS update_datetime
    FROM {IA_DOC_Supervision_Modifiers}
  ),

  -- This CTE returns spans of time for each supervision officer assignment
  -- Notes:
  --   - Supervision officer assignments are only overlapping in old data (pre 2003, ICON migration). Confirmed on initial ingest.
  --   - Double checked and can verify at time of ingest.
  supervision_officer_spans AS (
    SELECT DISTINCT
      OffenderCd,
      SupervisionStatusInformationId,
      CaseManagerStaffId,
      DATE(CaseManagerStartDt) AS start_date,
      DATE(CaseManagerEndDt) AS end_date,
      CAST(EnteredDt AS DATETIME) AS update_datetime
    FROM {IA_DOC_CaseManagers}
  ),

  -- This CTE returns spans of time for each supervision location
  -- Notes:
  --   - Supervision locations are only overlapping in old data (pre 2003, ICON migration). Confirmed on initial ingest.
  --   - Other fields like WorkUnitRegionId, WorkUnitRegionNm, WorkUnitNm are also available, so we could use this probably use this for location metadata
  supervision_location_spans AS (
    SELECT DISTINCT
      OffenderCd,
      SupervisionStatusInformationId,
      WorkUnitId,
      WorkUnitServiceType,
      DATE(WorkUnitStartDt) AS start_date,
      DATE(WorkUnitEndDt) AS end_date,
      CAST(EnteredDt AS DATETIME) as update_datetime
    FROM {IA_DOC_OffenderWorkUnits}
    LEFT JOIN {IA_DOC_MAINT_WorkUnits} USING(WorkUnitId)
  ),

  -- This CTE returns spans of time for each supervision specialty
  -- Notes:
  --   - Supervision specialties can expected to be overlapping
  supervision_specialties_spans AS (
    SELECT DISTINCT
      OffenderCd,
      SupervisionStatusInformationId,
      Specialty,
      SpecialtyReasonForChange AS SpecialtyReasonForChange,
      DATE(SpecialtyStartDt) AS start_date,
      DATE(SpecialtyEndDt) AS end_date, 
      CAST(EnteredDt AS DATETIME) AS update_datetime
    FROM {IA_DOC_Specialties}
  ),

  -- This CTE collects all the different movements and movement dates from the three different sources with 
  -- movement information (specialties, statuses, and movements).  
  -- Notes:
  --   - We also define a movement_rank variable to distinguish movements from different sources, used later for deterministic ordering.
  --   - The data for specialties and statuses movements already come at the SupervisionStatusInformationId level,
  --     but for movements that come from the main movements table, we derive SupervisionStatusInformationId using
  --     the MovementSourceId field, which is the FK for a record in another table that does contain SupervisionStatusInformationId 
  --     and is linked with this movement record.  If MovementSourceId is NULL (meaning the movement is not related to a record update in another table), merges on 
  --     all distinct SupervisionStatusInformationIds for supervision status spans that were active as of the movement date.
  --  - We filter out movements that don't provide us any relevant information

  movements AS (
    SELECT *
    FROM (

      -- Include parole supervision statuses starts as their own movement since they're only included in the movements as a prison status end, and we filter those out
      SELECT DISTINCT
        OffenderCd,
        "Release to Iowa Parole" AS movement,
        start_date AS movement_date,
        update_datetime,
        SupervisionStatusInformationId,
        1 AS movement_rank,
        NULL AS MovementId
      FROM supervision_status_spans
      WHERE SupervisionStatus = 'Parole'

      UNION ALL
      
      -- Include "Pretrial Release Without Supervision" supervision statuses starts as their own movement since they are entered as a null movement at the start.
      SELECT DISTINCT
        OffenderCd,
        "Release to Iowa Pretrial Release Without Supervision" AS movement,
        start_date AS movement_date,
        update_datetime,
        SupervisionStatusInformationId,
        1 AS movement_rank,
        NULL AS MovementId
      FROM supervision_status_spans
      WHERE SupervisionStatus = 'Pretrial Release Without Supervision'

      UNION ALL

      -- Include "Pretrial Release With Supervision" supervision statuses starts as their own movement since they are otherwise otherwise null in the movements table.
      SELECT DISTINCT
        OffenderCd,
        "Release to Iowa Pretrial Release With Supervision" AS movement,
        start_date AS movement_date,
        update_datetime,
        SupervisionStatusInformationId,
        1 AS movement_rank,
        NULL AS MovementId
      FROM supervision_status_spans
      WHERE SupervisionStatus = 'Pretrial Release With Supervision'

      UNION ALL

      SELECT DISTINCT 
        OffenderCd,
        SupervisionStatusReasonForChange AS movement,
        end_date AS movement_date,
        update_datetime,
        SupervisionStatusInformationId,
        2 AS movement_rank,
        NULL AS MovementId
      FROM supervision_status_spans
      WHERE SupervisionStatusReasonForChange IS NOT NULL

      UNION ALL

      SELECT 
        m.OffenderCd,
        MovementDesc AS movement,
        DATE(MovementDt) AS movement_date,
        CAST(EnteredDt AS DATETIME) AS update_datetime,
        CASE WHEN MovementSource IS NULL THEN s.SupervisionStatusInformationId
             WHEN MovementSource IN ('SSE', 'SSS') THEN m.MovementSourceId
             WHEN MovementSource IN ('SME', 'SMS') THEN mod.SupervisionStatusInformationId
             END AS SupervisionStatusInformationId,
        3 AS movement_rank,
        CAST(MovementId AS INT64) AS MovementId
      FROM {IA_DOC_Movements} m
      LEFT JOIN supervision_status_spans s
        ON m.OffenderCd = s.OffenderCd
        AND m.MovementSource IS NULL
        AND DATE(m.MovementDt) BETWEEN s.start_date AND COALESCE(s.end_date, DATE(9999,9,9))
      LEFT JOIN supervision_modifier_spans mod
        ON m.OffenderCd = mod.OffenderCd
        AND m.MovementSource in ('SME', 'SMS')
        AND m.MovementSourceId = mod.SlSupervisionModifierId
      WHERE MovementSource IS NULL OR MovementSource IN ('SSE', 'SSS', 'SME', 'SMS')

      UNION ALL 

      SELECT DISTINCT
        OffenderCd,
        SpecialtyReasonForChange AS movement,
        end_date AS movement_date,
        update_datetime,
        SupervisionStatusInformationId,
        4 AS movement_rank,
        NULL AS MovementId
      FROM supervision_specialties_spans
      WHERE SpecialtyReasonForChange IS NOT NULL
    )
    WHERE movement NOT IN(
      'Completed Requirements',
      'Noncompliant/Behavioral Issues'
      )
  ),

  -- This CTE compiles all the relevant dates and movements associated with a specific OffenderCd and SupervisionStatusInformationId
  -- Notes:
  --   - For the movements, we add an ordering variable called movement_order for deterministic sorting

  critical_dates AS (
    SELECT 
      OffenderCd, 
      SupervisionStatusInformationId,
      movement_date as start_date, 
      movement, 
      ROW_NUMBER() OVER(PARTITION BY OffenderCd, SupervisionStatusInformationId, movement_date ORDER BY movement_rank, MovementId, update_datetime, movement) AS movement_order
    FROM movements

    UNION DISTINCT

    SELECT DISTINCT OffenderCd, SupervisionStatusInformationId, start_date, CAST(NULL AS STRING) as movement, CAST(NULL AS INT64) AS movement_order
    FROM supervision_level_spans

    UNION DISTINCT 

    SELECT DISTINCT OffenderCd, SupervisionStatusInformationId, end_date, CAST(NULL AS STRING) as movement, CAST(NULL AS INT64) AS movement_order
    FROM supervision_level_spans

    UNION DISTINCT 

    SELECT DISTINCT OffenderCd, SupervisionStatusInformationId, start_date, CAST(NULL AS STRING) as movement, CAST(NULL AS INT64) AS movement_order
    FROM supervision_status_spans

    UNION DISTINCT 

    SELECT DISTINCT OffenderCd, SupervisionStatusInformationId, end_date, CAST(NULL AS STRING) as movement, CAST(NULL AS INT64) AS movement_order
    FROM supervision_status_spans

    UNION DISTINCT 

    SELECT DISTINCT OffenderCd, SupervisionStatusInformationId, start_date, CAST(NULL AS STRING) as movement, CAST(NULL AS INT64) AS movement_order
    FROM supervision_officer_spans

    UNION DISTINCT 

    SELECT DISTINCT OffenderCd, SupervisionStatusInformationId, end_date, CAST(NULL AS STRING) as movement, CAST(NULL AS INT64) AS movement_order
    FROM supervision_officer_spans

    UNION DISTINCT 

    SELECT DISTINCT OffenderCd, SupervisionStatusInformationId, start_date, CAST(NULL AS STRING) as movement, CAST(NULL AS INT64) AS movement_order
    FROM supervision_location_spans

    UNION DISTINCT 

    SELECT DISTINCT OffenderCd, SupervisionStatusInformationId, end_date, CAST(NULL AS STRING) as movement, CAST(NULL AS INT64) AS movement_order
    FROM supervision_location_spans

    UNION DISTINCT

    SELECT DISTINCT OffenderCd, SupervisionStatusInformationId, start_date, CAST(NULL AS STRING) as movement, CAST(NULL AS INT64) AS movement_order
    FROM supervision_specialties_spans

    UNION DISTINCT 

    SELECT DISTINCT OffenderCd, SupervisionStatusInformationId, end_date, CAST(NULL AS STRING) as movement, CAST(NULL AS INT64) AS movement_order
    FROM supervision_specialties_spans

    UNION DISTINCT

    SELECT DISTINCT OffenderCd, SupervisionStatusInformationId, start_date, CAST(NULL AS STRING) as movement, CAST(NULL AS INT64) AS movement_order
    FROM supervision_modifier_spans

    UNION DISTINCT

    SELECT DISTINCT OffenderCd, SupervisionStatusInformationId, end_date, CAST(NULL AS STRING) as movement, CAST(NULL AS INT64) AS movement_order
    FROM supervision_modifier_spans
  ),

  -- This CTE creates a CTE for spans of time using the critical dates defined in the previous CTE.
  -- Notes:
  --   - This CTE only keeps duplicate start dates if each duplicate start date is coming from a distinct movement 
  --   - This CTE also creates a variabled called next_movement that corresponds with the movement associated with the next start date
  tiny_spans AS (
    SELECT *,
      LEAD(start_date) OVER(w) AS end_date,
      LEAD(movement) OVER(w) AS next_movement
    FROM (
      SELECT *
      FROM critical_dates
      WHERE start_date IS NOT NULL
      -- only keep multiple copies of a start date if it is coming from the movements data
      QUALIFY RANK() OVER(PARTITION BY OffenderCd, SupervisionStatusInformationId, start_date ORDER BY (movement IS NOT NULL) DESC) = 1
    )
    WINDOW w AS (PARTITION BY OffenderCd, SupervisionStatusInformationId ORDER BY start_date, movement_order)
  ),

  -- This CTE joins all relevant attribute information (supervison status, specialty, officer, location, modifier, level)
  -- onto each of the time spans based on start and end date, OffenderCd, and SupervisionStatusInformationId.
  -- Note: 
  --   - Because there are certain attributes that could be overlapping, this might create duplicate/overlapping periods.
  --     We will rely on sessions downstream to resolve these overlaps
  --   - We filter down to only time spans where there is an active supervision status
  all_joined AS (
    SELECT DISTINCT
        ts.OffenderCd,
        status.SupervisionStatusInformationId,
        ts.start_date,
        ts.end_date,
        ts.movement,
        ts.next_movement,
        ts.movement_order,
        status.SupervisionStatus,
        officer.CaseManagerStaffId,
        specialty.Specialty,
        location.WorkUnitId,
        location.WorkUnitServiceType,
        level.SupervisionLevel,
        modifier.SupervisionModifier
      FROM tiny_spans ts
      LEFT JOIN supervision_status_spans status
        ON  ts.OffenderCd = status.OffenderCd
        AND ts.start_date >= status.start_date
        AND status.SupervisionStatusInformationId = ts.SupervisionStatusInformationId
        AND COALESCE(ts.end_date, DATE(9999,9,9)) <= COALESCE(status.end_date, DATE(9999,9,9))
        AND status.end_date IS DISTINCT FROM ts.start_date
      LEFT JOIN supervision_officer_spans officer
        ON  ts.OffenderCd = officer.OffenderCd
        AND ts.start_date >= officer.start_date
        AND status.SupervisionStatusInformationId = officer.SupervisionStatusInformationId
        AND COALESCE(ts.end_date, DATE(9999,9,9)) <= COALESCE(officer.end_date, DATE(9999,9,9))
        AND officer.end_date IS DISTINCT FROM ts.start_date
      LEFT JOIN supervision_location_spans location
        ON  ts.OffenderCd = location.OffenderCd
        AND ts.start_date >= location.start_date
        AND status.SupervisionStatusInformationId = location.SupervisionStatusInformationId
        AND COALESCE(ts.end_date, DATE(9999,9,9)) <= COALESCE(location.end_date, DATE(9999,9,9))
        AND location.end_date IS DISTINCT FROM ts.start_date
      LEFT JOIN supervision_specialties_spans specialty
        ON  ts.OffenderCd = specialty.OffenderCd
        AND ts.start_date >= specialty.start_date
        AND status.SupervisionStatusInformationId = specialty.SupervisionStatusInformationId
        AND COALESCE(ts.end_date, DATE(9999,9,9)) <= COALESCE(specialty.end_date, DATE(9999,9,9))
        AND specialty.end_date IS DISTINCT FROM ts.start_date
      LEFT JOIN supervision_level_spans level
        ON  ts.OffenderCd = level.OffenderCd
        AND ts.start_date >= level.start_date
        AND status.SupervisionStatusInformationId = level.SupervisionStatusInformationId
        AND COALESCE(ts.end_date, DATE(9999,9,9)) <= COALESCE(level.end_date, DATE(9999,9,9))
        AND level.end_date IS DISTINCT FROM ts.start_date
      LEFT JOIN supervision_modifier_spans modifier
        ON  ts.OffenderCd = modifier.OffenderCd
        AND ts.start_date >= modifier.start_date
        AND status.SupervisionStatusInformationId = modifier.SupervisionStatusInformationId
        AND COALESCE(ts.end_date, DATE(9999,9,9)) <= COALESCE(modifier.end_date, DATE(9999,9,9))
        AND modifier.end_date IS DISTINCT FROM ts.start_date
      WHERE SupervisionStatus IS NOT NULL
      QUALIFY RANK() OVER(PARTITION BY OffenderCd, SupervisionStatusInformationId, start_date, end_date ORDER BY level.update_datetime desc) = 1 #Narrowing to one unique supervision level span (SupervisionStatusInformationId) per tiny span
  ),
  
  -- This CTE cleans up the final results
  -- Notes:
  --   - Creates a period_seq_num value that defines the sequence/ordering of periods within each OffenderCd and SupervisionStatusInformationId
  --   - Drops duplicate zero day periods that have the same movement and next_movement
  final AS (
    SELECT * EXCEPT(movement_order),
      ROW_NUMBER() OVER(w) AS period_seq_num
    FROM all_joined
    -- get rid of duplicate movements on same day periods
    WHERE start_date IS DISTINCT FROM end_date OR movement IS DISTINCT FROM next_movement
    WINDOW w AS (PARTITION BY OffenderCd, SupervisionStatusInformationId 
                ORDER BY start_date, end_date NULLS LAST, movement_order, 
                          Specialty, -- there could be multiple specialties active at the same time
                          WorkUnitId, -- in old data (pre 2003) there are cases with multiple work units active at the same time
                          CaseManagerStaffId, -- in old data (pre 2003) there are cases with multiple case managers active at the same time,
                          SupervisionModifier -- in old data (pre 2003) there are cases with multiple modifiers active at the same time,
                          )
  )

  SELECT * FROM final
  
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ia",
    ingest_view_name="supervision_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
