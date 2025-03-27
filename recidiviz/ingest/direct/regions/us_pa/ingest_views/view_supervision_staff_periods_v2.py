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

"""

Query containing supervision staff periods information.

In PA, there are three different sources for staff information:
- RECIDIVIZ_REFERENCE_staff_roster: which is the manually compiled roster that PA research 
  provides us from time to time.  This includes all staff information for current staff including 
staff names, employee numbers, position numbers, location codes, role descriptions, and supervisor information.
- dbo_RelAgentHistory: which is the supervision officer assignment table that includes staff names, 
  employee numbers, position numbers, and location codes for all supervising officers assigned to a
  client and those supervising officers' supervisors.
- dbo_PRS_FACT_PAROLEE_CNTC_SUMRY: which is the supervision contacts table that includes 
  staff employee numbers, role description, location codes, and names.

Across these two sources, there are two different types of id numbers:
- Employee numbers, which should be unique to an individual staff person
- Position numbers, which are unique to a specific job opening/position, and therefore is 
  attached with multiple staff people over time.

Given that employee number is sometimes missing in the data, and may not exist in the future 
since it's not a part of PA's new staff database, we set up state staff ingest to 
ingest state staff external ids based on both the original employee numbers (EmpNums) and 
a new id we'll construct and call position number ids (PosNoIds) that takes the structure 
of `<position number>-<staff last name><staff first initial>`.

This view creates state staff location, role, supervisor, and caseload type periods using all three sources of data,
prioritizing information from the roster over the other sources if there are conflicts.

"""


from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.ingest.direct.regions.us_pa.ingest_views.template_staff import (
    staff_view_template,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

staff_base = staff_view_template()

VIEW_QUERY_TEMPLATE = f"""
    WITH {staff_base},

    -- For each record in the roster, this CTE calculates the next roster update datetime after 
    -- the current record's update datetime, as well as the most recent roster update datetime
    -- across all roster transfers.
    roster_dates AS (
        SELECT *,
          LEAD(start_date) OVER(ORDER BY start_date) AS next_roster_date,
          MAX(start_date) OVER(PARTITION BY True) as most_recent_roster_date
        FROM (
          SELECT DISTINCT 
            start_date
          FROM roster_parsed
        )
    ),

    -- For each person (identified by sort key), this CTE calculates the last date the person
    -- appeared in the roster, as well as the date of the first roster date without a record for this
    -- person.
    max_roster_dates AS (
      SELECT
        sort_key,
        last_appearance_date,
        next_roster_date AS first_roster_date_without
      FROM (
        SELECT DISTINCT COALESCE(EmpNum, PosNoId) as sort_key,
          MAX(start_date) OVER(PARTITION BY COALESCE(EmpNum, PosNoId)) as last_appearance_date
        FROM roster_parsed
      )
      LEFT JOIN roster_dates ON last_appearance_date  = start_date
    ),

    -- This CTE parses the reference file we have mapping PosNos to agent types
    specialty_agents_periods AS (
        SELECT
            LTRIM(PosNo6, '0') AS PosNo,
            AgentType,
            DATE(update_datetime) AS start_date,
            LEAD(DATE(update_datetime)) OVER(PARTITION BY PosNo6 ORDER BY update_datetime) AS end_date
        FROM {{specialty_agents@ALL}}
        WHERE LTRIM(PosNo6, '0') IS NOT NULL
    ),

    -- Since PosNo's could get added to this reference file over time, this CTE calculates
    -- the earliest date each PosNo appears in the reference file (used later in initial_dates_with_agent_type_deduplicated)
    min_speciality_agents_dates AS (
        SELECT PosNo, MIN(start_date) as earliest_specialty_data_date
        FROM specialty_agents_periods
        GROUP BY 1
    ),
    
    -- This CTE unions all the distinct dates associated with each staff person's role, location, and supervisor information from each of the 4 sources.
    -- This CTE also imposes a sort priority, prioritizing data from the roster first, then supervisor data from RelAgent,
    -- then officer data from RelAgent, and then finally data from the contacts data.
    initial_dates AS (
        SELECT 
            "roster" AS source,
            COALESCE(m.EmpNum, r.EmpNum) AS EmpNum,
            r.PosNoId,
            role_description,
            org_cd,
            COALESCE(m_sup.EmpNum, supervisor_EmpNum, supervisor_PosNoId) AS supervisor_id,
            start_date,
            1 AS sort_priority
        FROM roster_parsed r
        LEFT JOIN ids_map m USING(PosNoId)
        LEFT JOIN ids_map m_sup on r.supervisor_PosNoId = m_sup.PosNoId

        UNION ALL

        SELECT 
            "RelAgent" AS source,
            COALESCE(m.EmpNum, a.EmpNum) AS EmpNum,
            a.PosNoId,
            role_description,
            org_cd,
            COALESCE(m_sup.EmpNum, supervisor_EmpNum, supervisor_PosNoId) AS supervisor_id,
            a.start_date,
            3 AS sort_priority
        FROM agents_from_RelAgent a
        LEFT JOIN ids_map m USING(PosNoId)
        LEFT JOIN ids_map m_sup on a.supervisor_PosNoId = m_sup.PosNoId

        UNION ALL

        SELECT 
            "RelAgent" AS source,
            COALESCE(m.EmpNum, s.EmpNum) AS EmpNum,
            PosNoId,
            role_description,
            org_cd,
            CAST(NULL AS STRING) AS supervisor_id,
            s.start_date,
            2 AS sort_priority
        FROM supervisors_from_RelAgent s
        LEFT JOIN ids_map m USING(PosNoId)

        UNION ALL

        SELECT 
            "contacts" AS source,
            EmpNum,
            CAST(NULL AS STRING) AS PosNoId,
            role_description,
            org_cd,
            CAST(NULL AS STRING) AS supervisor_id,
            start_date,
            4 AS sort_priority
        FROM staff_from_contacts
    ),

    -- This CTE takes the initial dates and joins on caseload type information (aka specialties) using PosNoId.
    -- We only hydrate AgentType for start dates that occur on or after the earliest date we received specialty 
    -- information for that PosNoId.  Since we don't receive PosNoIds via the contacts data, AgentType is always
    -- NULL for rows in this CTE from the contacts data.
    initial_dates_with_agent_type_deduplicated AS (
      SELECT 
            source,
            COALESCE(EmpNum, PosNoId) as sort_key,
            role_description,
            org_cd,
            supervisor_id,
            CASE WHEN source = "contacts" 
                    THEN NULL
                WHEN i.start_date >= earliest_specialty_data_date 
                    THEN COALESCE(UPPER(AgentType), "GENERAL")
                ELSE NULL
                END AS AgentType,
            i.start_date
        FROM (
            SELECT *, 
                CASE WHEN ARRAY_LENGTH(SPLIT(PosNoId, "-")) > 0 
                     THEN SPLIT(PosNoId, "-")[OFFSET(0)] 
                     ELSE NULL
                     END AS PosNo
            FROM initial_dates
        ) i
        LEFT JOIN specialty_agents_periods ref 
            ON ref.PosNo = i.PosNo
            AND i.start_date >=  ref.start_date
            AND i.start_date < COALESCE(ref.end_date, DATE(9999,9,9))
        LEFT JOIN min_speciality_agents_dates m 
            ON m.PosNo = i.PosNo
        QUALIFY ROW_NUMBER() OVER(PARTITION by sort_key, start_date ORDER BY sort_priority, supervisor_id NULLS LAST, org_cd NULLS LAST, AgentType NULLS LAST, role_description) = 1
    ),
    
    -- This CTE creates initial staff periods by:
    --   - inferring AgentType, supervisor ids, and org codes using the last seen value for each field.  There
    --     could be cases where our inferrence is incorrect since we haven't received regular staff data historically,
    --     but should be more accurate in recent times (since we started receiving the roster).
    --   - calculating the end date of each period as the next start date.  For cases where the staff person 
    --     stopped appearing in the roster, we replace the end date (if non null) with the next roster date 
    --     (where we stopped seeing the staff person)
    --   - keeping only periods where either the end date is not null, the source of the info was from the roster,
    --     and periods that have started after the last roster receipt date.  In other words, we only keep only periods
    --     that were created from the roster, or for data that came in after the last roster reciept date.  This weeds out
    --     all staff data that only ever appear once in dbo_RelAgent or the contacts data a long time ago but haven't
    --     since appeared in the roster data (which we are going to assume as having all current staff).

    initial_periods AS (
        SELECT * EXCEPT(source)
        FROM (
            SELECT 
                * EXCEPT(end_date),
                CASE WHEN last_appearance_date IS NOT NULL AND last_appearance_date < most_recent_roster_date AND start_date < most_recent_roster_date
                        THEN COALESCE(end_date, GREATEST(first_roster_date_without, start_date))
                    ELSE end_date
                    END AS end_date
            FROM (
                SELECT * EXCEPT(supervisor_id, AgentType, org_cd),
                    LAST_VALUE(AgentType IGNORE NULLS) OVER(w) AS AgentType,
                    LAST_VALUE(supervisor_id IGNORE NULLS) OVER(w) AS supervisor_id,
                    LAST_VALUE(org_cd IGNORE NULLS) OVER(w) AS org_cd,
                    LEAD(start_date) OVER(PARTITION BY sort_key ORDER BY start_date) AS end_date
                FROM initial_dates_with_agent_type_deduplicated
                WINDOW w AS (PARTITION BY sort_key ORDER BY start_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            )
            LEFT JOIN max_roster_dates USING(sort_key)
            LEFT JOIN (SELECT DISTINCT most_recent_roster_date FROM roster_dates) ON 1=1
        )
        WHERE end_date IS NOT NULL 
           OR source = "roster"
           OR start_date > most_recent_roster_date
    ),
    
    -- This CTE uses aggregate_adjacent_spans to aggregate adjacent periods
    final_periods AS (
        {aggregate_adjacent_spans(
            table_name="initial_periods",
            attribute=["role_description", "org_cd", "AgentType", "supervisor_id"],
            index_columns=["sort_key"])}
    )

    -- This CTE creates a period id and then creates the sort_key_type and supervisor_id_type
    -- to identify the id types for each staff person and their supervisor (if not null).
    SELECT * EXCEPT(session_id, date_gap_id),
        ROW_NUMBER() OVER(PARTITION BY sort_key ORDER BY start_date) AS period_id,
        CASE WHEN sort_key like '%-%' THEN "PosNoId"
             WHEN sort_key IS NOT NULL THEN "EmpNum"
             ELSE NULL
             END AS sort_key_type,
        CASE WHEN supervisor_id like '%-%' THEN "PosNoId"
             WHEN supervisor_id IS NOT NULL THEN "EmpNum"
             ELSE NULL
             END AS supervisor_id_type
    FROM final_periods
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="supervision_staff_periods_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
