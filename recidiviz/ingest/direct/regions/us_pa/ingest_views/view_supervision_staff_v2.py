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
Query containing supervision staff information.

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

This view compiles state staff and external id information from all three sources, prioritizing the 
most recent record from the highest priority source to deduplicate.
"""

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

    -- This CTE unions together all info needed for the StateStaff entity from all four  sources.
    -- In addition, we assign a priority order for which source to defer to in case of conflicting 
    -- information, assigning the roster data the highest priority

    all_sources AS (

        SELECT DISTINCT
            COALESCE(m.EmpNum, r.EmpNum) AS EmpNum,
            last_name,
            first_name,
            CAST(NULL AS STRING) as middle_name,
            CAST(NULL AS STRING) as suffix,
            PosNoId,
            Email,
            1 AS priority_order,
            start_date
        FROM roster_parsed r
        LEFT JOIN ids_map m USING(PosNoId)

        UNION ALL

        SELECT DISTINCT
            COALESCE(m.EmpNum, a.EmpNum) AS EmpNum,
            last_name,
            first_name,
            middle_name,
            suffix,
            PosNoId,
            CAST(NULL AS STRING) AS EMAIL,
            2 AS priority_order,
            start_date
        FROM agents_from_RelAgent a
        LEFT JOIN ids_map m USING(PosNoId)

        UNION ALL

        SELECT DISTINCT
            COALESCE(m.EmpNum, s.EmpNum) AS EmpNum,
            last_name,
            first_name,
            middle_name,
            suffix,
            PosNoId,
            CAST(NULL AS STRING) AS EMAIL,
            2 AS priority_order,
            start_date
        FROM supervisors_from_RelAgent s
        LEFT JOIN ids_map m USING(PosNoId)

        UNION ALL

        -- As of 9/2024, EmpNum is never null in this table
        -- If we ever get to having null EmpNums in this table, we would have no way to connect contacts to agents
        SELECT DISTINCT
            EmpNum,
            last_name,
            first_name,
            CAST(NULL AS STRING) as middle_name,
            CAST(NULL AS STRING) as suffix,
            CAST(NULL AS STRING) AS PosNoId,
            CAST(NULL AS STRING) AS Email,
            2 AS priority_order,
            start_date
        FROM staff_from_contacts c
        WHERE EmpNum IS NOT NULL
    )

    -- This CTE creates a sort_key variable that represents the ID value to sort and partition 
    -- by.  This sort_key uses EmpNum if EmpNum is not null, otherwise it uses PosNoId.  Then,
    -- for each sort_key value, this CTE keeps the staff information from the most recent
    -- record from the highest priority source, and then merges on all associated PosNoIds 
    -- as determined by the ids_map to be aggregated into a single list for each EmpNum.
    -- For records where EmpNum is NULL, there will only be a single PosNoId in the PosNoIds list.
    SELECT 
        info.EmpNum,
        last_name,
        first_name,
        middle_name,
        suffix,
        Email,
        STRING_AGG(DISTINCT COALESCE(ids.PosNoId, info.PosNoId) ORDER BY COALESCE(ids.PosNoId, info.PosNoId)) as PosNoIds
    FROM (
        SELECT DISTINCT *,
            COALESCE(EmpNum, PosNoId)  AS sort_key
        FROM all_sources
        QUALIFY ROW_NUMBER() OVER(PARTITION BY sort_key ORDER BY priority_order, start_date DESC, Email NULLS LAST, last_name NULLS LAST, first_name NULLS LAST, middle_name NULLS LAST, PosNoId NULLS LAST) = 1
    ) info
    LEFT JOIN ids_map ids USING(EmpNum)
    GROUP BY 1,2,3,4,5,6

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="supervision_staff_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
