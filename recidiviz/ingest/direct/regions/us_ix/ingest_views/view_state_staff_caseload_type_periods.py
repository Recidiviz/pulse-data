# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License AS published by
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
"""Query that generates state staff caseload type periods information (which is pulled FROM the supervisor roster)."""

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.ingest.direct.regions.us_ix.ingest_views.query_fragments import (
    CURRENT_ATLAS_EMPLOYEE_INFO_CTE,
    SUPERVISOR_ROSTER_EMPLOYEE_IDS_CTE,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f""",
    {CURRENT_ATLAS_EMPLOYEE_INFO_CTE},
    {SUPERVISOR_ROSTER_EMPLOYEE_IDS_CTE},
    -- join employee IDs back onto full roster information
    all_periods AS (
        SELECT
            officer_EmployeeId,
            UPPER(CASELOAD_TYPE_OR_NOTES) AS caseload_type_raw,
            ACTIVE,
            update_datetime AS start_date,
            LEAD(update_datetime) over(PARTITION BY officer_EmployeeId ORDER BY update_datetime) AS end_date,
            MAX(update_datetime) OVER(PARTITION BY officer_EmployeeId) AS last_appearance_date,
            MAX(update_datetime) OVER(PARTITION BY TRUE) AS last_file_update_datetime
        FROM {{RECIDIVIZ_REFERENCE_supervisor_roster@ALL}} roster
        LEFT JOIN employee_ids e_ids USING(OFFICER_FIRST_NAME, OFFICER_LAST_NAME, DIST)
    ),
    all_employee_periods AS (
        SELECT
            EmployeeId,
            et.EmployeeTypeName,
            update_datetime AS start_date,
            StaffId,
            LAG(et.EmployeeTypeName) OVER (PARTITION BY EmployeeId ORDER BY update_datetime) as prev_EmployeeTypeName
        FROM {{ref_Employee@ALL}} ee
        LEFT JOIN {{ref_EmployeeType}} et ON ee.EmployeeTypeId = et.EmployeeTypeId
        WHERE ee.EmployeeTypeId IS NOT NULL
    ),
    transitional_periods AS (
        SELECT 
            EmployeeId AS officer_EmployeeId,
            EmployeeTypeName,
            start_date,
            LEAD(start_date) OVER (PARTITION BY EmployeeId ORDER BY start_date) as end_date,
            StaffId
        FROM all_employee_periods  
        WHERE EmployeeTypeName <> prev_EmployeeTypeName OR prev_EmployeeTypeName IS NULL
    ),
    preliminary_periods AS (
        SELECT
            officer_EmployeeId,
            caseload_type_raw,
            ACTIVE,
            start_date,
            end_date
        FROM all_periods
        WHERE (start_date < last_appearance_date or start_date = last_file_update_datetime)
    ),
    aggregated_periods AS (
        {aggregate_adjacent_spans(
            table_name="preliminary_periods",
            attribute=["caseload_type_raw", "ACTIVE"],
            index_columns=["officer_EmployeeId"])}
    ),
    caseload_periods AS (
        SELECT
            officer_EmployeeId,
            caseload_type_raw,
            start_date,
            end_date
        FROM aggregated_periods
        WHERE ACTIVE = 'Y' and officer_EmployeeId is not null
    ),
    specialized_caseload_periods AS (
        -- ADMINISTRATIVE_SUPERVISION
        SELECT *, 'ADMINISTRATIVE_SUPERVISION' AS caseload_type
        FROM caseload_periods
        where caseload_type_raw like '%LSU%'

        UNION ALL 
        
        -- COMMUNITY_FACILITY
        SELECT *, 'COMMUNITY_FACILITY' AS caseload_type
        FROM caseload_periods
        where caseload_type_raw like '%CRC%'

        UNION ALL

        -- DRUG_COURT
        SELECT *, 'DRUG_COURT' AS caseload_type
        FROM caseload_periods
        where     
            caseload_type_raw like '%DC%' OR
            caseload_type_raw like '%DRUG%'
        
        UNION ALL

        -- ELECTRONIC_MONITORING
        SELECT *, 'ELECTRONIC_MONITORING' AS caseload_type
        FROM caseload_periods
        where caseload_type_raw like '%GEO%'

        UNION ALL
        
        -- INTENSIVE
        SELECT *, 'INTENSIVE' AS caseload_type
        FROM caseload_periods
        where caseload_type_raw like '%HIGH RISK%'

        UNION ALL

        -- MENTAL_HEALTH
        SELECT *, 'MENTAL_HEALTH' AS caseload_type
        FROM caseload_periods
        where 
            caseload_type_raw like '%MENTAL%' or
            caseload_type_raw like '%MH%'

        UNION ALL

        -- OTHER_COURT
        SELECT *, 'OTHER_COURT' AS caseload_type
        FROM caseload_periods
        where
            caseload_type_raw like '%FAMILY COURT%' OR
            caseload_type_raw like '%WOOD COURT%' OR
            caseload_type_raw like '%DUI%'

        UNION ALL

        -- SEX_OFFENSE
        SELECT *, 'SEX_OFFENSE' AS caseload_type
        FROM caseload_periods
        WHERE 
            caseload_type_raw like '%SO%' or
            caseload_type_raw like '%SEX OFFENDER%' 

        UNION ALL
        
        -- VETERANS_COURT
        SELECT *, 'VETERANS_COURT' AS caseload_type
        FROM caseload_periods
        WHERE caseload_type_raw like '%VET%'    

        UNION ALL

        -- DOMESTIC_VIOLENCE
        SELECT *, 'DOMESTIC_VIOLENCE' AS caseload_type
        FROM caseload_periods
        WHERE 
            caseload_type_raw LIKE '%DV%' OR 
            caseload_type_raw LIKE '%DOMESTIC VIOLENCE%'
        
        UNION ALL

        -- INTERSTATE
        SELECT *, 'OTHER' AS caseload_type
        FROM caseload_periods
        WHERE 
            caseload_type_raw LIKE '%INTERSTATE%'

        UNION ALL

        -- TRANSITIONAL
        SELECT
            officer_EmployeeId,
            EmployeeTypeName AS caseload_type_raw,
            start_date,
            CAST(end_date AS DATETIME),
            'TRANSITIONAL' AS caseload_type
        FROM (
            SELECT 
                *
            FROM transitional_periods
            WHERE EmployeeTypeName in ("Business Operations Manager","Business Operations Manager - PSI") 
            OR LOWER(StaffId) LIKE "%bench%"
            OR TO_HEX(SHA256(officer_EmployeeId)) = 
            "f826ecc1611f62c4707ad52da9883fc15f69a1cddda43250c364fc2457ba4b06")
    ),
    general_caseload_periods AS (
        SELECT *, 'GENERAL' AS caseload_type
        FROM caseload_periods
        WHERE caseload_type_raw like '%GEN%'
        
        UNION DISTINCT
        -- QUESTION: do we want values like "DOSAGE" to have a GENERAL type or OTHER? 
        SELECT *, 'GENERAL' AS caseload_type
        FROM 
        (
            SELECT *
            FROM caseload_periods
            
            EXCEPT DISTINCT

            SELECT
                officer_EmployeeId,
                caseload_type_raw,
                start_date,
                end_date
            FROM specialized_caseload_periods
        ) other_uncategorized
    )

    SELECT
        officer_EmployeeId,
        caseload_type,
        caseload_type_raw,
        start_date,
        end_date,
        ROW_NUMBER() 
            OVER(PARTITION BY officer_EmployeeId 
                 ORDER BY start_date, end_date NULLS LAST, caseload_type)
            AS period_id
    FROM (
        SELECT * FROM general_caseload_periods
        union all 
        SELECT * FROM specialized_caseload_periods
    ) sub

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="state_staff_caseload_type_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
