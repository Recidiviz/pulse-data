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
"""Query that generates state staff role periods and location periods information."""

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
    WITH 
    ref_Employee_latest as (
        SELECT *
        FROM (
            SELECT 
                EmployeeId,
                FirstName,
                LastName,
                ROW_NUMBER() OVER(PARTITION BY EmployeeId ORDER BY update_datetime DESC) as rn
            FROM {{ref_Employee@ALL}}
        ) all_rows
        WHERE rn = 1
    ),
    preliminary_periods as (
        SELECT
            EmployeeId,
            EmployeeTypeName,
            LocationId,
            emp.InActive,
            update_datetime as start_date,
            LEAD(update_datetime) over(PARTITION BY EmployeeId ORDER BY update_datetime) as end_date
        FROM {{ref_Employee@ALL}} emp
        LEFT JOIN {{ref_EmployeeType}} ref USING(EmployeeTypeId)
    ),
    final_periods as (
        {aggregate_adjacent_spans(
            table_name="preliminary_periods",
            attribute=["EmployeeTypeName", "LocationId", "InActive"],
            index_columns=["EmployeeId"])}
    )
    SELECT
        EmployeeId,
        FirstName,
        LastName,
        UPPER(EmployeeTypeName) as EmployeeTypeName,
        LocationId,
        start_date,
        end_date,
        row_number() OVER(partition by EmployeeId order by start_date, end_date nulls last) as period_id
    FROM final_periods
    LEFT JOIN ref_Employee_latest ref USING(EmployeeId)
    -- For now, narrow down to just supervision officers since that's our only current use case for StateStaff
    WHERE 
        InActive <> '1'
        AND
        (
            UPPER(EmployeeTypeName) like '%P&P%' OR
            UPPER(EmployeeTypeName) like '%PROBATION%' OR
            UPPER(EmployeeTypeName) like '%PAROLE%' OR
            UPPER(EmployeeTypeName) like '%SUPERVISION%' OR
            -- Include other specialists that are assigned to clients
            UPPER(EmployeeTypeName) in ('BUSINESS OPERATIONS MANAGER - PSI', 
                                        'INTERSTATE COMPACT', 
                                        'BUSINESS OPERATIONS MANAGER',
                                        'REENTRY MANAGER')
        )
        AND 
        (   
            UPPER(EmployeeTypeName) not like '%HEARING%' AND
            UPPER(EmployeeTypeName) not like '%COMMISSION%'
        )
        -- exclude the leadership folks that are going to be ingested in a different view (state_staff_role_location_periods_leadership)
        AND EmployeeId not in (SELECT EmployeeId FROM {{RECIDIVIZ_REFERENCE_leadership_roster}})
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="state_staff_role_location_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
