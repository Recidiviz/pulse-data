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
"""Query that generates state staff supervisor periods information."""

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans_postgres,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
    WITH 
    -- compile up to date info about active employees
    current_atlas_employee_info as (
        SELECT
            EmployeeId,
            UPPER(FirstName) as FirstName,
            UPPER(LastName) as LastName,
            UPPER(LocationName) as LocationName,
            UPPER(Email) as Email
        FROM {{ref_Employee}} emp
        LEFT JOIN {{ref_Location}} USING(LocationId)
        WHERE emp.Inactive = '0'
    ),
    -- since the roster currently only comes with officer email and names, we have to link on EmployeeId ourselves
    -- we'll first try matching by email (which works most of the time) and then try matching on name/district
    -- from our matches, we'll prioritize email matches over name/district matches
    employee_ids as (
        SELECT
            OFFICER_FIRST_NAME,
            OFFICER_LAST_NAME,
            DIST,
            COALESCE(EmployeeId_email_match, EmployeeId_name_match) as officer_EmployeeId
        FROM (
            SELECT
                DISTINCT *,
                emp_ref_email.EmployeeId as EmployeeId_email_match,
                emp_ref_name.EmployeeId as EmployeeId_name_match,
                ROW_NUMBER() 
                    OVER(PARTITION BY OFFICER_FIRST_NAME, OFFICER_LAST_NAME, DIST
                         ORDER BY emp_ref_email.EmployeeId NULLS LAST, emp_ref_name.EmployeeId) as row_number
            FROM {{RECIDIVIZ_REFERENCE_supervisor_roster@ALL}} roster
            LEFT JOIN current_atlas_employee_info emp_ref_email on UPPER(replace(roster.OFFICER_EMAIL, ',', '.')) = emp_ref_email.Email
            LEFT JOIN current_atlas_employee_info emp_ref_name 
                on UPPER(roster.OFFICER_FIRST_NAME) = emp_ref_name.FirstName
                and UPPER(roster.OFFICER_LAST_NAME) = emp_ref_name.LastName
                and STARTS_WITH(emp_ref_name.LocationName, CONCAT('DISTRICT ', SUBSTR(DIST, 2, 1)))       
        ) sub
        -- filter to the most likely match where either the email or name matched
        WHERE row_number = 1
          AND (EmployeeId_email_match is not null or EmployeeId_name_match is not null)
    ),
    -- now, we'll do the same matching to get supervisor employeeId
    -- we'll first try matching by email (which works most of the time) and then try matching on name/dist
    -- from our matches, we'll prioritize email matches over name/dist matches
    -- NOTE: there is a small possibility that there's a supervisor who has an email that doesn't match 
    --       AND is part of state/regional leadership, in which they wouldn't be matched here since their district 
    --       wouldn't match with their location in the ref_Employee.  Since they wouldn't match, anyone with that 
    --       supervisor would have a NULL supervisor for this supervisor period.  BUT I think it's overall still fine
    --       because the supervisor would be getting a regional manager or state leadership view of outliers anyways,
    --       and not a supervisor view.
    supervisor_ids as (
        SELECT
            SUPERVISOR_FIRST_NAME,
            SUPERVISOR_LAST_NAME,
            DIST,
            COALESCE(EmployeeId_email_match, EmployeeId_name_match) as supervisor_EmployeeId
        FROM (
            SELECT
                DISTINCT *,
                emp_ref_email.EmployeeId as EmployeeId_email_match,
                emp_ref_name.EmployeeId as EmployeeId_name_match,
                ROW_NUMBER() 
                    OVER(PARTITION BY SUPERVISOR_FIRST_NAME, SUPERVISOR_LAST_NAME
                         ORDER BY emp_ref_email.EmployeeId NULLS LAST, emp_ref_name.EmployeeId) as row_number
            FROM {{RECIDIVIZ_REFERENCE_supervisor_roster@ALL}} roster
            LEFT JOIN current_atlas_employee_info emp_ref_email on UPPER(replace(roster.SUPERVISOR_EMAIL, ',', '.')) = emp_ref_email.Email
            LEFT JOIN current_atlas_employee_info emp_ref_name 
                on UPPER(roster.SUPERVISOR_FIRST_NAME) = emp_ref_name.FirstName
                and UPPER(roster.SUPERVISOR_LAST_NAME) = emp_ref_name.LastName 
                and STARTS_WITH(emp_ref_name.LocationName, CONCAT('DISTRICT ', SUBSTR(DIST, 2, 1)))     
        ) sub
        -- filter to the most likely match where either the email or name/dist matched
        WHERE row_number = 1
          AND (EmployeeId_email_match is not null or EmployeeId_name_match is not null)
    ),
    -- join employee IDs back onto full roster information
    all_periods as (
        SELECT
            officer_EmployeeId,
            supervisor_EmployeeId,
            ACTIVE,
            update_datetime as start_date,
            LEAD(update_datetime) over(PARTITION BY officer_EmployeeId ORDER BY update_datetime) as end_date,
            MAX(update_datetime) OVER(PARTITION BY officer_EmployeeId) as last_appearance_date,
            MAX(update_datetime) OVER(PARTITION BY TRUE) as last_file_update_datetime
        FROM {{RECIDIVIZ_REFERENCE_supervisor_roster@ALL}} roster
        LEFT JOIN employee_ids e_ids USING(OFFICER_FIRST_NAME, OFFICER_LAST_NAME, DIST)
        LEFT JOIN supervisor_ids s_ids USING(SUPERVISOR_FIRST_NAME, SUPERVISOR_LAST_NAME, DIST)
    ),
    preliminary_periods as (
        SELECT
            officer_EmployeeId,
            supervisor_EmployeeId,
            ACTIVE,
            start_date,
            end_date
        FROM all_periods
        WHERE (start_date < last_appearance_date or start_date = last_file_update_datetime)
    ),
    final_periods as (
        {aggregate_adjacent_spans_postgres(
            table_name="preliminary_periods",
            attribute=["supervisor_EmployeeId", "ACTIVE"],
            index_columns=["officer_EmployeeId"])}
    )
    SELECT
        officer_EmployeeId,
        supervisor_EmployeeId,
        start_date,
        end_date,
        row_number() OVER(partition by officer_EmployeeId order by start_date, end_date nulls last) as period_id
    FROM final_periods
    WHERE ACTIVE = 'Y'
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="state_staff_supervisor_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="officer_EmployeeId, period_id",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
