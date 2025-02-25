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
"""A view revealing when an officer from the state staff roster fails to be linked to
an employee_id and is excluded."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SUPERVISOR_ROSTER_EXCLUSION_VIEW_NAME = "supervisor_roster_exclusions"

SUPERVISOR_ROSTER_EXCLUSION_DESCRIPTION = """This view surfaces when an officer from the state staff roster fails to be linked to
an employee_id and is excluded from the list"""

SUPERVISOR_ROSTER_EXCLUSION_QUERY_TEMPLATE = """
WITH current_atlas_employee_info AS (
    SELECT
        EmployeeId,
        UPPER(emp.FirstName) as FirstName,
        UPPER(emp.LastName) as LastName,
        UPPER(LocationName) as LocationName,
        UPPER(Email) as Email,
        ROW_NUMBER() 
            OVER(PARTITION BY UPPER(Email), 
                            UPPER(LocationName),
                            UPPER(FirstName),
                            UPPER(LastName)
                ORDER BY emp.Inactive,
                            (DATE(emp.InsertDate)) DESC,
                            CAST(EmployeeId as INT64) DESC) as recency_rnk
    FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ref_Employee_latest` emp
    LEFT JOIN `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ref_Location_latest` USING(LocationId)
    ),
    ranked_current_atlas_employee_info AS (
    SELECT
        *
    FROM current_atlas_employee_info
        WHERE recency_rnk = 1
    ),
    employee_exclusions AS ( 
    SELECT DISTINCT *,
                emp_ref_email.EmployeeId as EmployeeId_email_match,
                emp_ref_name.EmployeeId as EmployeeId_name_match,
                ROW_NUMBER() 
                    OVER(PARTITION BY OFFICER_FIRST_NAME, OFFICER_LAST_NAME, DIST
                         ORDER BY emp_ref_email.EmployeeId NULLS LAST, emp_ref_name.EmployeeId) as row_num
            FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_supervisor_roster_latest` roster
            LEFT JOIN ranked_current_atlas_employee_info emp_ref_email on UPPER(replace(roster.OFFICER_EMAIL, ',', '.')) = emp_ref_email.Email
            LEFT JOIN ranked_current_atlas_employee_info emp_ref_name 
                ON UPPER(roster.OFFICER_FIRST_NAME) = emp_ref_name.FirstName
                AND UPPER(roster.OFFICER_LAST_NAME) = emp_ref_name.LastName
                AND STARTS_WITH(emp_ref_name.LocationName, CONCAT('DISTRICT ', SUBSTR(DIST, 2, 1)))    
    WHERE emp_ref_email.EmployeeId IS NULL AND emp_ref_name.EmployeeId IS NULL),
    supervisor_exclusions AS ( 
    SELECT DISTINCT *,
                emp_ref_email.EmployeeId as EmployeeId_email_match,
                emp_ref_name.EmployeeId as EmployeeId_name_match,
                ROW_NUMBER() 
                    OVER(PARTITION BY SUPERVISOR_FIRST_NAME, SUPERVISOR_LAST_NAME
                         ORDER BY emp_ref_email.EmployeeId NULLS LAST, emp_ref_name.EmployeeId) as row_num
            FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_supervisor_roster_latest` roster
            LEFT JOIN ranked_current_atlas_employee_info emp_ref_email on UPPER(replace(roster.OFFICER_EMAIL, ',', '.')) = emp_ref_email.Email
            LEFT JOIN ranked_current_atlas_employee_info emp_ref_name 
                ON UPPER(roster.SUPERVISOR_FIRST_NAME) = emp_ref_name.FirstName
                AND UPPER(roster.SUPERVISOR_LAST_NAME) = emp_ref_name.LastName
                AND STARTS_WITH(emp_ref_name.LocationName, CONCAT('DISTRICT ', SUBSTR(DIST, 2, 1)))    
    WHERE emp_ref_email.EmployeeId IS NULL AND emp_ref_name.EmployeeId IS NULL)

    SELECT  
        OFFICER_FIRST_NAME AS FIRST_NAME,
        OFFICER_LAST_NAME AS LAST_NAME,
        DIST,
        "OFFICER" AS POSITION,
        COALESCE(EmployeeId_email_match, EmployeeId_name_match) AS EmployeeId,
        "US_IX" AS region_code
    FROM employee_exclusions
        WHERE row_num = 1
    UNION ALL
    SELECT  
        SUPERVISOR_FIRST_NAME AS FIRST_NAME,
        SUPERVISOR_LAST_NAME AS LAST_NAME,
        DIST,
        "SUPERVISOR" AS POSITION,
        COALESCE(EmployeeId_email_match, EmployeeId_name_match) AS EmployeeId,
        "US_IX" AS region_code
    FROM supervisor_exclusions
        WHERE row_num = 1
"""

SUPERVISOR_ROSTER_EXCLUSION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SUPERVISOR_ROSTER_EXCLUSION_VIEW_NAME,
    view_query_template=SUPERVISOR_ROSTER_EXCLUSION_QUERY_TEMPLATE,
    description=SUPERVISOR_ROSTER_EXCLUSION_DESCRIPTION,
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISOR_ROSTER_EXCLUSION_VIEW_BUILDER.build_and_print()
