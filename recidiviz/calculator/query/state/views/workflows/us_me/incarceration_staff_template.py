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
"""View logic to prepare US_ME incarceration staff data for Workflows"""

from recidiviz.calculator.query.bq_utils import get_pseudonymized_id_query_str

US_ME_INCARCERATION_STAFF_TEMPLATE = f"""
    WITH 
    caseload_staff_ids AS (
        SELECT DISTINCT
            officer_id AS id,
            state_code,
        FROM `{{project_id}}.{{workflows_dataset}}.resident_record_materialized`
        WHERE state_code = "US_ME"
        AND officer_id IS NOT NULL
    )
    , caseload_staff AS (
        SELECT DISTINCT
            ids.id,
            ids.state_code,
            CAST(NULL AS STRING) AS district,
            LOWER(state_table.Email_Tx) AS email,
            UPPER(state_table.First_Name) as given_names,
            UPPER(state_table.Last_Name) as surname,
            CAST(NULL AS STRING) AS role_subtype,
            {get_pseudonymized_id_query_str("IF(state_code = 'US_IX', 'US_ID', state_code) || id")} AS pseudonymized_id 
        FROM caseload_staff_ids ids
        -- TODO(#31900): Pull this from sessions preprocessing, not raw data.
        LEFT JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_900_EMPLOYEE_latest` state_table
            ON state_table.Employee_Id = ids.id
    )
    SELECT 
        {{columns}}
    FROM caseload_staff
"""
