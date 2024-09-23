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
"""View logic to prepare US_AZ incarceration staff data for Workflows"""


from recidiviz.calculator.query.bq_utils import get_pseudonymized_id_query_str

US_AZ_INCARCERATION_STAFF_TEMPLATE = f"""
    WITH
    caseload_staff_ids AS (
        SELECT DISTINCT
            officer_id AS id,
            state_code,
        FROM `{{project_id}}.{{workflows_dataset}}.resident_record_materialized`
        WHERE state_code = "US_AZ"
        AND officer_id IS NOT NULL
    )
    , caseload_staff AS (
        SELECT DISTINCT
            ids.id,
            ids.state_code,
            CAST(NULL AS STRING) AS district,
            CAST(NULL AS STRING) AS email,
            UPPER(state_table.FIRST_NAME) as given_names,
            UPPER(state_table.SURNAME) as surname,
            CAST(NULL AS STRING) AS role_subtype,
            {get_pseudonymized_id_query_str("IF(state_code = 'US_IX', 'US_ID', state_code) || id")} AS pseudonymized_id 
        FROM caseload_staff_ids ids
        INNER JOIN `{{project_id}}.{{us_az_raw_data_up_to_date_dataset}}.PERSON_latest` state_table
            ON PERSON_ID = ids.id
    )
    SELECT 
        {{columns}}
    FROM caseload_staff
"""
