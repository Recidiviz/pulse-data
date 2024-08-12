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
"""View logic to prepare US_ND incarceration staff data for Workflows"""


US_ND_INCARCERATION_STAFF_TEMPLATE = """
    WITH
    caseload_staff_ids AS (
        SELECT DISTINCT
            officer_id AS id,
            state_code,
        FROM `{project_id}.{workflows_dataset}.resident_record_materialized`
        WHERE state_code = "US_ND"
        AND officer_id IS NOT NULL
    )
    , caseload_staff AS (
        SELECT DISTINCT
            ids.id,
            ids.state_code,
            CAST(NULL AS STRING) AS district,
            CAST(NULL AS STRING) AS email,
            UPPER(state_table.FIRST_NAME) as given_names,
            UPPER(state_table.LAST_NAME) as surname,
            CAST(NULL AS STRING) AS role_subtype,
        FROM caseload_staff_ids ids
        INNER JOIN `{project_id}.{us_nd_raw_data_up_to_date_dataset}.recidiviz_elite_staff_members_latest` state_table
            ON REPLACE(REPLACE(state_table.STAFF_ID, '.00',''), ',', '') = ids.id
    )
    SELECT 
        {columns}
    FROM caseload_staff
"""
