# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""View logic to prepare US_MO incarceration staff data for Workflows"""

US_MO_INCARCERATION_STAFF_TEMPLATE = """
    WITH 

    -- In MO, we treat facilities as staff with caseloads instead of using individual case managers.
    caseload_staff AS (
        SELECT DISTINCT
            facility_id AS id,
            state_code,
            -- TODO(#18886) Use facility name as name instead of ID
            facility_id AS name,
            CAST(NULL AS STRING) AS district,
            CAST(NULL AS STRING) AS email,
            false AS has_caseload,
            true AS has_facility_caseload,
            facility_id as given_names,
            "" as surname,
        FROM `{project_id}.{workflows_dataset}.resident_record_materialized`
        WHERE state_code = "US_MO"
        AND facility_id IS NOT NULL
    )
    -- TODO(#18886) Add people that should have access to tool
    SELECT 
        {columns}
    FROM caseload_staff
"""
