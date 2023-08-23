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

"""View logic to prepare US_CA supervision staff data for Workflows"""

from recidiviz.calculator.query.state.views.workflows.us_ca.shared_ctes import (
    US_CA_MOST_RECENT_CLIENT_DATA,
)

US_CA_SUPERVISION_STAFF_TEMPLATE = f"""
    WITH
    most_recent_client_data AS (
        {US_CA_MOST_RECENT_CLIENT_DATA}
    )
    -- Assign officers to the district where they have the most clients
    , caseload_districts AS (
        SELECT BadgeNumber, ParoleUnit as district
            FROM most_recent_client_data
            WHERE
                ParoleUnit IS NOT NULL
            GROUP BY BadgeNumber, ParoleUnit
            QUALIFY ROW_NUMBER() OVER (PARTITION BY BadgeNumber ORDER BY COUNT(*) desc) = 1
    )
    SELECT
        state_staff.state_code,
        external_id AS id,
        JSON_VALUE(full_name, "$.given_names") AS given_names,
        JSON_VALUE(full_name, "$.surname") AS surname,
        UPPER(JSON_VALUE(full_name, "$.given_names") || " " || JSON_VALUE(full_name, "$.middle_names") || JSON_VALUE(full_name, "$.surname")) AS name,
        JSON_VALUE(full_name, "$.given_names") || "." || JSON_VALUE(full_name, "$.surname") || "@cdcr.ca.gov" AS email,
        TRUE AS has_caseload,
        FALSE AS has_facility_caseload,
        caseload_districts.district

    FROM `{{project_id}}.{{normalized_state_dataset}}.state_staff` state_staff
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_staff_external_id` eid
        USING(staff_id)
    LEFT JOIN caseload_districts ON eid.external_id = caseload_districts.BadgeNumber
    INNER JOIN (
        SELECT DISTINCT state_code, officer_id as external_id
        FROM `{{project_id}}.{{workflows_dataset}}.client_record_materialized`
        WHERE state_code="US_CA"
    ) USING(external_id)
    WHERE state_staff.state_code = "US_CA"
 """
