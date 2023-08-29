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
    US_CA_MOST_RECENT_AGENT_DATA,
    US_CA_MOST_RECENT_CLIENT_DATA,
)

US_CA_SUPERVISION_STAFF_TEMPLATE = f"""
    WITH
    most_recent_client_data AS (
        {US_CA_MOST_RECENT_CLIENT_DATA}
    )
    , most_recent_agent_data AS (
        {US_CA_MOST_RECENT_AGENT_DATA}
    )
    -- Assign officers to the district where they have the most clients
    -- Note that we treat units in CA as districts
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
        caseload_districts.district,
        CASE agents.AgentClassification
            -- Role descriptions from https://drive.google.com/file/d/1rsEHSCWgjdHLUVWo5rWpbx_nRnPB9Uf2/view,
            -- page 758
            -- "A Parole Agent I is a case-carrying parole agent who is assigned to a parole unit within DAPO."
            WHEN "Parole Agent I, AP" THEN "SUPERVISION_OFFICER"
            -- "A Parole Agent II (Supervisor) is the first-line supervisor responsible for the supervision of Parole Agent Is who are assigned to a parole unit within DAPO."
            WHEN "Parole Agent II, AP (Supv)" THEN "SUPERVISION_OFFICER_SUPERVISOR"
            -- "The unit supervisor is a Parole Agent III and the second-line supervisor responsible for the overall operation of the parole unit."
            WHEN "Parole Agent III, AP" THEN "SUPERVISION_DISTRICT_MANAGER"
            ELSE NULL
            END
            AS role_subtype

    FROM `{{project_id}}.{{normalized_state_dataset}}.state_staff` state_staff
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_staff_external_id` eid
        USING(staff_id)
    LEFT JOIN caseload_districts ON eid.external_id = caseload_districts.BadgeNumber
    LEFT JOIN most_recent_agent_data agents
        ON eid.external_id = agents.BadgeNumber
        AND caseload_districts.district = agents.ParoleUnit
    INNER JOIN (
        SELECT DISTINCT state_code, officer_id as external_id
        FROM `{{project_id}}.{{workflows_dataset}}.client_record_materialized`
        WHERE state_code="US_CA"
    ) USING(external_id)
    WHERE state_staff.state_code = "US_CA"
 """
