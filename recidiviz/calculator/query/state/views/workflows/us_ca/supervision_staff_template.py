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

US_CA_SUPERVISION_STAFF_TEMPLATE = """
    WITH
    -- Assign officers with caseloads to the district where they have the most clients
    caseload_districts AS (
        SELECT officer_id as BadgeNumber, ParoleUnit as district
            FROM `{project_id}.{us_ca_raw_data_up_to_date_dataset}.PersonParole_latest`
            -- TODO(#22427) Once PersonParole has badge numbers, delete this join on client record
            INNER JOIN `{project_id}.{workflows_dataset}.client_record_materialized`
                ON OffenderId=person_external_id
            WHERE
                ParoleUnit IS NOT NULL
            GROUP BY officer_id, ParoleUnit
            QUALIFY ROW_NUMBER() OVER (PARTITION BY officer_id ORDER BY COUNT(*) desc) = 1
    ),
    -- Assign officers without caseloads only if they're associated with a singular district
    no_caseload_districts AS (
        SELECT BadgeNumber, ANY_VALUE(ParoleUnit) AS district
            FROM `{project_id}.{us_ca_raw_data_up_to_date_dataset}.AgentParole_latest`
            WHERE ParoleUnit IS NOT NULL
            GROUP BY BadgeNumber, ParoleUnit
            HAVING count(DISTINCT ParoleUnit)=1
    )
    SELECT
        state_staff.state_code,
        external_id AS id,
        JSON_VALUE(full_name, "$.given_names") AS given_names,
        JSON_VALUE(full_name, "$.surname") AS surname,
        UPPER(JSON_VALUE(full_name, "$.given_names") || " " || JSON_VALUE(full_name, "$.middle_names") || JSON_VALUE(full_name, "$.surname")) AS name,
        "PLACEHOLDER." || JSON_VALUE(full_name, "$.given_names") || "." || JSON_VALUE(full_name, "$.surname") || "@cdcr.ca.gov" AS email,
        external_id IN (
            SELECT DISTINCT
                officer_id
            FROM `{project_id}.{workflows_dataset}.client_record_materialized`
            WHERE state_code="US_CA"
                AND officer_id IS NOT NULL
        ) AS has_caseload,
        FALSE AS has_facility_caseload,
        IFNULL(caseload_districts.district, no_caseload_districts.district) AS district

    FROM `{project_id}.{normalized_state_dataset}.state_staff` state_staff
    LEFT JOIN `{project_id}.{normalized_state_dataset}.state_staff_external_id` eid
        USING(staff_id)
    LEFT JOIN caseload_districts ON eid.external_id = caseload_districts.BadgeNumber
    LEFT JOIN no_caseload_districts ON eid.external_id = no_caseload_districts.BadgeNumber
    WHERE
        state_staff.state_code = "US_CA"
 """
