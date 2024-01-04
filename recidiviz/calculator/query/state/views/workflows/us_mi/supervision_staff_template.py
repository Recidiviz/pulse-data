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

"""View logic to prepare US_MI supervision staff data for Workflows"""

US_MI_SUPERVISION_STAFF_TEMPLATE = """
    WITH officers_with_caseload AS (
        SELECT DISTINCT officer_id
        FROM `{project_id}.{workflows_dataset}.client_record_materialized`
        WHERE state_code = 'US_MI'
    )
    SELECT
        "US_MI" AS state_code,
        officer_id AS id,
        -- If there are staff members that don't have a district in the staff record, they won't
        -- show up in search results for states that restrict search by district. This is a pretty
        -- common class of tickets we get from external users, so filling it in from the roster
        -- means that in this case, it can be fixed via the admin panel.
        IFNULL(supervision_district_id, roster.district) AS district,
        email,
        TRUE AS has_caseload,
        FALSE AS has_facility_caseload,
        given_names,
        surname,
        CAST(NULL AS STRING) AS role_subtype
    FROM officers_with_caseload
    INNER JOIN `{project_id}.reference_views.current_staff_materialized` current_staff
        ON current_staff.state_code = "US_MI"
        AND current_staff.external_id = officers_with_caseload.officer_id
    LEFT JOIN `{project_id}.reference_views.product_roster_materialized` roster
        ON current_staff.state_code = roster.state_code
        AND current_staff.email = roster.email_address
 """
