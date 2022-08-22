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

"""View logic to prepare US_TN supervision staff data for Workflows"""

US_TN_SUPERVISION_STAFF_TEMPLATE = """
    WITH staff_from_report AS (
        SELECT DISTINCT officer_id AS logic_staff
        FROM `{project_id}.{analyst_views_dataset}.us_tn_compliant_reporting_logic_materialized`
    ), leadership_users AS (
        SELECT
            COALESCE(staff.StaffID, external_id, email_address) AS id,
            "US_TN" AS state_code,
            first_name || " " || last_name AS name,
            CAST(null AS STRING) AS district,
            LOWER(email_address) AS email,
        FROM `{project_id}.{static_reference_tables_dataset}.us_tn_leadership_users` leadership
        LEFT JOIN `{project_id}.{us_tn_raw_data_up_to_date_dataset}.Staff_latest` staff
        ON UPPER(leadership.first_name) = staff.FirstName
            AND UPPER(leadership.last_name) = staff.LastName
            AND staff.Status = 'A'
    ), staff_users AS (
        SELECT
            StaffID as id,
            "US_TN" AS state_code,
            FirstName || " " || LastName AS name,
            facilities.district AS district,
            LOWER(roster.email_address) AS email,
            logic_staff IS NOT NULL AS has_caseload,
        FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.Staff_latest` staff
        LEFT JOIN staff_from_report
        ON logic_staff = StaffID
        LEFT JOIN `{project_id}.{static_reference_tables_dataset}.us_tn_roster` roster
        ON roster.external_id = staff.UserID
        LEFT JOIN `{project_id}.{external_reference_dataset}.us_tn_supervision_locations` facilities
        ON staff.SiteID=facilities.site_code
        WHERE Status = 'A'
            AND StaffTitle IN ('PAOS', 'PARO', 'PRBO', 'PRBP', 'PRBM', 'SECR', 'ADSC')
    )

    SELECT * FROM staff_users WHERE id NOT IN (SELECT id FROM leadership_users)

    UNION ALL

    SELECT
        leadership_users.*,
        logic_staff IS NOT NULL AS has_caseload,
    FROM leadership_users
    LEFT JOIN staff_from_report
    ON leadership_users.id = staff_from_report.logic_staff
"""
