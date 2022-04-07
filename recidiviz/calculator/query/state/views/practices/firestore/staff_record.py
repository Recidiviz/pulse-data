#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""View to prepare staff records regarding compliant reporting for export to the frontend."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.datasets.static_data.config import EXTERNAL_REFERENCE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STAFF_RECORD_VIEW_NAME = "staff_record"

STAFF_RECORD_DESCRIPTION = """
    Nested staff records to be exported to Firestore to power the Compliant Reporting dashboard in TN.
    """

STAFF_RECORD_QUERY_TEMPLATE = """
    /*{description}*/
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
        LEFT JOIN `{project_id}.us_tn_raw_data_up_to_date_views.Staff_latest` staff
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
        FROM `{project_id}.us_tn_raw_data_up_to_date_views.Staff_latest` staff
        LEFT JOIN staff_from_report
        ON logic_staff = StaffID
        LEFT JOIN `{project_id}.{static_reference_tables_dataset}.us_tn_roster` roster
        ON roster.external_id = staff.UserID
        LEFT JOIN `{project_id}.{external_reference_dataset}.us_tn_supervision_locations` facilities
        ON staff.SiteID=facilities.site_code
        WHERE Status = 'A'
            AND StaffTitle IN ('PAOS', 'PARO', 'PRBO', 'PRBP', 'PRBM')
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

STAFF_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PRACTICES_VIEWS_DATASET,
    view_id=STAFF_RECORD_VIEW_NAME,
    view_query_template=STAFF_RECORD_QUERY_TEMPLATE,
    description=STAFF_RECORD_DESCRIPTION,
    static_reference_tables_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    analyst_views_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    external_reference_dataset=EXTERNAL_REFERENCE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STAFF_RECORD_VIEW_BUILDER.build_and_print()
