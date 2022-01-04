# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""
Data to populate the overdue discharge alert email

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.overdue_discharge_alert.overdue_discharge_alert_data_views
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

OVERDUE_DISCHARGE_ALERT_DATA_VIEW_NAME = "overdue_discharge_alert_data"

OVERDUE_DISCHARGE_ALERT_DATA_DESCRIPTION = """
 Monthly data regarding an officer's success in discharging people from supervision, recommending early discharge
 from supervision, and keeping cases in compliance with state standards.
 """

DISCHARGE_STRUCT_FRAGMENT = """STRUCT (
                projected_discharges.person_external_id,
                CONCAT(projected_discharges.first_name, " ", projected_discharges.last_name) as full_name,
                FORMAT_DATE("%x", projected_discharges.projected_end_date) AS expiration_date
            )"""

OVERDUE_DISCHARGE_ALERT_DATA_QUERY_TEMPLATE = """
/*{description}*/
# TODO(#9988) Replace roster query with a recipients reference table
WITH base_recipients AS (
    SELECT 'US_ID' AS state_code, external_id, email_address  FROM `{project_id}.{static_reference_dataset}.us_id_roster`
    UNION ALL
    SELECT state_code, officer_external_id AS external_id, email_address FROM `{project_id}.{static_reference_dataset}.po_report_recipients`
    WHERE state_code = 'US_PA'
), overdue_discharge_alert_recipients AS (
    SELECT
        base_recipients.state_code,
        base_recipients.email_address,
        base_recipients.external_id,
        officer_district.district,
        TRIM(SPLIT(ARRAY_AGG(augmented_agent_info.given_names)[SAFE_OFFSET(0)], ' ')[SAFE_OFFSET(0)]) AS officer_given_name
    FROM base_recipients
    JOIN `{project_id}.{reference_views_dataset}.augmented_agent_info` augmented_agent_info USING (state_code, external_id)
    JOIN `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized` officer_district
        ON officer_district.state_code = base_recipients.state_code
        AND officer_district.officer_external_id = base_recipients.external_id
    GROUP BY state_code, district, email_address, external_id
)
SELECT
    EXTRACT(YEAR FROM CURRENT_DATE('US/Eastern')) AS review_year,
    EXTRACT(MONTH FROM CURRENT_DATE('US/Eastern')) AS review_month,
    overdue_discharge_alert_recipients.state_code,
    overdue_discharge_alert_recipients.district,
    overdue_discharge_alert_recipients.email_address,
    overdue_discharge_alert_recipients.officer_given_name,
    ARRAY_AGG(
        IF(projected_end_date <= CURRENT_DATE('US/Eastern'), {discharge_struct}, NULL)
        IGNORE NULLS
        ORDER BY projected_discharges.projected_end_date ASC
    ) AS overdue_discharges,

    ARRAY_AGG(
        IF(projected_end_date > CURRENT_DATE('US/Eastern'), {discharge_struct}, NULL)
        IGNORE NULLS
        ORDER BY projected_discharges.projected_end_date ASC
    ) AS upcoming_discharges,
FROM `{project_id}.{analyst_dataset}.projected_discharges_materialized` projected_discharges
INNER JOIN overdue_discharge_alert_recipients
    ON overdue_discharge_alert_recipients.state_code = projected_discharges.state_code
    AND overdue_discharge_alert_recipients.external_id = projected_discharges.supervising_officer_external_id
LEFT OUTER JOIN `{project_id}.{reference_views_dataset}.overdue_discharge_alert_exclusions` overdue_discharge_alert_exclusions
    ON overdue_discharge_alert_exclusions.state_code = projected_discharges.state_code
    AND overdue_discharge_alert_exclusions.person_external_id = projected_discharges.person_external_id
WHERE
    overdue_discharge_alert_exclusions.state_code IS NULL
    AND projected_end_date <= DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 60 DAY)
GROUP BY
    overdue_discharge_alert_recipients.state_code,
    overdue_discharge_alert_recipients.district,
    overdue_discharge_alert_recipients.email_address,
    overdue_discharge_alert_recipients.officer_given_name
ORDER BY overdue_discharge_alert_recipients.email_address;
"""

OVERDUE_DISCHARGE_ALERT_DATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    description=OVERDUE_DISCHARGE_ALERT_DATA_DESCRIPTION,
    dataset_id=dataset_config.OVERDUE_DISCHARGE_ALERT_DATASET,
    view_id=OVERDUE_DISCHARGE_ALERT_DATA_VIEW_NAME,
    should_materialize=True,
    view_query_template=OVERDUE_DISCHARGE_ALERT_DATA_QUERY_TEMPLATE,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    po_report_dataset=dataset_config.PO_REPORT_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    discharge_struct=DISCHARGE_STRUCT_FRAGMENT,
)

OVERDUE_DISCHARGE_ALERT_VIEW_BUILDERS = [OVERDUE_DISCHARGE_ALERT_DATA_VIEW_BUILDER]

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OVERDUE_DISCHARGE_ALERT_DATA_VIEW_BUILDER.build_and_print()
