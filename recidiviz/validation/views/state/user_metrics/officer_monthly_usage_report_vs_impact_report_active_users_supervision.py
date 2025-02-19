# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Compare distinct active users count between the usage reports and impact reports"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

OFFICER_MONTHLY_USAGE_REPORT_VS_IMPACT_REPORT_ACTIVE_USERS_SUPERVISION_VIEW_NAME = (
    "officer_monthly_usage_report_vs_impact_report_active_users_supervision"
)

OFFICER_MONTHLY_USAGE_REPORT_VS_IMPACT_REPORT_ACTIVE_USERS_SUPERVISION_DESCRIPTION = """Compare distinct active users count between the usage reports and impact reports"""

OFFICER_MONTHLY_USAGE_REPORT_VS_IMPACT_REPORT_ACTIVE_USERS_SUPERVISION_QUERY_TEMPLATE = """
SELECT
    state_code,
    state_code AS region_code,
    start_date,
    end_date,
    officer_monthly_usage_report_active_users_supervision,
    impact_report_active_users_supervision,
FROM (
    SELECT
        state_code,
        start_date,
        end_date,
        COUNTIF(actions_taken_this_month > 0) AS officer_monthly_usage_report_active_users_supervision,
    FROM
        `{project_id}.{user_metrics_dataset}.officer_monthly_usage_report_materialized`
    GROUP BY
        1, 2, 3
) user_metrics
INNER JOIN (
    SELECT
        state_code,
        start_date,
        end_date,
        distinct_active_users_supervision AS impact_report_active_users_supervision,
    FROM
        `{project_id}.{impact_reports_dataset}.usage__justice_involved_state_aggregated_metrics_materialized`
    WHERE
        period = "MONTH"
) impact_reports
USING
    (state_code, start_date, end_date)
"""

OFFICER_MONTHLY_USAGE_REPORT_VS_IMPACT_REPORT_ACTIVE_USERS_SUPERVISION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=OFFICER_MONTHLY_USAGE_REPORT_VS_IMPACT_REPORT_ACTIVE_USERS_SUPERVISION_VIEW_NAME,
    view_query_template=OFFICER_MONTHLY_USAGE_REPORT_VS_IMPACT_REPORT_ACTIVE_USERS_SUPERVISION_QUERY_TEMPLATE,
    description=OFFICER_MONTHLY_USAGE_REPORT_VS_IMPACT_REPORT_ACTIVE_USERS_SUPERVISION_DESCRIPTION,
    impact_reports_dataset=state_dataset_config.IMPACT_REPORTS_DATASET_ID,
    user_metrics_dataset=state_dataset_config.USER_METRICS_DATASET_ID,
    projects_to_deploy={GCP_PROJECT_PRODUCTION},
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_PRODUCTION):
        OFFICER_MONTHLY_USAGE_REPORT_VS_IMPACT_REPORT_ACTIVE_USERS_SUPERVISION_VIEW_BUILDER.build_and_print()
