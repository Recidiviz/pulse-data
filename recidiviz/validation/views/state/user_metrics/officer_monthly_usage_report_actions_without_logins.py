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
"""Flag cases when an officer_id has monthly actions taken without any monthly logins recorded"""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

OFFICER_MONTHLY_USAGE_REPORT_ACTIONS_WITHOUT_LOGINS_VIEW_NAME = (
    "officer_monthly_usage_report_actions_without_logins"
)

OFFICER_MONTHLY_USAGE_REPORT_ACTIONS_WITHOUT_LOGINS_DESCRIPTION = """Cases when an officer_id has monthly actions taken without any monthly logins recorded"""

OFFICER_MONTHLY_USAGE_REPORT_ACTIONS_WITHOUT_LOGINS_QUERY_TEMPLATE = """
    SELECT
        state_code,
        state_code as region_code,
        start_date,
        end_date,
        officer_id,
        total_logins_this_month,
        actions_taken_this_month,
    FROM
        `{project_id}.{usage_reports_dataset}.officer_monthly_usage_report_materialized`
    WHERE
        # Ignore any events before July 2024 since there are some older Recidiviz impersonation
        # usage events that are still incorrectly attributed to an officer
        start_date >= "2024-07-01"
        AND total_logins_this_month = 0
        AND actions_taken_this_month > 0
"""

OFFICER_MONTHLY_USAGE_REPORT_ACTIONS_WITHOUT_LOGINS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=OFFICER_MONTHLY_USAGE_REPORT_ACTIONS_WITHOUT_LOGINS_VIEW_NAME,
    view_query_template=OFFICER_MONTHLY_USAGE_REPORT_ACTIONS_WITHOUT_LOGINS_QUERY_TEMPLATE,
    description=OFFICER_MONTHLY_USAGE_REPORT_ACTIONS_WITHOUT_LOGINS_DESCRIPTION,
    usage_reports_dataset=state_dataset_config.USER_METRICS_DATASET_ID,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OFFICER_MONTHLY_USAGE_REPORT_ACTIONS_WITHOUT_LOGINS_VIEW_BUILDER.build_and_print()
