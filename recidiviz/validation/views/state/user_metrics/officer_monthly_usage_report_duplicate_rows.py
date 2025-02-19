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
"""Flag cases when an officer_id is duplicated across a single report_date within officer_monthly_usage_report"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

OFFICER_MONTHLY_USAGE_REPORT_DUPLICATE_ROWS_VIEW_NAME = (
    "officer_monthly_usage_report_duplicate_rows"
)

OFFICER_MONTHLY_USAGE_REPORT_DUPLICATE_ROWS_DESCRIPTION = """Duplicate officer emails found in officer_monthly_usage_report for one report_date"""

OFFICER_MONTHLY_USAGE_REPORT_DUPLICATE_ROWS_QUERY_TEMPLATE = """
    SELECT
        state_code,
        state_code as region_code,
        start_date,
        end_date,
        officer_email,
        COUNT(*) AS report_rows,
    FROM
        `{project_id}.{usage_reports_dataset}.officer_monthly_usage_report_materialized`
    GROUP BY
        1, 2, 3, 4, 5
    HAVING
        report_rows > 1
"""

OFFICER_MONTHLY_USAGE_REPORT_DUPLICATE_ROWS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=OFFICER_MONTHLY_USAGE_REPORT_DUPLICATE_ROWS_VIEW_NAME,
    view_query_template=OFFICER_MONTHLY_USAGE_REPORT_DUPLICATE_ROWS_QUERY_TEMPLATE,
    description=OFFICER_MONTHLY_USAGE_REPORT_DUPLICATE_ROWS_DESCRIPTION,
    usage_reports_dataset=state_dataset_config.USER_METRICS_DATASET_ID,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OFFICER_MONTHLY_USAGE_REPORT_DUPLICATE_ROWS_VIEW_BUILDER.build_and_print()
