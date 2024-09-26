#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
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
"""View that contains impact tracking metrics for the TN Compliant Reporting
Launch.

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.dashboard.impact.us_tn_compliant_reporting_launch.py
"""

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "us_tn_compliant_reporting_workflows_impact"

_VIEW_DESCRIPTION = "View that contains metrics for the TN Compliant Reporting Launch."

_QUERY_TEMPLATE = """
-- get variant assignment dates
WITH variant_assignments AS (
    SELECT
        state_code,
        variant_id,
        variant_date,
    FROM
        `{project_id}.experiments_metadata.experiment_assignments_materialized`
    WHERE
        experiment_id = "US_TN_COMPLIANT_REPORTING_WORKFLOWS"
        AND variant_id IN ("WORKFLOWS_LAUNCH")
),
joined_query AS (
    -- join aggregated metrics with variant assignments
    SELECT
        state_code,
        district AS supervision_district,
        district_name,
        variant_id,
        variant_date,
        start_date,
        end_date,
        -- since `start_date` is currently always the first of the month,
        -- month "0" is not always fully treated. By starting with person-level
        -- data we can ensure that `start_date` lines up perfectly with `variant_date`
        DATE_DIFF(start_date, variant_date, MONTH) AS months_since_treatment,
        avg_daily_population,
        avg_population_limited_supervision_level,
    FROM
        `{project_id}.aggregated_metrics.supervision_district_aggregated_metrics_materialized`
    INNER JOIN
        variant_assignments
    USING
        (state_code)
    WHERE
        state_code = "US_TN"
        AND period = "MONTH"
)
SELECT {columns} FROM joined_query;

"""

US_TN_COMPLIANT_REPORTING_WORKFLOWS_IMPACT_VIEW_BUILDER = (
    SelectedColumnsBigQueryViewBuilder(
        dataset_id=dataset_config.IMPACT_DASHBOARD_DATASET,
        view_id=_VIEW_NAME,
        view_query_template=_QUERY_TEMPLATE,
        columns=[
            "state_code",
            "supervision_district",
            "district_name",
            "variant_id",
            "variant_date",
            "start_date",
            "end_date",
            "months_since_treatment",
            "avg_daily_population",
            "avg_population_limited_supervision_level",
        ],
        description=_VIEW_DESCRIPTION,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_WORKFLOWS_IMPACT_VIEW_BUILDER.build_and_print()
