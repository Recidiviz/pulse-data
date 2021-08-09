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

"""A view which provides a comparison of total revocation counts summed across all dimensional breakdowns
in all of the views that support breakdowns of revocations by officer."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

REVOCATION_MATRIX_COMPARISON_REVOCATIONS_BY_OFFICER_VIEW_NAME = (
    "revocation_matrix_comparison_revocations_by_officer"
)

REVOCATION_MATRIX_COMPARISON_REVOCATIONS_BY_OFFICER_DESCRIPTION = """
Revocation matrix comparison of summed revocation counts by officer """

REVOCATION_MATRIX_COMPARISON_REVOCATIONS_BY_OFFICER_QUERY_TEMPLATE = """
    /*{description}*/
    WITH by_officer as (
      SELECT
        state_code as region_code, metric_period_months, level_1_supervision_location, level_2_supervision_location,
        officer, SUM(revocation_count) as total_revocations
      FROM `{project_id}.{view_dataset}.revocations_matrix_distribution_by_officer`
      WHERE supervision_level = 'ALL'
        AND supervision_type = 'ALL'
        AND charge_category = 'ALL'
        AND violation_type = 'ALL'
        AND reported_violations = 'ALL'
        AND admission_type = 'ALL'
      GROUP BY state_code, metric_period_months, level_1_supervision_location, level_2_supervision_location, officer
    ), caseload_counts AS (
      SELECT
        state_code as region_code, metric_period_months,
        level_1_supervision_location, level_2_supervision_location, officer,
        COUNT(DISTINCT state_id) as total_revocations
      FROM `{project_id}.{view_dataset}.revocations_matrix_filtered_caseload`
      GROUP BY state_code, metric_period_months, level_1_supervision_location, level_2_supervision_location, officer
    )
    
   SELECT
      region_code,
      metric_period_months,
      level_1_supervision_location, level_2_supervision_location,
      officer,
      bo.total_revocations as officer_sum,
      c.total_revocations as caseload_sum
    FROM by_officer bo
    JOIN caseload_counts c
    USING (region_code, metric_period_months, level_1_supervision_location, level_2_supervision_location, officer)
"""

REVOCATION_MATRIX_COMPARISON_REVOCATIONS_BY_OFFICER_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=REVOCATION_MATRIX_COMPARISON_REVOCATIONS_BY_OFFICER_VIEW_NAME,
    view_query_template=REVOCATION_MATRIX_COMPARISON_REVOCATIONS_BY_OFFICER_QUERY_TEMPLATE,
    description=REVOCATION_MATRIX_COMPARISON_REVOCATIONS_BY_OFFICER_DESCRIPTION,
    view_dataset=state_dataset_config.DASHBOARD_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATION_MATRIX_COMPARISON_REVOCATIONS_BY_OFFICER_VIEW_BUILDER.build_and_print()
