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

"""A view which ensures the month-over-month chart is accurately showing event-based revocation counts."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

REVOCATION_MATRIX_COMPARISON_BY_MONTH_VIEW_NAME = (
    "revocation_matrix_comparison_by_month"
)

REVOCATION_MATRIX_COMPARISON_BY_MONTH_DESCRIPTION = """ 
Revocation matrix comparison of summed revocation counts by month """

REVOCATION_MATRIX_COMPARISON_BY_MONTH_QUERY_TEMPLATE = """
    /*{description}*/
    WITH event_based_counts AS (
      SELECT state_code as region_code, year, month, COUNT(*) as total_revocations
      FROM `{project_id}.{reference_views_dataset}.event_based_revocations_for_matrix_materialized`
        WHERE revocation_admission_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Pacific'), MONTH), INTERVAL 35 MONTH)
      GROUP BY state_code, year, month
    ),
    month_counts AS (
      SELECT state_code as region_code, year, month, total_revocations
      FROM `{project_id}.{view_dataset}.revocations_matrix_events_by_month`
      WHERE DATE(year, month, 1) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Pacific'), MONTH),
                                                      INTERVAL 35 MONTH)
        AND admission_type = 'ALL' AND violation_type = 'ALL' AND reported_violations = 'ALL' 
        AND supervision_type = 'ALL' AND supervision_level = 'ALL' AND charge_category = 'ALL' 
        AND level_1_supervision_location = 'ALL' AND level_2_supervision_location = 'ALL'
      GROUP BY state_code, year, month, total_revocations
    )
    SELECT e.region_code, e.year, e.month, e.total_revocations as reference_sum, m.total_revocations as month_sum
    FROM event_based_counts e
    JOIN month_counts m USING(region_code, year, month)
"""

REVOCATION_MATRIX_COMPARISON_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=REVOCATION_MATRIX_COMPARISON_BY_MONTH_VIEW_NAME,
    view_query_template=REVOCATION_MATRIX_COMPARISON_BY_MONTH_QUERY_TEMPLATE,
    description=REVOCATION_MATRIX_COMPARISON_BY_MONTH_DESCRIPTION,
    view_dataset=state_dataset_config.DASHBOARD_VIEWS_DATASET,
    reference_views_dataset=state_dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATION_MATRIX_COMPARISON_BY_MONTH_VIEW_BUILDER.build_and_print()
