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
"""Rates of successful supervision completion by month."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_SUCCESS_BY_MONTH_VIEW_NAME = 'supervision_success_by_month'

SUPERVISION_SUCCESS_BY_MONTH_VIEW_DESCRIPTION = """Rates of successful supervision completion by month."""

SUPERVISION_SUCCESS_BY_MONTH_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH success_classifications AS (
      SELECT 
        state_code,
        year as projected_year,
        month as projected_month,
        supervision_type,
        IFNULL(district, 'EXTERNAL_UNKNOWN') as district,
        -- Only count as success if all completed periods were successful per person
        -- Take the MIN so that successful_termination is 1 only if all periods were 1 (successful)
        MIN(successful_completion_count) as successful_termination,
        person_id,
      FROM `{project_id}.{metrics_dataset}.supervision_success_metrics`
      JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months, metric_type),
      UNNEST ([{grouped_districts}, 'ALL']) AS district
      WHERE methodology = 'EVENT'
        AND metric_period_months = 1
        AND person_id IS NOT NULL
        AND month IS NOT NULL
        AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
      GROUP BY state_code, projected_year, projected_month, district, supervision_type, person_id
    ), success_counts AS (
      SELECT
        state_code,
        projected_year,
        projected_month,
        district,
        supervision_type,
        COUNT(DISTINCT IF(successful_termination = 1, person_id, NULL)) AS successful_termination_count,
        COUNT(DISTINCT(person_id)) AS projected_completion_count
      FROM success_classifications
      GROUP BY state_code, projected_year, projected_month, district, supervision_type
    )
    
    
    SELECT
        *,
        IEEE_DIVIDE(successful_termination_count, projected_completion_count) as success_rate
    FROM success_counts
    ORDER BY state_code, projected_year, projected_month, supervision_type
    """

SUPERVISION_SUCCESS_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_SUCCESS_BY_MONTH_VIEW_NAME,
    view_query_template=SUPERVISION_SUCCESS_BY_MONTH_VIEW_QUERY_TEMPLATE,
    description=SUPERVISION_SUCCESS_BY_MONTH_VIEW_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    grouped_districts=bq_utils.supervision_specific_district_groupings('supervising_district_external_id',
                                                                       'judicial_district_code'),
    district_dimension=bq_utils.unnest_district()
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        SUPERVISION_SUCCESS_BY_MONTH_VIEW_BUILDER.build_and_print()
