# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Successful and unsuccessful terminations of supervision by month."""
# pylint: disable=trailing-whitespace

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW_NAME = 'supervision_termination_by_type_by_month'

SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_DESCRIPTION = """
 Supervision termination by type and by month.
 The counts of supervision that were projected to end in a given month and
 that have ended by now, broken down by whether or not the
 supervision ended because of a revocation or successful completion.
"""

SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
        state_code, projected_year, projected_month,
        COUNTIF(successful_termination) AS successful_termination,
        COUNTIF(NOT(successful_termination)) AS revocation_termination,
        supervision_type,
        IFNULL(district, 'EXTERNAL_UNKNOWN') as district
    FROM (
      SELECT 
        state_code, year as projected_year, month as projected_month,
        -- Only count as success if all completed periods were successful per person
        -- successful_termination is True only if all periods were successfully completed
        LOGICAL_AND(successful_completion) as successful_termination,
        supervision_type,
        district
      FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_success_metrics_materialized`,
      {district_dimension},
      {supervision_type_dimension}
      WHERE methodology = 'EVENT'
        AND metric_period_months = 1
        AND person_id IS NOT NULL
        AND month IS NOT NULL
        AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
      GROUP BY state_code, year, month, supervision_type, district, person_id
    )
    WHERE supervision_type in ('ALL', 'PAROLE', 'PROBATION')
    GROUP BY state_code, projected_year, projected_month, supervision_type, district
    ORDER BY state_code, projected_year, projected_month, district, supervision_type
    """

SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW_NAME,
    view_query_template=SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_QUERY_TEMPLATE,
    dimensions=['state_code', 'projected_year', 'projected_month', 'supervision_type', 'district'],
    description=SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    district_dimension=bq_utils.unnest_district(),
    supervision_type_dimension=bq_utils.unnest_supervision_type(),
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW_BUILDER.build_and_print()
