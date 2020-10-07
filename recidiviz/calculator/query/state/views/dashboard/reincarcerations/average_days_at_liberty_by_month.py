# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
#
# This program is free software: you can redistribute it AND/or modify
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
"""Average days at liberty for reincarcerations by month."""
# pylint: disable=trailing-whitespace
from recidiviz.calculator.query import bq_utils
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW_NAME = 'avg_days_at_liberty_by_month'

AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_DESCRIPTION = \
    """Average days at liberty for reincarcerations by month """

AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code, year, month,
      COUNT(DISTINCT person_id) AS returns,
      AVG(days_at_liberty) AS avg_liberty
    FROM `{project_id}.{metrics_dataset}.recidivism_count_metrics`
    {filter_to_most_recent_job_id_for_metric}
    WHERE methodology = 'PERSON'
      AND person_id IS NOT NULL
      AND metric_period_months = 1
      AND month IS NOT NULL
      AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
      -- TODO(#3123): enforce positive days at liberty earlier in the pipeline
      AND days_at_liberty >= 0
    GROUP BY state_code, year, month
    ORDER BY state_code, year, month
    """
AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW_NAME,
    view_query_template=AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_QUERY_TEMPLATE,
    dimensions=['state_code', 'year', 'month'],
    description=AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    filter_to_most_recent_job_id_for_metric=bq_utils.filter_to_most_recent_job_id_for_metric(
        reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET)
)


if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        AVERAGE_DAYS_AT_LIBERTY_BY_MONTH_VIEW_BUILDER.build_and_print()
