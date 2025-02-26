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
"""Reincarcerations by month."""
# pylint: disable=trailing-whitespace

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REINCARCERATIONS_BY_MONTH_VIEW_NAME = 'reincarcerations_by_month'

REINCARCERATIONS_BY_MONTH_DESCRIPTION = """ Reincarcerations by month """

REINCARCERATIONS_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code, year, month, district,
      IFNULL(ret.returns, 0) as returns,
      IFNULL(adm.total_admissions, 0) as total_admissions
    FROM (
      SELECT
        state_code, year, month,
        district,
        COUNT(person_id) as total_admissions
      FROM `{project_id}.{reference_views_dataset}.event_based_admissions`
      GROUP BY state_code, year, month, district
    ) adm
    LEFT JOIN (
      SELECT
        state_code, year, month,
        district,
        COUNT(person_id) AS returns
      FROM `{project_id}.{metrics_dataset}.recidivism_count_metrics`
      {filter_to_most_recent_job_id_for_metric},
      {district_dimension}
      WHERE methodology = 'PERSON'
        AND person_id IS NOT NULL
        AND metric_period_months = 1
        AND month IS NOT NULL
        AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL 3 YEAR))
      GROUP BY state_code, year, month, district
    ) ret
    USING (state_code, year, month, district)
    WHERE district IS NOT NULL
    ORDER BY state_code, year, month, district
    """

REINCARCERATIONS_BY_MONTH_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REINCARCERATIONS_BY_MONTH_VIEW_NAME,
    view_query_template=REINCARCERATIONS_BY_MONTH_QUERY_TEMPLATE,
    dimensions=['state_code', 'year', 'month', 'district'],
    description=REINCARCERATIONS_BY_MONTH_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    district_dimension=bq_utils.unnest_district(
        district_column='county_of_residence'),
    filter_to_most_recent_job_id_for_metric=bq_utils.filter_to_most_recent_job_id_for_metric(
        reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET)
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        REINCARCERATIONS_BY_MONTH_VIEW_BUILDER.build_and_print()
