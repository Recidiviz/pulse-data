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
"""Admissions minus releases (net change in incarcerated population) by metric
period months.
"""
# pylint: disable=trailing-whitespace

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.dashboard.admissions.admissions_versus_releases_by_month import (
    ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW_NAME = "admissions_versus_releases_by_period"

ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_DESCRIPTION = (
    """Monthly admissions versus releases by metric month period."""
)

ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
      state_code,
      metric_period_months,
      district,
      IFNULL(admission_count, 0) AS admission_count,
      IFNULL(release_count, 0) AS release_count,
      IFNULL(inc_pop.month_end_population, 0) AS month_end_population,
      IFNULL(admission_count, 0) - IFNULL(release_count, 0) as population_change
    FROM (
      SELECT
        state_code, metric_period_months,
        district,
        COUNT(DISTINCT person_id) as admission_count
      FROM `{project_id}.{reference_views_dataset}.event_based_admissions`,
      {metric_period_dimension}
      WHERE {metric_period_condition}
      GROUP BY state_code, metric_period_months, district
    ) admissions
    FULL OUTER JOIN (
      SELECT
        state_code,
        metric_period_months,
        district,
        COUNT(DISTINCT person_id) as release_count
      FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_release_metrics_materialized` m,
      {district_dimension},
      {metric_period_dimension}
      WHERE {metric_period_condition}
      GROUP BY state_code, metric_period_months, district
    ) releases
    USING (state_code, district, metric_period_months)
    FULL OUTER JOIN (
      SELECT
        state_code,
        district,
        COUNT(DISTINCT person_id) AS month_end_population,
        metric_period_months
      FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_materialized` m,
      {district_dimension},
      {metric_period_dimension}
      WHERE 
         -- Get population count for first day of the period
        date_of_stay = DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Pacific'), MONTH), INTERVAL metric_period_months - 1 MONTH)
      GROUP BY state_code, district, metric_period_months
    ) inc_pop
    USING (state_code, district, metric_period_months)
    WHERE district IS NOT NULL
    ORDER BY state_code, metric_period_months, district
"""

ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW_NAME,
    view_query_template=ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_QUERY_TEMPLATE,
    dimensions=["state_code", "metric_period_months", "district"],
    description=ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    district_dimension=bq_utils.unnest_district(district_column="county_of_residence"),
    metric_period_dimension=bq_utils.unnest_metric_period_months(),
    metric_period_condition=bq_utils.metric_period_condition(month_offset=1),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_BUILDER.build_and_print()
