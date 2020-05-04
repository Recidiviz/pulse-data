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
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW_NAME = \
    'admissions_versus_releases_by_period'

ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_DESCRIPTION = \
    """Monthly admissions versus releases by metric month period."""

ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_QUERY = \
    """
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
      FROM `{project_id}.{reference_dataset}.event_based_admissions`,
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
      FROM `{project_id}.{metrics_dataset}.incarceration_release_metrics` m
      JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months),
      {district_dimension},
      {metric_period_dimension}
      WHERE methodology = 'EVENT'
        AND person_id IS NOT NULL
        AND m.metric_period_months = 1
        AND {metric_period_condition}
        AND job.metric_type = 'INCARCERATION_RELEASE'
      GROUP BY state_code, metric_period_months, district
    ) releases
    USING (state_code, district, metric_period_months)
    FULL OUTER JOIN (
      SELECT
        state_code,
        district,
        COUNT(DISTINCT person_id) AS month_end_population,
        metric_period_months
      FROM `{project_id}.{metrics_dataset}.incarceration_population_metrics` m
      JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months),
      {district_dimension},
      {metric_period_dimension}
      WHERE methodology = 'EVENT'
        AND person_id IS NOT NULL
        AND m.metric_period_months = 1
        AND {prior_month_metric_period_dimension}
        AND job.metric_type = 'INCARCERATION_POPULATION'
      GROUP BY state_code, district, metric_period_months
    ) inc_pop
    USING (state_code, district, metric_period_months)
    WHERE district IS NOT NULL
    ORDER BY state_code, metric_period_months, district
""".format(
        description=ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        reference_dataset=REFERENCE_DATASET,
        district_dimension=bq_utils.unnest_district(district_column='county_of_residence'),
        metric_period_dimension=bq_utils.unnest_metric_period_months(),
        metric_period_condition=bq_utils.metric_period_condition(month_offset=1),
        prior_month_metric_period_dimension=bq_utils.metric_period_condition(month_offset=0),
    )

ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW = BigQueryView(
    view_id=ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW_NAME,
    view_query=ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_QUERY
)

if __name__ == '__main__':
    print(ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW.view_id)
    print(ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW.view_query)
