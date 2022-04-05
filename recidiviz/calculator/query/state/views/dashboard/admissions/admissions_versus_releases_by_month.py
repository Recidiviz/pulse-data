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
"""Admissions minus releases (net change in incarcerated population)"""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_NAME = \
    'admissions_versus_releases_by_month'

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_DESCRIPTION = \
    """ Monthly admissions versus releases """

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code, year, month, district, 
      IFNULL(admission_count, 0) AS admission_count, 
      IFNULL(release_count, 0) AS release_count, 
      IFNULL(month_end_population, 0) AS month_end_population, 
      IFNULL(admission_count, 0) - IFNULL(release_count, 0) as population_change
    FROM (
      SELECT
        state_code, year, month, 
        district,
        COUNT(DISTINCT person_id) AS admission_count
      FROM `{project_id}.{reference_dataset}.event_based_admissions`
      GROUP BY state_code, year, month, district
    ) admissions
    FULL OUTER JOIN (
      SELECT
        state_code, year, month, 
        district,
        COUNT(DISTINCT person_id) AS release_count
      FROM `{project_id}.{metrics_dataset}.incarceration_release_metrics`
      JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months, metric_type),
      {district_dimension}
      WHERE methodology = 'EVENT'
        AND metric_period_months = 1
        AND person_id IS NOT NULL
        AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
      GROUP BY state_code, year, month, district
    ) releases
    USING (state_code, year, month, district)
    FULL OUTER JOIN (
      SELECT
        state_code,
        EXTRACT(YEAR FROM incarceration_month_end_date) AS year,
        EXTRACT(MONTH FROM incarceration_month_end_date) AS month,
        district,
        COUNT(DISTINCT person_id) AS month_end_population
      FROM `{project_id}.{metrics_dataset}.incarceration_population_metrics`,
        -- Convert the "month end" data in the incarceration_population_metrics to the "prior month end" by adding 1 month to the date
        UNNEST([DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH)]) AS incarceration_month_end_date
      JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months, metric_type),
      {district_dimension}
      WHERE methodology = 'PERSON'
        AND metric_period_months = 0
        AND person_id IS NOT NULL
        AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 4 YEAR))
        -- Get population count for the last day of the month
        AND date_of_stay = DATE_SUB(DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH), INTERVAL 1 DAY)
      GROUP BY state_code, year, month, district
    ) inc_pop
    USING (state_code, year, month, district)
    WHERE year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
      AND district IS NOT NULL
    ORDER BY state_code, district, year, month 
"""

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_NAME,
    view_query_template=ADMISSIONS_VERSUS_RELEASES_BY_MONTH_QUERY_TEMPLATE,
    description=ADMISSIONS_VERSUS_RELEASES_BY_MONTH_DESCRIPTION,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    district_dimension=bq_utils.unnest_district(
        district_column='county_of_residence')
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_BUILDER.build_and_print()
