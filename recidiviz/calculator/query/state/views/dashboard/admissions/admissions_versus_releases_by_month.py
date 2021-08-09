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

from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_NAME = "admissions_versus_releases_by_month"

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_DESCRIPTION = (
    """ Monthly admissions versus releases """
)

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_QUERY_TEMPLATE = """
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
      FROM `{project_id}.{reference_views_dataset}.event_based_admissions`
      GROUP BY state_code, year, month, district
    ) admissions
    FULL OUTER JOIN (
      SELECT
        state_code, year, month, 
        district,
        COUNT(DISTINCT person_id) AS release_count
      FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_release_metrics_materialized`,
      {district_dimension}
      WHERE {thirty_six_month_filter}
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
      FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_materialized`,
        -- Convert the "month end" data in the incarceration_population_metrics to the "prior month end" by adding 1 month to the date
        UNNEST([DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH)]) AS incarceration_month_end_date,
      {district_dimension}
      WHERE year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 4 YEAR))
        -- Get population count for the last day of the month
        AND date_of_stay = LAST_DAY(DATE(year, month, 1), MONTH)
      GROUP BY state_code, year, month, district
    ) inc_pop
    USING (state_code, year, month, district)
    WHERE {thirty_six_month_filter}
      AND district IS NOT NULL
    ORDER BY state_code, district, year, month 
"""

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_NAME,
    view_query_template=ADMISSIONS_VERSUS_RELEASES_BY_MONTH_QUERY_TEMPLATE,
    dimensions=("state_code", "year", "month", "district"),
    description=ADMISSIONS_VERSUS_RELEASES_BY_MONTH_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    district_dimension=bq_utils.unnest_district(district_column="county_of_residence"),
    thirty_six_month_filter=bq_utils.thirty_six_month_filter(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_BUILDER.build_and_print()
