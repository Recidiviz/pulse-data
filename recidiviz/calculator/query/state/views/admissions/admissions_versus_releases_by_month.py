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
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_NAME = \
    'admissions_versus_releases_by_month'

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_DESCRIPTION = \
    """ Monthly admissions versus releases """

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_QUERY = \
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
        IFNULL(county_of_residence, 'ALL') AS district,
        SUM(count) AS admission_count
      FROM `{project_id}.{metrics_dataset}.incarceration_admission_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'EVENT'
        AND metric_period_months = 1
        AND facility IS NULL
        AND age_bucket IS NULL
        AND race IS NULL
        AND ethnicity IS NULL
        AND gender IS NULL
        AND person_id IS NULL
        AND person_external_id IS NULL
        AND specialized_purpose_for_incarceration IS NULL
        AND admission_reason IS NULL
        AND admission_reason_raw_text IS NULL
        AND admission_date IS NULL
        AND supervision_type_at_admission IS NULL
        AND year >= EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))
        AND job.metric_type = 'INCARCERATION_ADMISSION'
      GROUP BY state_code, year, month, county_of_residence
    ) admissions
    FULL OUTER JOIN (
      SELECT
        state_code, year, month, 
        IFNULL(county_of_residence, 'ALL') AS district,
        SUM(count) AS release_count
      FROM `{project_id}.{metrics_dataset}.incarceration_release_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'EVENT'
        AND metric_period_months = 1
        AND release_reason IS NULL
        AND facility IS NULL
        AND age_bucket IS NULL
        AND race IS NULL
        AND ethnicity IS NULL
        AND gender IS NULL
        AND person_id IS NULL
        AND person_external_id IS NULL
        AND year >= EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))
        AND job.metric_type = 'INCARCERATION_RELEASE'
      GROUP BY state_code, year, month, county_of_residence
    ) releases
    USING (state_code, year, month, district)
    FULL OUTER JOIN (
      SELECT
        state_code,
        EXTRACT(YEAR FROM incarceration_month_end_date) AS year,
        EXTRACT(MONTH FROM incarceration_month_end_date) AS month,
        IFNULL(county_of_residence, 'ALL') AS district,
        count AS month_end_population
      FROM `{project_id}.{metrics_dataset}.incarceration_population_metrics`,
        -- Convert the "month end" data in the incarceration_population_metrics to the "prior month end" by adding 1 month to the date
        UNNEST([DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH)]) AS incarceration_month_end_date
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND metric_period_months = 1
        AND facility IS NULL
        AND age_bucket IS NULL
        AND race IS NULL
        AND ethnicity IS NULL
        AND gender IS NULL
        AND person_id IS NULL
        AND person_external_id IS NULL
        AND most_serious_offense_statute IS NULL
        AND admission_reason IS NULL
        AND admission_reason_raw_text IS NULL
        AND supervision_type_at_admission IS NULL
        AND year >= EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL - 4 YEAR))
        AND job.metric_type = 'INCARCERATION_POPULATION'
    ) inc_pop
    USING (state_code, year, month, district)
    WHERE year >= EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL - 3 YEAR))
    ORDER BY state_code, district, year, month 
""".format(
        description=ADMISSIONS_VERSUS_RELEASES_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        views_dataset=VIEWS_DATASET,
        metrics_dataset=METRICS_DATASET,
    )

ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW_NAME,
    view_query=ADMISSIONS_VERSUS_RELEASES_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW.view_id)
    print(ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW.view_query)
