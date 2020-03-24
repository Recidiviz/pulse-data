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
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

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
        IFNULL(county_of_residence, 'ALL') AS district,
        SUM(count) as admission_count
      FROM `{project_id}.{metrics_dataset}.incarceration_admission_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
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
        AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
        AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
        AND job.metric_type = 'INCARCERATION_ADMISSION'
      GROUP BY state_code, metric_period_months, district
    ) admissions
    FULL OUTER JOIN (
      SELECT
        state_code,
        metric_period_months,
        IFNULL(county_of_residence, 'ALL') AS district,
        SUM(count) as release_count
      FROM `{project_id}.{metrics_dataset}.incarceration_release_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND release_reason IS NULL
        AND facility IS NULL
        AND age_bucket IS NULL
        AND race IS NULL
        AND ethnicity IS NULL
        AND gender IS NULL
        AND person_id IS NULL
        AND person_external_id IS NULL
        AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
        AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
        AND job.metric_type = 'INCARCERATION_RELEASE'
      GROUP BY state_code, metric_period_months, district
    ) releases
    USING (state_code, district, metric_period_months)
    FULL OUTER JOIN (
      SELECT
        state_code,
        IFNULL(county_of_residence, 'ALL') AS district,
        count AS month_end_population,
        metric_period_months
      FROM `{project_id}.{metrics_dataset}.incarceration_population_metrics`,
        UNNEST([1,3,6,12,36]) AS metric_period_months
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND facility IS NULL
        AND age_bucket IS NULL
        AND race IS NULL
        AND ethnicity IS NULL
        AND gender IS NULL
        AND person_id IS NULL
        AND person_external_id IS NULL
        AND most_serious_offense_ncic_code IS NULL
        AND most_serious_offense_statute IS NULL
        AND admission_reason IS NULL
        AND admission_reason_raw_text IS NULL
        AND supervision_type_at_admission IS NULL
        AND job.metric_type = 'INCARCERATION_POPULATION'
        AND year = EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL metric_period_months MONTH))
        AND month = EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL metric_period_months MONTH))
    ) inc_pop
    USING (state_code, district, metric_period_months)
    ORDER BY state_code, metric_period_months, district
""".format(
        description=ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        views_dataset=VIEWS_DATASET,
    )

ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW = bqview.BigQueryView(
    view_id=ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW_NAME,
    view_query=ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_QUERY
)

if __name__ == '__main__':
    print(ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW.view_id)
    print(ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW.view_query)
