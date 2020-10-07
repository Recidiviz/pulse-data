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
"""US_ID released community performance during COVID for the 2020 and 2019 cohorts"""
# pylint: disable=trailing-whitespace,line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import COVID_REPORT_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_RELEASED_COMMUNITY_PERFORMANCE_VIEW_NAME = 'us_id_released_community_performance'

US_ID_RELEASED_COMMUNITY_PERFORMANCE_DESCRIPTION = \
    """US_ID released community performance data per COVID report week"""

US_ID_RELEASED_COMMUNITY_PERFORMANCE_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH report_dates AS (
      -- Date ranges for each report
      SELECT
        week_num,
        2020 - year_val AS report_year,
        DATE_SUB(start_date, INTERVAL year_val YEAR) AS report_start_date,
        DATE_SUB(end_date, INTERVAL year_val YEAR) AS report_end_date,
        DATE(2020 - year_val, 3, 13) AS earliest_start_date
      FROM `{project_id}.{reference_dataset}.covid_report_weeks`,
      UNNEST([0, 1]) AS year_val
      WHERE week_num > 0
        AND state_code = 'US_ID'
    ),
    covid_release_cohorts AS (
      SELECT
        -- Separate the cohorts by release & incarceration reasons
        CASE WHEN m.purpose_for_incarceration = 'GENERAL' AND m.release_reason = 'SENTENCE_SERVED' THEN 'TERMERS'
             WHEN m.purpose_for_incarceration = 'TREATMENT_IN_PRISON' THEN 'RIDERS'
             WHEN m.purpose_for_incarceration = 'GENERAL' AND m.release_reason = 'CONDITIONAL_RELEASE' AND m.supervision_type_at_release = 'PAROLE' THEN 'PAROLE_RELEASED'
             ELSE 'other'
        END AS cohort,
        week_num, report_year, m.state_code, m.person_id, m.release_date AS start_date, NULL AS termination_date,
        ROW_NUMBER() OVER (PARTITION BY state_code, person_id, week_num, report_year, purpose_for_incarceration, release_reason ORDER BY release_date DESC) AS release_rank
      FROM `{project_id}.{metrics_dataset}.incarceration_release_metrics` m
      {filter_to_most_recent_job_id_for_metric}
      JOIN report_dates
        ON release_date BETWEEN earliest_start_date AND report_end_date
      WHERE state_code = 'US_ID'
        AND methodology = 'EVENT'
        AND metric_period_months = 1
        AND release_reason NOT IN ('RELEASED_FROM_TEMPORARY_CUSTODY', 'TRANSFER')
    ),
    most_recent_release AS (
      SELECT * EXCEPT (release_rank)
      FROM covid_release_cohorts
      WHERE release_rank = 1
    ),
    incarcerations AS (
      SELECT
        week_num, report_year, state_code, person_id,
        admission_date, incarceration_type
      FROM `{project_id}.{covid_report_dataset}.event_based_revocations_and_admissions` inc
      JOIN covid_release_cohorts
        USING (state_code, person_id)
      JOIN report_dates USING (week_num, report_year)
        -- Only count admissions that occured after the release date
      WHERE inc.admission_date > covid_release_cohorts.start_date
        AND inc.admission_date <= report_end_date
        AND state_code = 'US_ID'
    )
    SELECT
      week_num, report_year, c.state_code, c.cohort, c.person_id, c.start_date,
      DATE_DIFF(report_end_date, GREATEST(earliest_start_date, c.start_date), DAY) AS days_since_start,
      DATE_DIFF(i.admission_date, GREATEST(earliest_start_date, c.start_date), DAY) AS days_until_admission,
      DATE_DIFF(COALESCE(i.admission_date, report_end_date), GREATEST(earliest_start_date, c.start_date), DAY) AS survival_days,
      (i.admission_date IS NULL OR i.admission_date >= r.report_start_date) AS report_week_cohort,
      i.admission_date, i.incarceration_type
    FROM most_recent_release c
    LEFT JOIN incarcerations i
      USING (state_code, person_id, week_num, report_year)
    JOIN report_dates r USING (week_num, report_year)
    ORDER BY week_num, report_year, survival_days, days_until_admission DESC, start_date
"""


US_ID_RELEASED_COMMUNITY_PERFORMANCE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.COVID_REPORT_DATASET,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
    view_id=US_ID_RELEASED_COMMUNITY_PERFORMANCE_VIEW_NAME,
    view_query_template=US_ID_RELEASED_COMMUNITY_PERFORMANCE_QUERY_TEMPLATE,
    description=US_ID_RELEASED_COMMUNITY_PERFORMANCE_DESCRIPTION,
    covid_report_dataset=COVID_REPORT_DATASET,
    filter_to_most_recent_job_id_for_metric=bq_utils.filter_to_most_recent_job_id_for_metric(
        reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET)
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_RELEASED_COMMUNITY_PERFORMANCE_VIEW_BUILDER.build_and_print()
