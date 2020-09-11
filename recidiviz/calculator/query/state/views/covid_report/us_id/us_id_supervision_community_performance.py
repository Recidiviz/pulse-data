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
"""US_ID supervision community performance during COVID for the 2020 and 2019 cohorts"""
# pylint: disable=trailing-whitespace,line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import COVID_REPORT_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_SUPERVISION_COMMUNITY_PERFORMANCE_VIEW_NAME = 'us_id_supervision_community_performance'

US_ID_SUPERVISION_COMMUNITY_PERFORMANCE_DESCRIPTION = \
    """US_ID supervision community performance data per COVID report week"""

US_ID_SUPERVISION_COMMUNITY_PERFORMANCE_QUERY_TEMPLATE = \
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
    dataflow_supervision AS (
      -- Get the people on supervision during the report period, use the dataflow
      -- output to exclude anyone that was incarcerated while supervised
      SELECT DISTINCT
        week_num, report_year, state_code, person_id,
        supervision_type AS supervision_period_supervision_type
      FROM `{project_id}.{reference_dataset}.event_based_supervision_populations`
      JOIN report_dates
        -- Only take the supervision pop during the report time (excludes anyone who is incarcerated at the same time)
        ON month BETWEEN EXTRACT(MONTH FROM earliest_start_date) AND EXTRACT(MONTH FROM report_end_date)
        AND year BETWEEN EXTRACT(YEAR FROM earliest_start_date) AND EXTRACT(YEAR FROM report_end_date)
      WHERE state_code = 'US_ID'
        AND supervision_type IN ('PROBATION', 'PAROLE', 'DUAL')
    ),
    unique_person_supervision AS (
      -- Gather the supervision start & termination dates for everyone on supervision during the report period
      SELECT DISTINCT
        week_num, report_year, state_code, person_id,
        start_date, termination_date,
        -- Count DUAL supervision as PAROLE for the report
        CASE WHEN supervision_period_supervision_type = 'DUAL' THEN 'PAROLE' ELSE supervision_period_supervision_type END AS cohort,
        ROW_NUMBER() OVER(PARTITION BY week_num, report_year, state_code, person_id,
                          CASE WHEN supervision_period_supervision_type = 'DUAL' THEN 'PAROLE' ELSE supervision_period_supervision_type END
                          ORDER BY start_date DESC, termination_date DESC) AS supervision_rank
      FROM `{project_id}.{state_dataset}.state_supervision_period`
      -- Count people who have been on supervision during the report period
      JOIN report_dates
        ON start_date <= report_end_date
        AND COALESCE(termination_date, CURRENT_DATE()) >= earliest_start_date
      JOIN dataflow_supervision
        USING (state_code, person_id, supervision_period_supervision_type, week_num, report_year)
      WHERE external_id IS NOT NULL
        AND supervision_period_supervision_type IN ('PROBATION', 'PAROLE', 'DUAL')
    ),
    incarcerations AS (
      -- Select all incarcerations (new admissions and revocations) for those on supervision
      SELECT
        week_num, report_year, state_code, person_id,
        admission_date, incarceration_type
      FROM `{project_id}.{covid_report_dataset}.event_based_revocations_and_admissions` inc
      JOIN unique_person_supervision
        USING (state_code, person_id)
      JOIN report_dates
        USING (week_num, report_year)
      -- Only count admissions that occurred after the release date, during the Covid report period
      WHERE inc.admission_date > unique_person_supervision.start_date
        AND inc.admission_date >= earliest_start_date
        AND inc.admission_date <= report_end_date
        AND supervision_rank = 1
    )
    SELECT
      week_num, report_year, s.state_code, s.cohort, s.person_id,
      s.start_date, s.termination_date,
      i.admission_date, i.incarceration_type,
      DATE_DIFF(report_end_date, GREATEST(earliest_start_date, s.start_date), DAY) AS days_since_start,
      DATE_DIFF(i.admission_date, GREATEST(earliest_start_date, s.start_date), DAY) AS days_until_admission,
      DATE_DIFF(COALESCE(i.admission_date, report_end_date), GREATEST(earliest_start_date, s.start_date), DAY) AS survival_days,
      -- Indicate if this person should be counted as active during this report period
      CASE WHEN admission_date IS NOT NULL THEN admission_date >= report_start_date
           WHEN termination_date IS NOT NULL THEN termination_date >= report_start_date
           ELSE TRUE END AS report_week_cohort
    FROM unique_person_supervision s
    LEFT JOIN incarcerations i
      USING (state_code, person_id, week_num, report_year)
    JOIN report_dates r USING (week_num, report_year)
    WHERE supervision_rank = 1
"""


US_ID_SUPERVISION_COMMUNITY_PERFORMANCE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.COVID_REPORT_DATASET,
    reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
    view_id=US_ID_SUPERVISION_COMMUNITY_PERFORMANCE_VIEW_NAME,
    view_query_template=US_ID_SUPERVISION_COMMUNITY_PERFORMANCE_QUERY_TEMPLATE,
    description=US_ID_SUPERVISION_COMMUNITY_PERFORMANCE_DESCRIPTION,
    covid_report_dataset=COVID_REPORT_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_SUPERVISION_COMMUNITY_PERFORMANCE_VIEW_BUILDER.build_and_print()
