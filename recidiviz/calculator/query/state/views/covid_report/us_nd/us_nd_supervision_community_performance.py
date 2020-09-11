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
"""US_ND supervision community performance during COVID for the 2020 and 2019 cohorts"""
# pylint: disable=trailing-whitespace,line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import COVID_REPORT_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_SUPERVISION_COMMUNITY_PERFORMANCE_VIEW_NAME = 'us_nd_supervision_community_performance'

US_ND_SUPERVISION_COMMUNITY_PERFORMANCE_DESCRIPTION = \
    """US_ND supervision community performance data per COVID report week"""

US_ND_SUPERVISION_COMMUNITY_PERFORMANCE_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH report_dates AS (
      -- Date ranges for each report
      SELECT
        week_num,
        2020 - year_val AS report_year,
        DATE_SUB(start_date, INTERVAL year_val YEAR) AS report_start_date,
        DATE_SUB(end_date, INTERVAL year_val YEAR) AS report_end_date,
        DATE(2020 - year_val, 3, 19) AS earliest_start_date
      FROM `{project_id}.{reference_dataset}.covid_report_weeks`,
      UNNEST([0, 1]) AS year_val
      WHERE week_num > 0
        AND state_code = 'US_ND'
    ),
    reincarcerations AS (
      -- Gather all incarceration admissions during each report period
      SELECT DISTINCT
        week_num, report_year,
        state_code, person_id,
        admission_date
      FROM `{project_id}.{reference_dataset}.event_based_admissions`
      JOIN report_dates
        ON admission_date BETWEEN earliest_start_date AND report_end_date
      WHERE admission_reason IN ('NEW_ADMISSION', 'PAROLE_REVOCATION', 'PROBATION_REVOCATION')
    ),
    supervision_periods AS (
      -- Get the people on supervision during the report period, use the dataflow
      -- output to exclude anyone that was incarcerated while supervised
      SELECT DISTINCT
        week_num, report_year, state_code, person_id, supervision_type
      FROM `{project_id}.{reference_dataset}.event_based_supervision_populations`
      JOIN report_dates
        -- Only take the supervision pop during the report time
        ON month BETWEEN EXTRACT(MONTH FROM earliest_start_date) AND EXTRACT(MONTH FROM report_end_date)
        AND year BETWEEN EXTRACT(YEAR FROM earliest_start_date) AND EXTRACT(YEAR FROM report_end_date)
      WHERE supervision_type != 'ALL'
    ),
    unique_person_supervision AS (
      -- Select all supervision periods (start & termination dates) that were open during the report period
      SELECT DISTINCT
          week_num, report_year, state_code, person_id, start_date, termination_date, supervision_type,
          ROW_NUMBER() OVER(PARTITION BY week_num, report_year, state_code, person_id, supervision_type ORDER BY start_date DESC, termination_date DESC) AS supervision_rank
      FROM `{project_id}.{state_dataset}.state_supervision_period` rsp
      JOIN supervision_periods dfsp
        USING (state_code, person_id, supervision_type)
      JOIN report_dates
        USING (week_num, report_year)
      WHERE start_date <= report_end_date
        AND (termination_date IS NULL OR termination_date >= earliest_start_date)
        AND external_id IS NOT NULL
    )
    SELECT
      week_num, report_year, state_code, person_id, supervision_type,
      -- Indicate if the supervision was started for COVID reasons in 2020
      (report_year = 2020 AND covid_releases.person_id IS NOT NULL) AS covid_flag,
      -- Indicate if this person should be counted as active during this report period
      CASE WHEN admission_date IS NOT NULL THEN admission_date >= report_start_date
           WHEN termination_date IS NOT NULL THEN termination_date >= report_start_date
           ELSE TRUE END AS report_week_cohort,
      start_date, termination_date,
      DATE_DIFF(COALESCE(admission_date, report_end_date), GREATEST(earliest_start_date, start_date), DAY) AS survival_days,
      DATE_DIFF(report_end_date, GREATEST(earliest_start_date, start_date), DAY) AS days_since_start,
      DATE_DIFF(admission_date, GREATEST(earliest_start_date, start_date), DAY) AS days_until_admission,
      admission_date IS NOT NULL AS event_flag,
      admission_date
    FROM unique_person_supervision
    JOIN report_dates USING (week_num, report_year)
    LEFT JOIN `{project_id}.{covid_report_dataset}.us_nd_covid_special_releases` covid_releases
      USING (state_code, person_id)
    LEFT JOIN reincarcerations USING (state_code, person_id, week_num, report_year)
    -- Only count 1 row per person/report/supervision type
    WHERE supervision_rank = 1
        -- Only count admissions after the supervision start
        AND (reincarcerations.admission_date > start_date OR reincarcerations.admission_date IS NULL)
    ORDER BY week_num, report_year, survival_days, days_until_admission DESC, start_date
"""


US_ND_SUPERVISION_COMMUNITY_PERFORMANCE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.COVID_REPORT_DATASET,
    reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
    view_id=US_ND_SUPERVISION_COMMUNITY_PERFORMANCE_VIEW_NAME,
    view_query_template=US_ND_SUPERVISION_COMMUNITY_PERFORMANCE_QUERY_TEMPLATE,
    description=US_ND_SUPERVISION_COMMUNITY_PERFORMANCE_DESCRIPTION,
    covid_report_dataset=COVID_REPORT_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_SUPERVISION_COMMUNITY_PERFORMANCE_VIEW_BUILDER.build_and_print()
