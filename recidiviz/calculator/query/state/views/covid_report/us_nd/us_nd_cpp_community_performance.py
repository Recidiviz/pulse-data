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
"""US_ND CPP community performance during COVID for the 2020 and 2019 cohorts"""
# pylint: disable=trailing-whitespace,line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import COVID_REPORT_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_CPP_COMMUNITY_PERFORMANCE_VIEW_NAME = 'us_nd_cpp_community_performance'

US_ND_CPP_COMMUNITY_PERFORMANCE_DESCRIPTION = \
    """US_ND CPP community performance data per COVID report week"""

US_ND_CPP_COMMUNITY_PERFORMANCE_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH report_dates AS (
      SELECT
        week_num,
        start_date AS report_start_date,
        end_date AS report_end_date,
        DATE(2020, 3, 19) AS earliest_start_date
      FROM `{project_id}.{reference_dataset}.covid_report_weeks`
      WHERE week_num > 0
        AND state_code = 'US_ND'
    ),
    reincarcerations AS (
      -- Count all incarceration admissions including temporary holds but excluding admissions to CPP
      SELECT DISTINCT
        week_num, state_code, person_id, admission_date,
        DATE_DIFF(report_end_date, admission_date, DAY) AS days_since_admission,
        incarceration_type,
        -- Take the first admission date per person
        ROW_NUMBER() OVER (PARTITION BY state_code, person_id, week_num ORDER BY admission_date) AS admission_rank
      FROM `{project_id}.{state_dataset}.state_incarceration_period` ip
      JOIN report_dates
      ON ip.admission_date BETWEEN earliest_start_date AND report_end_date
        AND facility != 'CPP'
    ),
    first_reincarceration AS (
      -- Pick 1 row per person/report
      SELECT week_num, state_code, person_id, admission_date, days_since_admission, incarceration_type
      FROM reincarcerations
      WHERE admission_rank = 1
    ),
    total_cases AS (
      -- Combine all CPP records from supervision and incarceration periods
      SELECT
        week_num, state_code, person_id, external_id,
        start_date, termination_date,
        'SUPERVISION' AS cpp_type
      FROM `{project_id}.{state_dataset}.state_supervision_period`
      -- Only pick people released specifically for COVID CPP
      JOIN `{project_id}.{covid_report_dataset}.us_nd_covid_special_releases` covid_releases
        USING (state_code, person_id)
      JOIN report_dates
        ON start_date BETWEEN earliest_start_date AND report_end_date
      WHERE supervision_type_raw_text = 'COMMUNITY PLACEMENT PGRM'

      UNION ALL

      SELECT
        week_num, state_code, person_id, external_id,
        admission_date AS start_date, release_date AS termination_date,
        'INCARCERATION' AS cpp_type
      FROM `{project_id}.{state_dataset}.state_incarceration_period`
      -- Only pick people released specifically for COVID CPP
      JOIN `{project_id}.{covid_report_dataset}.us_nd_covid_special_releases` covid_releases
        USING (state_code, person_id)
      JOIN report_dates
        ON ((admission_date BETWEEN earliest_start_date AND report_end_date)
            OR (release_date BETWEEN earliest_start_date AND report_end_date))
      WHERE facility = 'CPP'
    ),
    first_case_per_person AS (
      -- Select 1 row per person/report
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY state_code, person_id, week_num ORDER BY start_date, cpp_type, termination_date DESC, external_id) AS case_rank
      FROM total_cases
    )
    SELECT
      week_num, state_code, person_id, start_date, termination_date, cpp_type,
      -- Indicate if this person should be counted as active during this report period
      CASE WHEN admission_date IS NOT NULL THEN admission_date >= report_start_date
           WHEN termination_date IS NOT NULL THEN termination_date >= report_start_date
           ELSE TRUE END AS report_week_cohort,
      DATE_DIFF(COALESCE(admission_date, report_end_date), start_date, DAY) AS survival_days,
      DATE_DIFF(report_end_date, start_date, DAY) AS days_since_start,
      DATE_DIFF(admission_date, start_date, DAY) AS days_until_admission,
      admission_date IS NOT NULL AS event_flag,
      admission_date,
      incarceration_type
    FROM first_case_per_person
    LEFT JOIN first_reincarceration
      USING (state_code, person_id, week_num)
    LEFT JOIN report_dates USING (week_num)
    WHERE case_rank = 1
      AND (start_date < admission_date OR admission_date IS NULL)
    ORDER BY week_num, survival_days, days_until_admission DESC, start_date
"""


US_ND_CPP_COMMUNITY_PERFORMANCE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.COVID_REPORT_DATASET,
    reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
    view_id=US_ND_CPP_COMMUNITY_PERFORMANCE_VIEW_NAME,
    view_query_template=US_ND_CPP_COMMUNITY_PERFORMANCE_QUERY_TEMPLATE,
    description=US_ND_CPP_COMMUNITY_PERFORMANCE_DESCRIPTION,
    covid_report_dataset=COVID_REPORT_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_CPP_COMMUNITY_PERFORMANCE_VIEW_BUILDER.build_and_print()
