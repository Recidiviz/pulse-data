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
"""US_ND Admissions to CPP (Community Placement Program) by week"""
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ADMISSIONS_TO_CPP_BY_WEEK_VIEW_NAME = "admissions_to_cpp_by_week"

ADMISSIONS_TO_CPP_BY_WEEK_DESCRIPTION = (
    """US_ND Admissions to CPP (Community Placement Program) by week"""
)

ADMISSIONS_TO_CPP_BY_WEEK_QUERY_TEMPLATE = """
    /*{description}*/
      -- Incarceration in US_ND state prison to be used for COVID analysis --
      WITH us_nd_state_incarceration_period AS
        (SELECT * EXCEPT (rownum)
        FROM
          (SELECT
            *,
            -- De-duplicating by person_id, admission_date and facility
            row_number() OVER (PARTITION BY person_id, admission_date, facility ORDER BY release_date ASC, incarceration_period_id DESC) as rownum
          FROM
            `{project_id}.{base_dataset}.state_incarceration_period`
          WHERE incarceration_type = 'STATE_PRISON'
          AND state_code = 'US_ND'
          AND (admission_reason IS NULL OR admission_reason != 'TEMPORARY_CUSTODY'))
        WHERE rownum = 1
        AND person_id NOT IN
        -- These are not actually people. They are related to the PREA (Prison Rape Elimination Act) hotline for the given facility. -- 
        (SELECT person_id FROM
        (SELECT person_id, COUNTIF(admission_reason_raw_text = 'PREA') as prea_count
        FROM `{project_id}.{base_dataset}.state_incarceration_period` 
        WHERE state_code = 'US_ND'
        GROUP BY person_id)
        WHERE prea_count > 0))
    
    
    SELECT
      *,
      IFNULL(admission_count - LAG(admission_count) OVER (PARTITION BY state_code ORDER BY week_num), 0) as admission_count_diff
    FROM
      (SELECT
        state_code,
        week_num,
        start_date,
        end_date,
        COUNT(DISTINCT(person_id)) as admission_count
      FROM
        (SELECT
          *
        FROM
          (SELECT
            report.state_code,
            week_num,
            start_date,
            end_date,
            person_id,
            admission_date,
            row_number() OVER (PARTITION BY person_id ORDER BY admission_date ASC) as admission_num
          FROM
            `{project_id}.{reference_views_dataset}.covid_report_weeks` report
          LEFT JOIN
          (SELECT
            state_code,
            person_id,
            admission_date
            FROM
              us_nd_state_incarceration_period
            WHERE facility = 'CPP'
          ) cpp
        ON report.state_code = cpp.state_code AND admission_date BETWEEN start_date AND end_date)
        WHERE admission_num = 1)
      GROUP BY state_code, week_num, start_date, end_date)
    ORDER BY state_code, week_num
"""

ADMISSIONS_TO_CPP_BY_WEEK_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.COVID_REPORT_DATASET,
    view_id=ADMISSIONS_TO_CPP_BY_WEEK_VIEW_NAME,
    view_query_template=ADMISSIONS_TO_CPP_BY_WEEK_QUERY_TEMPLATE,
    description=ADMISSIONS_TO_CPP_BY_WEEK_DESCRIPTION,
    base_dataset=dataset_config.STATE_BASE_DATASET,
    covid_report_dataset=dataset_config.COVID_REPORT_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ADMISSIONS_TO_CPP_BY_WEEK_VIEW_BUILDER.build_and_print()
