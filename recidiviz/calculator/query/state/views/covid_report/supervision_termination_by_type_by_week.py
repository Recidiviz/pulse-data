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
"""Supervision terminations by week, with subset of discharge terminations"""
# pylint: disable=trailing-whitespace,line-too-long
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET, COVID_REPORT_DATASET, \
    REFERENCE_TABLES_DATASET

SUPERVISION_TERMINATIONS_BY_TYPE_BY_WEEK_VIEW_NAME = 'supervision_terminations_by_type_by_week'

SUPERVISION_TERMINATIONS_BY_TYPE_BY_WEEK_DESCRIPTION = \
    """ Supervision terminations by week, with subset of discharge terminations """

SUPERVISION_TERMINATIONS_BY_TYPE_BY_WEEK_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH supervision_periods_for_report AS (
      SELECT * FROM `{project_id}.{base_dataset}.state_supervision_period` 
      WHERE (state_code != 'US_ID' OR supervision_period_supervision_type NOT IN ('INVESTIGATION', 'INFORMAL_PROBATION'))
     ),
     supervision_terminations AS (
      SELECT
        state_code, person_id, start_date, termination_date,
        supervision_period_id, termination_reason
      FROM supervision_periods_for_report p1
      WHERE termination_date IS NOT NULL
    ),
    overlapping_open_period AS (
      -- Find any overlapping supervision period (started on or before the termination, ended after the termination)
      SELECT
        p1.supervision_period_id
      FROM supervision_terminations p1
      JOIN supervision_periods_for_report p2
        USING (state_code, person_id)
      WHERE p1.supervision_period_id != p2.supervision_period_id
        -- Find any overlapping supervision period (started on or before the termination, ended on or after the termination)
        AND p2.start_date <= p1.termination_date
        AND p1.termination_date < COALESCE(p2.termination_date, CURRENT_DATE())
    )

    SELECT
      state_code,
      week_num,
      start_date,
      end_date,
      termination_count,
      IFNULL(termination_count - LAG(termination_count) OVER (PARTITION BY state_code ORDER BY week_num), 0) as termination_count_diff,
      discharge_count,
      IFNULL(discharge_count - LAG(discharge_count) OVER (PARTITION BY state_code ORDER BY week_num), 0) as discharge_count_diff,
    FROM
      (SELECT
        report.state_code,
        week_num,
        start_date,
        end_date,
        COUNT(DISTINCT(person_id)) as termination_count,
        COUNT(DISTINCT IF(termination_reason = 'DISCHARGE', person_id, NULL)) as discharge_count,
      FROM
        `{project_id}.{reference_dataset}.covid_report_weeks` report
      LEFT JOIN
        (SELECT
          state_code,
          termination_date,
          termination_reason,
          person_id
        FROM supervision_terminations
        LEFT JOIN overlapping_open_period USING (supervision_period_id)
          -- Do not count any discharges that are overlapping with another open supervision period
          WHERE overlapping_open_period.supervision_period_id IS NULL) terminations
      ON report.state_code = terminations.state_code AND termination_date BETWEEN start_date AND end_date
      GROUP BY state_code, week_num, start_date, end_date)
    ORDER BY state_code, week_num
"""

SUPERVISION_TERMINATIONS_BY_TYPE_BY_WEEK_VIEW = BigQueryView(
    dataset_id=dataset_config.COVID_REPORT_DATASET,
    view_id=SUPERVISION_TERMINATIONS_BY_TYPE_BY_WEEK_VIEW_NAME,
    view_query_template=SUPERVISION_TERMINATIONS_BY_TYPE_BY_WEEK_QUERY_TEMPLATE,
    description=SUPERVISION_TERMINATIONS_BY_TYPE_BY_WEEK_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    covid_report_dataset=COVID_REPORT_DATASET,
    reference_dataset=REFERENCE_TABLES_DATASET
)

if __name__ == '__main__':
    print(SUPERVISION_TERMINATIONS_BY_TYPE_BY_WEEK_VIEW.view_id)
    print(SUPERVISION_TERMINATIONS_BY_TYPE_BY_WEEK_VIEW.view_query)
