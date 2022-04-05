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
"""
The average change in LSIR score by month of scheduled supervision termination.
Per ND-specific request, compares the LSIR score at termination to the second
LSIR score of the person's supervision.
"""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config

AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW_NAME = 'average_change_lsir_score_by_month'

AVERAGE_CHANGE_LSIR_SCORE_MONTH_DESCRIPTION = """
    The average change in LSIR score by month of scheduled supervision 
    termination. Per ND-request, compares the LSIR score at termination to the 
    second LSIR score of the person's supervision.
"""

AVERAGE_CHANGE_LSIR_SCORE_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code, termination_year, termination_month,
      IFNULL(AVG(average_score_change), 0.0) AS average_change,
      supervision_type,
      district
    FROM (
      SELECT
        state_code, year as termination_year, month as termination_month,
        average_score_change,
        supervision_type,
        district,
        -- Use the most recent termination per person/year/month/supervision/district
        ROW_NUMBER() OVER (PARTITION BY state_code, year, month, supervision_type, district, person_id
                           ORDER BY termination_date DESC) AS supervision_rank
      FROM `{project_id}.{metrics_dataset}.terminated_supervision_assessment_score_change_metrics`
      JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months),
      {district_dimension},
      {supervision_dimension}
      WHERE methodology = 'EVENT'
        AND metric_period_months = 1
        AND assessment_type = 'LSIR'
        AND person_id IS NOT NULL
        AND month IS NOT NULL
        AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
        AND job.metric_type = 'SUPERVISION_ASSESSMENT_CHANGE'
    )
    WHERE supervision_type IN ('ALL', 'PAROLE', 'PROBATION')
      AND supervision_rank = 1
    GROUP BY state_code, termination_year, termination_month, supervision_type, district
    ORDER BY state_code, termination_year, termination_month, district, supervision_type
    """

AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW = BigQueryView(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW_NAME,
    view_query_template=AVERAGE_CHANGE_LSIR_SCORE_MONTH_QUERY_TEMPLATE,
    description=AVERAGE_CHANGE_LSIR_SCORE_MONTH_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    district_dimension=bq_utils.unnest_district(),
    supervision_dimension=bq_utils.unnest_supervision_type(),
)

if __name__ == '__main__':
    print(AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW.view_id)
    print(AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW.view_query)
