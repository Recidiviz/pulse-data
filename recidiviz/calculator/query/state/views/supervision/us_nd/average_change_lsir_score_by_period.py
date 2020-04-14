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
"""The average change in LSIR score by metric period months of scheduled
supervision termination. Per ND-request, compares the LSIR score at
termination to the second LSIR score of the person's supervision.
"""
# pylint: disable=trailing-whitespace

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_VIEW_NAME = \
    'average_change_lsir_score_by_period'

AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_DESCRIPTION = """
    The average change in LSIR score by metric period months of scheduled 
    supervision termination. Per ND-request, compares the LSIR score at 
    termination to the second LSIR score of the person's supervision.
"""

AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_QUERY = \
    """
    /*{description}*/
    SELECT 
      state_code, metric_period_months, 
      IFNULL(average_score_change, 0.0) as average_change,
      IFNULL(supervision_type, 'ALL') as supervision_type, 
      IFNULL(supervising_district_external_id, 'ALL') as district
    FROM `{project_id}.{metrics_dataset}.terminated_supervision_assessment_score_change_metrics`
    JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
      USING (state_code, job_id, year, month, metric_period_months)
    WHERE methodology = 'PERSON'
      AND assessment_score_bucket IS NULL
      AND assessment_type = 'LSIR'
      AND age_bucket IS NULL
      AND race IS NULL
      AND ethnicity IS NULL
      AND gender IS NULL
      AND case_type IS NULL
      AND person_id IS NULL
      AND person_external_id IS NULL
      AND supervising_officer_external_id IS NULL
      AND termination_reason IS NULL
      AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
      AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
      AND IFNULL(supervision_type, 'ALL') in ('ALL', 'PAROLE', 'PROBATION')
      AND job.metric_type = 'SUPERVISION_ASSESSMENT_CHANGE'
    ORDER BY state_code, district, supervision_type, metric_period_months
    """.format(
        description=AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        reference_dataset=REFERENCE_DATASET,
    )

AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_VIEW = bqview.BigQueryView(
    view_id=AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_VIEW_NAME,
    view_query=AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_QUERY
)

if __name__ == '__main__':
    print(AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_VIEW.view_id)
    print(AVERAGE_CHANGE_LSIR_SCORE_BY_PERIOD_VIEW.view_query)
