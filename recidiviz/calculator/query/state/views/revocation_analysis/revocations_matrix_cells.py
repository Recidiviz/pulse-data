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
"""Revocations Matrix Cells."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

REVOCATIONS_MATRIX_CELLS_VIEW_NAME = 'revocations_matrix_cells'

REVOCATIONS_MATRIX_CELLS_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation by metric period month.
 This counts all individuals admitted to prison for a revocation of probation or parole, broken down by number of 
 violations leading up to the revocation and the most severe violation. 
 """

REVOCATIONS_MATRIX_CELLS_QUERY = \
    """
    /*{description}*/
    SELECT 
      state_code, 
      violation_type, 
      reported_violations, 
      SUM(total_revocations) as total_revocations, 
      supervision_type, 
      charge_category, 
      district, 
      metric_period_months
    FROM (
      SELECT 
        state_code,
        CASE WHEN most_severe_violation_type = 'TECHNICAL' AND most_severe_violation_type_subtype = 'SUBSTANCE_ABUSE' 
          THEN most_severe_violation_type_subtype 
          ELSE most_severe_violation_type END as violation_type, 
        IF(response_count > 8, 8, response_count) as reported_violations,
        count as total_revocations, 
        supervision_type, 
        IFNULL(case_type, 'ALL') AS charge_category, 
        IFNULL(supervising_district_external_id, 'ALL') as district,
      metric_period_months
      FROM `{project_id}.{metrics_dataset}.supervision_revocation_analysis_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND month IS NOT NULL
        AND supervision_type IS NOT NULL
        AND response_count IS NOT NULL
        AND response_count > 0
        AND most_severe_violation_type IS NOT NULL
        AND most_severe_violation_type_subtype IS NOT NULL
        AND most_severe_response_decision IS NULL
        AND assessment_score_bucket IS NULL
        AND assessment_type IS NULL
        AND revocation_type = 'REINCARCERATION'
        AND source_violation_type IS NULL
        AND supervising_officer_external_id IS NULL
        AND age_bucket IS NULL
        AND race IS NULL
        AND ethnicity IS NULL
        AND gender IS NULL
        AND person_external_id IS NULL
        AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
        AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
        AND job.metric_type = 'SUPERVISION_REVOCATION_ANALYSIS'
    )
    GROUP BY state_code, violation_type, reported_violations, supervision_type, charge_category, district, metric_period_months
    ORDER BY state_code, district, metric_period_months, violation_type, reported_violations, supervision_type
    """.format(
        description=REVOCATIONS_MATRIX_CELLS_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        views_dataset=VIEWS_DATASET,
        )

REVOCATIONS_MATRIX_CELLS_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_MATRIX_CELLS_VIEW_NAME,
    view_query=REVOCATIONS_MATRIX_CELLS_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_MATRIX_CELLS_VIEW.view_id)
    print(REVOCATIONS_MATRIX_CELLS_VIEW.view_query)
