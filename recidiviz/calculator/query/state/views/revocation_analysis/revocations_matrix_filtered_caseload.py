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
"""Revocations Matrix Filtered Caseload."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state import dataset_config

REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_NAME = 'revocations_matrix_filtered_caseload'

REVOCATIONS_MATRIX_FILTERED_CASELOAD_DESCRIPTION = """
 Person-level violation and caseload information for all of the people revoked to prison from supervision.
 """

REVOCATIONS_MATRIX_FILTERED_CASELOAD_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code,
      person_external_id AS state_id,
      supervising_officer_external_id AS officer,
      most_severe_response_decision AS officer_recommendation,
      violation_history_description AS violation_record,
      supervising_district_external_id AS district,
      supervision_type,
      case_type AS charge_category,
      assessment_score_bucket AS risk_level,
      CASE WHEN most_severe_violation_type = 'TECHNICAL' THEN
        CASE WHEN most_severe_violation_type_subtype = 'SUBSTANCE_ABUSE' THEN most_severe_violation_type_subtype
             WHEN most_severe_violation_type_subtype = 'LAW_CITATION' THEN 'MISDEMEANOR'
             ELSE most_severe_violation_type END
        ELSE most_severe_violation_type
        END AS violation_type,
      IF(response_count > 8, 8, response_count) AS reported_violations,
      metric_period_months
    FROM `{project_id}.{metrics_dataset}.supervision_revocation_analysis_metrics`
    JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
      USING (state_code, job_id, year, month, metric_period_months)
    WHERE methodology = 'PERSON'
      AND revocation_type = 'REINCARCERATION'
      AND person_external_id IS NOT NULL
      AND month IS NOT NULL
      AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
      AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
      AND job.metric_type = 'SUPERVISION_REVOCATION_ANALYSIS'
    ORDER BY metric_period_months, violation_record
    """

REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW = BigQueryView(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_FILTERED_CASELOAD_QUERY_TEMPLATE,
    description=REVOCATIONS_MATRIX_FILTERED_CASELOAD_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
)

if __name__ == '__main__':
    print(REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW.view_id)
    print(REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW.view_query)
