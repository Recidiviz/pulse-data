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
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config, state_specific_query_strings
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

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
      {state_specific_officer_recommendation},
      violation_history_description AS violation_record,
      supervising_district_external_id AS district,
      supervision_type,
      {state_specific_supervision_level},
      case_type AS charge_category,
      assessment_score_bucket AS risk_level,
      {most_severe_violation_type_subtype_grouping},
      IF(response_count > 8, 8, response_count) AS reported_violations,
      metric_period_months
    FROM `{project_id}.{metrics_dataset}.supervision_revocation_analysis_metrics`
    JOIN `{project_id}.{reference_views_dataset}.most_recent_job_id_by_metric_and_state_code_materialized` job
      USING (state_code, job_id, year, month, metric_period_months, metric_type)
    WHERE methodology = 'PERSON'
      AND revocation_type = 'REINCARCERATION'
      AND person_external_id IS NOT NULL
      AND month IS NOT NULL
      AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
      AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
    ORDER BY state_code, metric_period_months, violation_record
    """

REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_FILTERED_CASELOAD_QUERY_TEMPLATE,
    dimensions=['state_code', 'metric_period_months', 'district', 'supervision_type', 'supervision_level',
                'charge_category', 'risk_level', 'violation_type', 'reported_violations'],
    description=REVOCATIONS_MATRIX_FILTERED_CASELOAD_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    most_severe_violation_type_subtype_grouping=
    state_specific_query_strings.state_specific_most_severe_violation_type_subtype_grouping(),
    state_specific_officer_recommendation=state_specific_query_strings.state_specific_officer_recommendation(),
    state_specific_supervision_level=state_specific_query_strings.state_specific_supervision_level()
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_BUILDER.build_and_print()
