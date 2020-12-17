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
from recidiviz.calculator.query import bq_utils
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
      IFNULL(person_external_id, 'UNKNOWN') AS state_id,
      officer,
      officer_recommendation,
      violation_record,
      district,
      supervision_type,
      supervision_level,
      charge_category,
      risk_level,
      violation_type,
      reported_violations,
      metric_period_months
    FROM `{project_id}.{reference_views_dataset}.revocations_matrix_by_person` 
    WHERE district != 'ALL'
    AND supervision_type != 'ALL'
    AND charge_category != 'ALL'
    AND supervision_level != 'ALL'
    ORDER BY state_code, metric_period_months, violation_record
    """

REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_FILTERED_CASELOAD_QUERY_TEMPLATE,
    dimensions=['state_code', 'metric_period_months', 'district', 'supervision_type', 'supervision_level',
                'charge_category', 'risk_level', 'violation_type', 'reported_violations', 'state_id', 'officer'],
    description=REVOCATIONS_MATRIX_FILTERED_CASELOAD_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    most_severe_violation_type_subtype_grouping=
    state_specific_query_strings.state_specific_most_severe_violation_type_subtype_grouping(),
    state_specific_officer_recommendation=state_specific_query_strings.state_specific_officer_recommendation(),
    state_specific_supervision_level=state_specific_query_strings.state_specific_supervision_level(),
    filter_to_most_recent_job_id_for_metric=bq_utils.filter_to_most_recent_job_id_for_metric(
        reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET)
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_BUILDER.build_and_print()
