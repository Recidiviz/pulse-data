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
"""Event based revocations to support various revocation matrix views."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

EVENT_BASED_REVOCATIONS_FOR_MATRIX_VIEW_NAME = 'event_based_revocations_for_matrix'

EVENT_BASED_REVOCATIONS_FOR_MATRIX_DESCRIPTION = """
    Event based revocations to support various revocation matrix views
 """

EVENT_BASED_REVOCATIONS_FOR_MATRIX_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
        state_code,
        year,
        month,
        revocation_admission_date,
        most_severe_violation_type,
        most_severe_violation_type_subtype,
        response_count,
        person_id,
        person_external_id,
        gender,
        assessment_score_bucket,
        age_bucket,
        prioritized_race_or_ethnicity,
        supervision_type,
        supervision_level,
        case_type,
        supervising_district_external_id AS district,
        supervising_officer_external_id AS officer
    FROM `{project_id}.{metrics_dataset}.supervision_revocation_analysis_metrics` 
    {filter_to_most_recent_job_id_for_metric}
    WHERE methodology = 'EVENT'
        AND metric_period_months = 1
        AND revocation_type = 'REINCARCERATION'
        AND month IS NOT NULL
        AND person_id IS NOT NULL
    """

EVENT_BASED_REVOCATIONS_FOR_MATRIX_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=EVENT_BASED_REVOCATIONS_FOR_MATRIX_VIEW_NAME,
    view_query_template=EVENT_BASED_REVOCATIONS_FOR_MATRIX_QUERY_TEMPLATE,
    description=EVENT_BASED_REVOCATIONS_FOR_MATRIX_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    filter_to_most_recent_job_id_for_metric=bq_utils.filter_to_most_recent_job_id_for_metric(
        reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET)
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        EVENT_BASED_REVOCATIONS_FOR_MATRIX_VIEW_BUILDER.build_and_print()
