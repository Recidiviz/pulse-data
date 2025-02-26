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
"""Matrix of supervision terminations with violation response count and most severe violation per person."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_NAME = 'supervision_termination_matrix_by_person'

SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_DESCRIPTION = """
 Matrix of supervision terminations with violation response count and most severe violation per person.
 This lists all individuals who had a supervision period end for one of the following reasons:
 'DISCHARGE', 'EXPIRATION', 'SUSPENSION', 'INTERNAL_UNKNOWN', 'EXTERNAL_UNKNOWN', 'DEATH', or 'REVOCATION'. Revocation
 terminations are counted on the date of the revocation admission to prison, not on the date of the supervision
 termination. These terminations are broken down by number of violations and the most severe violation in a 12 month
 window leading up to the termination."""

SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    
    /* Supervision case terminations. */
    WITH terminations_matrix AS (
        SELECT
            state_code,
            year,
            month,
            most_severe_violation_type,
            most_severe_violation_type_subtype,
            IF(response_count > 8, 8, response_count) as reported_violations,
            person_id,
            person_external_id,
            gender,
            assessment_score_bucket,
            age_bucket,
            race,
            ethnicity,
            supervision_type,
            case_type,
            supervising_district_external_id AS district,
            supervising_officer_external_id AS officer,
            termination_date,
            FALSE AS is_revocation
        FROM `{project_id}.{metrics_dataset}.supervision_termination_metrics`
        JOIN `{project_id}.{reference_views_dataset}.most_recent_job_id_by_metric_and_state_code_materialized` job
        USING (state_code, job_id, year, month, metric_period_months, metric_type)
        WHERE methodology = 'EVENT'
            AND termination_reason IN ('DISCHARGE', 'EXPIRATION', 'SUSPENSION', 'INTERNAL_UNKNOWN', 'EXTERNAL_UNKNOWN', 'DEATH')
            AND month IS NOT NULL
            AND person_id IS NOT NULL
            AND metric_period_months = 1
            AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
    ), revocations_matrix AS (
        SELECT
            * EXCEPT(revocation_admission_date),
            revocation_admission_date AS termination_date,
            TRUE as is_revocation
        FROM `{project_id}.{reference_views_dataset}.event_based_revocations_for_matrix`
    ), revocations_and_terminations AS (
      SELECT * FROM terminations_matrix 
        UNION ALL
      SELECT * FROM revocations_matrix
    ), person_based_terminations AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY state_code, metric_period_months, person_id ORDER BY is_revocation DESC, termination_date DESC) as ranking
      FROM revocations_and_terminations,
      {metric_period_dimension}
      WHERE {metric_period_condition}
    )
    
    
   SELECT
        state_code,
        metric_period_months, 
        {most_severe_violation_type_subtype_grouping},
        reported_violations,
        supervision_type,
        charge_category,
        district,
        officer,
        person_id,
        person_external_id,
        gender,
        age_bucket,
        IFNULL(assessment_score_bucket, 'OVERALL') as risk_level,
        race,
        ethnicity
    FROM person_based_terminations,
    {district_dimension},
    {supervision_dimension},
    {charge_category_dimension}
    WHERE ranking = 1
      AND supervision_type IN ('ALL', 'DUAL', 'PAROLE', 'PROBATION')
      AND district IS NOT NULL
    """

SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_NAME,
    view_query_template=SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_QUERY_TEMPLATE,
    description=SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    most_severe_violation_type_subtype_grouping=bq_utils.most_severe_violation_type_subtype_grouping(),
    district_dimension=bq_utils.unnest_district('district'),
    supervision_dimension=bq_utils.unnest_supervision_type(),
    charge_category_dimension=bq_utils.unnest_charge_category(),
    metric_period_dimension=bq_utils.unnest_metric_period_months(),
    metric_period_condition=bq_utils.metric_period_condition()
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_BUILDER.build_and_print()
