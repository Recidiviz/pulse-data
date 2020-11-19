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
"""Supervision Matrix by Person."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config, state_specific_query_strings
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_MATRIX_BY_PERSON_VIEW_NAME = 'supervision_matrix_by_person'

SUPERVISION_MATRIX_BY_PERSON_DESCRIPTION = """
 Supervision matrix of violation response count and most severe violation per person.
 This lists all individuals on probation/parole/dual supervision, broken down by number of
 violations and the most severe violation while on supervision.
 """

SUPERVISION_MATRIX_BY_PERSON_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH supervision_matrix AS (
        SELECT
            state_code, year, month,
            most_severe_violation_type,
            most_severe_violation_type_subtype,
            response_count,
            person_id, person_external_id,
            gender,
            assessment_score_bucket,
            age_bucket,
            prioritized_race_or_ethnicity,
            supervision_type,
            supervision_level,
            case_type,
            supervising_district_external_id AS district,
            supervising_officer_external_id AS officer,
            date_of_supervision,
            FALSE AS is_revocation
        FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`
        {filter_to_most_recent_job_id_for_metric}
        WHERE methodology = 'EVENT'
            AND metric_period_months = 0
            AND month IS NOT NULL
            AND person_id IS NOT NULL
            AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
    ), revocations_matrix AS (
        SELECT
            * EXCEPT(revocation_admission_date),
            revocation_admission_date AS date_of_supervision,
            TRUE as is_revocation
        FROM `{project_id}.{reference_views_dataset}.event_based_revocations_for_matrix`
    ), revocations_and_supervisions AS (
      SELECT * FROM supervision_matrix
        UNION ALL
      SELECT * FROM revocations_matrix
    ), person_based_supervision AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY state_code, metric_period_months, person_id ORDER BY is_revocation DESC, date_of_supervision DESC) as ranking
      FROM revocations_and_supervisions,
      {metric_period_dimension}
      WHERE {metric_period_condition}
    )

    SELECT
        state_code,
        metric_period_months, 
        {most_severe_violation_type_subtype_grouping},
        IF(response_count > 8, 8, response_count) as reported_violations,
        supervision_type,
        {state_specific_supervision_level},
        charge_category, district, officer,
        person_id, person_external_id,
        gender, age_bucket,
        {state_specific_assessment_bucket},
        prioritized_race_or_ethnicity
    FROM person_based_supervision,
    {district_dimension},
    {supervision_type_dimension},
    {supervision_level_dimension},
    {charge_category_dimension}
    WHERE ranking = 1
      AND supervision_type IN ('ALL', 'DUAL', 'PAROLE', 'PROBATION')
      AND district IS NOT NULL
    """

SUPERVISION_MATRIX_BY_PERSON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=SUPERVISION_MATRIX_BY_PERSON_VIEW_NAME,
    view_query_template=SUPERVISION_MATRIX_BY_PERSON_QUERY_TEMPLATE,
    description=SUPERVISION_MATRIX_BY_PERSON_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    most_severe_violation_type_subtype_grouping=
    state_specific_query_strings.state_specific_most_severe_violation_type_subtype_grouping(),
    state_specific_assessment_bucket=
    state_specific_query_strings.state_specific_assessment_bucket(output_column_name='risk_level'),
    state_specific_supervision_level=state_specific_query_strings.state_specific_supervision_level(),
    district_dimension=bq_utils.unnest_district('district'),
    supervision_type_dimension=bq_utils.unnest_supervision_type(),
    supervision_level_dimension=bq_utils.unnest_column('supervision_level', 'supervision_level'),
    charge_category_dimension=bq_utils.unnest_charge_category(),
    metric_period_dimension=bq_utils.unnest_metric_period_months(),
    metric_period_condition=bq_utils.metric_period_condition(),
    filter_to_most_recent_job_id_for_metric=bq_utils.filter_to_most_recent_job_id_for_metric(
        reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET)
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_MATRIX_BY_PERSON_VIEW_BUILDER.build_and_print()
