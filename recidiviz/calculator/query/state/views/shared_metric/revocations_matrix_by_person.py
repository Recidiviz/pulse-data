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
"""Revocations Matrix by Person."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_BY_PERSON_VIEW_NAME = "revocations_matrix_by_person"

REVOCATIONS_MATRIX_BY_PERSON_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation per person.
 This lists all individuals admitted to prison for a revocation of supervision, with number of
 violations leading up to the revocation and the most severe violation. If a person has had multiple revocation
 admissions within a metric period, the most recent revocation admission is used.
 """

REVOCATIONS_MATRIX_BY_PERSON_QUERY_TEMPLATE = """
    WITH revocations AS (
        SELECT
            state_code,
            metric_period_months,
            admission_type,
            admission_date,
            {most_severe_violation_type_subtype_grouping},
            IF(response_count > 8, 8, response_count) as reported_violations,
            -- For record of the actual number of responses --
            response_count,
            person_id,
            person_external_id,
            gender,
            /* TODO(#39399): Confirm what types/classes of assessments are included here
            (and update logic, if needed, to only pull relevant ones). */
            assessment_score_bucket,
            age_bucket,
            prioritized_race_or_ethnicity,
            supervision_type,
            supervision_level,
            supervision_level_raw_text,
            case_type,
            level_1_supervision_location,
            level_2_supervision_location,
            officer,
            officer_full_name,
            officer_recommendation,
            violation_record,
            violation_type_frequency_counter,
            ROW_NUMBER() OVER (PARTITION BY state_code, metric_period_months, person_id
                               ORDER BY admission_date DESC,
                               (admission_type = 'LEGAL_REVOCATION') DESC,
                               supervision_type, supervision_level, case_type, level_1_supervision_location,
                               level_2_supervision_location, officer) as ranking
        FROM `{project_id}.{shared_metric_views_dataset}.event_based_commitments_from_supervision_for_matrix_materialized`,
        {metric_period_dimension}
        WHERE {metric_period_condition}
        AND {state_specific_supervision_type_inclusion_filter}
    ), admission_type_history AS (
        SELECT 
            state_code,
            metric_period_months,
            person_id,
            {state_specific_admission_history_description}
        FROM revocations
        GROUP BY state_code, metric_period_months, person_id
    ), person_based_unnested AS (
        SELECT
            state_code,
            metric_period_months,
            admission_type,
            admission_date,
            violation_type,
            reported_violations,
            response_count,
            supervision_type,
            {state_specific_supervision_level},
            charge_category,
            level_1_supervision_location,
            level_2_supervision_location,
            officer,
            officer_full_name,
            person_id,
            person_external_id,
            IFNULL(gender, 'EXTERNAL_UNKNOWN') as gender,
            age_bucket,
            {state_specific_assessment_bucket},
            IFNULL(prioritized_race_or_ethnicity, 'EXTERNAL_UNKNOWN') AS prioritized_race_or_ethnicity,
            officer_recommendation,
            violation_record,
            violation_type_frequency_counter
        FROM revocations,
        {admission_type_dimension},
        {level_1_supervision_location_dimension},
        {level_2_supervision_location_dimension},
        {supervision_type_dimension},
        {supervision_level_dimension},
        {charge_category_dimension},
        {reported_violations_dimension},
        {violation_type_dimension}
        WHERE ranking = 1
    )
    
    SELECT
        state_code,
        metric_period_months,
        admission_type,
        admission_history_description,
        admission_date,
        violation_type,
        reported_violations,
        response_count,
        supervision_type,
        supervision_level,
        charge_category,
        level_1_supervision_location,
        level_2_supervision_location,
        officer,
        officer_full_name,
        person_id,
        person_external_id,
        gender,
        age_bucket,
        risk_level,
        prioritized_race_or_ethnicity,
        officer_recommendation,
        violation_record,
        violation_type_frequency_counter
    FROM person_based_unnested
    LEFT JOIN admission_type_history USING (state_code, metric_period_months, person_id)
    WHERE supervision_type IN ('ALL', 'DUAL', 'PAROLE', 'PROBATION')
      AND {state_specific_supervision_location_optimization_filter}
      AND {state_specific_dimension_filter}
      AND (reported_violations = 'ALL' OR violation_type != 'ALL')
    """

REVOCATIONS_MATRIX_BY_PERSON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_BY_PERSON_VIEW_NAME,
    should_materialize=True,
    view_query_template=REVOCATIONS_MATRIX_BY_PERSON_QUERY_TEMPLATE,
    description=REVOCATIONS_MATRIX_BY_PERSON_DESCRIPTION,
    shared_metric_views_dataset=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    most_severe_violation_type_subtype_grouping=state_specific_query_strings.state_specific_most_severe_violation_type_subtype_grouping(),
    level_1_supervision_location_dimension=bq_utils.unnest_column(
        "level_1_supervision_location", "level_1_supervision_location"
    ),
    level_2_supervision_location_dimension=bq_utils.unnest_column(
        "level_2_supervision_location", "level_2_supervision_location"
    ),
    supervision_type_dimension=bq_utils.unnest_supervision_type(),
    supervision_level_dimension=bq_utils.unnest_column(
        "supervision_level", "supervision_level"
    ),
    charge_category_dimension=bq_utils.unnest_charge_category(),
    violation_type_dimension=bq_utils.unnest_column("violation_type", "violation_type"),
    reported_violations_dimension=bq_utils.unnest_reported_violations(),
    metric_period_dimension=bq_utils.unnest_metric_period_months(),
    metric_period_condition=bq_utils.metric_period_condition(),
    state_specific_admission_history_description=state_specific_query_strings.state_specific_admission_history_description(),
    state_specific_assessment_bucket=state_specific_query_strings.state_specific_assessment_bucket(
        output_column_name="risk_level"
    ),
    state_specific_supervision_level=state_specific_query_strings.state_specific_supervision_level(),
    state_specific_supervision_location_optimization_filter=state_specific_query_strings.state_specific_supervision_location_optimization_filter(),
    state_specific_dimension_filter=state_specific_query_strings.state_specific_dimension_filter(
        filter_admission_type=True
    ),
    state_specific_supervision_type_inclusion_filter=state_specific_query_strings.state_specific_supervision_type_inclusion_filter(),
    admission_type_dimension=bq_utils.unnest_column("admission_type", "admission_type"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_BY_PERSON_VIEW_BUILDER.build_and_print()
