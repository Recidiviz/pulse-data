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
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_MATRIX_BY_PERSON_VIEW_NAME = "supervision_matrix_by_person"

SUPERVISION_MATRIX_BY_PERSON_DESCRIPTION = """
 Supervision matrix of violation response count and most severe violation per person.
 This lists all individuals on probation/parole/dual supervision, broken down by number of
 violations and the most severe violation while on supervision.
 """

SUPPORTED_SUPERVISION_TYPES = [
    StateSupervisionPeriodSupervisionType.DUAL,
    # INTERNAL_UNKNOWN is included for historical consistency, they are only counted in
    # the "ALL" supervision_type group for US_MO
    StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
    StateSupervisionPeriodSupervisionType.PAROLE,
    StateSupervisionPeriodSupervisionType.PROBATION,
]

SUPERVISION_MATRIX_BY_PERSON_QUERY_TEMPLATE = """
    WITH supervision_with_agent_info AS (
        SELECT
            * EXCEPT(state_code, supervision_type),
            metric.state_code,
            {age_bucket},
            -- We drop commas in agent names since we use commas as the delimiters in the export
            -- TODO(#8674): Use agent_external_id instead of agent_external_id_with_full_name
            -- once the FE is using the officer_full_name field for names
            REPLACE(IFNULL(agent.agent_external_id_with_full_name, 'EXTERNAL_UNKNOWN'), ',', '') AS officer,
            REPLACE(COALESCE(agent.full_name, 'UNKNOWN'), ',', '') AS officer_full_name,
            -- Use the most recent supported supervision type in place of absconsion/bench warrant periods
            LAST_VALUE(IF(supervision_type IN ('{supported_supervision_types}'), supervision_type, NULL) IGNORE NULLS)
              OVER (PARTITION BY metric.state_code, person_id
                    ORDER BY date_of_supervision, supervision_type, supervision_level, case_type,
                         level_1_supervision_location_external_id, level_2_supervision_location_external_id,
                         staff.external_id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS supervision_type,
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_metrics_materialized` metric
        LEFT JOIN
            `{project_id}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` staff
        ON
            metric.supervising_officer_staff_id = staff.staff_id
        LEFT JOIN `{project_id}.reference_views.state_staff_with_names` agent
        ON metric.state_code = agent.state_code AND metric.supervising_officer_staff_id = agent.staff_id 
    ), supervision_matrix AS (
        SELECT
            state_code,
            year, month,
            most_severe_violation_type,
            most_severe_violation_type_subtype,
            response_count,
            person_id, person_external_id,
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
            IFNULL(level_1_supervision_location_external_id, 'EXTERNAL_UNKNOWN') AS level_1_supervision_location,
            IFNULL(level_2_supervision_location_external_id, 'EXTERNAL_UNKNOWN') AS level_2_supervision_location,
            officer,
            officer_full_name,
            {state_specific_recommended_for_revocation},
            date_of_supervision,
            FALSE AS is_revocation
        FROM supervision_with_agent_info
        WHERE {thirty_six_month_filter}
        AND {state_specific_supervision_type_inclusion_filter}
    ), revocations_matrix AS (
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
            supervision_level_raw_text,
            case_type,
            level_1_supervision_location,
            level_2_supervision_location,
            officer,
            officer_full_name,
            recommended_for_revocation,
            admission_date AS date_of_supervision,
            TRUE as is_revocation
        FROM `{project_id}.{shared_metric_views_dataset}.event_based_commitments_from_supervision_for_matrix_materialized`
    ), revocations_and_supervisions AS (
      SELECT * FROM supervision_matrix
        UNION ALL
      SELECT * FROM revocations_matrix
    ), person_based_supervision AS (
      SELECT
        state_code,
        metric_period_months, 
        {most_severe_violation_type_subtype_grouping},
        IF(response_count > 8, 8, response_count) as reported_violations,
        supervision_type,
        {state_specific_supervision_level},
        case_type,
        level_1_supervision_location,
        level_2_supervision_location,
        officer,
        officer_full_name,
        recommended_for_revocation,
        person_id, person_external_id,
        IFNULL(gender, 'EXTERNAL_UNKNOWN') AS gender,
        age_bucket,
        {state_specific_assessment_bucket},
        IFNULL(prioritized_race_or_ethnicity, 'EXTERNAL_UNKNOWN') AS prioritized_race_or_ethnicity,
      FROM revocations_and_supervisions,
      {metric_period_dimension}
      WHERE {metric_period_condition}
      QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, metric_period_months, person_id
                           ORDER BY is_revocation DESC, date_of_supervision DESC,
                                    supervision_type, supervision_level, case_type, level_1_supervision_location,
                                    level_2_supervision_location, officer) = 1
    ), unnested_supervision AS (
      SELECT
        state_code,
        metric_period_months,
        violation_type,
        reported_violations,
        supervision_type,
        supervision_level,
        charge_category,
        level_1_supervision_location,
        level_2_supervision_location,
        officer,
        officer_full_name,
        recommended_for_revocation,
        person_id,
        person_external_id,
        gender,
        age_bucket,
        risk_level,
        prioritized_race_or_ethnicity
      FROM person_based_supervision,
      {level_1_supervision_location_dimension},
      {level_2_supervision_location_dimension},
      {supervision_type_dimension},
      {supervision_level_dimension},
      {charge_category_dimension},
      {reported_violations_dimension},
      {violation_type_dimension}
    )
    
    SELECT
        state_code,
        metric_period_months,
        violation_type,
        reported_violations,
        supervision_type,
        supervision_level,
        charge_category,
        level_1_supervision_location,
        level_2_supervision_location,
        officer,
        officer_full_name,
        recommended_for_revocation,
        person_id,
        person_external_id,
        gender,
        age_bucket,
        risk_level,
        prioritized_race_or_ethnicity
    FROM unnested_supervision
    WHERE supervision_type IN ('ALL', 'DUAL', 'PAROLE', 'PROBATION')
      AND {state_specific_supervision_location_optimization_filter}
      AND {state_specific_dimension_filter}
      AND (reported_violations = 'ALL' OR violation_type != 'ALL')
    """

SUPERVISION_MATRIX_BY_PERSON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    view_id=SUPERVISION_MATRIX_BY_PERSON_VIEW_NAME,
    should_materialize=True,
    view_query_template=SUPERVISION_MATRIX_BY_PERSON_QUERY_TEMPLATE,
    description=SUPERVISION_MATRIX_BY_PERSON_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    shared_metric_views_dataset=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    most_severe_violation_type_subtype_grouping=state_specific_query_strings.state_specific_most_severe_violation_type_subtype_grouping(),
    state_specific_assessment_bucket=state_specific_query_strings.state_specific_assessment_bucket(
        output_column_name="risk_level"
    ),
    state_specific_supervision_level=state_specific_query_strings.state_specific_supervision_level(),
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
    state_specific_supervision_location_optimization_filter=state_specific_query_strings.state_specific_supervision_location_optimization_filter(),
    thirty_six_month_filter=bq_utils.thirty_six_month_filter(),
    state_specific_dimension_filter=state_specific_query_strings.state_specific_dimension_filter(),
    state_specific_supervision_type_inclusion_filter=state_specific_query_strings.state_specific_supervision_type_inclusion_filter(),
    state_specific_recommended_for_revocation=state_specific_query_strings.state_specific_recommended_for_revocation(),
    age_bucket=bq_utils.age_bucket_grouping(age_column="metric.age"),
    supported_supervision_types=(
        "','".join([start_reason.value for start_reason in SUPPORTED_SUPERVISION_TYPES])
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_MATRIX_BY_PERSON_VIEW_BUILDER.build_and_print()
