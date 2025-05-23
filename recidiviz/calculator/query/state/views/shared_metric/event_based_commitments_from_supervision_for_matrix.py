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
"""Event based commitments from supervision to support various matrix views."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import age_bucket_grouping
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_VIEW_NAME = (
    "event_based_commitments_from_supervision_for_matrix"
)

EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_DESCRIPTION = """
Event based commitment from supervision admissions to support various matrix views
"""

EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_QUERY_TEMPLATE = """
    WITH metrics AS (
        SELECT
            metrics.state_code,
            year,
            month,
            {state_specific_admission_type},
            admission_date,
            most_severe_violation_type,
            most_severe_violation_type_subtype,
            response_count,
            person_id,
            -- TODO(#41554): Replace secondary_person_external_id with the 
            -- display_person_external_id from reference_views.product_display_person_external_ids
            -- for system_type = "SUPERVISION" - this should allow us to deprecate the
            -- secondary_person_external_id pipeline output entirely.
            secondary_person_external_id AS person_external_id,
            gender,
            /* TODO(#39399): Confirm what types/classes of assessments are included here
            (and update logic, if needed, to only pull relevant ones). */
            assessment_score_bucket,
            {age_bucket},
            prioritized_race_or_ethnicity,
            supervision_type,
            supervision_level,
            supervision_level_raw_text,
            case_type,
            IFNULL(level_1_supervision_location_external_id, 'EXTERNAL_UNKNOWN') AS level_1_supervision_location,
            IFNULL(level_2_supervision_location_external_id, 'EXTERNAL_UNKNOWN') AS level_2_supervision_location,
            metrics.supervising_officer_staff_id,
            staff.external_id,
            {state_specific_most_recent_officer_recommendation},
            {state_specific_recommended_for_revocation},
            violation_history_description AS violation_record,
            violation_type_frequency_counter
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population_materialized` metrics
        LEFT JOIN
            `{project_id}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` staff
        ON
            metrics.supervising_officer_staff_id = staff.staff_id
        WHERE {state_specific_admission_type_inclusion_filter}
    )
    
    SELECT 
        metrics.*,
        agent.* EXCEPT (state_code, legacy_supervising_officer_external_id),
        -- We drop commas in agent names since we use commas as the delimiters in the export
        -- TODO(#8674): Use agent_external_id instead of agent_external_id_with_full_name
        -- once the FE is using the officer_full_name field for names
        REPLACE(IFNULL(agent.agent_external_id_with_full_name, 'EXTERNAL_UNKNOWN'), ',', '') AS officer,
        REPLACE(COALESCE(agent.full_name, 'UNKNOWN'), ',', '') AS officer_full_name,
    FROM
        metrics
    LEFT JOIN
        `{project_id}.reference_views.state_staff_with_names` agent
    ON metrics.state_code = agent.state_code AND metrics.supervising_officer_staff_id = agent.staff_id
    """

EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    view_id=EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_VIEW_NAME,
    should_materialize=True,
    view_query_template=EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_QUERY_TEMPLATE,
    description=EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    state_specific_most_recent_officer_recommendation=state_specific_query_strings.state_specific_officer_recommendation(
        input_col="most_recent_response_decision", optional_prefix="metrics"
    ),
    state_specific_recommended_for_revocation=state_specific_query_strings.state_specific_recommended_for_revocation(
        optional_prefix="metrics"
    ),
    state_specific_admission_type_inclusion_filter=state_specific_query_strings.state_specific_admission_type_inclusion_filter(
        optional_prefix="metrics"
    ),
    state_specific_admission_type=state_specific_query_strings.state_specific_admission_type(
        optional_prefix="metrics"
    ),
    age_bucket=age_bucket_grouping(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_VIEW_BUILDER.build_and_print()
