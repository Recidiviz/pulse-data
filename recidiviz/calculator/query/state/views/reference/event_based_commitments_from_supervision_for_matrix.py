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
    /*{description}*/
    SELECT
        state_code,
        year,
        month,
        {state_specific_admission_type},
        admission_date,
        most_severe_violation_type,
        most_severe_violation_type_subtype,
        response_count,
        person_id,
        secondary_person_external_id AS person_external_id,
        gender,
        assessment_score_bucket,
        age_bucket,
        prioritized_race_or_ethnicity,
        supervision_type,
        supervision_level,
        supervision_level_raw_text,
        case_type,
        IFNULL(level_1_supervision_location_external_id, 'EXTERNAL_UNKNOWN') AS level_1_supervision_location,
        IFNULL(level_2_supervision_location_external_id, 'EXTERNAL_UNKNOWN') AS level_2_supervision_location,
        -- TODO(#6115): Stop dropping commas once we are using a different delimiter in the export
        REPLACE(IFNULL(supervising_officer_external_id, 'EXTERNAL_UNKNOWN'), ',', '') AS officer,
        {state_specific_most_recent_officer_recommendation},
        {state_specific_recommended_for_revocation},
        violation_history_description AS violation_record,
        violation_type_frequency_counter
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_commitment_from_supervision_metrics_materialized`
    WHERE {state_specific_admission_type_inclusion_filter}
    """

EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_VIEW_NAME,
    should_materialize=True,
    view_query_template=EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_QUERY_TEMPLATE,
    description=EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    state_specific_most_recent_officer_recommendation=state_specific_query_strings.state_specific_officer_recommendation(
        input_col="most_recent_response_decision"
    ),
    state_specific_recommended_for_revocation=state_specific_query_strings.state_specific_recommended_for_revocation(),
    state_specific_admission_type_inclusion_filter=state_specific_query_strings.state_specific_admission_type_inclusion_filter(),
    state_specific_admission_type=state_specific_query_strings.state_specific_admission_type(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_VIEW_BUILDER.build_and_print()
