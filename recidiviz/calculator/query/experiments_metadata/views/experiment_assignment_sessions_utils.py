# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Contains helper functions that return view builders mapping all experiment 
assignments to all possible units of observation based on the unit type, in order to 
generate a sessionized view of experiment + variant assignment."""


from recidiviz.aggregated_metrics.assignment_sessions_view_builder import (
    get_assignment_query_for_unit_of_analysis,
    has_configured_assignment_query,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    MAGIC_START_DATE,
    list_to_query_string,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.experiments_metadata.dataset_config import (
    EXPERIMENTS_METADATA_DATASET,
)
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def get_experiment_assignment_query_for_unit_type(
    experiment_unit_of_analysis_type: MetricUnitOfAnalysisType,
    unit_of_observation_type: MetricUnitOfObservationType,
) -> str:
    """Returns a query template that maps all experiment assignments of
    a given unit of analysis type to the specified unit of observation type
    based on the assignment on the variant_date."""
    assignment_query = get_assignment_query_for_unit_of_analysis(
        experiment_unit_of_analysis_type, unit_of_observation_type
    )
    # Identify the set of index columns that should be used to join the experiment
    # unit_type to the assignment query.
    # For STATE experiment assignments, the unit_type is just state_code.
    # For all other experiment assignments, the unit type is a concatenation of the
    # unit of analysis primary key columns with the exception of state_code.
    unit_type_index_columns = (
        ["state_code"]
        if experiment_unit_of_analysis_type == MetricUnitOfAnalysisType.STATE_CODE
        else sorted(
            x
            for x in MetricUnitOfAnalysis.for_type(
                experiment_unit_of_analysis_type
            ).primary_key_columns
            if x != "state_code"
        )
    )
    unit_type_query_string = list_to_query_string(
        unit_type_index_columns, table_prefix="assignments"
    )

    return f"""
    SELECT
        experiment_id,
        variant_id,
        variant_date,
        {MetricUnitOfObservation(type=unit_of_observation_type).get_primary_key_columns_query_string(prefix="assignments")}
    FROM
        `{{project_id}}.experiments_metadata.experiment_assignments_materialized` experiments
    INNER JOIN
        ({assignment_query}) assignments
    ON
        experiments.unit_type = "{experiment_unit_of_analysis_type.value}"
        AND experiments.state_code = assignments.state_code
        AND unit_id = CONCAT({unit_type_query_string})
        AND variant_date BETWEEN assignments.start_date AND {nonnull_end_date_exclusive_clause("assignments.end_date_exclusive")}
"""


def get_experiment_assignment_sessions(
    unit_of_observation_type: MetricUnitOfObservationType,
) -> SimpleBigQueryViewBuilder:
    """Returns a view builder for the assignment query between a given experiment
    + variant and the specified unit of observation over all time, with index columns
    for the unit of observation, experiment_id, variant_id, and a flag indicating
    whether that unit has received treatment.

    The query fills in periods of time prior to the first assigned variant date with
    is_treated = FALSE. After a unit has been assigned to a variant, we keep their
    assignment session open until the next variant_date for a given experiment. This
    ensures that a unit of observation will only be assigned to a single variant at a
    time as part of an experiment. If the variant is "CONTROL", the unit will have
    is_treated = FALSE; otherwise, is_treated = TRUE after the variant date has passed.
    """

    view_id = f"experiment_assignments_{unit_of_observation_type.short_name}"
    view_description = f"View of experiment variant assignments to {unit_of_observation_type.short_name}"

    # Union together assignment queries from all experiment unit_types
    all_assignment_queries_query_fragment = "\nUNION ALL".join(
        [
            get_experiment_assignment_query_for_unit_type(
                unit_of_analysis_type, unit_of_observation_type
            )
            for unit_of_analysis_type in MetricUnitOfAnalysisType
            if has_configured_assignment_query(
                unit_of_analysis_type, unit_of_observation_type
            )
            # Only support assignment queries that have a granularity that is
            # smaller or equivalent to the STATE unit of analysis.
            and unit_of_analysis_type
            not in (
                MetricUnitOfAnalysisType.EXPERIMENT_VARIANT,
                MetricUnitOfAnalysisType.ALL_STATES,
            )
        ]
    )
    unit_of_observation_primary_keys_query_fragment = MetricUnitOfObservation(
        type=unit_of_observation_type
    ).get_primary_key_columns_query_string()
    query_template = f"""
WITH all_assignment_queries AS (
    {all_assignment_queries_query_fragment}
)
-- sessions pre-variant date
SELECT
    {unit_of_observation_primary_keys_query_fragment},
    CAST("{MAGIC_START_DATE}" AS DATE) AS start_date,
    variant_date AS end_date_exclusive,
    experiment_id,
    variant_id,
    variant_date,
    -- all units are untreated before the first variant date
    FALSE is_treated,
FROM
    all_assignment_queries
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY {unit_of_observation_primary_keys_query_fragment}, experiment_id
    ORDER BY variant_date, variant_id
) = 1

UNION ALL

-- sessions post-variant date
SELECT
    {unit_of_observation_primary_keys_query_fragment},
    variant_date AS start_date,
    LEAD(variant_date) OVER (
        PARTITION BY {unit_of_observation_primary_keys_query_fragment}, experiment_id
        ORDER BY variant_date, variant_id
    ) AS end_date_exclusive,
    experiment_id,
    variant_id,
    variant_date,
    -- Unless a unit is assigned to the CONTROL variant, all units are treated after the variant date
    variant_id NOT LIKE "%CONTROL%" AS is_treated,
FROM
    all_assignment_queries
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY {unit_of_observation_primary_keys_query_fragment}, experiment_id
    ORDER BY variant_date, variant_id
) = 1
"""
    return SimpleBigQueryViewBuilder(
        dataset_id=EXPERIMENTS_METADATA_DATASET,
        view_id=view_id,
        view_query_template=query_template,
        description=view_description,
        should_materialize=True,
        clustering_fields=["state_code", "experiment_id", "variant_id"],
    )


EXPERIMENT_ASSIGNMENT_SESSIONS_VIEW_BUILDERS: list[SimpleBigQueryViewBuilder] = [
    get_experiment_assignment_sessions(unit_of_observation_type)
    for unit_of_observation_type in MetricUnitOfObservationType
]

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        get_experiment_assignment_sessions(
            MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER
        ).build_and_print()
