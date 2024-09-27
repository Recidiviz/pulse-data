# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Generates view builder creating spans of assignment to a level of analysis for a specified population"""
from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
    create_sub_sessions_with_attributes,
    list_to_query_string,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    POPULATION_TYPE_TO_SPAN_SELECTOR_BY_UNIT_OF_OBSERVATION,
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    METRIC_UNITS_OF_ANALYSIS_BY_TYPE,
    MetricUnitOfAnalysisType,
    get_assignment_query_for_unit_of_analysis,
)
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)


def generate_metric_assignment_sessions_view_builder(
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    unit_of_observation_type: MetricUnitOfObservationType,
    population_type: MetricPopulationType,
) -> SimpleBigQueryViewBuilder:
    """
    Takes as input a unit of analysis (indicating the type of aggregation), a unit of observation
    (indicating the type of unit that is assigned to the unit of aggregation), and population.
    Returns a SimpleBigQueryViewBuilder where each row is a continuous time period during which
    a unit of observation is associated with the specified aggregation level indicated by the unit of analysis.
    """
    unit_of_analysis = METRIC_UNITS_OF_ANALYSIS_BY_TYPE[unit_of_analysis_type]
    unit_of_observation = MetricUnitOfObservation(type=unit_of_observation_type)

    unit_of_analysis_name = unit_of_analysis_type.short_name
    unit_of_observation_name = unit_of_observation_type.short_name

    population_name = population_type.population_name_short
    view_id = f"{population_name}_{unit_of_analysis_name}_metrics_{unit_of_observation_name}_assignment_sessions"
    view_description = f"""Query that extracts sessionized views of the relationship between units of analysis
for use in the {unit_of_analysis_name}_metrics table, based on {unit_of_observation_name} assignments to 
{unit_of_analysis_name}.
"""
    # list of all primary key columns from unit of observation that aren't already one of the primary key columns
    # of the unit of analysis
    child_primary_key_columns = [
        col
        for col in sorted(unit_of_observation.primary_key_columns)
        if col not in unit_of_analysis.primary_key_columns
    ]
    child_primary_key_columns_query_string = (
        list_to_query_string(child_primary_key_columns) + ",\n"
        if child_primary_key_columns
        else ""
    )

    population_query = POPULATION_TYPE_TO_SPAN_SELECTOR_BY_UNIT_OF_OBSERVATION[
        population_type
    ][unit_of_observation_type].generate_span_selector_query()

    query_template = f"""
WITH
-- define population in terms of unit of observation
sample AS (
    {population_query}
)
-- {unit_of_observation_name} assignments to {unit_of_analysis_name}
, assign AS (
    SELECT
        {child_primary_key_columns_query_string}{unit_of_analysis.get_primary_key_columns_query_string()},
        start_date,
        end_date_exclusive,
        1 AS dummy,
    FROM ({get_assignment_query_for_unit_of_analysis(unit_of_analysis_type, unit_of_observation_type)})
    WHERE
        CONCAT({unit_of_analysis.get_primary_key_columns_query_string()}) IS NOT NULL
)
-- if assigned unit not always in sample population, take intersection of exclusive periods
-- to determine the start and end dates of assignment
, potentially_adjacent_spans AS (
    {create_intersection_spans(
        table_1_name="assign", 
        table_2_name="sample", 
        index_columns=sorted(unit_of_observation.primary_key_columns),
        include_zero_day_intersections=True,
        table_1_columns=[col for col in unit_of_analysis.primary_key_columns if col not in sorted(unit_of_observation.primary_key_columns)] + ["dummy"],
        table_2_columns=[]
    )}
)
,
{create_sub_sessions_with_attributes(
    table_name="potentially_adjacent_spans", 
    index_columns=sorted(unit_of_observation.primary_key_columns), 
    end_date_field_name="end_date_exclusive"
)}
, sub_sessions_with_attributes_distinct AS (
    SELECT DISTINCT *
    FROM sub_sessions_with_attributes
)
-- Re-sessionize all intersecting spans
, {unit_of_analysis_name}_assignments AS (
    SELECT
        {child_primary_key_columns_query_string}{unit_of_analysis.get_primary_key_columns_query_string()},
        session_id,
        MIN(start_date) AS assignment_date,
        MAX({nonnull_end_date_clause("end_date_exclusive")}) AS end_date_exclusive,
    FROM (
        SELECT
            * EXCEPT(date_gap),
            SUM(IF(date_gap, 1, 0)) OVER (
                PARTITION BY {child_primary_key_columns_query_string}{unit_of_analysis.get_primary_key_columns_query_string()}
                ORDER BY start_date, {nonnull_end_date_clause("end_date_exclusive")}
            ) AS session_id,
        FROM (
            SELECT
                *,
                IFNULL(
                    LAG(end_date_exclusive) OVER(
                        PARTITION BY {child_primary_key_columns_query_string}{unit_of_analysis.get_primary_key_columns_query_string()}
                        ORDER BY start_date, {nonnull_end_date_clause("end_date_exclusive")}
                    ) != start_date, TRUE
                ) AS date_gap,
            FROM
                sub_sessions_with_attributes_distinct
        )
    )
    GROUP BY {child_primary_key_columns_query_string}{unit_of_analysis.get_primary_key_columns_query_string()}, session_id
)
SELECT 
    * EXCEPT(session_id, end_date_exclusive),
    {revert_nonnull_end_date_clause("end_date_exclusive")} AS end_date,
    {revert_nonnull_end_date_clause("end_date_exclusive")} AS end_date_exclusive,
FROM {unit_of_analysis_name}_assignments
"""
    return SimpleBigQueryViewBuilder(
        dataset_id=AGGREGATED_METRICS_DATASET_ID,
        view_id=view_id,
        view_query_template=query_template,
        description=view_description,
        clustering_fields=sorted(unit_of_analysis.primary_key_columns),
        should_materialize=True,
    )
