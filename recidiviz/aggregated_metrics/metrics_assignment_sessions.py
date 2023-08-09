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
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulation,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
)


def generate_metric_assignment_sessions_view_builder(
    aggregation_level: MetricUnitOfAnalysis,
    population: MetricPopulation,
) -> SimpleBigQueryViewBuilder:
    """
    Takes as input the aggregation level and population.
    Returns a SimpleBigQueryViewBuilder where each row is a continuous time period during which
    a client in the population is associated with the specified aggregation level (e.g. officer).
    """

    level_name = aggregation_level.level_name_short
    population_name = population.population_name_short
    view_id = f"{population_name}_{level_name}_metrics_assignment_sessions"
    view_description = f"""Subquery that extracts appropriate rows from compartment_sessions
for use in the {level_name}_metrics table.
"""

    query_template = f"""
WITH
-- define population
sample AS (
    {population.get_population_query()}
)
-- client assignments to {level_name}
, assign AS (
    SELECT
        person_id,
        {aggregation_level.get_index_column_rename_query_string()},
        start_date,
        end_date_exclusive,
    FROM ({aggregation_level.client_assignment_query})
    WHERE
        CONCAT({aggregation_level.get_original_columns_query_string()}) IS NOT NULL
)
-- if client not always in sample population, take intersection of exclusive periods
-- to determine the start and end dates of assignment
, potentially_adjacent_spans AS (
    {create_intersection_spans(
        table_1_name="assign", 
        table_2_name="sample", 
        index_columns=["person_id"],
        include_zero_day_intersections=True,
        table_1_columns=aggregation_level.index_columns,
        table_2_columns=[]
    )}
)
,
{create_sub_sessions_with_attributes(
    table_name="potentially_adjacent_spans", 
    index_columns=["person_id"], 
    end_date_field_name="end_date_exclusive"
)}
, sub_sessions_with_attributes_distinct AS (
    SELECT DISTINCT *
    FROM sub_sessions_with_attributes
)
-- Re-sessionize all intersecting spans
, {level_name}_assignments AS (
    SELECT
        {aggregation_level.get_index_columns_query_string()},
        person_id,
        session_id,
        MIN(start_date) AS assignment_date,
        MAX({nonnull_end_date_clause("end_date_exclusive")}) AS end_date_exclusive,
    FROM (
        SELECT
            * EXCEPT(date_gap),
            SUM(IF(date_gap, 1, 0)) OVER (
                PARTITION BY {aggregation_level.get_index_columns_query_string()}, person_id 
                ORDER BY start_date, {nonnull_end_date_clause("end_date_exclusive")}
            ) AS session_id,
        FROM (
            SELECT
                *,
                IFNULL(
                    LAG(end_date_exclusive) OVER(
                        PARTITION BY {aggregation_level.get_index_columns_query_string()}, person_id 
                        ORDER BY start_date, {nonnull_end_date_clause("end_date_exclusive")}
                    ) != start_date, TRUE
                ) AS date_gap,
            FROM
                sub_sessions_with_attributes_distinct
        )
    )
    GROUP BY {aggregation_level.get_index_columns_query_string()}, person_id, session_id
)
SELECT 
    * EXCEPT(session_id, end_date_exclusive),
    {revert_nonnull_end_date_clause("end_date_exclusive")} AS end_date,
    {revert_nonnull_end_date_clause("end_date_exclusive")} AS end_date_exclusive,
FROM {level_name}_assignments
"""
    return SimpleBigQueryViewBuilder(
        dataset_id=AGGREGATED_METRICS_DATASET_ID,
        view_id=view_id,
        view_query_template=query_template,
        description=view_description,
        clustering_fields=aggregation_level.primary_key_columns,
        should_materialize=True,
    )
