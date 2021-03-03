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
"""Provides a template for calculating a Justice Counts metric by month."""

from typing import Dict, Iterable, List, Type

import attr

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.justice_counts import dataset_config
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.tools.justice_counts import manual_upload


CALCULATED_METRIC_BY_MONTH_TEMPLATE = """
/*{description}*/

-- FETCH DATA
-- Gets all of the table definitions that provide sufficient data to be used to calculate the given metric. This ensures
-- the data has all of the dimensions we need and isn't filtered further than is acceptable.
WITH sufficient_table_defs as (
  SELECT *
  FROM `{project_id}.{base_dataset}.report_table_definition_materialized`
  WHERE
    system = '{system}' AND metric_type = '{metric_type}' AND measurement_type in ('AVERAGE', 'DELTA', 'INSTANT') AND
    -- Exclude table definitions with filters on other dimensions. Note, we don't actually check the dimension value of
    -- the filter at this point.
    NOT EXISTS(
      SELECT filtered_dimensions FROM UNNEST(filtered_dimensions) AS filtered_dimension
      WHERE filtered_dimension NOT IN ({input_allowed_filters})
    )
    -- Exclude table definitions that don't have all the necessary dimensions.
    {input_required_dimensions_clause}
),

-- Gets the cells for these table definitions and flattens the filtered and aggregated dimensions into a single set of
-- dimensions, represented as an array of dimension identifiers and a parallel array of values.
flattened as (
  SELECT
    instance.id as instance_id, cell.id as cell_id,
    ARRAY_CONCAT(definition.filtered_dimensions, definition.aggregated_dimensions) as dimensions,
    ARRAY_CONCAT(definition.filtered_dimension_values, cell.aggregated_dimension_values) as dimension_values,
    cell.value
  FROM sufficient_table_defs definition
  JOIN `{project_id}.{base_dataset}.report_table_instance_materialized` instance
    ON instance.report_table_definition_id = definition.id
  JOIN `{project_id}.{base_dataset}.cell_materialized` cell
    ON cell.report_table_instance_id = instance.id
),

-- Zips the two arrays into an array of (dimension, dimension_value) structs.
flattened_zipped as (
  SELECT
    instance_id, cell_id,
    -- Aggregate the unnested dimensions into a single array for that cell, pulling in the dimension value as well
    ARRAY_AGG(STRUCT<dimension string, dimension_value string>(dimension, dimension_values[OFFSET(dimension_offset)])) as dimension_values,
    -- Each cell only has a single value, which has been exploded out to multiple rows. Since we are grouping by cell we
    -- can just pick any value since they are all the same
    ANY_VALUE(value) as value
  FROM flattened, UNNEST(dimensions) AS dimension WITH OFFSET dimension_offset
  GROUP BY instance_id, cell_id
),

-- FILTER DATA
-- Filters the cells to only those that match the provided filters.
filtered_values as (
  SELECT instance_id, cell_id, dimension_values, value
  FROM (
    SELECT
      *, ARRAY(
        SELECT dimension_value FROM UNNEST(dimension_values)
        -- Filters the dimension array, only keeping ones that match the filters.
        WHERE {dimensions_match_filter_clause}
      ) as matching_filters
    FROM flattened_zipped
  ) as matched
  -- Only keep cells where with all filters present.
  WHERE ARRAY_LENGTH(matched.matching_filters) = {num_filtered_dimensions}
),

-- SPATIAL AGGREGATION
-- Note: Right now we do spatial then time which works because we don't ever do spatial aggregation across tables.
aggregated_values as (
  SELECT
    instance_id,
    ANY_VALUE(grouped_dimensions) as dimensions,
    ANY_VALUE(num_original_dimensions) as num_original_dimensions,
    ARRAY_CONCAT_AGG(collapsed_dimension_values) as collapsed_dimension_values,
    SUM(value) as value
  FROM (
    SELECT
      instance_id, cell_id, ARRAY_LENGTH(dimension_values) as num_original_dimensions,
      -- Rebuild the dimensions array, only including dimensions we want to aggregate by
      ARRAY(
        SELECT STRUCT<dimension string, dimension_value string>(dimension, dimension_value) FROM UNNEST(dimension_values)
        WHERE dimension IN ({aggregated_dimension_identifiers})
      ) as grouped_dimensions,
      ARRAY(
        SELECT dimension_value FROM UNNEST(dimension_values)
        WHERE ENDS_WITH(dimension, '/raw')
      ) as collapsed_dimension_values,
      value
    FROM filtered_values) as grouped
  -- BigQuery doesn't allow grouping by an array, so we build a string instead. Uses '|' as a delimeter, relying on this
  -- not being present in dimension values to avoid conflicts.
  GROUP BY
    instance_id,
    ARRAY_TO_STRING(ARRAY(SELECT dimension_value FROM UNNEST(grouped_dimensions) ORDER BY dimension), '|')
),

-- TIME AGGREGATION
-- Get necessary columns from the table instance's parents.
joined_data as (
  SELECT
    report.source_id as source_id, report.id as report_id, report.type as report_type, report.publish_date as publish_date,
    definition.id as definition_id, definition.measurement_type,
    instance.id as instance_id,
    time_window_start, time_window_end, DATE_TRUNC(DATE_SUB(time_window_end, INTERVAL 1 DAY), MONTH) as start_of_month,
    dimensions,
    ARRAY_TO_STRING(ARRAY(SELECT dimension_value FROM UNNEST(dimensions) ORDER BY dimension), '|') as dimensions_string,
    num_original_dimensions,
    collapsed_dimension_values,
    value
  FROM `{project_id}.{base_dataset}.report_table_instance_materialized` instance
  JOIN `{project_id}.{base_dataset}.report_table_definition_materialized` definition
    ON definition.id = report_table_definition_id
  JOIN `{project_id}.{base_dataset}.report_materialized` report
    ON instance.report_id = report.id
  JOIN aggregated_values
    ON aggregated_values.instance_id = instance.id
),

-- Aggregate values for the same (source, report type, table definition, start_of_month)
--  - DELTA: Sum them
--  - INSTANT: Pick one
combined_periods_data as (
  -- TODO(#5517): ensure non-overlapping, contiguous blocks (e.g. four weeks of a month)
  SELECT
    source_id, report_type, ARRAY_AGG(report_id) as report_ids, MAX(publish_date) as publish_date,
    definition_id, ANY_VALUE(measurement_type) as measurement_type,
    start_of_month, MIN(time_window_start) as time_window_start, MAX(time_window_end) as time_window_end,
    -- Since BQ won't allow us to group by dimensions, we are grouping by dimensions_string and just pick any dimensions
    -- since they are all the same for a single dimensions_string.
    ANY_VALUE(dimensions) as dimensions,
    dimensions_string,
    -- This will be the same since we grouped by defintion_id
    ANY_VALUE(num_original_dimensions) as num_original_dimensions,
    ARRAY_CONCAT_AGG(collapsed_dimension_values) as collapsed_dimension_values,
    SUM(value) as value
  FROM (
    -- TODO(#5517): order by how much of the month it covers, then time_window_end, then publish date?
    -- Prioritize data that is the most recent for a month (time_window_end), then data that was published most recently
    -- (publish_date), then data from definitions with the fewest dimensions as that will help us accidentally avoid
    -- missing categories (num_original_dimensions)
    SELECT *, ROW_NUMBER()
      OVER(PARTITION BY source_id, report_type, definition_id, start_of_month, dimensions_string
           ORDER BY time_window_end DESC, publish_date DESC, num_original_dimensions ASC) as ordinal
    FROM joined_data
  ) as partitioned
  -- If it is DELTA then sum them, otherwise just pick one
  -- Note: For AVERAGE we could do a weighted average instead, but for now dropping is okay
  WHERE measurement_type = 'DELTA' OR partitioned.ordinal = 1
  GROUP BY source_id, report_type, definition_id, start_of_month, dimensions_string
),

-- Pick a single value for each dimension value, month combination
-- Note: This means that we could pick different sources for different values of a single comprehensive dimension.
-- Initially here we were grouping by (state, month) but now all dimensions are included. What we might want is to only
-- use non comprehensive dimensions here so that we can enforce that the same source is used for each value
-- of a comprehensive dimension. We could achieve this by doing time aggregation only on report table instances and then
-- bring in cells to do the spatial aggregation. Or we could split this off, group by non comprehensive dimensions and
-- aggregate comprehensive dimensions, and take into account the coverage of dimension values when deciding which to
-- use (e.g. one source reports on more race categories than another).
dropped_periods_data as (
  SELECT source_id, report_type, report_ids, start_of_month, time_window_start, time_window_end,
         dimensions, dimensions_string, collapsed_dimension_values, value
  FROM (
    -- TODO(#5517): order by how much of the month it covers, then time_window_end, then publish date?
    SELECT *, ROW_NUMBER()
      OVER(PARTITION BY dimensions_string, start_of_month
           ORDER BY time_window_end DESC, publish_date DESC, num_original_dimensions ASC) as ordinal
    FROM (
      -- For now, just filter out any DELTA points that don't cover the same number of days as the month
      SELECT * FROM combined_periods_data
      WHERE measurement_type != 'DELTA' OR (DATE_DIFF(time_window_end, time_window_start, DAY) = EXTRACT(DAY FROM LAST_DAY(start_of_month)))
    ) as filtered_to_month
  -- TODO(#5517): If this covers more than just that month, do we want to drop more as well? Or are we okay with window
  -- being quarter but output period being month? Same question for if it covers less?
  ) as partitioned
  WHERE partitioned.ordinal = 1
),

-- COMPARISON
-- Compare against the most recent data that is at least one year older
compared_data as (
  SELECT source_id, report_type, report_ids, start_of_month, time_window_end,
         dimensions, collapsed_dimension_values, value, compare_start_of_month, compare_value
  FROM (
    SELECT
      base_data.source_id, base_data.report_type, base_data.report_ids, base_data.start_of_month, base_data.time_window_end,
      base_data.dimensions, base_data.collapsed_dimension_values, base_data.value,
      compared_data1.start_of_month as compare_start_of_month, compared_data1.value as compare_value,
      ROW_NUMBER() OVER (
        PARTITION BY base_data.dimensions_string, base_data.start_of_month, base_data.time_window_end
        -- Orders by recency of the compared data
        ORDER BY DATE_DIFF(base_data.start_of_month, compared_data1.start_of_month, DAY)) as ordinal
    FROM dropped_periods_data base_data
    -- Explodes to every row that is at least a year older
    LEFT JOIN dropped_periods_data compared_data1 ON
      (base_data.dimensions_string  = compared_data1.dimensions_string AND
       compared_data1.start_of_month <= DATE_SUB(base_data.start_of_month, INTERVAL 1 YEAR))
  ) as joined_with_prior
  -- Picks the row with the compared data that is the most recent
  WHERE joined_with_prior.ordinal = 1
)

SELECT {aggregated_dimensions_array_columns_to_single_value_columns_clause},
       '{metric_output_name}' as metric,
       EXTRACT(YEAR from start_of_month) as year,
       EXTRACT(MONTH from start_of_month) as month,
       DATE_SUB(time_window_end, INTERVAL 1 DAY) as date_reported,
       source.name as source_name,
       report.url as source_url,
       report_type as report_name,
       ARRAY(
         SELECT DISTINCT(collapsed_dimension_value)
         FROM UNNEST(collapsed_dimension_values) as collapsed_dimension_value
         ORDER BY collapsed_dimension_value
       ) as raw_source_categories,
       value as value,
       -- If there is no row at least a year back to compare to, the following columns will be NULL.
       EXTRACT(YEAR from compare_start_of_month) as compared_to_year,
       EXTRACT(MONTH from compare_start_of_month) as compared_to_month,
       value - compare_value as value_change,
       -- Note: This can return NULL in the case of divide by zero
       SAFE_DIVIDE((value - compare_value), compare_value) as percentage_change
FROM (
    SELECT 
        *,
        {aggregated_dimensions_array_split_to_columns_clause}
    FROM compared_data
) as unnested_dimensions
JOIN `{project_id}.{base_dataset}.source_materialized` source
  ON source_id = source.id
JOIN `{project_id}.{base_dataset}.report_materialized` report
  -- Just pick any report and use its URL
  ON report_ids[ORDINAL(1)] = report.id
ORDER BY {aggregated_dimension_columns}, metric, year DESC, month DESC
"""


@attr.s(frozen=True)
class Aggregation:
    dimension: Type[manual_upload.Dimension] = attr.ib()

    # Whether the data for this dimension must be comprehensive to be included in the output. Here comprehensive means
    # that adding up all the values for this dimension would give us the correct total.
    #
    # For example, if calculating prison population for a particular state by race it is likely that you want all
    # people to be included and attributed to a race. If data for only a subset of races reported by the state is
    # included in a particular table, that table should not be used for this calculation.
    #
    # OTOH, if calculating prison population by state, we just want to get the most state coverage we can and it is
    # acceptable if we are missing data for some states. Note, this is comprehensive across the whole dimension, not
    # within a particular value. We always expect the data for a particular value, e.g. population for a single state
    # in this case, to be correct and include all people in that population.
    #
    # The effect on the query is that data for comprehensive dimensions must come from a single table instance, whereas
    # data for non-comprehensive dimension can be assembled from multiple table instances, reports, or sources.
    comprehensive: bool = attr.ib()


@attr.s(frozen=True)
class CalculatedMetricByMonth:
    """Represents a metric and describes how to calculate it"""

    system: schema.System = attr.ib()
    metric: schema.MetricType = attr.ib()

    # Only include data that matches these filters.
    filtered_dimensions: List[manual_upload.Dimension] = attr.ib()

    # Aggregate the data, keeping these dimensions. The key is used as the name of the column in the output.
    aggregated_dimensions: Dict[str, Aggregation] = attr.ib()

    output_name: str = attr.ib()

    @property
    def view_prefix(self) -> str:
        return self.output_name.lower()

    @property
    def _comprehensive_aggregations(self) -> List[Type[manual_upload.Dimension]]:
        return [
            aggregation.dimension
            for aggregation in self.aggregated_dimensions.values()
            if aggregation.comprehensive
        ]

    @property
    def _noncomprehensive_aggregations(self) -> List[Type[manual_upload.Dimension]]:
        return [
            aggregation.dimension
            for aggregation in self.aggregated_dimensions.values()
            if not aggregation.comprehensive
        ]

    @property
    def input_allowed_filters(self) -> List[Type[manual_upload.Dimension]]:
        """Filters that a table definition can have and still be used as input for this calculation."""
        return [
            type(filtered_dimension) for filtered_dimension in self.filtered_dimensions
        ] + self._noncomprehensive_aggregations

    @property
    def input_required_aggregations(self) -> Iterable[Aggregation]:
        """Dimensions that a table definition must include to be used as input for this calculation."""
        return self.aggregated_dimensions.values()


class CalculatedMetricByMonthViewBuilder(SimpleBigQueryViewBuilder):
    """Factory class for building views that calculate metrics by month."""

    def __init__(
        self, *, dataset_id: str, metric_to_calculate: CalculatedMetricByMonth
    ):
        # For determining which table definitions can be used.
        input_allowed_filters = ", ".join(
            [
                f"'{dimension.dimension_identifier()}'"
                for dimension in metric_to_calculate.input_allowed_filters
            ]
        )

        input_required_dimension_conditions: List[str] = []
        for aggregation in metric_to_calculate.input_required_aggregations:
            columns = ["aggregated_dimensions"]
            if not aggregation.comprehensive:
                columns.append("filtered_dimensions")
            input_required_dimension_conditions.append(
                f"AND '{aggregation.dimension.dimension_identifier()}' "
                f"IN UNNEST(ARRAY_CONCAT({', '.join(columns)}))"
            )
        input_required_dimensions_clause = " ".join(input_required_dimension_conditions)

        # For filtering data
        if metric_to_calculate.filtered_dimensions:
            dimensions_match_filter_clause = " OR ".join(
                [
                    f"(dimension = '{dimension.dimension_identifier()}' "
                    f"AND dimension_value = '{dimension.dimension_value}')"
                    for dimension in metric_to_calculate.filtered_dimensions
                ]
            )
        else:
            dimensions_match_filter_clause = "FALSE"

        # For spatial aggregation
        aggregated_dimension_identifiers = ", ".join(
            [
                f"'{aggregation.dimension.dimension_identifier()}'"
                for aggregation in metric_to_calculate.aggregated_dimensions.values()
            ]
        )

        # For moving aggregate dimensions to columns in output
        aggregated_dimensions_array_columns_to_single_value_columns_clause = ", ".join(
            [
                f"unnested_dimensions.{column_name}_array[ORDINAL(1)] as {column_name}"
                for column_name in metric_to_calculate.aggregated_dimensions
            ]
        )
        aggregated_dimensions_array_split_to_columns_clause = ", ".join(
            [
                f"ARRAY(SELECT dimension_value FROM UNNEST(dimensions) "
                f"WHERE dimension = '{aggregation.dimension.dimension_identifier()}') as {column_name}_array"
                for column_name, aggregation in metric_to_calculate.aggregated_dimensions.items()
            ]
        )

        aggregated_dimension_columns = ", ".join(
            metric_to_calculate.aggregated_dimensions
        )

        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{metric_to_calculate.view_prefix}_by_month",
            view_query_template=CALCULATED_METRIC_BY_MONTH_TEMPLATE,
            should_materialize=True,
            # Query Format Arguments
            description=f"{metric_to_calculate.output_name} by month",
            base_dataset=dataset_config.JUSTICE_COUNTS_BASE_DATASET,
            system=metric_to_calculate.system.value,
            metric_type=metric_to_calculate.metric.value,
            input_allowed_filters=input_allowed_filters,
            input_required_dimensions_clause=input_required_dimensions_clause,
            dimensions_match_filter_clause=dimensions_match_filter_clause,
            num_filtered_dimensions=str(len(metric_to_calculate.filtered_dimensions)),
            aggregated_dimension_identifiers=aggregated_dimension_identifiers,
            aggregated_dimensions_array_columns_to_single_value_columns_clause=aggregated_dimensions_array_columns_to_single_value_columns_clause,
            aggregated_dimensions_array_split_to_columns_clause=aggregated_dimensions_array_split_to_columns_clause,
            metric_output_name=metric_to_calculate.output_name,
            aggregated_dimension_columns=aggregated_dimension_columns,
        )
