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

import enum
from typing import Dict, Iterable, List, Set, Type

import attr

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.calculator.query.justice_counts import dataset_config
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.tools.justice_counts import manual_upload


FETCH_AND_FILTER_METRIC_VIEW_TEMPLATE = """
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

-- Gets the cells for these table definitions and flattens the filtered and aggregated
-- dimensions into a single set of dimensions, represented as an array of dimension
-- identifiers and a parallel array of values.
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
    -- Aggregate the unnested dimensions into a single array for that cell, pulling in
    -- the dimension value as well
    ARRAY_AGG(
        STRUCT<dimension string, dimension_value string>(dimension, dimension_values[OFFSET(dimension_offset)])
    ) as dimensions,
    -- Each cell only has a single value, which has been exploded out to multiple rows.
    -- Since we are grouping by cell we can just pick any value since they are all the same
    ANY_VALUE(value) as value
  FROM flattened, UNNEST(dimensions) AS dimension WITH OFFSET dimension_offset
  GROUP BY instance_id, cell_id
)

-- FILTER DATA
-- Filters the cells to only those that match the provided filters.
SELECT instance_id, cell_id, dimensions, value
FROM (
  SELECT
    *, ARRAY(
      SELECT dimension_value FROM UNNEST(dimensions)
      -- Filters the dimension array, only keeping ones that match the filters.
      WHERE {dimensions_match_filter_clause}
    ) as matching_filters
  FROM flattened_zipped
) as matched
-- Only keep cells where with all filters present.
WHERE ARRAY_LENGTH(matched.matching_filters) = {num_filtered_dimensions}
"""


class FetchAndFilterMetricViewBuilder(SimpleBigQueryViewBuilder):
    """Factory class for building views that fetch and filter to relevant data for a metric.

    Reads directly from the justice counts base dataset. Output has the following columns:
    * instance_id: The report instance that the data is from
    * cell_id: The cell the data is from
    * dimensions: The dimensions of the data, including both filtered and aggregated dimensions
        in the form ARRAY<STRUCT<dimension string, dimension_value string>>

    Example Output:

    | instance_id | cell_id | dimensions                                                                                    | value |
    | ----------- | ------- | --------------------------------------------------------------------------------------------- | ----- |
    | 1           | 1       | [("global/location/state", "US_XX")]                                                          | 4     |
    | 1           | 2       | [("global/location/state", "US_YY")]                                                          | 5     |
    | 2           | 3       | [("global/location/state", "US_ZZ"), ("global/gender", "FEMALE"), ("global/gender/raw", "F")] | 6     |
    """

    def __init__(
        self, *, dataset_id: str, metric_to_calculate: "CalculatedMetricByMonth"
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

        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{metric_to_calculate.view_prefix}_fetch",
            view_query_template=FETCH_AND_FILTER_METRIC_VIEW_TEMPLATE,
            # Query Format Arguments
            description=f"{metric_to_calculate.output_name} raw data",
            base_dataset=dataset_config.JUSTICE_COUNTS_BASE_DATASET,
            system=metric_to_calculate.system.value,
            metric_type=metric_to_calculate.metric.value,
            input_allowed_filters=input_allowed_filters,
            input_required_dimensions_clause=input_required_dimensions_clause,
            dimensions_match_filter_clause=dimensions_match_filter_clause,
            num_filtered_dimensions=str(len(metric_to_calculate.filtered_dimensions)),
        )


SPATIAL_AGGREGATION_VIEW_TEMPLATE = """
/*{description}*/

SELECT
    {partition_columns_clause}{select_columns_clause}
    ANY_VALUE(num_original_dimensions) as num_original_dimensions,
    ANY_VALUE(grouped_dimensions) AS dimensions,
    dimensions_string,
    ARRAY_CONCAT_AGG(
        collapsed_dimension_values ORDER BY ARRAY_TO_STRING(collapsed_dimension_values, '|')
    ) AS collapsed_dimension_values,
    {value_columns_clause}
FROM (
    SELECT
        *,
        -- BigQuery doesn't allow grouping by an array, so we build a string instead.
        -- Uses '|' as a delimeter, relying on this not being present in dimension
        -- values to avoid conflicts.
        ARRAY_TO_STRING(ARRAY(SELECT dimension_value FROM UNNEST(grouped_dimensions)
                              ORDER BY dimension), '|') AS dimensions_string
    FROM (
        SELECT
            {all_columns_clause}
            ARRAY_LENGTH(dimensions) as num_original_dimensions,
            ARRAY(
                SELECT dimension_struct
                FROM UNNEST(dimensions) dimension_struct
                -- If should_group is False, then this is an original row we should keep
                -- as is, otherwise we should only keep the specified dimensions.
                WHERE NOT should_group OR dimension_struct.dimension IN ({dimensions_to_keep_clause})
            ) AS grouped_dimensions,
            ARRAY(
                SELECT dimension_value
                FROM UNNEST(dimensions)
                WHERE {collapse_dimensions_filter}
            ) AS collapsed_dimension_values
        FROM
            `{project_id}.{input_dataset}.{input_table}`,
            UNNEST(ARRAY[{should_group_values}]) AS should_group
    ) as filtered_dimensions
) as filtered_dimensions_with_dimensions_string
GROUP BY {partition_columns_clause}dimensions_string
"""


class SpatialAggregationViewBuilder(SimpleBigQueryViewBuilder):
    """Aggregates the data, keeping only the specified dimensions and summing the values.

    Requires the input table to have a `dimensions` column that is of type
    ARRAY<STRUCT<dimension string, dimension_value string>>. Any other columns that
    should be kept in the output must be included in one of the builder arguments.

    Output will also contain the following columns:
    * dimensions_string: the dimensions values concatenated into a string, used to group by
    * num_original_dimensions: the number of dimensions the row had prior to aggregation
    * collapsed_dimension_values: the values from any collapsed dimensions

    **Simple Example:**

    Arguments:
    * partition_columns={"date"}
    * context_columns={}
    * dimensions_to_keep={manual_upload.State}
    * value_columns={"value"}

    Input Table:
    | date       | dimensions                                                        | value |
    | ---------- | ----------------------------------------------------------------- | ----- |
    | 2020-12-01 | [('global/location/state', 'US_XX'), ('global/gender', 'MALE')]   | 5     |
    | 2020-12-01 | [('global/location/state', 'US_XX'), ('global/gender', 'FEMALE')] | 1     |
    | 2020-12-01 | [('global/location/state', 'US_YY'), ('global/gender', 'MALE')]   | 10    |
    | 2020-12-01 | [('global/location/state', 'US_YY'), ('global/gender', 'FEMALE')] | 2     |
    | 2021-01-01 | [('global/location/state', 'US_XX'), ('global/gender', 'MALE')]   | 8     |
    | 2021-01-01 | [('global/location/state', 'US_XX'), ('global/gender', 'FEMALE')] | 1     |

    Output Table:
    | date       | dimensions                           | value | num_original_dimensions | dimensions_string | collapsed_dimension_values |
    | ---------- | ------------------------------------ | ----- | ----------------------- | ----------------- | -------------------------- |
    | 2020-12-01 | [('global/location/state', 'US_XX')] | 6     | 2                       | US_XX             | []                         |
    | 2020-12-01 | [('global/location/state', 'US_YY')] | 12    | 2                       | US_YY             | []                         |
    | 2021-01-01 | [('global/location/state', 'US_XX')] | 9     | 2                       | US_XX             | []                         |

    **Extended Example:**

    Arguments:
    * partition_columns={"date"}
    * context_columns={"source_id": ContextAggregation.ARRAY}
    * dimensions_to_keep={manual_upload.State}
    * value_columns={"value"}
    * collapse_dimensions_filter="dimension = 'global/gender'"
    * keep_original=True

    Input Table:
    | date       | source_id | dimensions                                                        | value |
    | ---------- | --------- | ----------------------------------------------------------------- | ----- |
    | 2020-12-01 | 1         | [('global/location/state', 'US_XX'), ('global/gender', 'MALE')]   | 5     |
    | 2020-12-01 | 1         | [('global/location/state', 'US_XX'), ('global/gender', 'FEMALE')] | 1     |
    | 2020-12-01 | 2         | [('global/location/state', 'US_YY'), ('global/gender', 'MALE')]   | 10    |
    | 2020-12-01 | 3         | [('global/location/state', 'US_YY'), ('global/gender', 'FEMALE')] | 2     |
    | 2021-01-01 | 1         | [('global/location/state', 'US_XX'), ('global/gender', 'MALE')]   | 8     |
    | 2021-01-01 | 1         | [('global/location/state', 'US_XX'), ('global/gender', 'FEMALE')] | 1     |

    Output Table:
    | date       | source_id | dimensions                                                        | value | num_original_dimensions | dimensions_string | collapsed_dimension_values |
    | ---------- | --------- | ----------------------------------------------------------------- | ----- | ----------------------- | ----------------- | -------------------------- |
    | 2020-12-01 | [1]       | [('global/location/state', 'US_XX'), ('global/gender', 'MALE')]   | 5     | 2                       | US_XX|MALE        | ['MALE']                   |
    | 2020-12-01 | [1]       | [('global/location/state', 'US_XX'), ('global/gender', 'FEMALE')] | 1     | 2                       | US_XX|FEMALE      | ['FEMALE']                 |
    | 2020-12-01 | [1]       | [('global/location/state', 'US_XX')]                              | 6     | 2                       | US_XX             | ['MALE', 'FEMALE']         |
    | 2020-12-01 | [2]       | [('global/location/state', 'US_YY'), ('global/gender', 'MALE')]   | 10    | 2                       | US_XX|MALE        | ['MALE']                   |
    | 2020-12-01 | [3]       | [('global/location/state', 'US_YY'), ('global/gender', 'FEMALE')] | 2     | 2                       | US_XX|FEMALE      | ['FEMALE']                 |
    | 2020-12-01 | [2, 3]    | [('global/location/state', 'US_YY')]                              | 12    | 2                       | US_YY             | ['MALE', 'FEMALE']         |
    | 2021-01-01 | [1]       | [('global/location/state', 'US_XX'), ('global/gender', 'MALE')]   | 8     | 2                       | US_XX|MALE        | ['MALE']                   |
    | 2021-01-01 | [1]       | [('global/location/state', 'US_XX'), ('global/gender', 'FEMALE')] | 1     | 2                       | US_XX|FEMALE      | ['FEMALE']                 |
    | 2021-01-01 | [1]       | [('global/location/state', 'US_XX')]                              | 9     | 2                       | US_XX             | ['MALE', 'FEMALE']         |
    """

    class ContextAggregation(enum.Enum):
        ANY = "ANY"
        ARRAY = "ARRAY"
        ARRAY_CONCAT = "ARRAY_CONCAT"
        MAX = "MAX"
        MIN = "MIN"

    def __init__(
        self,
        *,
        dataset_id: str,
        metric_name: str,
        input_view: BigQueryViewBuilder,
        partition_columns: Set[str],
        partition_dimensions: Set[Type[manual_upload.Dimension]],
        context_columns: Dict[str, ContextAggregation],
        value_columns: Set[str],
        collapse_dimensions_filter: str = "FALSE",
        keep_original: bool = False,
    ):
        """Initializes the view builder and query based on the specified aggregation options.

        Args:
            partition_columns: Columns that should not be aggregated across; these
                are included in the group by.
            partition_dimensions: Dimensions that should not be aggregated across;
                these will be included in the group by, all other dimensions will be
                dropped and aggregated across.
            context_columns: Columns that should be aggregated across, but not
                summed, in order to provide more context to a given metric result row.
            value_columns: Columns that contain the values to sum to produce a metric
                value; these will be summed.
            collapse_dimensions_filter: A clause used to include any dimension values
                in an array to keep around for context. Defaults to not including any
                dimension values.
            keep_original: If true, keeps the unaggregated values around as their own
                rows in addition to the aggregated rows. Defaults to False.
        """
        all_columns: List[str] = list(partition_columns)

        select_clauses: List[str] = []
        for column, aggregation in context_columns.items():
            if aggregation is self.ContextAggregation.ANY:
                formatter = "ANY_VALUE({column})"
            elif aggregation is self.ContextAggregation.ARRAY:
                formatter = "ARRAY_AGG(DISTINCT {column})"
            elif aggregation is self.ContextAggregation.ARRAY_CONCAT:
                formatter = "ARRAY_CONCAT_AGG({column})"
            elif aggregation is self.ContextAggregation.MAX:
                formatter = "MAX({column})"
            elif aggregation is self.ContextAggregation.MIN:
                formatter = "MIN({column})"
            else:
                raise ValueError(f"Unsupported context aggregation: {aggregation}")
            select_clauses.append(formatter.format(column=column) + f" AS {column}")
        all_columns.extend(context_columns)

        value_columns_clause = ", ".join(
            f"SUM({column}) AS {column}" for column in value_columns
        )
        all_columns.extend(value_columns)

        dimensions_to_keep_clause = ", ".join(
            f"'{dimension.dimension_identifier()}'"
            for dimension in partition_dimensions
        )

        should_group_values = "TRUE"
        if keep_original:
            should_group_values += ", FALSE"

        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{view_prefix_for_metric_name(metric_name)}_dropped_{'_'.join(sorted(dimension.__name__.lower() for dimension in partition_dimensions))}",
            view_query_template=SPATIAL_AGGREGATION_VIEW_TEMPLATE,
            # Query Format Arguments
            description=f"{metric_name} aggregated by dimensions",
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
            all_columns_clause="".join(f"{column}, " for column in all_columns),
            partition_columns_clause="".join(
                f"{column}, " for column in partition_columns
            ),
            select_columns_clause="".join(f"{clause}, " for clause in select_clauses),
            value_columns_clause=value_columns_clause,
            dimensions_to_keep_clause=dimensions_to_keep_clause,
            collapse_dimensions_filter=collapse_dimensions_filter,
            should_group_values=should_group_values,
        )


AGGREGATE_TO_MONTH_VIEW_TEMPLATE = """
/*{description}*/

WITH joined_data as (
  SELECT
    report.source_id as source_id,
    report.id as report_id,
    report.type as report_type,
    report.publish_date as publish_date,
    definition.id as definition_id,
    definition.measurement_type,
    instance.id as instance_id,
    instance.time_window_start,
    instance.time_window_end,
    DATE_TRUNC(DATE_SUB(instance.time_window_end, INTERVAL 1 DAY), MONTH) as start_of_month,
    input.dimensions,
    input.dimensions_string,
    input.num_original_dimensions,
    input.collapsed_dimension_values,
    input.value
  FROM `{project_id}.{base_dataset}.report_table_instance_materialized` instance
  JOIN `{project_id}.{base_dataset}.report_table_definition_materialized` definition
    ON definition.id = report_table_definition_id
  JOIN `{project_id}.{base_dataset}.report_materialized` report
    ON instance.report_id = report.id
  JOIN `{project_id}.{input_dataset}.{input_table}` input
    ON input.instance_id = instance.id
),

-- Aggregate values for the same (source, report type, table definition, start_of_month)
--  - DELTA: Sum them
--  - INSTANT: Pick one
combined_periods_data as (
  -- TODO(#5517): ensure non-overlapping, contiguous blocks (e.g. four weeks of a month)
  SELECT
    source_id, report_type, ARRAY_AGG(DISTINCT report_id) as report_ids, MAX(publish_date) as publish_date,
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
)

-- Pick a single value for each dimension value, month combination
-- Note: This means that we could pick different sources for different values of a single comprehensive dimension.
-- Initially here we were grouping by (state, month) but now all dimensions are included. What we might want is to only
-- use non comprehensive dimensions here so that we can enforce that the same source is used for each value
-- of a comprehensive dimension. We could achieve this by doing time aggregation only on report table instances and then
-- bring in cells to do the spatial aggregation. Or we could split this off, group by non comprehensive dimensions and
-- aggregate comprehensive dimensions, and take into account the coverage of dimension values when deciding which to
-- use (e.g. one source reports on more race categories than another).
SELECT source_id, report_type, report_ids, publish_date, start_of_month, time_window_start, time_window_end,
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
"""


class MetricByMonthViewBuilder(SimpleBigQueryViewBuilder):
    """Factory class for building views that calculate metrics by month.

    This takes data from an input view and joins it against tables from the base dataset
    to produce the output data by month.

    Required input columns:

    * `instance_id`: The id of the report_table_instance.
    * `dimensions`: List of dimensions, in the form ARRAY<STRUCT<dimension string, dimension_value string>>.
    * `dimensions_string`: The dimension values concatenated into a string, to be used in group bys.
    * `num_original_dimensions`: The number of dimensions that the table originally
      had, to be used in prioritizing which point to use if there are multiple that
      represent the same metric.
    * `collapsed_dimension_values`: List of values for dimensions that were dropped.
    * `value`: The actual value for the metric.

    **Example:**

    Input Table:
    | instance_id | dimensions                           | value | num_original_dimensions | dimensions_string | collapsed_dimension_values |
    | ----------- | ------------------------------------ | ----- | ----------------------- | ----------------- | -------------------------- |
    | 1           | [('global/location/state', 'US_XX')] | 6     | 2                       | US_XX             | ['M', 'F']                 |
    | 2           | [('global/location/state', 'US_XX')] | 9     | 2                       | US_XX             | ['M', 'F']                 |
    | 3           | [('global/location/state', 'US_YY')] | 12    | 2                       | US_YY             | ['Male', 'Female']         |
    | 4           | [('global/location/state', 'US_YY')] | 15    | 2                       | US_YY             | ['Male', 'Female']         |
    | 5           | [('global/location/state', 'US_XX')] | 10    | 0                       | US_XX             | [] .                       |

    Additional data is pulled from the base dataset.

    Output Table:
    | start_of_month | dimensions                           | value | source_id | report_type | report_ids | publish_date | time_window_start | time_window_end | dimensions_string | collapsed_dimension_values |
    | -------------- | ------------------------------------ | ----- | --------- | ----------- | ---------- | ------------ | ----------------- | --------------- | ----------------- | -------------------------- |
    | 2020-11-01     | [('global/location/state', 'US_XX')] | 6     | 1         | Annual Data | [1]        | 2020-12-07   | 2020-11-01        | 2020-12-01      | US_XX             | ['M', 'F']                 |
    | 2020-12-01     | [('global/location/state', 'US_XX')] | 10    | 3         | Facts       | [5]        | 2021-01-02   | 2020-12-01        | 2021-01-01      | US_XX             |                            |
    | 2020-11-01     | [('global/location/state', 'US_YY')] | 12    | 2         | Dashboard   | [3]        | 2020-12-15   | 2020-11-01        | 2020-12-01      | US_YY             | ['Male', 'Female']         |
    | 2020-12-01     | [('global/location/state', 'US_YY')] | 15    | 2         | Dashboard   | [4]        | 2021-01-15   | 2020-12-01        | 2021-01-01      | US_YY             | ['Male', 'Female']         |

    """

    def __init__(
        self,
        *,
        dataset_id: str,
        metric_name: str,
        input_view: BigQueryViewBuilder,
    ):
        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{view_prefix_for_metric_name(metric_name)}_by_month",
            view_query_template=AGGREGATE_TO_MONTH_VIEW_TEMPLATE,
            # Query Format Arguments
            description=f"{metric_name} by month",
            base_dataset=dataset_config.JUSTICE_COUNTS_BASE_DATASET,
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
        )


def calculate_metric_view_chain(
    dataset_id: str, metric_to_calculate: "CalculatedMetricByMonth"
) -> List[SimpleBigQueryViewBuilder]:
    fetch_and_filter = FetchAndFilterMetricViewBuilder(
        dataset_id=dataset_id, metric_to_calculate=metric_to_calculate
    )
    # Note: Right now we do spatial then time which works because we don't ever do
    # spatial aggregation across tables.
    spatial = SpatialAggregationViewBuilder(
        dataset_id=dataset_id,
        metric_name=metric_to_calculate.output_name,
        input_view=fetch_and_filter,
        partition_columns={"instance_id"},
        partition_dimensions=set(
            aggregation.dimension
            for aggregation in metric_to_calculate.aggregated_dimensions.values()
        ),
        context_columns={},
        value_columns={"value"},
        collapse_dimensions_filter="ENDS_WITH(dimension, '/raw')",
    )
    by_month = MetricByMonthViewBuilder(
        dataset_id=dataset_id,
        metric_name=metric_to_calculate.output_name,
        input_view=spatial,
    )
    return [fetch_and_filter, spatial, by_month]


COMPARISON_VIEW_TEMPLATE = """
/*{description}*/

-- COMPARISON
-- Compare against the most recent data that is at least one year older
SELECT * EXCEPT(ordinal)
FROM (
    SELECT
        base_data.*,
        compared_data.start_of_month as compare_start_of_month, compared_data.{value_column} as compare_{value_column},
        ROW_NUMBER() OVER (
        PARTITION BY base_data.dimensions_string, base_data.start_of_month
        -- Orders by recency of the compared data
        ORDER BY DATE_DIFF(base_data.start_of_month, compared_data.start_of_month, DAY)) as ordinal
    FROM `{project_id}.{input_dataset}.{input_table}` base_data
    -- Explodes to every row that is at least a year older
    LEFT JOIN `{project_id}.{compare_dataset}.{compare_table}` compared_data ON
        (base_data.dimensions_string  = compared_data.dimensions_string AND
        compared_data.start_of_month <= DATE_SUB(base_data.start_of_month, INTERVAL 1 YEAR))
) as joined_with_prior
-- Picks the row with the compared data that is the most recent
WHERE joined_with_prior.ordinal = 1
"""


class CompareToPriorYearViewBuilder(SimpleBigQueryViewBuilder):
    """Factory class for building views that compare monthly metrics to the prior year.

    Required input columns (must be present in both input and compare tables):
    * start_of_month
    * dimensions_string
    * {value_column}

    The output includes all columns from the input unchanged, plus two additional columns:
    * compare_start_of_month
    * compare_{value_column}

    **Example:**

    Input Table (used for compare also):

    | start_of_month | dimensions_string | value | publish_date |
    |--------------- | ----------------- | ----- | ------------ |
    | 2020-06-01     | US_XX             | 1000  | 2020-07-12   |
    | 2019-12-01     | US_XX             | 900   | 2020-01-24   |
    | 2019-06-01     | US_XX             | 800   | 2019-08-02   |
    | 2018-12-01     | US_XX             | 700   | 2019-01-17   |

    Output Table:

    | start_of_month | dimensions_string | value | publish_date | compare_start_of_month | compare_value |
    | -------------- | ----------------- | ----- | ------------ | ---------------------- | ------------- |
    | 2020-06-01     | US_XX             | 1000  | 2020-07-12   | 2019-06-01             | 800           |
    | 2019-12-01     | US_XX             | 900   | 2020-01-24   | 2018-12-01             | 700           |
    | 2019-06-01     | US_XX             | 800   | 2019-08-02   |                        |               |
    | 2018-12-01     | US_XX             | 700   | 2019-01-17   |                        |               |
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        metric_name: str,
        input_view: BigQueryViewBuilder,
        value_column: str = "value",
    ):

        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{view_prefix_for_metric_name(metric_name)}_compared",
            view_query_template=COMPARISON_VIEW_TEMPLATE,
            # Query Format Arguments
            description=f"{metric_name} comparison -- {input_view.view_id} compared to a year prior",
            base_dataset=dataset_config.JUSTICE_COUNTS_BASE_DATASET,
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
            compare_dataset=input_view.dataset_id,
            compare_table=input_view.view_id,
            value_column=value_column,
        )


DIMENSIONS_TO_COLUMNS_TEMPLATE = """
SELECT
    *,
    {aggregated_dimensions_to_columns_clause}
FROM `{project_id}.{input_dataset}.{input_table}`
"""


class DimensionsToColumnsViewBuilder(SimpleBigQueryViewBuilder):
    """Factory class for moving aggregate dimensions to columns in output.

    Example:

    metric_to_calculate.aggregated_dimensions={'state_code': StateCode, 'gender': Gender}

    Input Table:
    | date       | dimensions                                                      | value |
    | ---------- | --------------------------------------------------------------- | ----- |
    | 2020-12-01 | [('global/location/state', 'US_XX'), ('global/gender', 'MALE')] | 1234  |
    | 2020-12-01 | [('global/location/state', 'US_YY'), ('global/gender', 'MALE')] | 5678  |

    Output Table:
    | date       | dimensions                                                      | state_code | gender | value |
    | ---------- | --------------------------------------------------------------- | ---------- | ------ | ----- |
    | 2020-12-01 | [('global/location/state', 'US_XX'), ('global/gender', 'MALE')] | US_XX      | MALE   | 1234  |
    | 2020-12-01 | [('global/location/state', 'US_YY'), ('global/gender', 'MALE')] | US_XX      | MALE   | 5678  |
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        metric_name: str,
        aggregations: Dict[str, "Aggregation"],
        input_view: BigQueryViewBuilder,
    ):
        aggregated_dimensions_to_columns_clause = ", ".join(
            [
                f"(SELECT dimension_value FROM UNNEST(dimensions) "
                f"WHERE dimension = '{aggregation.dimension.dimension_identifier()}') as {column_name}"
                for column_name, aggregation in aggregations.items()
            ]
        )

        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{view_prefix_for_metric_name(metric_name)}_with_dimensions",
            view_query_template=DIMENSIONS_TO_COLUMNS_TEMPLATE,
            # Query Format Arguments
            description=f"{metric_name} with dimensions",
            base_dataset=dataset_config.JUSTICE_COUNTS_BASE_DATASET,
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
            aggregated_dimensions_to_columns_clause=aggregated_dimensions_to_columns_clause,
        )


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


def view_prefix_for_metric_name(metric_name: str) -> str:
    return metric_name.lower()


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
        return view_prefix_for_metric_name(self.output_name)

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
