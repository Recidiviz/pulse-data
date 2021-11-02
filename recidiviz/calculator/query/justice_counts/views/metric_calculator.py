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
from typing import Dict, Iterable, List, Optional, Set, Type

import attr

from recidiviz.big_query.big_query_view import (
    BigQueryAddress,
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

    def __init__(self, *, dataset_id: str, metric_to_calculate: "CalculatedMetric"):
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
                column_clause = f"ANY_VALUE({column})"
            elif aggregation is self.ContextAggregation.ARRAY:
                column_clause = f"ARRAY_AGG(DISTINCT {column})"
            elif aggregation is self.ContextAggregation.ARRAY_CONCAT:
                column_clause = f"ARRAY_CONCAT_AGG({column})"
            elif aggregation is self.ContextAggregation.MAX:
                column_clause = f"MAX({column})"
            elif aggregation is self.ContextAggregation.MIN:
                column_clause = f"MIN({column})"
            else:
                raise ValueError(f"Unsupported context aggregation: {aggregation}")
            select_clauses.append(f"{column_clause} AS {column}")
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


MONTHLY_DATE_PARTITION_VIEW_TEMPLATE = """
  SELECT
    input.*,
    DATE_ADD(DATE_TRUNC(instance.time_window_start, MONTH), INTERVAL 1 MONTH) as start_date_partition,
    -- Ends are exclusive, so we subtract a day before calculating the partition
    DATE_ADD(DATE_TRUNC(DATE_SUB(instance.time_window_end, INTERVAL 1 DAY), MONTH), INTERVAL 1 MONTH) as end_date_partition
  FROM `{project_id}.{base_dataset}.report_table_instance_materialized` instance
  JOIN `{project_id}.{input_dataset}.{input_table}` input
    ON input.instance_id = instance.id
"""


class MonthlyDatePartitionViewBuilder(SimpleBigQueryViewBuilder):
    """Determines the dates to use to partition data points into months.

    There is a partition for every month, and the partition date is based on the end
    date of the partition that a data point falls in. E.g.
    Data Date  -> Partition Date
    2020-01-01 -> 2020-02-01
    2020-01-02 -> 2020-02-01
    ...
    2020-01-31 -> 2020-02-01
    2020-02-01 -> 2020-03-01

    Required input columns:

    * `instance_id`: The id of the report_table_instance.

    **Example:**

    Input Table:
    | instance_id | ... |
    | ----------- | --- |
    | 1           | ... |
    | 1           | ... |
    | 3           | ... |
    | 4           | ... |

    Report Table Instances:
    | instance_id | time_window_start | time_window_end |
    | ----------- | ----------------- | --------------- |
    | 1           | 2020-01-01        | 2020-02-01      |
    | 3           | 2020-01-31        | 2021-02-01      |
    | 4           | 2020-01-01        | 2021-01-01      |

    Output Table:
    | instance_id | ... | start_date_partition | end_date_partition |
    | ----------- | --- | -------------------- | ------------------ |
    | 1           | ... | 2020-02-01           | 2020-02-01         |
    | 1           | ... | 2020-02-01           | 2020-02-01         |
    | 3           | ... | 2020-02-01           | 2020-02-01         |
    | 4           | ... | 2020-02-01           | 2021-02-01         |
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
            view_id=f"{view_prefix_for_metric_name(metric_name)}_monthly_partitions",
            view_query_template=MONTHLY_DATE_PARTITION_VIEW_TEMPLATE,
            # Query Format Arguments
            description=f"{metric_name} with monthly date partitions",
            base_dataset=dataset_config.JUSTICE_COUNTS_BASE_DATASET,
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
        )


ANNUAL_DATE_PARTITION_VIEW_TEMPLATE = """
  SELECT
    input.*,
    DATE(
        CASE
            WHEN time_window_start_month >= EXTRACT(MONTH from month_boundary.date_partition)
            THEN time_window_start_year + 1
            ELSE time_window_start_year
        END,
        EXTRACT(MONTH from month_boundary.date_partition),
        1
    ) as start_date_partition,
    DATE(
        CASE
            WHEN time_window_end_month >= EXTRACT(MONTH from month_boundary.date_partition)
            THEN time_window_end_year + 1
            ELSE time_window_end_year
        END,
        EXTRACT(MONTH from month_boundary.date_partition),
        1
    ) as end_date_partition
  FROM (
      select
        *,
        EXTRACT(MONTH from instance.time_window_start) as time_window_start_month,
        EXTRACT(YEAR from instance.time_window_start) as time_window_start_year,
        EXTRACT(MONTH from DATE_SUB(instance.time_window_end, INTERVAL 1 DAY)) as time_window_end_month,
        EXTRACT(YEAR from DATE_SUB(instance.time_window_end, INTERVAL 1 DAY)) as time_window_end_year
      from `{project_id}.{base_dataset}.report_table_instance_materialized` instance
  ) instance
  JOIN `{project_id}.{input_dataset}.{input_table}` input
    ON input.instance_id = instance.id
  JOIN `{project_id}.{date_partition_dataset}.{date_partition_table}` month_boundary
    ON input.dimensions_string = month_boundary.state_code
"""


class AnnualDatePartitionViewBuilder(SimpleBigQueryViewBuilder):
    """Determines the dates to use to partition data points into years.

    Much like MonthlyDatePartitionViewBuilder, but instead of partitioning data into
    months, partitions them into years. Unlike for months, the partitions are year long
    but are not forced to align with calendar years, only with months. The month
    boundary to use is determined by `date_partition_table` and can differ for each
    state. For instance if for a given state the boundary is 2020-07-01, then data is
    partitioned as follows:
    Data Date  -> Partition Date
    2019-07-01 -> 2020-07-01
    ...
    2020-06-30 -> 2020-07-01
    2020-07-01 -> 2021-07-01

    Required input columns:

    * `instance_id`: The id of the report_table_instance.
    * `dimensions_string`: The data must be per state, so this should only contain the state_code.

    Required date partition table columns:

    * `state_code`: The state code (e.g. US_XX)
    * `date_partition`: The first day of a month to use as the month boundary for each partiton.

    **Example:**

    Input Table:
    | instance_id | dimensions_string | ... |
    | ----------- | ----------------- | --- |
    | 1           | US_XX             | ... |
    | 1           | US_XX             | ... |
    | 3           | US_YY             | ... |
    | 4           | US_YY             | ... |
    | 5           | US_YY             | ... |

    Date Partition Table:
    | state_code | date_partition |
    | ---------- | -------------- |
    | US_XX      | 2020-07-01     |
    | US_YY      | 2015-01-01     |

    Report Table Instances:
    | instance_id | time_window_start | time_window_end |
    | ----------- | ----------------- | --------------- |
    | 1           | 2020-01-01        | 2020-02-01      |
    | 3           | 2020-01-31        | 2021-02-01      |
    | 4           | 2020-01-01        | 2021-01-01      |
    | 5           | 2019-07-01        | 2020-07-01      |

    Output Table:
    | instance_id | ... | start_date_partition | end_date_partition |
    | ----------- | --- | -------------------- | ------------------ |
    | 1           | ... | 2020-07-01           | 2020-07-01         |
    | 1           | ... | 2020-07-01           | 2020-07-01         |
    | 3           | ... | 2021-01-01           | 2021-01-01         |
    | 4           | ... | 2021-01-01           | 2021-01-01         |
    | 5           | ... | 2020-01-01           | 2021-01-01         |
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        metric_name: str,
        input_view: BigQueryViewBuilder,
        date_partition_table: BigQueryAddress,
    ):
        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{view_prefix_for_metric_name(metric_name)}_annual_partitions",
            view_query_template=ANNUAL_DATE_PARTITION_VIEW_TEMPLATE,
            # Query Format Arguments
            description=f"{metric_name} with annual date partitions",
            base_dataset=dataset_config.JUSTICE_COUNTS_BASE_DATASET,
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
            date_partition_dataset=date_partition_table.dataset_id,
            date_partition_table=date_partition_table.table_id,
        )


AGGREGATE_TO_DATE_PARTITIONS_VIEW_TEMPLATE = """
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
    input.start_date_partition,
    input.end_date_partition,
    input.dimensions,
    input.dimensions_string,
    input.num_original_dimensions,
    input.collapsed_dimension_values,
    input.value,
    DATE_DIFF(time_window_end, time_window_start, DAY) AS time_window_days
  FROM `{project_id}.{input_dataset}.{input_table}` input
  JOIN `{project_id}.{base_dataset}.report_table_instance_materialized` instance
    ON input.instance_id = instance.id
  JOIN `{project_id}.{base_dataset}.report_table_definition_materialized` definition
    ON definition.id = report_table_definition_id
  JOIN `{project_id}.{base_dataset}.report_materialized` report
    ON instance.report_id = report.id
),

-- Aggregate values for the same (source, report type, table definition, end_date_partition)
--  - DELTA: Sum them
--  - INSTANT/AVERAGE/PERSON_BASED_DELTA: Pick one
combined_periods_data as (
  SELECT
    source_id, report_type, ARRAY_AGG(DISTINCT report_id) as report_ids, MAX(publish_date) as publish_date,
    definition_id, ANY_VALUE(measurement_type) as measurement_type,
    end_date_partition as date_partition,
    MIN(time_window_start) as time_window_start,
    MAX(time_window_end) as time_window_end,
    SUM(time_window_days) as time_window_days,
    -- Since BQ won't allow us to group by dimensions, we are grouping by dimensions_string and just pick any dimensions
    -- since they are all the same for a single dimensions_string.
    ANY_VALUE(dimensions) as dimensions,
    dimensions_string,
    -- This will be the same since we grouped by defintion_id
    ANY_VALUE(num_original_dimensions) as num_original_dimensions,
    ARRAY_CONCAT_AGG(collapsed_dimension_values) as collapsed_dimension_values,
    SUM(value) as value
  FROM (
    -- Prioritize data that covers the largest part of the partition (time_window_days),
    -- is the most recent (time_window_end), then data that was published most recently
    -- (publish_date), then data from definitions with the fewest dimensions as that
    -- will help us accidentally avoid missing categories (num_original_dimensions)
    SELECT
      *,
      ROW_NUMBER() OVER(period_partition) as ordinal,
      SUM(time_window_days) OVER(period_partition) as time_window_days_running_total
    FROM joined_data
    WHERE start_date_partition = end_date_partition
    WINDOW period_partition AS (
      PARTITION BY source_id, report_type, definition_id, end_date_partition, dimensions_string
      ORDER BY time_window_days DESC, time_window_end DESC, publish_date DESC, num_original_dimensions ASC
    )
  ) as partitioned
  -- If it is DELTA then sum them, otherwise just pick one
  -- Note: For AVERAGE we could do a weighted average instead, but for now dropping is okay
  WHERE
    partitioned.ordinal = 1
    OR (measurement_type = 'DELTA'
      -- Only allow periods to be combined up to the size of the partition to avoid double counting days.
      AND time_window_days_running_total <= DATE_DIFF(end_date_partition, DATE_SUB(end_date_partition, INTERVAL 1 {partition_part}), DAY))
  GROUP BY source_id, report_type, definition_id, end_date_partition, dimensions_string
)

-- Pick a single value for each dimension value, month combination
-- Note: This means that we could pick different sources for different values of a single comprehensive dimension.
-- Initially here we were grouping by (state, month) but now all dimensions are included. What we might want is to only
-- use non comprehensive dimensions here so that we can enforce that the same source is used for each value
-- of a comprehensive dimension. We could achieve this by doing time aggregation only on report table instances and then
-- bring in cells to do the spatial aggregation. Or we could split this off, group by non comprehensive dimensions and
-- aggregate comprehensive dimensions, and take into account the coverage of dimension values when deciding which to
-- use (e.g. one source reports on more race categories than another).
SELECT
  source_id, report_type, report_ids, publish_date, measurement_type,
  date_partition, time_window_start, time_window_end,
  dimensions, dimensions_string, collapsed_dimension_values, value
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER(
      PARTITION BY dimensions_string, date_partition
      ORDER BY
        -- Prefer instant measurements over ADP
        IF(measurement_type = 'INSTANT', 0, 1) ASC,
        time_window_end DESC, publish_date DESC, num_original_dimensions ASC,
        -- Tie breakers to make it deterministic
        source_id ASC, report_type ASC, measurement_type ASC
    ) as ordinal
  FROM (
    SELECT * FROM combined_periods_data
    WHERE
      -- Only keep instant points that have data in the last month of the partition. This
      -- is important to prevent pulling instant measurements forward more than a month.
      (measurement_type = 'INSTANT'
        AND DATE_ADD(DATE_TRUNC(DATE_SUB(time_window_end, INTERVAL 1 DAY), MONTH), INTERVAL 1 MONTH) = date_partition)
      -- All other points must fully cover the partition.
      OR (
        -- Both the difference between the min start and max end of the combined periods
        -- and the sum of the actual number of days covered by the combined periods must
        -- match the number of days as are in the partition.
        DATE_DIFF(time_window_end, time_window_start, DAY) = DATE_DIFF(date_partition, DATE_SUB(date_partition, INTERVAL 1 {partition_part}), DAY)
        AND time_window_days = DATE_DIFF(date_partition, DATE_SUB(date_partition, INTERVAL 1 {partition_part}), DAY))
  ) as filtered_to_month
) as partitioned
WHERE partitioned.ordinal = 1
"""


class AggregateToDatePartitionsViewBuilder(SimpleBigQueryViewBuilder):
    """Factory class for building views that aggregates metrics up to their date partitions.

    This both sums delta metrics from the same source that fall into the same partition
    and then picks a single point for each partition, dimension combination.

    This takes data from an input view and joins it against tables from the base dataset
    to produce the output data by month.

    Required input columns:

    * `instance_id`: The id of the report_table_instance.
    * `start_date_partition`: The date partition that time_window_start falls into.
    * `end_date_partition`: The date partition that time_window_end falls into.
    * `dimensions`: List of dimensions, in the form ARRAY<STRUCT<dimension string, dimension_value string>>.
    * `dimensions_string`: The dimension values concatenated into a string, to be used in group bys.
    * `num_original_dimensions`: The number of dimensions that the table originally
      had, to be used in prioritizing which point to use if there are multiple that
      represent the same metric.
    * `collapsed_dimension_values`: List of values for dimensions that were dropped.
    * `value`: The actual value for the metric.

    **Example:**

    Input Table:
    | instance_id | start_date_partition | end_date_partition | dimensions                           | value | num_original_dimensions | dimensions_string | collapsed_dimension_values |
    | ----------- | -------------------- | ------------------ | ------------------------------------ | ----- | ----------------------- | ----------------- | -------------------------- |
    | 1           | 2020-12-01           | 2020-12-01         | [('global/location/state', 'US_XX')] | 6     | 2                       | US_XX             | ['M', 'F']                 |
    | 2           | 2021-01-01           | 2021-01-01         | [('global/location/state', 'US_XX')] | 9     | 2                       | US_XX             | ['M', 'F']                 |
    | 3           | 2020-12-01           | 2020-12-01         | [('global/location/state', 'US_YY')] | 12    | 2                       | US_YY             | ['Male', 'Female']         |
    | 4           | 2021-01-01           | 2021-01-01         | [('global/location/state', 'US_YY')] | 15    | 2                       | US_YY             | ['Male', 'Female']         |
    | 5           | 2021-01-01           | 2021-01-01         | [('global/location/state', 'US_XX')] | 10    | 0                       | US_XX             | []                         |
    | 6           | 2021-01-01           | 2021-01-01         | [('global/location/state', 'US_ZZ')] | 30    | 0                       | US_ZZ             | []                         |
    | 7           | 2021-01-01           | 2021-01-01         | [('global/location/state', 'US_ZZ')] | 40    | 0                       | US_ZZ             | []                         |

    Additional data is pulled from the base dataset.

    Output Table:
    | date_partition | dimensions                           | value | source_id | report_type | report_ids | publish_date | time_window_start | time_window_end | dimensions_string | collapsed_dimension_values |
    | -------------- | ------------------------------------ | ----- | --------- | ----------- | ---------- | ------------ | ----------------- | --------------- | ----------------- | -------------------------- |
    | 2020-12-01     | [('global/location/state', 'US_XX')] | 6     | 1         | Annual Data | [1]        | 2020-12-07   | 2020-11-01        | 2020-12-01      | US_XX             | ['M', 'F']                 |
    | 2021-01-01     | [('global/location/state', 'US_XX')] | 10    | 3         | Facts       | [5]        | 2021-01-02   | 2020-12-01        | 2021-01-01      | US_XX             |                            |
    | 2020-12-01     | [('global/location/state', 'US_YY')] | 12    | 2         | Dashboard   | [3]        | 2020-12-15   | 2020-11-01        | 2020-12-01      | US_YY             | ['Male', 'Female']         |
    | 2021-01-01     | [('global/location/state', 'US_YY')] | 15    | 2         | Dashboard   | [4]        | 2021-01-15   | 2020-12-01        | 2021-01-01      | US_YY             | ['Male', 'Female']         |
    | 2021-01-01     | [('global/location/state', 'US_ZZ')] | 70    |           | Weekly      | [6, 7]     | 2021-01-15   | 2020-12-01        | 2021-01-01      | US_ZZ             | []                         |

    """

    def __init__(
        self,
        *,
        dataset_id: str,
        metric_name: str,
        input_view: BigQueryViewBuilder,
        time_aggregation: "TimeAggregation",
    ):
        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{view_prefix_for_metric_name(metric_name)}_aggregated_{time_aggregation.value}",
            view_query_template=AGGREGATE_TO_DATE_PARTITIONS_VIEW_TEMPLATE,
            # Query Format Arguments
            description=f"{metric_name} aggregated to date partitions",
            base_dataset=dataset_config.JUSTICE_COUNTS_BASE_DATASET,
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
            partition_part=time_aggregation.to_date_part(),
        )


class TimeAggregation(enum.Enum):
    ANNUAL = "annual"
    MONTHLY = "monthly"

    def to_date_part(self) -> str:
        if self is TimeAggregation.ANNUAL:
            return "YEAR"
        if self is TimeAggregation.MONTHLY:
            return "MONTH"
        raise ValueError(f"Invalid TimeAggregation: {self}")


def fetch_metric_view_chain(
    dataset_id: str, metric_to_calculate: "CalculatedMetric"
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
    return [fetch_and_filter, spatial]


def time_aggregation_view_chain(
    input_view: SimpleBigQueryViewBuilder,
    dataset_id: str,
    metric_to_calculate: "CalculatedMetric",
    time_aggregation: TimeAggregation,
    date_partition_table: Optional[BigQueryAddress] = None,
) -> List[SimpleBigQueryViewBuilder]:
    """Returns a list of views that aggregate the fetched data to the specified frequency"""
    aggregation_partition_view: SimpleBigQueryViewBuilder
    if time_aggregation is TimeAggregation.MONTHLY:
        if date_partition_table is not None:
            raise ValueError("date_partition_table may not be set for MONTHLY")
        aggregation_partition_view = MonthlyDatePartitionViewBuilder(
            dataset_id=dataset_id,
            metric_name=metric_to_calculate.output_name,
            input_view=input_view,
        )
    elif time_aggregation is TimeAggregation.ANNUAL:
        if date_partition_table is None:
            raise ValueError("date_partition_table must be set for ANNUAL")
        aggregation_partition_view = AnnualDatePartitionViewBuilder(
            dataset_id=dataset_id,
            metric_name=metric_to_calculate.output_name,
            input_view=input_view,
            date_partition_table=date_partition_table,
        )
    else:
        raise ValueError(f"Invalid TimeAggregation: {time_aggregation}")

    aggregated_view = AggregateToDatePartitionsViewBuilder(
        dataset_id=dataset_id,
        metric_name=metric_to_calculate.output_name,
        input_view=aggregation_partition_view,
        time_aggregation=time_aggregation,
    )

    return [aggregation_partition_view, aggregated_view]


def calculate_metric_view_chain(
    dataset_id: str,
    metric_to_calculate: "CalculatedMetric",
    time_aggregation: TimeAggregation,
    date_partition_table: Optional[BigQueryAddress] = None,
) -> List[SimpleBigQueryViewBuilder]:
    fetch_chain = fetch_metric_view_chain(dataset_id, metric_to_calculate)
    time_aggregation_chain = time_aggregation_view_chain(
        fetch_chain[-1],
        dataset_id,
        metric_to_calculate,
        time_aggregation,
        date_partition_table,
    )
    return fetch_chain + time_aggregation_chain


COMPARISON_VIEW_TEMPLATE = """
/*{description}*/

-- COMPARISON
-- Compare against the most recent data that is at least one year older
SELECT * EXCEPT(ordinal)
FROM (
    SELECT
        base_data.*,
        compared_data.date_partition as compare_date_partition, compared_data.{value_column} as compare_{value_column},
        ROW_NUMBER() OVER (
        PARTITION BY base_data.dimensions_string, base_data.date_partition
        -- Orders by recency of the compared data
        ORDER BY DATE_DIFF(base_data.date_partition, compared_data.date_partition, DAY)) as ordinal
    FROM `{project_id}.{input_dataset}.{input_table}` base_data
    -- Explodes to every row that is at least a year older
    LEFT JOIN `{project_id}.{compare_dataset}.{compare_table}` compared_data ON
        (base_data.dimensions_string  = compared_data.dimensions_string AND
        compared_data.date_partition <= DATE_SUB(base_data.date_partition, INTERVAL 1 YEAR))
) as joined_with_prior
-- Picks the row with the compared data that is the most recent
WHERE joined_with_prior.ordinal = 1
"""


class CompareToPriorYearViewBuilder(SimpleBigQueryViewBuilder):
    """Factory class for building views that compare monthly metrics to the prior year.

    Required input columns (must be present in both input and compare tables):
    * date_partition
    * dimensions_string
    * {value_column}

    The output includes all columns from the input unchanged, plus two additional columns:
    * compare_date_partition
    * compare_{value_column}

    **Example:**

    Input Table (used for compare also):

    | date_partition | dimensions_string | value | publish_date |
    |--------------- | ----------------- | ----- | ------------ |
    | 2020-06-01     | US_XX             | 1000  | 2020-07-12   |
    | 2019-12-01     | US_XX             | 900   | 2020-01-24   |
    | 2019-06-01     | US_XX             | 800   | 2019-08-02   |
    | 2018-12-01     | US_XX             | 700   | 2019-01-17   |

    Output Table:

    | date_partition | dimensions_string | value | publish_date | compare_date_partition | compare_value |
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
        compare_view: Optional[BigQueryViewBuilder] = None,
        value_column: str = "value",
    ):
        if compare_view is None:
            compare_view = input_view

        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{view_prefix_for_metric_name(metric_name)}_compared",
            view_query_template=COMPARISON_VIEW_TEMPLATE,
            # Query Format Arguments
            description=f"{metric_name} comparison -- {input_view.view_id} compared to a year prior",
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
            compare_dataset=compare_view.dataset_id,
            compare_table=compare_view.view_id,
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
        input_view: SimpleBigQueryViewBuilder,
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
            input_dataset=input_view.table_for_query.dataset_id,
            input_table=input_view.table_for_query.table_id,
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
class CalculatedMetric:
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
