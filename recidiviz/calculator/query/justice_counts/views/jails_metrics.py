# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

"""Creates BQ views to calculate all metrics by month for the jails part of Justice Counts."""

from typing import Dict, List

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.calculator.query.justice_counts import dataset_config
from recidiviz.calculator.query.justice_counts.views import metric_calculator
from recidiviz.datasets.static_data.config import EXTERNAL_REFERENCE_DATASET
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.tools.justice_counts import manual_upload
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_STATE_CODE_AGGREGATION = metric_calculator.Aggregation(
    dimension=manual_upload.State, comprehensive=False
)
_COUNTY_CODE_AGGREGATION = metric_calculator.Aggregation(
    dimension=manual_upload.County, comprehensive=False
)
_COUNTY_FIPS_AGGREGATION = metric_calculator.Aggregation(
    dimension=manual_upload.CountyFIPS, comprehensive=False
)

POPULATION_METRIC_NAME = "POPULATION_JAIL"
RATE_METRIC_NAME = "INCARCERATION_RATE_JAIL"
COUNTY_COVERAGE_METRIC_NAME = "PERCENTAGE_COVERED_COUNTY"

OUTPUT_DIMENSIONS = {
    "state_code": _STATE_CODE_AGGREGATION,
    "county_code": _COUNTY_CODE_AGGREGATION,
}

ADD_RESIDENT_POPULATION_VIEW_TEMPLATE = """
SELECT * EXCEPT(with_resident_ordinal)
FROM (
    SELECT input.*, resident.population as resident_population, resident.year as resident_population_year,
        ROW_NUMBER() OVER (
            PARTITION BY input.dimensions_string, input.date_partition
            -- Get the population for the closest year that we have.
            ORDER BY ABS(resident.year - EXTRACT(YEAR FROM DATE_SUB(input.date_partition, INTERVAL 1 DAY))) ASC
        ) as with_resident_ordinal
    FROM `{project_id}.{input_dataset}.{input_table}` input
    LEFT JOIN `{project_id}.{reference_dataset}.{reference_table}` resident
    ON resident.fips = (SELECT dimension_value FROM UNNEST(input.dimensions) WHERE dimension = '{dimension_identifier}')
) joined_with_resident
WHERE with_resident_ordinal = 1
"""


class AddResidentPopulationViewBuilder(SimpleBigQueryViewBuilder):
    """Factory class for building a view that adds the county resident population

    Relies on `dimensions_string` and `date_partition` forming a unique id for the input rows"""

    def __init__(
        self,
        *,
        dataset_id: str,
        metric_name: str,
        input_view: BigQueryViewBuilder,
    ):
        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{metric_calculator.view_prefix_for_metric_name(metric_name)}_with_resident",
            view_query_template=ADD_RESIDENT_POPULATION_VIEW_TEMPLATE,
            # Query Format Arguments
            description="",
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
            reference_dataset=EXTERNAL_REFERENCE_DATASET,
            reference_table="county_resident_populations",
            dimension_identifier=manual_upload.CountyFIPS.dimension_identifier(),
        )


INCARCERATION_RATE_VIEW_TEMPLATE = """
SELECT
    * EXCEPT(value),
    -- Calculates the incarceration rate as number of incarcerated individuals per 100k residents
    (value / resident_population) * 100000 as incarceration_rate_value
FROM `{project_id}.{input_dataset}.{input_table}`
"""


class IncarcerationRateViewBuilder(SimpleBigQueryViewBuilder):
    """Factory class for building a view that calculates incarceration rate"""

    def __init__(
        self,
        *,
        dataset_id: str,
        metric_name: str,
        input_view: BigQueryViewBuilder,
    ):
        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{metric_calculator.view_prefix_for_metric_name(metric_name)}_with_resident",
            view_query_template=INCARCERATION_RATE_VIEW_TEMPLATE,
            # Query Format Arguments
            description="",
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
        )


PERCENTAGE_COVERED_VIEW_TEMPLATE = """
WITH state_populations AS (
  SELECT
    state_code,
    COUNT(*) AS num_counties,
    SUM(resident.population) as total_population
  FROM (
    SELECT
      *,
      -- This just picks the most recent population that we have for each county.
      ROW_NUMBER() OVER (PARTITION BY fips ORDER BY year DESC) AS rn
    FROM
      `{project_id}.{reference_dataset}.{population_reference_table}` ) resident
  JOIN
    `{project_id}.{reference_dataset}.{county_reference_table}` county
  ON
    resident.fips = county.fips
  WHERE
    rn = 1
  GROUP BY state_code
)

SELECT
  input.*,
  IF(input.county_code IS NULL,
     CAST(ARRAY_LENGTH(input.collapsed_dimension_values) AS FLOAT) / state_info.num_counties,
     NULL) as percentage_covered_county,
  IF(input.county_code IS NULL,
     CAST(input.resident_population AS FLOAT) / state_info.total_population,
     NULL) as percentage_covered_population
FROM
  `{project_id}.{input_dataset}.{input_table}` input
JOIN state_populations state_info USING (state_code)

"""


class PercentageCoveredViewBuilder(SimpleBigQueryViewBuilder):
    """Factory class for building a view that calculates the percentage of the
    state's counties and population that are covered by the metric."""

    def __init__(
        self,
        *,
        dataset_id: str,
        metric_name: str,
        input_view: BigQueryViewBuilder,
    ):
        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{metric_calculator.view_prefix_for_metric_name(metric_name)}_percentage_covered",
            view_query_template=PERCENTAGE_COVERED_VIEW_TEMPLATE,
            # Query Format Arguments
            description=f"{metric_name} percentage covered",
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
            reference_dataset=EXTERNAL_REFERENCE_DATASET,
            population_reference_table="county_resident_populations",
            county_reference_table="county_fips",
        )


OUTPUT_VIEW_TEMPLATE = """
SELECT {aggregated_dimension_columns},
       '{metric_output_name}' as metric,
       EXTRACT(YEAR from DATE_SUB(date_partition, INTERVAL 1 DAY)) as year,
       EXTRACT(MONTH from DATE_SUB(date_partition, INTERVAL 1 DAY)) as month,
       DATE_SUB(time_window_end, INTERVAL 1 DAY) as date_reported,
       measurement_type as measurement_type,
       {value_column} as value,
       -- If there is no row at least a year back to compare to, the following columns will be NULL.
       EXTRACT(YEAR from DATE_SUB(compare_date_partition, INTERVAL 1 DAY)) as compared_to_year,
       EXTRACT(MONTH from DATE_SUB(compare_date_partition, INTERVAL 1 DAY)) as compared_to_month,
       {value_column} - compare_{value_column} as value_change,
       -- Note: This can return NULL in the case of divide by zero
       SAFE_DIVIDE(({value_column} - compare_{value_column}), compare_{value_column}) as percentage_change,
       percentage_covered_county as percentage_covered_county,
       percentage_covered_population as percentage_covered_population,
       publish_date as date_published
FROM `{project_id}.{input_dataset}.{input_table}`
"""


class JailOutputViewBuilder(SimpleBigQueryViewBuilder):
    """Factory class for creating corrections output from an input view."""

    def __init__(
        self,
        *,
        dataset_id: str,
        metric_name: str,
        aggregations: Dict[str, metric_calculator.Aggregation],
        value_column: str,
        input_view: BigQueryViewBuilder,
    ):
        aggregated_dimension_columns = ", ".join(aggregations)

        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{metric_calculator.view_prefix_for_metric_name(metric_name)}_output",
            view_query_template=OUTPUT_VIEW_TEMPLATE,
            should_materialize=True,
            # Query Format Arguments
            description=f"{metric_name} dashboard output",
            base_dataset=dataset_config.JUSTICE_COUNTS_BASE_DATASET,
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
            metric_output_name=metric_name,
            aggregated_dimension_columns=aggregated_dimension_columns,
            value_column=value_column,
        )


FILTERED_FOR_COMPARISON_VIEW_TEMPLATE = """
SELECT
  *
FROM
  `{project_id}.{input_dataset}.{input_table}` pivot,
  UNNEST(ARRAY['input', 'compare']) AS row_type
WHERE
  EXISTS(
    SELECT
      *
    FROM
      `{project_id}.{input_dataset}.{input_table}` lookaround
    WHERE
      lookaround.date_partition =
        CASE row_type
          WHEN 'input' THEN DATE_SUB(pivot.date_partition, INTERVAL 1 YEAR)
          WHEN 'compare' THEN DATE_ADD(pivot.date_partition, INTERVAL 1 YEAR)
        END
      AND pivot.dimensions_string = lookaround.dimensions_string
  )
"""


class FilteredForComparisonViewBuilder(SimpleBigQueryViewBuilder):
    """Creates a view over the input data that keeps around rows that have a matching
    row a year earlier to compare to.

    When aggregating away non-comprehensive dimensions (e.g. county), we need to ensure
    that the same set of dimension values was used when calculating the primary point
    and the comparison point (e.g. both points only include populations for Los Angeles
    and San Francisco counties).

    This is important when comparing jail populations, but isn't necessary when the
    values are already normalized like when comparing incarceration rates. In that case
    it is okay if a different set of counties was used for each point."""

    def __init__(
        self,
        *,
        dataset_id: str,
        metric_name: str,
        input_view: BigQueryViewBuilder,
    ):
        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{metric_calculator.view_prefix_for_metric_name(metric_name)}_filtered_for_comparison",
            view_query_template=FILTERED_FOR_COMPARISON_VIEW_TEMPLATE,
            # Query Format Arguments
            description=f"{metric_name} filtered for comparison",
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
        )


def transform_results_view_chain(
    metric_name: str, value_column: str, view_builder: SimpleBigQueryViewBuilder
) -> List[SimpleBigQueryViewBuilder]:
    dimensions_to_columns_view_builder = (
        metric_calculator.DimensionsToColumnsViewBuilder(
            dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
            metric_name=metric_name,
            aggregations=OUTPUT_DIMENSIONS,
            input_view=view_builder,
        )
    )
    percentage_covered_view_builder = PercentageCoveredViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_name=metric_name,
        input_view=dimensions_to_columns_view_builder,
    )
    output_view_builder = JailOutputViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_name=metric_name,
        aggregations=OUTPUT_DIMENSIONS,
        value_column=value_column,
        input_view=percentage_covered_view_builder,
    )
    return [
        dimensions_to_columns_view_builder,
        percentage_covered_view_builder,
        output_view_builder,
    ]


def get_jail_population_with_resident_chain() -> List[SimpleBigQueryViewBuilder]:
    jail_pop_view_chain = metric_calculator.calculate_metric_view_chain(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_to_calculate=metric_calculator.CalculatedMetric(
            system=schema.System.CORRECTIONS,
            metric=schema.MetricType.POPULATION,
            filtered_dimensions=[manual_upload.PopulationType.JAIL],
            aggregated_dimensions={
                "state_code": _STATE_CODE_AGGREGATION,
                "county_code": _COUNTY_CODE_AGGREGATION,
                "county_fips": _COUNTY_FIPS_AGGREGATION,
            },
            output_name=POPULATION_METRIC_NAME,
        ),
        time_aggregation=metric_calculator.TimeAggregation.MONTHLY,
    )

    jail_pop_with_resident_view_builder = AddResidentPopulationViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_name=POPULATION_METRIC_NAME,
        input_view=jail_pop_view_chain[-1],
    )
    return [
        *jail_pop_view_chain,
        jail_pop_with_resident_view_builder,
    ]


def _get_filtered_state_jail_population(
    jail_pop_with_resident_view_builder: SimpleBigQueryViewBuilder,
) -> List[SimpleBigQueryViewBuilder]:
    """Filters to only counties that have comparison data available and then aggregates
    up to state level populations.
    """
    filtered_for_comparison_builder = FilteredForComparisonViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_name=POPULATION_METRIC_NAME,
        input_view=jail_pop_with_resident_view_builder,
    )
    filtered_aggregated_to_state = metric_calculator.SpatialAggregationViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_name=POPULATION_METRIC_NAME + "_FILTERED",
        input_view=filtered_for_comparison_builder,
        partition_columns={"date_partition", "row_type"},
        partition_dimensions={manual_upload.State},
        context_columns={
            "source_id": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.ARRAY,
            "report_type": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.ARRAY,
            "report_ids": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.ARRAY_CONCAT,
            "time_window_end": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.MAX,
            "publish_date": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.MAX,
            # It is possible that there are different measurement types across counties, but for now they
            # are all instant so we don't handle that case.
            "measurement_type": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.ANY,
        },
        value_columns={"resident_population", "value"},
        collapse_dimensions_filter=f"dimension in ('{manual_upload.County.dimension_identifier()}')",
    )

    aggregated_to_state_input = SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        view_id=f"{metric_calculator.view_prefix_for_metric_name(POPULATION_METRIC_NAME)}_aggregated_filtered_input",
        view_query_template="""
        SELECT
          date_partition,
          source_id, report_type, report_ids, time_window_end, publish_date, measurement_type,
          num_original_dimensions, dimensions, dimensions_string, collapsed_dimension_values,
          resident_population, value
        FROM `{project_id}.{input_dataset}.{input_table}`
        WHERE row_type = 'input'""",
        description=f"{POPULATION_METRIC_NAME} aggregated to state, filtered to input",
        input_dataset=filtered_aggregated_to_state.dataset_id,
        input_table=filtered_aggregated_to_state.view_id,
    )
    aggregated_to_state_compare = SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        view_id=f"{metric_calculator.view_prefix_for_metric_name(POPULATION_METRIC_NAME)}_aggregated_filtered_compare",
        view_query_template="""
        SELECT
          date_partition,
          source_id, report_type, report_ids, time_window_end, publish_date, measurement_type,
          num_original_dimensions, dimensions, dimensions_string, collapsed_dimension_values,
          resident_population, value
        FROM `{project_id}.{input_dataset}.{input_table}`
        WHERE row_type = 'compare'""",
        description=f"{POPULATION_METRIC_NAME} aggregated to state, filtered to compare",
        input_dataset=filtered_aggregated_to_state.dataset_id,
        input_table=filtered_aggregated_to_state.view_id,
    )

    aggregated_to_state_compared = metric_calculator.CompareToPriorYearViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_name=POPULATION_METRIC_NAME + "_STATE",
        input_view=aggregated_to_state_input,
        compare_view=aggregated_to_state_compare,
    )

    return [
        filtered_for_comparison_builder,
        filtered_aggregated_to_state,
        aggregated_to_state_input,
        aggregated_to_state_compare,
        aggregated_to_state_compared,
    ]


def get_jail_population_with_state_chain(
    jail_pop_with_resident_view_builder: SimpleBigQueryViewBuilder,
) -> List[SimpleBigQueryViewBuilder]:
    """
    Calculate state wide jail populations.

    The interesting bit here is that when calculating state-wide jail populations, we
    want to only use counties that also have data exactly a year prior so that we can
    do a sane comparison between the two numbers.

    This takes a two part approach:
    - Filters the incoming rows to only include counties with a year earlier and then
      summing those up to state level populations.
    - Additionally, summing up all the incoming rows to state level populations so we
      can fall back to these when the filter produces nothing or is too limiting.
    """

    # Get the filtered statewide jail populations
    filtered_state_jail_population_chain = _get_filtered_state_jail_population(
        jail_pop_with_resident_view_builder
    )

    # Aggregate unfiltered rows up to the state level, keeping the county rows around
    # so that they end up with the same set of columns.
    unfiltered_aggregated_to_state = metric_calculator.SpatialAggregationViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_name=POPULATION_METRIC_NAME + "_UNFILTERED",
        input_view=jail_pop_with_resident_view_builder,
        partition_columns={"date_partition"},
        partition_dimensions={manual_upload.State},
        context_columns={
            "source_id": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.ARRAY,
            "report_type": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.ARRAY,
            "report_ids": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.ARRAY_CONCAT,
            "time_window_end": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.MAX,
            "publish_date": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.MAX,
            # It is possible that there are different measurement types across counties, but for now they
            # are all instant so we don't handle that case.
            "measurement_type": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.ANY,
        },
        value_columns={"resident_population", "value"},
        collapse_dimensions_filter=f"dimension in ('{manual_upload.County.dimension_identifier()}')",
        keep_original=True,
    )

    # Pull out the county rows separately and compare them.
    unfiltered_county_jail_pop = SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        view_id=f"{metric_calculator.view_prefix_for_metric_name(POPULATION_METRIC_NAME)}_unfiltered_county",
        view_query_template="""
        SELECT
          date_partition,
          source_id, report_type, report_ids, time_window_end, publish_date, measurement_type,
          num_original_dimensions, dimensions, dimensions_string, collapsed_dimension_values,
          resident_population, value
        FROM `{project_id}.{input_dataset}.{input_table}`
        WHERE '{county_dimension_identifier}' in (SELECT dimension from UNNEST(dimensions))
        """,
        description=f"{POPULATION_METRIC_NAME} aggregated, filtered back to county",
        input_dataset=unfiltered_aggregated_to_state.dataset_id,
        input_table=unfiltered_aggregated_to_state.view_id,
        county_dimension_identifier=manual_upload.County.dimension_identifier(),
    )
    county_compared = metric_calculator.CompareToPriorYearViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_name=POPULATION_METRIC_NAME + "_COUNTY",
        input_view=unfiltered_county_jail_pop,
    )

    # Pull out the unfiltered state rows to use as backup for the filtered data.
    unfiltered_state_jail_pop = SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        view_id=f"{metric_calculator.view_prefix_for_metric_name(POPULATION_METRIC_NAME)}_unfiltered_state",
        view_query_template="""
        SELECT * EXCEPT(ordinal)
        FROM (
          SELECT
            date_partition,
            source_id, report_type, report_ids, time_window_end, publish_date, measurement_type,
            num_original_dimensions, dimensions, dimensions_string, collapsed_dimension_values,
            resident_population, value,
            CAST(NULL as DATE) as compare_date_partition,
            CAST(NULL as NUMERIC) as compare_value,
            -- This is silly but is needed for the tests since they strip the EXCEPT clauses
            1 as ordinal
          FROM `{project_id}.{input_dataset}.{input_table}`
          WHERE ARRAY['{state_dimension_identifier}'] = ARRAY(SELECT dimension from UNNEST(dimensions))
        ) as foo
        """,
        description=f"{POPULATION_METRIC_NAME} aggregated, filtered to state",
        input_dataset=unfiltered_aggregated_to_state.dataset_id,
        input_table=unfiltered_aggregated_to_state.view_id,
        state_dimension_identifier=manual_upload.State.dimension_identifier(),
    )

    # Union the county populations, filtered statewide populations, and fill in with the
    # unfiltered statewide populations.
    unioned_jail_pop = SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        view_id=f"{metric_calculator.view_prefix_for_metric_name(POPULATION_METRIC_NAME)}_union",
        view_query_template="""
        SELECT * FROM `{project_id}.{input_dataset}.{county_table}`
        UNION ALL
        SELECT * FROM `{project_id}.{input_dataset}.{filtered_state_table}`
        UNION ALL
        SELECT * FROM `{project_id}.{input_dataset}.{unfiltered_state_table}` unfiltered
        WHERE NOT EXISTS (
            SELECT * FROM `{project_id}.{input_dataset}.{filtered_state_table}`
            WHERE date_partition = unfiltered.date_partition
            AND dimensions_string = unfiltered.dimensions_string
        )
        """,
        description=f"{POPULATION_METRIC_NAME} unioned state and county",
        input_dataset=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        county_table=county_compared.view_id,
        filtered_state_table=filtered_state_jail_population_chain[-1].view_id,
        unfiltered_state_table=unfiltered_state_jail_pop.view_id,
    )

    return filtered_state_jail_population_chain + [
        unfiltered_aggregated_to_state,
        unfiltered_county_jail_pop,
        county_compared,
        unfiltered_state_jail_pop,
        unioned_jail_pop,
    ]


def get_jail_incarceration_rate_chain(
    jail_pop_with_resident_view_builder: SimpleBigQueryViewBuilder,
) -> List[SimpleBigQueryViewBuilder]:
    """Calculate jail incarceration for both individual counties and state wide."""
    with_state_totals_builder = metric_calculator.SpatialAggregationViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_name=RATE_METRIC_NAME,
        input_view=jail_pop_with_resident_view_builder,
        partition_columns={"date_partition"},
        partition_dimensions={manual_upload.State},
        context_columns={
            "source_id": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.ARRAY,
            "report_type": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.ARRAY,
            "report_ids": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.ARRAY_CONCAT,
            "time_window_end": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.MAX,
            "publish_date": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.MAX,
            # It is possible that there are different measurement types across counties, but for now they
            # are all instant so we don't handle that case.
            "measurement_type": metric_calculator.SpatialAggregationViewBuilder.ContextAggregation.ANY,
        },
        value_columns={"resident_population", "value"},
        collapse_dimensions_filter=f"dimension in ('{manual_upload.County.dimension_identifier()}')",
        keep_original=True,
    )
    rate_builder = IncarcerationRateViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_name=RATE_METRIC_NAME,
        input_view=with_state_totals_builder,
    )

    comparison_view_builder = metric_calculator.CompareToPriorYearViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_name=RATE_METRIC_NAME,
        input_view=rate_builder,
        value_column="incarceration_rate_value",
    )
    return [with_state_totals_builder, rate_builder, comparison_view_builder]


class JailsMetricsBigQueryViewCollector(
    BigQueryViewCollector[SimpleBigQueryViewBuilder]
):
    """Builds out the DAG of view builders for jail calculations"""

    def __init__(self) -> None:
        self.metric_builders: List[SimpleBigQueryViewBuilder] = []

        # TODO(#7685): Add a fetch for comprehensive state data (for unified states).

        # Initial county jail populations, joined with resident populations
        jail_pop_chain = get_jail_population_with_resident_chain()
        self.metric_builders.extend(jail_pop_chain)
        jail_pop_builder = jail_pop_chain[-1]

        # Split into branches to calculate each metric:
        # 1. Calculate state wide jail populations alongside existing county populations
        state_jail_pop_chain = get_jail_population_with_state_chain(jail_pop_builder)
        state_jail_pop_chain.extend(
            transform_results_view_chain(
                POPULATION_METRIC_NAME, "value", state_jail_pop_chain[-1]
            )
        )
        self.metric_builders.extend(state_jail_pop_chain)

        # 2. Calculate incarceration rates (both county and state wide)
        jail_rate_chain = get_jail_incarceration_rate_chain(jail_pop_builder)
        jail_rate_chain.extend(
            transform_results_view_chain(
                RATE_METRIC_NAME, "incarceration_rate_value", jail_rate_chain[-1]
            )
        )
        self.metric_builders.extend(jail_rate_chain)

        # 3. Add separate metric county coverage of statewide incarceration rate
        county_coverage_view = SimpleBigQueryViewBuilder(
            dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
            view_id=metric_calculator.view_prefix_for_metric_name(
                COUNTY_COVERAGE_METRIC_NAME
            ),
            description="Pulls the county coverage for statewide incarceration rates "
            "into its own metric.",
            view_query_template="""
            SELECT
                state_code,
                county_code,
                '{metric_name}' as metric,
                year,
                month,
                date_reported,
                NULL as measurement_type,
                percentage_covered_county as value,
                CAST(NULL as INTEGER) as compared_to_year,
                CAST(NULL as INTEGER) as compared_to_month,
                CAST(NULL as NUMERIC) as value_change,
                CAST(NULL as FLOAT) as percentage_change,
                CAST(NULL as FLOAT) as percentage_covered_county,
                CAST(NULL as FLOAT) as percentage_covered_population,
                date_published
            FROM `{project_id}.{input_dataset}.{input_table}`
            WHERE county_code IS NULL
            """,
            input_dataset=jail_rate_chain[-1].table_for_query.dataset_id,
            input_table=jail_rate_chain[-1].table_for_query.table_id,
            metric_name=COUNTY_COVERAGE_METRIC_NAME,
        )
        self.metric_builders.append(county_coverage_view)

        unified_query_select_clauses = [
            f"SELECT * FROM `{{project_id}}.{{calculation_dataset}}.{view.table_for_query.table_id}`"
            for view in [
                state_jail_pop_chain[-1],
                jail_rate_chain[-1],
                county_coverage_view,
            ]
        ]

        self.unified_builder = SimpleBigQueryViewBuilder(
            dataset_id=dataset_config.JUSTICE_COUNTS_DASHBOARD_DATASET,
            view_id="unified_jails_metrics_monthly",
            view_query_template=" UNION ALL ".join(unified_query_select_clauses) + ";",
            description="Unified view of all calculated jails metrics by month",
            calculation_dataset=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        )

    def collect_view_builders(self) -> List[SimpleBigQueryViewBuilder]:
        return self.metric_builders + [self.unified_builder]


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        collector = JailsMetricsBigQueryViewCollector()
        for builder in collector.collect_view_builders():
            builder.build_and_print()
