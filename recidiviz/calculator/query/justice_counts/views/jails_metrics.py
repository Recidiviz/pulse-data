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
    * EXCEPT(value, resident_population),
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


# TODO(#6020): Implement percentage coverage
PERCENTAGE_COVERED_VIEW_TEMPLATE = """
SELECT *, NULL as percentage_covered_county, NULL as percentage_covered_population
FROM `{project_id}.{input_dataset}.{input_table}`
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


def transform_results_view_chain(
    metric_name: str, value_column: str, view_builder: SimpleBigQueryViewBuilder
) -> List[SimpleBigQueryViewBuilder]:
    comparison_view_builder = metric_calculator.CompareToPriorYearViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_name=metric_name,
        input_view=view_builder,
        value_column=value_column,
    )
    percentage_covered_view_builder = PercentageCoveredViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_name=metric_name,
        input_view=comparison_view_builder,
    )
    dimensions_to_columns_view_builder = (
        metric_calculator.DimensionsToColumnsViewBuilder(
            dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
            metric_name=metric_name,
            aggregations=OUTPUT_DIMENSIONS,
            input_view=percentage_covered_view_builder,
        )
    )
    output_view_builder = JailOutputViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_name=metric_name,
        aggregations=OUTPUT_DIMENSIONS,
        value_column=value_column,
        input_view=dimensions_to_columns_view_builder,
    )
    return [
        comparison_view_builder,
        percentage_covered_view_builder,
        dimensions_to_columns_view_builder,
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


def get_jail_incarceration_rate_chain(
    jail_pop_with_resident_view_builder: SimpleBigQueryViewBuilder,
) -> List[SimpleBigQueryViewBuilder]:
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
    return [with_state_totals_builder, rate_builder]


class JailsMetricsBigQueryViewCollector(
    BigQueryViewCollector[SimpleBigQueryViewBuilder]
):
    """Builds out the DAG of view builders for jail calculations"""

    def __init__(self) -> None:
        self.metric_builders: List[SimpleBigQueryViewBuilder] = []

        jail_pop_chain = get_jail_population_with_resident_chain()
        self.metric_builders.extend(jail_pop_chain)
        jail_pop_builder = jail_pop_chain[-1]

        jail_rate_chain = get_jail_incarceration_rate_chain(jail_pop_builder)
        self.metric_builders.extend(jail_rate_chain)
        jail_rate_builder = jail_rate_chain[-1]

        unified_query_select_clauses = []
        for metric_name, value_column, view_builder in [
            (POPULATION_METRIC_NAME, "value", jail_pop_builder),
            (RATE_METRIC_NAME, "incarceration_rate_value", jail_rate_builder),
        ]:
            transform_view_chain = transform_results_view_chain(
                metric_name, value_column, view_builder
            )
            self.metric_builders.extend(transform_view_chain)

            unified_query_select_clauses.append(
                f"SELECT * FROM `{{project_id}}.{{calculation_dataset}}.{transform_view_chain[-1].view_id}_materialized`"
            )

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
