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

from typing import List

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.calculator.query.justice_counts import dataset_config
from recidiviz.calculator.query.justice_counts.views import metric_by_month
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.tools.justice_counts import manual_upload
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_STATE_CODE_AGGREGATION = metric_by_month.Aggregation(
    dimension=manual_upload.State, comprehensive=False
)
_COUNTY_CODE_AGGREGATION = metric_by_month.Aggregation(
    dimension=manual_upload.County, comprehensive=False
)

METRICS = [
    metric_by_month.CalculatedMetricByMonth(
        system=schema.System.CORRECTIONS,
        metric=schema.MetricType.POPULATION,
        filtered_dimensions=[manual_upload.PopulationType.JAIL],
        aggregated_dimensions={
            "state_code": _STATE_CODE_AGGREGATION,
            "county_code": _COUNTY_CODE_AGGREGATION,
        },
        output_name="POPULATION_JAIL",
    ),
]

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
        metric_to_calculate: metric_by_month.CalculatedMetricByMonth,
        input_view: BigQueryViewBuilder,
    ):
        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{metric_to_calculate.view_prefix}_percentage_covered",
            view_query_template=PERCENTAGE_COVERED_VIEW_TEMPLATE,
            # Query Format Arguments
            description=f"{metric_to_calculate.output_name} percentage covered",
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
        )


OUTPUT_VIEW_TEMPLATE = """
SELECT {aggregated_dimension_columns},
       '{metric_output_name}' as metric,
       EXTRACT(YEAR from start_of_month) as year,
       EXTRACT(MONTH from start_of_month) as month,
       DATE_SUB(time_window_end, INTERVAL 1 DAY) as date_reported,
       value as value,
       -- If there is no row at least a year back to compare to, the following columns will be NULL.
       EXTRACT(YEAR from compare_start_of_month) as compared_to_year,
       EXTRACT(MONTH from compare_start_of_month) as compared_to_month,
       value - compare_value as value_change,
       -- Note: This can return NULL in the case of divide by zero
       SAFE_DIVIDE((value - compare_value), compare_value) as percentage_change,
       percentage_covered_county as percentage_covered_county,
       percentage_covered_population as percentage_covered_population
FROM `{project_id}.{input_dataset}.{input_table}`
"""


class JailOutputViewBuilder(SimpleBigQueryViewBuilder):
    """Factory class for creating corrections output from an input view."""

    def __init__(
        self,
        *,
        dataset_id: str,
        metric_to_calculate: metric_by_month.CalculatedMetricByMonth,
        input_view: BigQueryViewBuilder,
    ):
        aggregated_dimension_columns = ", ".join(
            metric_to_calculate.aggregated_dimensions
        )

        super().__init__(
            dataset_id=dataset_id,
            view_id=f"{metric_to_calculate.view_prefix}_output",
            view_query_template=OUTPUT_VIEW_TEMPLATE,
            should_materialize=True,
            # Query Format Arguments
            description=f"{metric_to_calculate.output_name} dashboard output",
            base_dataset=dataset_config.JUSTICE_COUNTS_BASE_DATASET,
            input_dataset=input_view.dataset_id,
            input_table=input_view.view_id,
            metric_output_name=metric_to_calculate.output_name,
            aggregated_dimension_columns=aggregated_dimension_columns,
        )


def view_chain_for_metric(
    metric: metric_by_month.CalculatedMetricByMonth,
) -> List[SimpleBigQueryViewBuilder]:
    calculate_view_builder = metric_by_month.CalculatedMetricByMonthViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_to_calculate=metric,
    )
    comparison_view_builder = metric_by_month.CompareToPriorYearViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_to_calculate=metric,
        input_view=calculate_view_builder,
    )
    percentage_covered_view_builder = PercentageCoveredViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_to_calculate=metric,
        input_view=comparison_view_builder,
    )
    dimensions_to_columns_view_builder = metric_by_month.DimensionsToColumnsViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_to_calculate=metric,
        input_view=percentage_covered_view_builder,
    )
    output_view_builder = JailOutputViewBuilder(
        dataset_id=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        metric_to_calculate=metric,
        input_view=dimensions_to_columns_view_builder,
    )
    return [
        calculate_view_builder,
        comparison_view_builder,
        percentage_covered_view_builder,
        dimensions_to_columns_view_builder,
        output_view_builder,
    ]


class JailsMetricsByMonthBigQueryViewCollector(
    BigQueryViewCollector[SimpleBigQueryViewBuilder]
):
    def __init__(self) -> None:
        self.metric_builders: List[SimpleBigQueryViewBuilder] = []
        unified_query_select_clauses = []
        for metric in METRICS:
            view_builders = view_chain_for_metric(metric)
            unified_query_select_clauses.append(
                f"SELECT * FROM `{{project_id}}.{{calculation_dataset}}.{view_builders[-1].view_id}_materialized`"
            )
            self.metric_builders.extend(view_builders)

        self.unified_builder = SimpleBigQueryViewBuilder(
            dataset_id=dataset_config.JUSTICE_COUNTS_DASHBOARD_DATASET,
            view_id="unified_jails_metrics_by_month",
            view_query_template=" UNION ALL ".join(unified_query_select_clauses) + ";",
            description="Unified view of all calculated jails metrics by month",
            calculation_dataset=dataset_config.JUSTICE_COUNTS_JAILS_DATASET,
        )

    def collect_view_builders(self) -> List[SimpleBigQueryViewBuilder]:
        return self.metric_builders + [self.unified_builder]


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        collector = JailsMetricsByMonthBigQueryViewCollector()
        for builder in collector.collect_view_builders():
            builder.build_and_print()
