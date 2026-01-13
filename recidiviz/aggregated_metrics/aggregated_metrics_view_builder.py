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
"""View builder for a view that calculates metrics for the provided metric
configurations and the specified |population_type|, |unit_of_analysis_type| and
|time_period|.
"""

from more_itertools import one
from tabulate import tabulate

from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
)
from recidiviz.aggregated_metrics.query_building.aggregated_metric_query_utils import (
    AggregatedMetricClassType,
)
from recidiviz.aggregated_metrics.query_building.build_aggregated_metric_query import (
    build_aggregated_metric_query_template,
)
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.utils.types import assert_type


def _metrics_description_table(metrics: list[AggregatedMetric]) -> str:
    """Returns a formatted table with the name, output column name, and description of
    each metric in the provided list.
    """
    table_data = []

    observation_type_category: str = one(
        {metric.observation_type.observation_type_category() for metric in metrics}
    )
    for metric in metrics:
        table_data.append(
            (
                metric.display_name,
                metric.name,
                metric.description,
                metric.observation_type.value,
            )
        )

    return tabulate(
        [row for row in table_data if row is not None],
        headers=[
            "Name",
            "Column",
            "Description",
            f"{observation_type_category.capitalize()} observation type",
        ],
        tablefmt="github",
    )


def aggregated_metric_view_description(
    population_type: MetricPopulationType,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    metric_class: AggregatedMetricClassType,
    time_period: MetricTimePeriodConfig | None,
    metrics: list[AggregatedMetric] | None,
) -> str:
    """Builds a docstring for an aggregated metrics view with the given parameters."""

    if time_period:
        if not time_period.description:
            raise ValueError(
                f"Found no description for time period that is being used to generate "
                f"a view description: {time_period}."
            )
        time_period_clause = f"""
Contains metrics only for: {time_period.description}.
"""
    else:
        time_period_clause = ""

    if issubclass(metric_class, PeriodEventAggregatedMetric):
        base_description = f"""
Metrics for the {population_type.population_name_short} population calculated using
event observations across an entire analysis period, disaggregated by {unit_of_analysis_type.short_name}.
{time_period_clause}
All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
"""
    elif issubclass(metric_class, PeriodSpanAggregatedMetric):
        base_description = f"""
Metrics for the {population_type.population_name_short} population calculated using
span observations across an entire analysis period, disaggregated by {unit_of_analysis_type.short_name}.
{time_period_clause}
All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
"""
    elif issubclass(metric_class, AssignmentEventAggregatedMetric):
        base_description = f"""
Metrics for the {population_type.population_name_short} population calculated using
events over some window following assignment, for all assignments
during an analysis period, disaggregated by {unit_of_analysis_type.short_name}.
{time_period_clause}
All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
"""
    elif issubclass(metric_class, AssignmentSpanAggregatedMetric):
        base_description = f"""
Metrics for the {population_type.population_name_short} population calculated using
spans over some window following assignment, for all assignments
during an analysis period, disaggregated by {unit_of_analysis_type.short_name}.
{time_period_clause}
All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
"""
    else:
        raise ValueError(f"Unexpected metric class type: [{metric_class}]")

    if not metrics:
        return base_description

    return f"""{base_description}
# Metrics
{_metrics_description_table(metrics)}
"""


class AggregatedMetricsBigQueryViewBuilder(BigQueryViewBuilder[BigQueryView]):
    """View builder for a view that calculates metrics for the provided metric
    configurations and the specified |population_type|, |unit_of_analysis_type| and
    |time_period|.
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        population_type: MetricPopulationType,
        unit_of_analysis_type: MetricUnitOfAnalysisType,
        metric_class: AggregatedMetricClassType,
        metrics: list[AggregatedMetric],
        time_period: MetricTimePeriodConfig,
        collection_tag: str | None,
        disaggregate_by_observation_attributes: list[str] | None,
    ) -> None:
        self.dataset_id = dataset_id
        self.view_id = self._build_view_id(
            population_type=population_type,
            unit_of_analysis_type=unit_of_analysis_type,
            metric_class=metric_class,
            time_period=time_period,
            collection_tag=collection_tag,
        )
        self.population_type = population_type
        self.unit_of_analysis_type = unit_of_analysis_type
        self.metric_class = metric_class
        self.metrics = metrics
        self.time_period = time_period
        self.disaggregate_by_observation_attributes = (
            disaggregate_by_observation_attributes
        )

        self.view_query_template = build_aggregated_metric_query_template(
            population_type=population_type,
            unit_of_analysis_type=unit_of_analysis_type,
            metric_class=metric_class,
            metrics=metrics,
            time_period=time_period,
            disaggregate_by_observation_attributes=disaggregate_by_observation_attributes,
        )
        self.projects_to_deploy = None
        self.materialized_address = self._build_materialized_address(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            materialized_address_override=None,
            should_materialize=True,
        )

    @staticmethod
    def _build_view_id(
        population_type: MetricPopulationType,
        unit_of_analysis_type: MetricUnitOfAnalysisType,
        metric_class: AggregatedMetricClassType,
        time_period: MetricTimePeriodConfig,
        collection_tag: str | None,
    ) -> str:
        collection_tag_part = "" if not collection_tag else f"{collection_tag}__"
        population_name = population_type.population_name_short
        unit_of_analysis_name = unit_of_analysis_type.short_name
        metric_class_name = metric_class.metric_class_name_lower()
        return f"{collection_tag_part}{population_name}_{unit_of_analysis_name}_{metric_class_name}_aggregated_metrics__{assert_type(time_period.config_name, str)}"

    @property
    def output_columns(self) -> list[str]:
        return [
            *self.unit_of_analysis.primary_key_columns,
            "start_date",
            "end_date",
            f"{MetricTimePeriodConfig.METRIC_TIME_PERIOD_PERIOD_COLUMN}",
            *(
                attribute
                for attribute in self.disaggregate_by_observation_attributes or []
            ),
            *sorted(metric.name for metric in self.metrics),
        ]

    @property
    def description(self) -> str:
        return aggregated_metric_view_description(
            population_type=self.population_type,
            unit_of_analysis_type=self.unit_of_analysis_type,
            time_period=self.time_period,
            metric_class=self.metric_class,
            metrics=self.metrics,
        )

    @property
    def bq_description(self) -> str:
        return aggregated_metric_view_description(
            population_type=self.population_type,
            unit_of_analysis_type=self.unit_of_analysis_type,
            time_period=self.time_period,
            metric_class=self.metric_class,
            # Don't list metrics to keep the description short
            metrics=None,
        )

    @property
    def unit_of_analysis(self) -> MetricUnitOfAnalysis:
        return MetricUnitOfAnalysis.for_type(self.unit_of_analysis_type)

    @property
    def clustering_fields(self) -> list[str]:
        """Cluster the output on the unit of analysis primary keys, which are the most
        likely to be joined against in subsequent queries.
        """
        return self.unit_of_analysis.primary_key_columns

    def _build(
        self, *, sandbox_context: BigQueryViewSandboxContext | None
    ) -> BigQueryView:
        return BigQueryView(
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            description=self.description,
            bq_description=self.bq_description,
            view_query_template=self.view_query_template,
            materialized_address=self.materialized_address,
            clustering_fields=self.clustering_fields,
            sandbox_context=sandbox_context,
        )
