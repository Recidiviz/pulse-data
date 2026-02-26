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
    metric_output_column_bq_field_type,
)
from recidiviz.big_query.big_query_schema_utils import bq_field_type_to_column_class
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_column import BigQueryViewColumn, Date, String
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.utils.types import assert_type


def aggregated_metric_view_description(
    population_type: MetricPopulationType,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    metric_class: AggregatedMetricClassType,
    time_period: MetricTimePeriodConfig | None,
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
        return f"""
Metrics for the {population_type.population_name_short} population calculated using
event observations across an entire analysis period, disaggregated by {unit_of_analysis_type.short_name}.
{time_period_clause}
All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
"""
    if issubclass(metric_class, PeriodSpanAggregatedMetric):
        return f"""
Metrics for the {population_type.population_name_short} population calculated using
span observations across an entire analysis period, disaggregated by {unit_of_analysis_type.short_name}.
{time_period_clause}
All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
"""
    if issubclass(metric_class, AssignmentEventAggregatedMetric):
        return f"""
Metrics for the {population_type.population_name_short} population calculated using
events over some window following assignment, for all assignments
during an analysis period, disaggregated by {unit_of_analysis_type.short_name}.
{time_period_clause}
All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
"""
    if issubclass(metric_class, AssignmentSpanAggregatedMetric):
        return f"""
Metrics for the {population_type.population_name_short} population calculated using
spans over some window following assignment, for all assignments
during an analysis period, disaggregated by {unit_of_analysis_type.short_name}.
{time_period_clause}
All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
"""
    raise ValueError(f"Unexpected metric class type: [{metric_class}]")


def aggregated_metric_view_schema(
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    metrics: list[AggregatedMetric],
    disaggregate_by_observation_attributes: list[str] | None,
) -> list[BigQueryViewColumn]:
    """Returns the BigQuery schema for an aggregated metrics view.

    Column layout matches the output_columns property order:
    1. Primary key columns from the unit of analysis (REQUIRED)
    2. start_date (DATE REQUIRED)
    3. end_date (DATE REQUIRED)
    4. period (STRING REQUIRED)
    5. Disaggregation attributes (STRING NULLABLE)
    6. Metric columns sorted by name (INTEGER or FLOAT NULLABLE)
    """
    unit_of_analysis = MetricUnitOfAnalysis.for_type(unit_of_analysis_type)

    columns: list[BigQueryViewColumn] = []

    for pk_col in unit_of_analysis.primary_key_columns:
        pk_type = unit_of_analysis.primary_key_column_type(pk_col)
        pk_column_cls = bq_field_type_to_column_class(pk_type)
        columns.append(
            pk_column_cls(
                name=pk_col,
                description=f"Primary key: {pk_col}",
                mode="REQUIRED",
            )
        )

    columns.append(
        Date(
            name="start_date",
            description="Analysis period start date (inclusive)",
            mode="REQUIRED",
        )
    )
    columns.append(
        Date(
            name="end_date",
            description="Analysis period end date (exclusive)",
            mode="REQUIRED",
        )
    )
    columns.append(
        String(
            name="period",
            description="A string descriptor for the analysis period length. One of: 'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR' or 'CUSTOM'.",
            mode="REQUIRED",
        )
    )

    for attr in disaggregate_by_observation_attributes or []:
        columns.append(
            String(
                name=attr,
                description=f"Disaggregation attribute: {attr}",
                mode="NULLABLE",
            )
        )

    for metric in sorted(metrics, key=lambda m: m.name):
        field_type = metric_output_column_bq_field_type(metric)
        metric_column_cls = bq_field_type_to_column_class(field_type)
        columns.append(
            metric_column_cls(
                name=metric.name,
                description=f"{metric.display_name}: {metric.description} (Observation Type: {metric.observation_type.value})",
                mode="NULLABLE",
            )
        )

    return columns


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
        )

    @property
    def bq_description(self) -> str:
        return aggregated_metric_view_description(
            population_type=self.population_type,
            unit_of_analysis_type=self.unit_of_analysis_type,
            time_period=self.time_period,
            metric_class=self.metric_class,
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

    @property
    def schema(self) -> list[BigQueryViewColumn]:
        return aggregated_metric_view_schema(
            unit_of_analysis_type=self.unit_of_analysis_type,
            metrics=self.metrics,
            disaggregate_by_observation_attributes=self.disaggregate_by_observation_attributes,
        )

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
            schema=self.schema,
        )
