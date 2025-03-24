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
"""Returns a query template to generate all metrics for specified unit of analysis,
population, and custom time periods.
"""
from collections import defaultdict
from datetime import datetime
from typing import List, Optional

from recidiviz.aggregated_metrics.aggregated_metrics_view_collector import (
    METRIC_CLASSES,
)
from recidiviz.aggregated_metrics.assignment_sessions_view_collector import (
    relevant_units_of_analysis_for_population_type,
)
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriod,
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
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
    metric_output_column_clause,
)
from recidiviz.utils.string_formatting import fix_indent


def get_custom_aggregated_metrics_query_template(
    metrics: List[AggregatedMetric],
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    population_type: MetricPopulationType,
    time_interval_unit: MetricTimePeriod,
    time_interval_length: int,
    min_end_date: datetime,
    max_end_date: Optional[datetime] = None,
    rolling_period_unit: Optional[MetricTimePeriod] = None,
    rolling_period_length: Optional[int] = None,
) -> str:
    """Returns a query template to generate all metrics for specified unit of analysis,
    population, and custom time periods.
    """
    if not metrics:
        raise ValueError("Must provide at least one metric - none provided.")
    if unit_of_analysis_type not in relevant_units_of_analysis_for_population_type(
        population_type
    ):
        raise ValueError(
            f"Unsupported population and unit of analysis pair: {unit_of_analysis_type.value}, {population_type.value}"
        )

    # Group metrics by metric class
    metrics_by_metric_class: dict[
        AggregatedMetricClassType, list[AggregatedMetric]
    ] = defaultdict(list)
    for metric in metrics:
        for metric_class in METRIC_CLASSES:
            if isinstance(metric, metric_class):
                metrics_by_metric_class[metric_class].append(metric)

    metric_time_period = MetricTimePeriodConfig(
        interval_unit=time_interval_unit,
        interval_length=time_interval_length,
        min_period_end_date=min_end_date.date(),
        max_period_end_date=max_end_date.date() if max_end_date else None,
        rolling_period_unit=rolling_period_unit,
        rolling_period_length=rolling_period_length,
        period_name=MetricTimePeriod.CUSTOM.value,
    )

    single_class_queries_by_class: dict[AggregatedMetricClassType, str] = {}
    for metric_class, metrics_for_class in metrics_by_metric_class.items():
        query_template = build_aggregated_metric_query_template(
            # TODO(#39489): Add support for custom population builder
            population_type=population_type,
            # TODO(#27010): Add support for custom unit of analysis builder
            unit_of_analysis_type=unit_of_analysis_type,
            metric_class=metric_class,
            metrics=metrics_for_class,
            time_period=metric_time_period,
            read_from_cached_assignments_by_time_period=False,
        )
        single_class_queries_by_class[metric_class] = query_template

    all_metric_class_ctes_query_template = ",\n".join(
        [
            f"""{metric_class.metric_class_name_lower()}_metrics AS (
{fix_indent(query_template, indent_level=4)}
)"""
            for metric_class, query_template in single_class_queries_by_class.items()
        ]
    )
    metric_class_cte_names = [
        f"{metric_class.metric_class_name_lower()}_metrics"
        for metric_class in single_class_queries_by_class
    ]
    metric_class_cte_names_sorted = sorted(metric_class_cte_names)

    metric_class_join = (
        ""
        if len(metric_class_cte_names_sorted) == 1
        else "".join(
            [
                f"""
FULL OUTER JOIN
    {cte_name}
USING
    ({MetricUnitOfAnalysis.for_type(unit_of_analysis_type).get_primary_key_columns_query_string()}, period, start_date, end_date)"""
                for cte_name in metric_class_cte_names_sorted[1:]
            ]
        )
    )

    final_select_metrics_query_fragment = ",\n".join(
        [metric_output_column_clause(metric) for metric in metrics]
    )

    query_template = f"""
WITH {all_metric_class_ctes_query_template}
SELECT
    {MetricUnitOfAnalysis.for_type(unit_of_analysis_type).get_primary_key_columns_query_string()},
    period,
    start_date,
    end_date,
{fix_indent(final_select_metrics_query_fragment, indent_level=4)}
FROM
    {metric_class_cte_names_sorted[0]}{metric_class_join}
"""

    return query_template
