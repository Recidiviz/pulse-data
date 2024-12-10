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
"""Helper for building aggregated metrics queries."""
from collections import defaultdict

from recidiviz.aggregated_metrics.assignments_by_time_period_view_builder import (
    AssignmentsByTimePeriodViewBuilder,
)
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    EventCountMetric,
    EventDistinctUnitCountMetric,
    EventValueMetric,
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
    metric_group_by_columns,
)
from recidiviz.aggregated_metrics.query_building.build_single_observation_type_aggregated_metric_query import (
    build_single_observation_type_aggregated_metric_query_template,
)
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.observation_type_utils import ObservationType
from recidiviz.utils.string_formatting import fix_indent

_OUTPUT_ROW_KEYS_CTE_NAME = "output_row_keys"


def _query_as_cte(*, cte_name: str, query: str) -> str:
    """Formats the given |query| string into a named CTE."""
    return f"""{cte_name} AS (
{fix_indent(query, indent_level=4)}
)"""


def _get_assignments_by_time_period_cte_name(
    unit_of_observation_type: MetricUnitOfObservationType,
) -> str:
    """Returns the name of the CTE that contains assignment by time period rows for the
    given CTE.
    """
    return f"{unit_of_observation_type.value.lower()}_assignments_by_time_period"


def _build_assignments_by_time_period_cte_queries(
    *,
    population_type: MetricPopulationType,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    metric_class: AggregatedMetricClassType,
    unit_of_observation_types: set[MetricUnitOfObservationType],
    time_period: MetricTimePeriodConfig,
) -> dict[str, str]:
    """Returns a dictionary mapping CTE name to the CTE query for all CTEs that produce
    assignments by time period rows (one per unit of observation type referenced by
    metrics).
    """
    cte_queries_by_name = {}
    for unit_of_observation_type in unit_of_observation_types:
        assignments_address = AssignmentsByTimePeriodViewBuilder.build_materialized_address(
            time_period=time_period,
            population_type=population_type,
            unit_of_observation_type=unit_of_observation_type,
            unit_of_analysis_type=unit_of_analysis_type,
            metric_time_period_join_type=metric_class.metric_time_period_join_type(),
        )

        assignments_by_time_period_query = assignments_address.select_query_template()
        cte_name = _get_assignments_by_time_period_cte_name(unit_of_observation_type)
        cte_queries_by_name[cte_name] = assignments_by_time_period_query
    return cte_queries_by_name


def _build_output_rows_cte_query(
    *,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    assignments_by_time_period_cte_names: list[str],
) -> str:
    """Returns a query with one row per primary key that will be produced by the overall
    aggregated metrics query. This will serve as the base table we will join all single
    observation type metric results to.
    """
    metric_primary_key_columns_clause = list_to_query_string(
        metric_group_by_columns(unit_of_analysis_type)
    )
    output_rows_cte_parts = []
    for assignments_by_time_period_cte_name in assignments_by_time_period_cte_names:
        output_rows_cte_parts.append(
            f"""
            SELECT DISTINCT {metric_primary_key_columns_clause}
            FROM {assignments_by_time_period_cte_name}
            """
        )

    return "\nUNION DISTINCT\n".join(
        fix_indent(p, indent_level=0) for p in output_rows_cte_parts
    )


def _build_single_observation_type_cte_queries(
    *,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    metric_class: AggregatedMetricClassType,
    metrics_by_observation_type: dict[ObservationType, list[AggregatedMetric]],
) -> dict[str, str]:
    """Returns a dictionary mapping CTE name to the CTE query for all CTEs that produce
    single observation type metrics.
    """
    cte_queries_by_name = {}
    for observation_type, metrics in metrics_by_observation_type.items():
        assignments_by_time_period_cte_name = _get_assignments_by_time_period_cte_name(
            observation_type.unit_of_observation_type
        )
        single_observation_metrics = (
            build_single_observation_type_aggregated_metric_query_template(
                observation_type=observation_type,
                unit_of_analysis_type=unit_of_analysis_type,
                metric_class=metric_class,
                single_observation_type_metrics=metrics,
                assignments_by_time_period_cte_name=assignments_by_time_period_cte_name,
            )
        )
        cte_name = f"{observation_type.name.lower()}_metrics"
        cte_queries_by_name[cte_name] = single_observation_metrics
    return cte_queries_by_name


def _metric_output_column_clause(metric: AggregatedMetric) -> str:
    """Returns the clause that should be used in the final select to produce this metric
    value.
    """

    if isinstance(metric, (EventCountMetric, EventDistinctUnitCountMetric)):
        #  We normalize NULL values to 0 to account for time periods when we did not
        #  count any observations and did not produce a row in the single
        #  observation-type CTE.
        return f"IFNULL({metric.name}, 0) AS {metric.name}"

    if isinstance(metric, EventValueMetric):
        return metric.name

    # TODO(#29291): Add support for other metric types as we support other metric class
    #  types.
    raise ValueError(f"Unexpected metric type [{type(metric)}]")


def _single_observation_type_cte_join_clause(
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    single_observation_type_cte_name: str,
) -> str:
    """Returns a join clause that will join the metrics columns from this single
    observation type CTE into the final result.
    """
    metric_primary_key_columns_clause = list_to_query_string(
        metric_group_by_columns(unit_of_analysis_type)
    )
    return f"""LEFT OUTER JOIN
    {single_observation_type_cte_name}
USING ({metric_primary_key_columns_clause})"""


def build_aggregated_metric_query_template(
    population_type: MetricPopulationType,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    metric_class: AggregatedMetricClassType,
    metrics: list[AggregatedMetric],
    time_period: MetricTimePeriodConfig,
) -> str:
    """Returns a query template (with a project_id format arg) that can be used to
    calculate metrics for the provided metric configurations and the specified
    |population_type| and |unit_of_analysis_type|.

    This query will produce metrics for all metric periods where a unit of analysis has
    assignments to any unit of observation referenced by these metrics, even if there
    are no recorded observations during that time period.
    """

    if not metrics:
        raise ValueError(
            "Attempting to build an aggregated metric query template with no metrics."
        )

    for metric in metrics:
        if not isinstance(metric, metric_class):
            raise ValueError(
                f"Found metric of type [{type(metric)}] which is not a subclass of "
                f"expected metric class [{metric_class}]"
            )

    metrics_by_observation_type: dict[
        ObservationType, list[AggregatedMetric]
    ] = defaultdict(list)
    for metric in metrics:
        metrics_by_observation_type[metric.observation_type].append(metric)

    # BUILD UP CTES
    cte_queries_by_name = {}
    assignments_by_time_period_ctes_by_name = (
        _build_assignments_by_time_period_cte_queries(
            time_period=time_period,
            population_type=population_type,
            unit_of_analysis_type=unit_of_analysis_type,
            metric_class=metric_class,
            unit_of_observation_types={
                observation_type.unit_of_observation_type
                for observation_type in metrics_by_observation_type
            },
        )
    )
    cte_queries_by_name.update(assignments_by_time_period_ctes_by_name)

    cte_queries_by_name[_OUTPUT_ROW_KEYS_CTE_NAME] = _build_output_rows_cte_query(
        unit_of_analysis_type=unit_of_analysis_type,
        assignments_by_time_period_cte_names=sorted(
            assignments_by_time_period_ctes_by_name
        ),
    )

    single_observation_type_ctes_by_name = _build_single_observation_type_cte_queries(
        unit_of_analysis_type=unit_of_analysis_type,
        metric_class=metric_class,
        metrics_by_observation_type=metrics_by_observation_type,
    )
    cte_queries_by_name.update(single_observation_type_ctes_by_name)

    ordered_cte_names = [
        *sorted(assignments_by_time_period_ctes_by_name),
        _OUTPUT_ROW_KEYS_CTE_NAME,
        *sorted(single_observation_type_ctes_by_name),
    ]

    ctes = ",\n".join(
        _query_as_cte(cte_name=cte_name, query=cte_queries_by_name[cte_name])
        for cte_name in ordered_cte_names
    )

    # BUILD UP OUTPUT COLUMNS CLAUSE
    unit_of_analysis: MetricUnitOfAnalysis = MetricUnitOfAnalysis.for_type(
        unit_of_analysis_type
    )
    output_cols = [
        *unit_of_analysis.primary_key_columns,
        f"{MetricTimePeriodConfig.METRIC_TIME_PERIOD_START_DATE_COLUMN} AS start_date",
        f"{MetricTimePeriodConfig.METRIC_TIME_PERIOD_END_DATE_EXCLUSIVE_COLUMN} AS end_date",
        MetricTimePeriodConfig.METRIC_TIME_PERIOD_PERIOD_COLUMN,
        *[
            _metric_output_column_clause(metric)
            for metric in sorted(metrics, key=lambda m: m.name)
        ],
    ]

    output_cols_str = ",\n".join(output_cols)

    # BUILD UP JOIN CLAUSE
    join_clauses = [
        _single_observation_type_cte_join_clause(
            unit_of_analysis_type=unit_of_analysis_type,
            single_observation_type_cte_name=name,
        )
        for name in single_observation_type_ctes_by_name
    ]
    join_clause = "\n".join(join_clauses)

    return f"""
WITH 
{fix_indent(ctes, indent_level=0)}
SELECT
{fix_indent(output_cols_str, indent_level=4)}
FROM {_OUTPUT_ROW_KEYS_CTE_NAME}
{join_clause}
"""
