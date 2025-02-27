# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Creates view builders that generate SQL views calculating period-span metrics"""
from typing import Dict, List

from recidiviz.aggregated_metrics.aggregated_metrics_utils import (
    get_joined_metrics_by_observation_type_query,
    get_unioned_time_granularity_clause,
)
from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.aggregated_metrics.metric_time_periods import MetricTimePeriod
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    PeriodSpanAggregatedMetric,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    join_on_columns_fragment,
    list_to_query_string,
    nonnull_current_date_clause,
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfObservation,
    MetricUnitOfObservationType,
)


def generate_period_span_aggregated_metrics_view_builder(
    unit_of_analysis: MetricUnitOfAnalysis,
    population_type: MetricPopulationType,
    metrics: List[PeriodSpanAggregatedMetric],
) -> SimpleBigQueryViewBuilder:
    """
    Returns a SimpleBigQueryViewBuilder that calculates PeriodSpan metrics for the specified
    unit of analysis, population, and set of metrics.
    """
    view_id = f"{population_type.population_name_short}_{unit_of_analysis.type.short_name}_period_span_aggregated_metrics"
    view_description = f"""
    Metrics for the {population_type.population_name_short} population calculated using
    spans across an entire analysis period, disaggregated by {unit_of_analysis.type.short_name}.

    All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
    """

    # helper function to get weekly and monthly aggregated CTEs
    def get_time_specific_ctes(metric_time_period: MetricTimePeriod) -> str:
        """
        Returns CTEs for weekly and monthly aggregated metrics, unioning together cte's
        from each unit of observation.
        """
        if metric_time_period not in (MetricTimePeriod.WEEK, MetricTimePeriod.MONTH):
            raise ValueError(
                f"Unsupported metric_time_period: {metric_time_period.value}"
            )
        # get query string across the set of unit of analysis index columns and unit of observation primary key columns

        metric_subqueries_by_unit_of_observation: List[str] = []
        metrics_by_unit_of_observation: Dict[
            MetricUnitOfObservation, List[PeriodSpanAggregatedMetric]
        ] = {
            unit_of_observation: [
                m for m in metrics if m.unit_of_observation == unit_of_observation
            ]
            for unit_of_observation in set(m.unit_of_observation for m in metrics)
        }
        for (
            unit_of_observation,
            metrics_for_unit_of_observation,
        ) in metrics_by_unit_of_observation.items():

            shared_columns_string = list_to_query_string(
                sorted(
                    {
                        *unit_of_observation.primary_key_columns,
                        *unit_of_analysis.index_columns,
                    }
                ),
                table_prefix="assign",
            )
            metric_aggregation_fragment = ",\n".join(
                [
                    metric.generate_aggregation_query_fragment(
                        span_start_date_col="ses.start_date",
                        span_end_date_col="ses.end_date",
                        period_start_date_col="pop.population_start_date",
                        period_end_date_col="pop.population_end_date",
                        original_span_start_date="ses.span_start_date",
                    )
                    for metric in metrics_for_unit_of_observation
                ]
            )
            # Only include the static attribute columns if the unit of observation is `person`.
            # This ensures that we don't have conflicting values for the static attributes
            # across assignment queries of different observation types.
            # TODO(#25676): Remove this logic once static attribute columns are joined in further downstream
            unit_of_analysis_join_columns_str = (
                unit_of_analysis.get_index_columns_query_string()
                if unit_of_observation.type == MetricUnitOfObservationType.PERSON_ID
                else unit_of_analysis.get_primary_key_columns_query_string()
            )
            metric_subquery = f"""
    SELECT
        {unit_of_analysis_join_columns_str},
        population_start_date AS start_date,
        population_end_date AS end_date,
        period,
        {metric_aggregation_fragment}
    FROM (
        SELECT
            {shared_columns_string},
            GREATEST(assign.assignment_date, spans.start_date) AS start_date,
            spans.start_date AS span_start_date,
            {revert_nonnull_end_date_clause(
                f"LEAST({nonnull_end_date_clause('spans.end_date')}, {nonnull_end_date_clause('assign.end_date')})"
            )} AS end_date,
            span,
            span_attributes,
        FROM
            `{{project_id}}.analyst_data.{unit_of_observation.type.value.lower()}_spans_materialized` AS spans
        INNER JOIN
            `{{project_id}}.aggregated_metrics.{population_type.population_name_short}_{unit_of_analysis.type.short_name}_metrics_{unit_of_observation.type.short_name}_assignment_sessions_materialized` assign
        ON
            {join_on_columns_fragment(columns=sorted(unit_of_observation.primary_key_columns), table1="spans", table2="assign")}
            AND (
              assign.assignment_date
                BETWEEN spans.start_date AND {nonnull_end_date_exclusive_clause("spans.end_date")}
              OR spans.start_date
                BETWEEN assign.assignment_date AND {nonnull_end_date_exclusive_clause("assign.end_date")}
            )
            -- Disregard zero day sample spans for calculating population metrics
            AND assign.assignment_date != {nonnull_end_date_clause("assign.end_date")}
    ) ses
    INNER JOIN
        time_periods pop
    ON
        ses.start_date < pop.population_end_date
        AND pop.population_start_date < {nonnull_current_date_clause("ses.end_date")}
    WHERE
        period = "{metric_time_period.value}"
    GROUP BY
        {unit_of_analysis_join_columns_str},
        population_start_date, population_end_date, period"""
            metric_subqueries_by_unit_of_observation.append(metric_subquery)
        # Construct query by joining metrics across all units of observation
        return get_joined_metrics_by_observation_type_query(
            unit_of_analysis, metric_subqueries_by_unit_of_observation, metrics
        )

    query_template = f"""
WITH time_periods AS (
    SELECT * FROM `{{project_id}}.aggregated_metrics.metric_time_periods_materialized`
)
, week_metrics AS ({get_time_specific_ctes(MetricTimePeriod.WEEK)})

, month_metrics AS ({get_time_specific_ctes(MetricTimePeriod.MONTH)})
""" + get_unioned_time_granularity_clause(
        unit_of_analysis=unit_of_analysis,
        metrics=metrics,
    )

    return SimpleBigQueryViewBuilder(
        dataset_id=AGGREGATED_METRICS_DATASET_ID,
        view_id=view_id,
        view_query_template=query_template,
        description=view_description,
        should_materialize=False,
        clustering_fields=unit_of_analysis.primary_key_columns,
    )
