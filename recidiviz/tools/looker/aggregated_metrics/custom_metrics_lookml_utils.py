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
"""Util functions to support generating new aggregated metrics on the fly within LookML"""

from typing import List

from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_DAILY_POPULATION,
)
from recidiviz.calculator.query.bq_utils import (
    nonnull_current_date_clause,
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    LookMLFieldParameter,
    LookMLViewField,
    ParameterLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    LookMLFieldType,
    LookMLSqlReferenceType,
)
from recidiviz.looker.lookml_view_source_table import LookMLViewSourceTable
from recidiviz.tools.looker.aggregated_metrics.aggregated_metrics_lookml_utils import (
    get_metric_explore_parameter,
    get_metric_value_measure,
    measure_for_metric,
)


def liquid_wrap(query_fragment: str, metric: AggregatedMetric, view_name: str) -> str:
    """
    Outputs a conditional liquid fragment that will display the provided query fragment
    if the metric is selected in the Explore.
    """
    return f"""{{% if {view_name}.{metric.name}_measure._in_query or {view_name}.metric_filter._parameter_value contains "{metric.name}" %}}
    {query_fragment},
        {{% endif %}}"""


def generate_period_span_metric_view(
    metrics: List[PeriodSpanAggregatedMetric], view_name: str
) -> LookMLView:
    """Generates LookMLView with derived table performing logic for a set of PeriodSpanAggregatedMetric objects"""
    analyst_dataset = ANALYST_VIEWS_DATASET
    metric_aggregation_fragment = (
        AVG_DAILY_POPULATION.generate_aggregation_query_fragment(
            span_start_date_col="ses.start_date",
            span_end_date_col="ses.end_date",
            period_start_date_col="time_period.start_date",
            period_end_date_col="time_period.end_date",
            original_span_start_date="ses.span_start_date",
        )
        + ",\n"
        + "\n".join(
            [
                liquid_wrap(
                    metric.generate_aggregation_query_fragment(
                        span_start_date_col="ses.start_date",
                        span_end_date_col="ses.end_date",
                        period_start_date_col="time_period.start_date",
                        period_end_date_col="time_period.end_date",
                        original_span_start_date="ses.span_start_date",
                    ),
                    metric,
                    view_name,
                )
                for metric in metrics
                if metric.name != "avg_daily_population"
            ]
        )
    )
    metric_measures = [
        measure_for_metric(
            metric, days_in_period_source=LookMLSqlReferenceType.TABLE_COLUMN
        )
        for metric in metrics
    ]
    derived_table_query = f"""
    WITH eligible_spans AS (
        SELECT
            assignments.state_code,
            assignments.unit_of_analysis,
            assignments.all_attributes,
            assignments.person_id,
            GREATEST(assignments.assignment_date, spans.start_date) AS start_date,
            {revert_nonnull_end_date_clause(
                f"LEAST({nonnull_end_date_clause('spans.end_date')}, {nonnull_end_date_clause('assignments.assignment_end_date')})"
            )} AS end_date,
            span,
            span_attributes,
        FROM
            `{analyst_dataset}.person_spans_materialized` AS spans
        INNER JOIN
            ${{attributes_{view_name}.SQL_TABLE_NAME}} assignments
        ON
            assignments.person_id = spans.person_id
            AND (
              assignments.assignment_date
                BETWEEN spans.start_date AND {nonnull_end_date_exclusive_clause("spans.end_date")}
              OR spans.start_date
                BETWEEN assignments.assignment_date AND {nonnull_end_date_exclusive_clause("assignments.assignment_end_date")}
            )
            -- Disregard zero day sample spans for calculating population metrics
            AND assignments.assignment_date != {nonnull_end_date_clause("assignments.assignment_end_date")}
    )
    SELECT
        -- assignments
        ses.state_code,
        ses.unit_of_analysis,
        ses.all_attributes,

        -- time_period
        time_period.period,
        time_period.start_date,
        time_period.end_date,
        {metric_aggregation_fragment}
    FROM
        eligible_spans ses
    INNER JOIN
        ${{time_periods_{view_name}.SQL_TABLE_NAME}} time_period
    ON
        ses.start_date < time_period.end_date
        AND time_period.start_date < {nonnull_current_date_clause("ses.end_date")}
        AND ses.person_id = time_period.person_id
    GROUP BY
        1, 2, 3, 4, 5, 6
    """
    return LookMLView(
        view_name=f"period_span_aggregated_metrics_{view_name}",
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[*metric_measures],
    )


def generate_period_event_metric_view(
    metrics: List[PeriodEventAggregatedMetric], view_name: str
) -> LookMLView:
    """Generates LookMLView with derived table performing logic for a set of PeriodEventAggregatedMetric objects"""
    analyst_dataset = ANALYST_VIEWS_DATASET
    metric_aggregation_fragment = "\n".join(
        [
            liquid_wrap(
                metric.generate_aggregation_query_fragment(
                    event_date_col="events.event_date"
                ),
                metric,
                view_name,
            )
            for metric in metrics
        ]
    )
    metric_measures = [
        measure_for_metric(
            metric, days_in_period_source=LookMLSqlReferenceType.TABLE_COLUMN
        )
        for metric in metrics
    ]
    derived_table_query = f"""
    SELECT
        -- assignments
        assignments.state_code,
        assignments.unit_of_analysis,
        assignments.all_attributes,

        -- time_period
        assignments.period,
        assignments.start_date,
        assignments.end_date,

        -- period_event metrics
        {metric_aggregation_fragment}
    FROM
        ${{time_periods_{view_name}.SQL_TABLE_NAME}} assignments
    LEFT JOIN
        `{analyst_dataset}.person_events_materialized` AS events
    ON
        events.person_id = assignments.person_id
        AND events.event_date BETWEEN GREATEST(assignments.assignment_date, assignments.start_date)
          AND LEAST(
            {nonnull_end_date_clause("assignments.assignment_end_date")},
            assignments.end_date
    )

    GROUP BY
    1, 2, 3, 4, 5, 6
    """
    return LookMLView(
        view_name=f"period_event_aggregated_metrics_{view_name}",
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[*metric_measures],
    )


def generate_assignment_span_metric_view(
    metrics: List[AssignmentSpanAggregatedMetric], view_name: str
) -> LookMLView:
    """Generates LookMLView with derived table performing logic for a set of AssignmentSpanAggregatedMetric objects"""
    analyst_dataset = ANALYST_VIEWS_DATASET
    metric_aggregation_fragment = "\n".join(
        [
            liquid_wrap(
                metric.generate_aggregation_query_fragment(
                    span_start_date_col="spans.start_date",
                    span_end_date_col="spans.end_date",
                    assignment_date_col="assignments.assignment_date",
                ),
                metric,
                view_name,
            )
            for metric in metrics
        ]
    )
    metric_measures = [
        measure_for_metric(
            metric, days_in_period_source=LookMLSqlReferenceType.TABLE_COLUMN
        )
        for metric in metrics
    ]
    derived_table_query = f"""
    SELECT
        -- assignments
        assignments.state_code,
        assignments.unit_of_analysis,
        assignments.all_attributes,

        -- time_period
        assignments.period,
        assignments.start_date,
        assignments.end_date,

        COUNT(DISTINCT CONCAT(assignments.person_id, assignments.assignment_date)) AS assignments,
        {metric_aggregation_fragment}
    FROM
        ${{time_periods_{view_name}.SQL_TABLE_NAME}} assignments
    LEFT JOIN
        `{analyst_dataset}.person_spans_materialized` spans
    ON
        assignments.person_id = spans.person_id
        AND (
            spans.start_date > assignments.assignment_date
            OR assignments.assignment_date BETWEEN spans.start_date
                AND {nonnull_end_date_exclusive_clause("spans.end_date")}
        )
    GROUP BY
        1, 2, 3, 4, 5, 6
    """
    return LookMLView(
        view_name=f"assignment_span_aggregated_metrics_{view_name}",
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[*metric_measures],
    )


def generate_assignment_event_metric_view(
    metrics: List[AssignmentEventAggregatedMetric], view_name: str
) -> LookMLView:
    """Generates LookMLView with derived table performing logic for a set of AssignmentEventAggregatedMetric objects"""
    analyst_dataset = ANALYST_VIEWS_DATASET
    metric_aggregation_fragment_inner = "\n".join(
        [
            liquid_wrap(
                metric.generate_aggregation_query_fragment(
                    event_date_col="events.event_date",
                    assignment_date_col="assignments.assignment_date",
                ),
                metric,
                view_name,
            )
            for metric in metrics
        ]
    )
    metric_aggregation_fragment_outer = "\n\t".join(
        [
            liquid_wrap(f"SUM({metric.name}) AS {metric.name}", metric, view_name)
            for metric in metrics
        ]
    )
    metric_measures = [
        measure_for_metric(
            metric, days_in_period_source=LookMLSqlReferenceType.TABLE_COLUMN
        )
        for metric in metrics
    ]
    derived_table_query = f"""
    SELECT
        -- assignments
        state_code,
        unit_of_analysis,
        all_attributes,

        -- time_period
        period,
        start_date,
        end_date,
        {metric_aggregation_fragment_outer}
    FROM (
        SELECT
            assignments.state_code,
            unit_of_analysis,
            assignments.start_date,
            assignments.end_date,
            period,
            assignments.person_id,
            assignments.assignment_date,
            assignments.all_attributes,
            {metric_aggregation_fragment_inner}
        FROM
            ${{time_periods_{view_name}.SQL_TABLE_NAME}} assignments
        LEFT JOIN
            `{analyst_dataset}.person_events_materialized` events
        ON
            assignments.person_id = events.person_id
            AND events.event_date >= assignments.assignment_date
        GROUP BY
            1, 2, 3, 4, 5, 6, 7, 8
    )
    GROUP BY
        1, 2, 3, 4, 5, 6
    """
    return LookMLView(
        view_name=f"assignment_event_aggregated_metrics_{view_name}",
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[*metric_measures],
    )


def custom_metrics_view_query_template(view_name: str) -> str:
    """Returns query template that unions together all LookML view dependencies to generate a custom metrics table"""

    liquid_assignment_type_check = (
        "{{" + f'% if {view_name}.assignment_type._parameter_value != "PERSON" %' + "}}"
    )
    derived_table_query = f"""
    WITH time_period_cte AS (
        SELECT
            *
        FROM
            ${{time_periods_{view_name}.SQL_TABLE_NAME}}
    )
    /* This cte is embedded in `assignments_and_attributes_cte`
    , assignments_cte AS (
        SELECT
          *
        FROM
          ${{assignment_sessions_{view_name}.SQL_TABLE_NAME}}
    )
    */
    , assignments_and_attributes_cte AS (
        SELECT
          *
        FROM
          ${{attributes_{view_name}.SQL_TABLE_NAME}}
    )

    -- map all_attributes to original columns
    , column_mapping AS (
        SELECT DISTINCT
            * EXCEPT (
            {liquid_assignment_type_check}
            person_id,
            {{% endif %}}
            assignment_date, assignment_end_date)
        FROM
            assignments_and_attributes_cte
    )

    -- period_span metrics
    , period_span_metrics AS (
        SELECT
            *
        FROM
            ${{period_span_aggregated_metrics_{view_name}.SQL_TABLE_NAME}}
    )

    -- period_event metrics
    , period_event_metrics AS (
        SELECT
            *
        FROM
            ${{period_event_aggregated_metrics_{view_name}.SQL_TABLE_NAME}}
    )

    -- assignment_span metrics
    , assignment_span_metrics AS (
        SELECT
            *
        FROM
            ${{assignment_span_aggregated_metrics_{view_name}.SQL_TABLE_NAME}}
    )

    -- assignment_event metrics
    , assignment_event_metrics AS (
        SELECT
            *
        FROM
            ${{assignment_event_aggregated_metrics_{view_name}.SQL_TABLE_NAME}}
    )

    -- join all metrics on unit-of-analysis and attribute struct to return original columns
    SELECT
        * EXCEPT(unit_of_analysis, all_attributes),
        DATE_DIFF(end_date, start_date, DAY) AS days_in_period,
    FROM
        column_mapping
    INNER JOIN
        period_span_metrics
    USING
        (state_code, unit_of_analysis, all_attributes)
    LEFT JOIN
        period_event_metrics
    USING
        (state_code, unit_of_analysis, all_attributes, period, start_date, end_date)
    LEFT JOIN
        assignment_span_metrics
    USING
        (state_code, unit_of_analysis, all_attributes, period, start_date, end_date)
    LEFT JOIN
        assignment_event_metrics
    USING
        (state_code, unit_of_analysis, all_attributes, period, start_date, end_date)
"""
    return derived_table_query


def generate_custom_metrics_view(
    metrics: List[AggregatedMetric],
    view_name: str,
    additional_view_fields: List[LookMLViewField],
) -> LookMLView:
    """Generates LookMLView with derived table that joins together metric view
    builders, analysis periods, and assignments to dynamically calculate metrics,
    referencing the provided view name."""
    derived_table_query = custom_metrics_view_query_template(view_name=view_name)
    metric_filter_parameter = get_metric_explore_parameter(
        metrics, "metric_filter"
    ).extend(
        additional_parameters=[
            LookMLFieldParameter.label("Metric Filter"),
            LookMLFieldParameter.description("Used to select one metric for a Look."),
        ]
    )
    metric_value_measure = get_metric_value_measure(view_name, metric_filter_parameter)
    measure_type_parameter = ParameterLookMLViewField(
        field_name="measure_type",
        parameters=[
            LookMLFieldParameter.type(LookMLFieldType.UNQUOTED),
            LookMLFieldParameter.description(
                "Used to select whether metric should be presented as a raw value or a normalized rate"
            ),
            LookMLFieldParameter.view_label("Metric Menu"),
            LookMLFieldParameter.allowed_value("Normalized", "normalized"),
            LookMLFieldParameter.allowed_value("Value", "value"),
            LookMLFieldParameter.default_value("value"),
        ],
    )
    return LookMLView(
        view_name=view_name,
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[
            measure_type_parameter,
            metric_filter_parameter,
            metric_value_measure,
            *additional_view_fields,
        ],
        included_paths=[
            f"/views/aggregated_metrics/subqueries/{view_name}/*",
            f"/views/aggregated_metrics/generated/{view_name}/subqueries/*",
        ],
        extended_views=[
            f"time_periods_{view_name}",
            f"assignment_sessions_{view_name}",
            f"attributes_{view_name}",
            f"period_span_aggregated_metrics_{view_name}",
            f"period_event_aggregated_metrics_{view_name}",
            f"assignment_span_aggregated_metrics_{view_name}",
            f"assignment_event_aggregated_metrics_{view_name}",
        ],
    )
