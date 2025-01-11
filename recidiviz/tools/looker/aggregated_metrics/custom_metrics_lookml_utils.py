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
"""Util functions to support generating new aggregated metrics on the fly within LookML"""

from typing import Dict, List, Optional, Tuple

from recidiviz.aggregated_metrics.assignment_sessions_view_builder import (
    get_metric_assignment_sessions_materialized_table_address,
    has_configured_assignment_query,
)
from recidiviz.aggregated_metrics.configuration.collections.standard import (
    UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE,
)
from recidiviz.aggregated_metrics.metric_time_period_config import MetricTimePeriod
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
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
    get_static_attributes_query_for_unit_of_analysis,
)
from recidiviz.calculator.query.bq_utils import (
    MAGIC_START_DATE,
    join_on_columns_fragment,
    list_to_query_string,
    nonnull_current_date_clause,
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.common.str_field_utils import snake_to_title
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    DimensionLookMLViewField,
    FilterLookMLViewField,
    LookMLFieldParameter,
    LookMLViewField,
    MeasureLookMLViewField,
    ParameterLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    LookMLFieldDatatype,
    LookMLFieldType,
    LookMLSqlReferenceType,
)
from recidiviz.looker.lookml_view_source_table import LookMLViewSourceTable
from recidiviz.observations.dataset_config import dataset_for_observation_type_cls
from recidiviz.observations.event_type import EventType
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.tools.looker.aggregated_metrics.aggregated_metrics_lookml_utils import (
    get_metric_explore_parameter,
    get_metric_value_measure,
    measure_for_metric,
)
from recidiviz.view_registry.deployed_views import (
    DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED,
)

# Loops through all configured population <> unit of analysis combinations and generates a dictionary
# that maps their combined name (assignment type) to a tuple with population and unit of analysis type
ASSIGNMENT_NAME_TO_TYPES = {
    f"{population_type.population_name_short}_{unit_of_analysis_type.short_name}".upper(): (
        population_type,
        unit_of_analysis_type,
    )
    for population_type, unit_of_analysis_types in UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE.items()
    for unit_of_analysis_type in unit_of_analysis_types
}

# Special case: add person unit of analysis for the whole justice involved population
ASSIGNMENT_NAME_TO_TYPES["PERSON"] = (
    MetricPopulationType.JUSTICE_INVOLVED,
    MetricUnitOfAnalysisType.PERSON_ID,
)


def liquid_wrap_metric(
    query_fragment: str, metric: AggregatedMetric, view_name: str
) -> str:
    """
    Outputs a conditional liquid fragment that will display the provided query fragment
    if the metric is selected in the Explore.
    """
    return f"""{{% if {view_name}.{metric.name}_measure._in_query or {view_name}.metric_filter._parameter_value contains "{metric.name}" %}}
    {query_fragment},
        {{% endif %}}"""


def liquid_wrap_json_field(query_fragment: str, field_name: str, view_name: str) -> str:
    """
    Outputs a conditional liquid fragment that will display the provided query fragment
    if the json field is selected in the Explore.
    """
    return f"""{{% if {view_name}.{field_name}._in_query or {view_name}.{field_name}._is_filtered %}}{query_fragment}{{% endif %}}"""


# TODO(#35917): Adapt this LookML generation helper to use optimized aggregated metrics
#  queries.
def generate_period_span_metric_view(
    metrics: List[PeriodSpanAggregatedMetric],
    view_name: str,
    unit_of_observation: MetricUnitOfObservation,
    json_field_filters: List[str],
) -> LookMLView:
    """Generates LookMLView with derived table performing logic for a set of PeriodSpanAggregatedMetric objects"""
    metric_aggregation_fragment = (
        AVG_DAILY_POPULATION.generate_aggregation_query_fragment(
            filter_observations_by_type=True,
            read_observation_attributes_from_json=True,
            observations_cte_name="ses",
            span_start_date_col="ses.start_date",
            span_end_date_col="ses.end_date",
            period_start_date_col="time_period.start_date",
            period_end_date_col="time_period.end_date",
            original_span_start_date="ses.span_start_date",
        )
        + ",\n"
        + "\n".join(
            [
                liquid_wrap_metric(
                    metric.generate_aggregation_query_fragment(
                        filter_observations_by_type=True,
                        read_observation_attributes_from_json=True,
                        observations_cte_name="ses",
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
    unit_of_observation_query_fragment = list_to_query_string(
        sorted(unit_of_observation.primary_key_columns),
        table_prefix="assignments",
    )
    assignments_with_attributes_view = (
        "assignments_with_attributes"
        if unit_of_observation.type == MetricUnitOfObservationType.PERSON_ID
        else "assignments"
    )
    all_attributes_field = (
        "assignments.all_attributes"
        if unit_of_observation.type == MetricUnitOfObservationType.PERSON_ID
        else "TO_JSON_STRING(STRUCT(TRUE AS dummy_attribute)) AS all_attributes"
    )
    field_filters_query_fragment_json = "\n".join(
        [
            liquid_wrap_json_field(
                f"JSON_VALUE(span_attributes, '$.{field}') AS {field},",
                field,
                view_name,
            )
            for field in json_field_filters
        ]
    )
    field_filters_group_by_query_fragment = (
        (
            "".join(
                [
                    liquid_wrap_json_field(
                        f", JSON_VALUE(span_attributes, '$.{field}')", field, view_name
                    )
                    for field in json_field_filters
                ]
            )
        )
        if len(json_field_filters) > 0
        else ""
    )

    spans_dataset_id = dataset_for_observation_type_cls(
        unit_of_observation=unit_of_observation.type, observation_type_cls=SpanType
    )
    # If this set of observations doesn't exist, use a dummy derived query table
    if spans_dataset_id in DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED:
        derived_table_query = f"""
    WITH eligible_spans AS (
        SELECT
            assignments.unit_of_analysis,
            {all_attributes_field},
            {unit_of_observation_query_fragment},
            GREATEST(assignments.assignment_date, spans.start_date) AS start_date,
            {revert_nonnull_end_date_clause(
        f"LEAST({nonnull_end_date_clause('spans.end_date')}, {nonnull_end_date_clause('assignments.assignment_end_date')})"
    )} AS end_date,
            span,
            span_attributes,
        FROM
            `{spans_dataset_id}.all_{unit_of_observation.type.short_name}_spans_materialized` AS spans
        INNER JOIN
            ${{{unit_of_observation.type.short_name}_{assignments_with_attributes_view}_{view_name}.SQL_TABLE_NAME}} assignments
        ON
            {join_on_columns_fragment(columns=unit_of_observation.primary_key_columns_ordered, table1="assignments", table2="spans")}
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
        ses.unit_of_analysis,
        ses.all_attributes,

        -- time_period
        time_period.period,
        time_period.start_date,
        time_period.end_date,

        -- additional disaggregation fields
        {field_filters_query_fragment_json}
        {metric_aggregation_fragment}
    FROM
        eligible_spans ses
    INNER JOIN
        ${{{unit_of_observation.type.short_name}_assignments_with_attributes_and_time_periods_{view_name}.SQL_TABLE_NAME}} time_period
    ON
        ses.start_date < time_period.span_end_date
        AND time_period.span_start_date < {nonnull_current_date_clause("ses.end_date")}
        AND {join_on_columns_fragment(columns=unit_of_observation.primary_key_columns_ordered, table1="ses", table2="time_period")}
    GROUP BY
        1, 2, 3, 4, 5{field_filters_group_by_query_fragment}
    """
    else:
        field_filters_dummy_query_fragment = "\n".join(
            [
                liquid_wrap_json_field(
                    f"CAST(NULL AS STRING) AS {field},",
                    field,
                    view_name,
                )
                for field in json_field_filters
            ]
        )
        derived_table_query = f"""
    -- Dummy table because this observation type does not exist yet
    SELECT
        CAST(NULL AS STRING) AS unit_of_analysis,
        CAST(NULL AS STRING) AS all_attributes,
        CAST(NULL AS STRING) AS period,
        CAST(NULL AS DATE) AS start_date,
        CAST(NULL AS DATE) AS end_date,
        {field_filters_dummy_query_fragment}
"""
    return LookMLView(
        view_name=f"{unit_of_observation.type.short_name}_period_span_aggregated_metrics_{view_name}",
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[*metric_measures],
    )


# TODO(#35917): Adapt this LookML generation helper to use optimized aggregated metrics
#  queries.
def generate_period_event_metric_view(
    metrics: List[PeriodEventAggregatedMetric],
    view_name: str,
    unit_of_observation: MetricUnitOfObservation,
    json_field_filters: List[str],
) -> LookMLView:
    """Generates LookMLView with derived table performing logic for a set of PeriodEventAggregatedMetric objects"""
    metric_aggregation_fragment = "\n".join(
        [
            liquid_wrap_metric(
                metric.generate_aggregation_query_fragment(
                    filter_observations_by_type=True,
                    read_observation_attributes_from_json=True,
                    observations_cte_name="events",
                    event_date_col="events.event_date",
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
    field_filters_query_fragment_json = "\n".join(
        [
            liquid_wrap_json_field(
                f"JSON_VALUE(event_attributes, '$.{field}') AS {field},",
                field,
                view_name,
            )
            for field in json_field_filters
        ]
    )
    field_filters_group_by_query_fragment = (
        (
            "".join(
                [
                    liquid_wrap_json_field(
                        f", JSON_VALUE(event_attributes, '$.{field}')", field, view_name
                    )
                    for field in json_field_filters
                ]
            )
        )
        if len(json_field_filters) > 0
        else ""
    )
    events_dataset_id = dataset_for_observation_type_cls(
        unit_of_observation=unit_of_observation.type, observation_type_cls=EventType
    )
    # If this set of observations doesn't exist, use a dummy derived query table
    if events_dataset_id in DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED:
        derived_table_query = f"""
    SELECT
        -- assignments
        assignments.unit_of_analysis,
        assignments.all_attributes,

        -- time_period
        assignments.period,
        assignments.start_date,
        assignments.end_date,

        -- additional disaggregation fields
        {field_filters_query_fragment_json}

        -- period_event metrics
        {metric_aggregation_fragment}
    FROM
        ${{{unit_of_observation.type.short_name}_assignments_with_attributes_and_time_periods_{view_name}.SQL_TABLE_NAME}} assignments
    LEFT JOIN
        `{events_dataset_id}.all_{unit_of_observation.type.short_name}_events_materialized` AS events
    ON
        {join_on_columns_fragment(columns=unit_of_observation.primary_key_columns_ordered, table1="events", table2="assignments")}
        AND events.event_date BETWEEN GREATEST(assignments.assignment_date, assignments.start_date)
        AND LEAST(
            {nonnull_end_date_clause("assignments.assignment_end_date")},
            DATE_SUB(assignments.end_date, INTERVAL 1 DAY)
    )

    GROUP BY
        1, 2, 3, 4, 5{field_filters_group_by_query_fragment}
    """
    else:
        field_filters_dummy_query_fragment = "\n".join(
            [
                liquid_wrap_json_field(
                    f"CAST(NULL AS STRING) AS {field},",
                    field,
                    view_name,
                )
                for field in json_field_filters
            ]
        )
        derived_table_query = f"""
    -- Dummy table because this observation type does not exist yet
    SELECT
        CAST(NULL AS STRING) AS unit_of_analysis,
        CAST(NULL AS STRING) AS all_attributes,
        CAST(NULL AS STRING) AS period,
        CAST(NULL AS DATE) AS start_date,
        CAST(NULL AS DATE) AS end_date,
        {field_filters_dummy_query_fragment}
"""
    return LookMLView(
        view_name=f"{unit_of_observation.type.short_name}_period_event_aggregated_metrics_{view_name}",
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[*metric_measures],
    )


# TODO(#35917): Adapt this LookML generation helper to use optimized aggregated metrics
#  queries.
def generate_assignment_span_metric_view(
    metrics: List[AssignmentSpanAggregatedMetric],
    view_name: str,
    unit_of_observation: MetricUnitOfObservation,
    json_field_filters: List[str],
) -> LookMLView:
    """Generates LookMLView with derived table performing logic for a set of AssignmentSpanAggregatedMetric objects"""
    unit_of_observation_query_fragment = list_to_query_string(
        sorted(unit_of_observation.primary_key_columns),
        table_prefix="assignments",
    )
    metric_aggregation_fragment = "\n".join(
        [
            liquid_wrap_metric(
                metric.generate_aggregation_query_fragment(
                    filter_observations_by_type=True,
                    read_observation_attributes_from_json=True,
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
    field_filters_query_fragment_json = "\n".join(
        [
            liquid_wrap_json_field(
                f"JSON_VALUE(span_attributes, '$.{field}') AS {field},",
                field,
                view_name,
            )
            for field in json_field_filters
        ]
    )
    field_filters_group_by_query_fragment = (
        (
            "".join(
                [
                    liquid_wrap_json_field(
                        f", JSON_VALUE(span_attributes, '$.{field}')", field, view_name
                    )
                    for field in json_field_filters
                ]
            )
        )
        if len(json_field_filters) > 0
        else ""
    )

    spans_dataset_id = dataset_for_observation_type_cls(
        unit_of_observation=unit_of_observation.type, observation_type_cls=SpanType
    )

    # If this set of observations doesn't exist, use a dummy derived query table
    if spans_dataset_id in DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED:
        derived_table_query = f"""
    SELECT
        -- assignments
        assignments.unit_of_analysis,
        assignments.all_attributes,

        -- time_period
        assignments.period,
        assignments.start_date,
        assignments.end_date,

        -- additional disaggregation fields
        {field_filters_query_fragment_json}

        COUNT(DISTINCT CONCAT({unit_of_observation_query_fragment}, assignments.assignment_date)) AS assignments,
        {metric_aggregation_fragment}
    FROM
        ${{{unit_of_observation.type.short_name}_assignments_with_attributes_and_time_periods_{view_name}.SQL_TABLE_NAME}} assignments
    LEFT JOIN
        `{spans_dataset_id}.all_{unit_of_observation.type.short_name}_spans_materialized` spans
    ON
        {join_on_columns_fragment(columns=unit_of_observation.primary_key_columns_ordered, table1="assignments", table2="spans")}
        AND (
            spans.start_date > assignments.assignment_date
            OR assignments.assignment_date BETWEEN spans.start_date
                AND {nonnull_end_date_exclusive_clause("spans.end_date")}
        )
    GROUP BY
        1, 2, 3, 4, 5{field_filters_group_by_query_fragment}
    """
    else:
        field_filters_dummy_query_fragment = "\n".join(
            [
                liquid_wrap_json_field(
                    f"CAST(NULL AS STRING) AS {field},",
                    field,
                    view_name,
                )
                for field in json_field_filters
            ]
        )
        derived_table_query = f"""
    -- Dummy table because this observation type does not exist yet
    SELECT
        CAST(NULL AS STRING) AS unit_of_analysis,
        CAST(NULL AS STRING) AS all_attributes,
        CAST(NULL AS STRING) AS period,
        CAST(NULL AS DATE) AS start_date,
        CAST(NULL AS DATE) AS end_date,
        {field_filters_dummy_query_fragment}
"""
    return LookMLView(
        view_name=f"{unit_of_observation.type.short_name}_assignment_span_aggregated_metrics_{view_name}",
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[*metric_measures],
    )


# TODO(#35917): Adapt this LookML generation helper to use optimized aggregated metrics
#  queries.
def generate_assignment_event_metric_view(
    metrics: List[AssignmentEventAggregatedMetric],
    view_name: str,
    unit_of_observation: MetricUnitOfObservation,
    json_field_filters: List[str],
) -> LookMLView:
    """Generates LookMLView with derived table performing logic for a set of AssignmentEventAggregatedMetric objects"""
    metric_aggregation_fragment_inner = "\n".join(
        [
            liquid_wrap_metric(
                metric.generate_aggregation_query_fragment(
                    filter_observations_by_type=True,
                    read_observation_attributes_from_json=True,
                    observations_cte_name="events",
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
            liquid_wrap_metric(
                f"SUM({metric.name}) AS {metric.name}", metric, view_name
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

    unit_of_observation_query_fragment = list_to_query_string(
        sorted(unit_of_observation.primary_key_columns),
        table_prefix="assignments",
    )
    field_filters_query_fragment_json = "\n".join(
        [
            liquid_wrap_json_field(
                f"JSON_VALUE(event_attributes, '$.{field}') AS {field},",
                field,
                view_name,
            )
            for field in json_field_filters
        ]
    )
    field_filters_group_by_query_fragment_json = (
        (
            "".join(
                [
                    liquid_wrap_json_field(
                        f", JSON_VALUE(event_attributes, '$.{field}')", field, view_name
                    )
                    for field in json_field_filters
                ]
            )
        )
        if len(json_field_filters) > 0
        else ""
    )
    field_filters_query_fragment = "".join(
        [
            liquid_wrap_json_field(f"{field},", field, view_name)
            for field in json_field_filters
        ]
    )
    field_filters_group_by_query_fragment = "".join(
        [
            liquid_wrap_json_field(f", {field}", field, view_name)
            for field in json_field_filters
        ]
    )

    events_dataset_id = dataset_for_observation_type_cls(
        unit_of_observation=unit_of_observation.type, observation_type_cls=EventType
    )

    # If this set of observations doesn't exist, use a dummy derived query table
    if events_dataset_id in DEPLOYED_DATASETS_THAT_HAVE_EVER_BEEN_MANAGED:
        derived_table_query = f"""
    SELECT
        -- assignments
        unit_of_analysis,
        all_attributes,

        -- time_period
        period,
        start_date,
        end_date,

        -- additional disaggregation fields
        {field_filters_query_fragment}

        {metric_aggregation_fragment_outer}
    FROM (
        SELECT
            unit_of_analysis,
            assignments.start_date,
            assignments.end_date,
            period,
            {unit_of_observation_query_fragment},
            assignments.assignment_date,
            assignments.all_attributes,
            {field_filters_query_fragment_json}
            {metric_aggregation_fragment_inner}
        FROM
            ${{{unit_of_observation.type.short_name}_assignments_with_attributes_and_time_periods_{view_name}.SQL_TABLE_NAME}} assignments
        LEFT JOIN
            `{events_dataset_id}.all_{unit_of_observation.type.short_name}_events_materialized` events
        ON
            {join_on_columns_fragment(columns=unit_of_observation.primary_key_columns_ordered, table1="assignments", table2="events")}
            AND events.event_date >= assignments.assignment_date
        GROUP BY
            unit_of_analysis,
            assignments.start_date,
            assignments.end_date,
            period,
            {unit_of_observation_query_fragment},
            assignments.assignment_date,
            assignments.all_attributes{field_filters_group_by_query_fragment_json}
    )
    GROUP BY
        1, 2, 3, 4, 5{field_filters_group_by_query_fragment}
    """
    else:
        field_filters_dummy_query_fragment = "\n".join(
            [
                liquid_wrap_json_field(
                    f"CAST(NULL AS STRING) AS {field},",
                    field,
                    view_name,
                )
                for field in json_field_filters
            ]
        )
        derived_table_query = f"""
    -- Dummy table because this observation type does not exist yet
    SELECT
        CAST(NULL AS STRING) AS unit_of_analysis,
        CAST(NULL AS STRING) AS all_attributes,
        CAST(NULL AS STRING) AS period,
        CAST(NULL AS DATE) AS start_date,
        CAST(NULL AS DATE) AS end_date,
        {field_filters_dummy_query_fragment}
"""
    return LookMLView(
        view_name=f"{unit_of_observation.type.short_name}_assignment_event_aggregated_metrics_{view_name}",
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[*metric_measures],
    )


def generate_assignments_view(
    view_name: str,
    assignment_types_dict: Dict[
        str, Tuple[MetricPopulationType, MetricUnitOfAnalysisType]
    ],
    unit_of_observation: MetricUnitOfObservation,
) -> LookMLView:
    """Generates LookMLView for all possible assignment types (combinations of population & unit of analysis types)"""

    def get_query_fragment_for_unit_of_analysis(assignment_type: str) -> str:
        if assignment_type not in assignment_types_dict:
            raise ValueError(f"Assignment type {assignment_type} not supported.")

        # For all assignment types that already have a materialized metric assignment session in bigquery,
        # generate a query fragment that combines the assignment session view with the static attributes query.
        population_type, unit_of_analysis_type = assignment_types_dict[assignment_type]
        unit_of_analysis = MetricUnitOfAnalysis.for_type(unit_of_analysis_type)
        primary_columns_str = unit_of_analysis.get_primary_key_columns_query_string()
        shared_columns = sorted(
            {
                *unit_of_observation.primary_key_columns,
                *unit_of_analysis.index_columns,
            }
        )
        shared_columns_string = list_to_query_string(shared_columns)
        static_attributes_source_table = (
            get_static_attributes_query_for_unit_of_analysis(
                unit_of_analysis_type, bq_view=False
            )
        )
        static_attributes_join_query_fragment = (
            f"""
                LEFT JOIN 
                    ({static_attributes_source_table})
                USING
                    ({primary_columns_str})
                """
            if static_attributes_source_table
            else ""
        )

        if has_configured_assignment_query(
            unit_of_analysis_type, unit_of_observation.type
        ):
            source_table_address = (
                get_metric_assignment_sessions_materialized_table_address(
                    unit_of_observation_type=unit_of_observation.type,
                    unit_of_analysis_type=unit_of_analysis_type,
                    population_type=population_type,
                ).to_str()
            )
            source_table = (
                f"{source_table_address}{static_attributes_join_query_fragment}"
            )
        else:
            # If no assignment table has been defined, create a dummy assignment table with all NULL fields
            placeholder_columns_query_string = ", ".join(
                [
                    f"NULL AS {col}"
                    if col == "person_id"
                    else f"CAST(NULL AS STRING) AS {col}"
                    for col in shared_columns
                ]
            )
            source_table = f"""(
                SELECT
                    {placeholder_columns_query_string},
                    DATE("{MAGIC_START_DATE}") AS assignment_date,
                    CAST(NULL AS DATE) AS end_date_exclusive,
            )"""

        return f"""
    {{% elsif {view_name}.assignment_type._parameter_value == "{assignment_type}" %}}
        SELECT
            {shared_columns_string},
            assignment_date,
            end_date_exclusive AS end_date,
            CONCAT({primary_columns_str}) AS unit_of_analysis,
        FROM
            {source_table}
"""

    # Assembles set of liquid-wrapped queries that pull the appropriate assignment sessions view based on the
    # selected assignment type parameter, for all assignment types.
    all_assignment_type_query_fragments = "".join(
        [
            get_query_fragment_for_unit_of_analysis(assignment_type)
            for assignment_type in assignment_types_dict
            if assignment_type != "PERSON"
        ]
    )

    # Assembles a query fragment for filtering over the set of index columns across all units of analysis
    all_attribute_columns = sorted(
        list(
            set(
                col
                for (_, unit_of_analysis_type) in assignment_types_dict.values()
                for col in MetricUnitOfAnalysis.for_type(
                    unit_of_analysis_type
                ).index_columns
            )
        )
    )
    attribute_columns_conditional = "\nAND ".join(
        [
            f"{{% condition {view_name}.{attr} %}}{attr}{{% endcondition %}}"
            for attr in all_attribute_columns
        ]
    )

    derived_table_query = f"""
/*
      Pull in {{assignment_type}}_metrics_person_assignment_sessions, which
      contains spans of assignment of clients to the unit of analysis. This CTE contains
      person_id and spans of assignment to columns that we will eventually aggregate by.

      Note: if you want to aggregate by attributes, that comes in with attributes.view
      */
      SELECT
      assignment_sessions.* EXCEPT(end_date),  -- unit of analysis + person_id, assignment_date
      assignment_sessions.end_date AS assignment_end_date,

      FROM (
        {{% if {view_name}.assignment_type._parameter_value == "PERSON" %}}
            SELECT
                state_code, person_id,
                start_date AS assignment_date,
                end_date_exclusive AS end_date,
                CONCAT(state_code, person_id) AS unit_of_analysis,
            FROM
                `sessions.system_sessions_materialized`
      {all_assignment_type_query_fragments}
      {{% else %}}
      NULL -- will throw error, should never happen
      {{% endif %}}
      -- allow upstream filtering to make queries faster
      ) assignment_sessions
      WHERE
      {attribute_columns_conditional}
      AND ( 
            assignment_date 
                BETWEEN DATE({{% parameter {view_name}.population_start_date %}}) 
                AND DATE({{% parameter {view_name}.population_end_date %}})
            OR DATE({{% parameter {view_name}.population_start_date %}}) 
                BETWEEN assignment_date 
                AND IFNULL(end_date, "9999-01-01")
      )
    """

    assignment_type_parameter = ParameterLookMLViewField(
        field_name="assignment_type",
        parameters=[
            LookMLFieldParameter.type(LookMLFieldType.UNQUOTED),
            LookMLFieldParameter.description(
                "The type of assignment that partially determines the unit of analysis and sets "
                "the relative date for time-since-assignment metrics"
            ),
            LookMLFieldParameter.view_label("Units of Analysis"),
            *[
                LookMLFieldParameter.allowed_value(
                    snake_to_title(assignment_type), assignment_type
                )
                for assignment_type in assignment_types_dict
            ],
            LookMLFieldParameter.default_value(sorted(assignment_types_dict)[0]),
        ],
    )

    unit_of_analysis_dimension = DimensionLookMLViewField(
        field_name="assignment_unit_of_analysis",
        parameters=[
            LookMLFieldParameter.description(
                "The unit of analysis set by `Assignment Type` filter"
            ),
            LookMLFieldParameter.type(LookMLFieldType.STRING),
            LookMLFieldParameter.view_label("Units of Analysis"),
            LookMLFieldParameter.sql("${TABLE}.unit_of_analysis"),
        ],
    )

    all_unit_of_analysis_dimensions = [
        DimensionLookMLViewField(
            field_name=attr,
            parameters=[
                LookMLFieldParameter.type(
                    LookMLFieldType.NUMBER
                    if attr == "person_id"
                    else LookMLFieldType.STRING
                ),
                LookMLFieldParameter.view_label("Units of Analysis"),
                LookMLFieldParameter.sql(f"${{TABLE}}.{attr}"),
            ],
        )
        for attr in all_attribute_columns
    ]

    return LookMLView(
        view_name=f"{unit_of_observation.type.short_name}_assignments_{view_name}",
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[
            assignment_type_parameter,
            unit_of_analysis_dimension,
            *all_unit_of_analysis_dimensions,
        ],
    )


def generate_person_assignments_with_attributes_view(
    view_name: str,
    time_dependent_person_attribute_query: str,
    time_dependent_person_attribute_fields: List[str],
    demographic_attribute_field_filters_with_suggestions: Dict[str, List[str]],
) -> LookMLView:
    """
    Generates LookMLView that joins assignment sessions view to attributes about the underlying justice-involved
    persons, to allow for on-the-fly filtering of the clients constituting an aggregated metric in the custom metrics
    explores.

    These attributes include 1) a set of default static attributes (race, gender, experiment assignment attributes)
    that are true for a given person over all time, and 2) time-dependent attribute fields supplied by the user
    and sourced from the time_dependent_person_attribute_query, which are true for a given person over a span of
    time defined by the `start_date` and `end_date_exclusive` of the sessionized input view.

    The `time_dependent_person_attribute_query` must include all fields in `time_dependent_person_attribute_fields`
    in addition to a `start_date` and `end_date_exclusive` field.
    """

    # Check that time-dependent attribute fields do not have overlap with any default static attributes
    static_attribute_fields = [
        "experiment_id",
        "variant_id",
        "variant_date",
        "gender",
        "is_female",
        "race",
        "is_nonwhite",
    ]
    repeat_attributes = list(
        set(static_attribute_fields) & set(time_dependent_person_attribute_fields)
    )
    if len(repeat_attributes) > 0:
        raise ValueError(
            f"Found time-dependent attribute field(s) that are already included as a static field: {repeat_attributes}"
        )

    time_dependent_person_attribute_fields_query_fragment_with_prefix = (
        "\n            ".join(
            [
                f"time_dependent_attributes.{field},"
                for field in time_dependent_person_attribute_fields
            ]
        )
    )
    if time_dependent_person_attribute_fields:
        time_dependent_person_attribute_fields_query_fragment_no_prefix = (
            "EXCEPT(" + ", ".join(time_dependent_person_attribute_fields) + ")"
        )
    else:
        time_dependent_person_attribute_fields_query_fragment_no_prefix = ""
    time_dependent_person_attribute_fields_liquid_wrap = "\n".join(
        [
            f"""
        {{% if {view_name}.{field}._in_query %}}
        {field},
        {{% endif %}}"""
            for field in time_dependent_person_attribute_fields
        ]
    )
    derived_table_query = f"""
    WITH assignments_with_time_dependent_attributes AS (
        SELECT
            assignment_sessions.* EXCEPT(assignment_date, assignment_end_date),
            {time_dependent_person_attribute_fields_query_fragment_with_prefix}
            GREATEST(assignment_sessions.assignment_date, time_dependent_attributes.start_date) AS assignment_date,
            {revert_nonnull_end_date_clause(
        f'LEAST({nonnull_end_date_clause("assignment_sessions.assignment_end_date")}, {nonnull_end_date_clause("time_dependent_attributes.end_date_exclusive")})'
    )} AS assignment_end_date,
        FROM
            ${{person_assignments_{view_name}.SQL_TABLE_NAME}} assignment_sessions
        LEFT JOIN
            ({time_dependent_person_attribute_query}) time_dependent_attributes
        ON
            assignment_sessions.person_id = time_dependent_attributes.person_id
        AND (
            assignment_sessions.assignment_date BETWEEN time_dependent_attributes.start_date AND {nonnull_end_date_clause("time_dependent_attributes.end_date_exclusive")}
            OR time_dependent_attributes.start_date BETWEEN assignment_sessions.assignment_date AND {nonnull_end_date_clause("assignment_sessions.assignment_end_date")}
        )
    )
    -- Join to static attribute tables
    SELECT
        assignments.* {time_dependent_person_attribute_fields_query_fragment_no_prefix},
        -- time-dependent attributes
        {time_dependent_person_attribute_fields_liquid_wrap}

        {{% if {view_name}.event_time_toggle._parameter_value == "true" or {view_name}.experiment_id._in_query %}}
        experiment_id,
        {{% endif %}}
        {{% if {view_name}.event_time_toggle._parameter_value == "true" or {view_name}.variant_id._in_query %}}
        variant_id,
        {{% endif %}}
        {{% if {view_name}.event_time_toggle._parameter_value == "true" or {view_name}.variant_date._in_query %}}
        -- for event time, specify date off which to calculate event time
        variant_date AS event_time_base_date,
        {{% else %}}
        assignments.assignment_date AS event_time_base_date,
        {{% endif %}}

        -- person demographics
        {{% if {view_name}.gender._in_query %}}
        pd.gender,
        {{% endif %}}
        {{% if {view_name}.is_female._in_query %}}
        pd.gender LIKE "FEMALE" AS is_female,
        {{% endif %}}
        {{% if {view_name}.race._in_query %}}
        pd.prioritized_race_or_ethnicity AS race,
        {{% endif %}}
        {{% if {view_name}.is_nonwhite._in_query %}}
        pd.prioritized_race_or_ethnicity != "WHITE" AS is_nonwhite,
        {{% endif %}}

        /*
        Here we create a column consisting of all attribute fields.
        This is for grouping by later on so we don't need to specify all columns (and repeat liquid
        in other views).
        */
        TO_JSON_STRING(STRUCT(
        -- fine units of analysis
        {{% if {view_name}.person_id._in_query %}}
        person_id,
        {{% endif %}}

        -- event time/experiment fields
        {{% if {view_name}.event_time_toggle._parameter_value == "true" or {view_name}.experiment_id._in_query %}}
        experiment_id,
        {{% endif %}}
        {{% if {view_name}.event_time_toggle._parameter_value == "true" or {view_name}.variant_id._in_query %}}
        variant_id,
        {{% endif %}}

        -- person demographics
        {{% if {view_name}.gender._in_query %}}
        pd.gender,
        {{% endif %}}
        {{% if {view_name}.is_female._in_query %}}
        pd.gender LIKE "FEMALE" AS is_female,
        {{% endif %}}
        {{% if {view_name}.race._in_query %}}
        pd.prioritized_race_or_ethnicity AS race,
        {{% endif %}}
        {{% if {view_name}.is_nonwhite._in_query %}}
        pd.prioritized_race_or_ethnicity != "WHITE" AS is_nonwhite,
        {{% endif %}}

        -- time-dependent attributes
        {time_dependent_person_attribute_fields_liquid_wrap}

        TRUE AS dummy_attribute -- here so last column not followed by comma and always non null
      )) AS all_attributes,

    FROM
        assignments_with_time_dependent_attributes assignments
    {{% if {view_name}.gender._in_query
        or {view_name}.is_female._in_query
        or {view_name}.race._in_query
        or {view_name}.is_nonwhite._in_query %}}
    INNER JOIN
        `sessions.person_demographics_materialized` pd
    USING
        (state_code, person_id)
    {{% endif %}}

    -- for event time, join experiment variants if necessary
    -- note: requires officer or state-level units of analysis
    {{% if {view_name}.event_time_toggle._parameter_value == "true"
        or {view_name}.experiment_id._in_query
        or {view_name}.variant_id._in_query
        or {view_name}.variant_date._in_query %}}
    {{% if {view_name}.assignment_type._parameter_value == "SUPERVISION_OFFICER" %}}
    INNER JOIN (
        SELECT
            *, officer_external_id AS officer_id,
        FROM
            `experiments_metadata.officer_assignments_materialized`
    ) 
    USING
        (state_code, officer_id)
    {{% else %}}
    INNER JOIN
        `experiments_metadata.state_assignments_materialized`
    USING
        (state_code)
    {{% endif %}}
    {{% endif %}}

      WHERE
      {{% condition {view_name}.gender %}}INITCAP(REPLACE(pd.gender, '_', ' ')){{% endcondition %}}
      AND {{% condition {view_name}.is_female %}}pd.gender LIKE "FEMALE"{{% endcondition %}}
      AND {{% condition {view_name}.race %}}INITCAP(REPLACE(pd.prioritized_race_or_ethnicity,'_',' ')){{% endcondition %}}
      AND {{% condition {view_name}.is_nonwhite %}}pd.prioritized_race_or_ethnicity != "WHITE"{{% endcondition %}}
      AND {{% condition {view_name}.experiment_id %}}experiment_id{{% endcondition %}}
      AND {{% condition {view_name}.variant_id %}}variant_id{{% endcondition %}}
      AND {{% condition {view_name}.variant_date %}}variant_date{{% endcondition %}}
    """

    time_dependent_attribute_dimensions = [
        DimensionLookMLViewField(
            field_name=field,
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.view_label("Attributes"),
                LookMLFieldParameter.group_label("Dynamic Attributes"),
                LookMLFieldParameter.sql(f"${{TABLE}}.{field}"),
            ],
        )
        for field in time_dependent_person_attribute_fields
    ]

    experiment_attribute_dimensions = [
        DimensionLookMLViewField(
            field_name="experiment_id",
            parameters=[
                LookMLFieldParameter.description(
                    "ID of an experiment a client (or their officer) is associated with"
                ),
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.view_label("Attributes"),
                LookMLFieldParameter.group_label("Experiments"),
                LookMLFieldParameter.sql("${TABLE}.experiment_id"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="variant_id",
            parameters=[
                LookMLFieldParameter.description(
                    "Variant of an experiment a client (or their officer)"
                ),
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.view_label("Attributes"),
                LookMLFieldParameter.group_label("Experiments"),
                LookMLFieldParameter.sql("${TABLE}.variant_id"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="variant_date",
            parameters=[
                LookMLFieldParameter.description(
                    "The client's officer's variant assignment date"
                ),
                LookMLFieldParameter.type(LookMLFieldType.DATE),
                LookMLFieldParameter.datatype(LookMLFieldDatatype.DATE),
                LookMLFieldParameter.view_label("Attributes"),
                LookMLFieldParameter.group_label("Experiments"),
                LookMLFieldParameter.sql("${TABLE}.event_time_base_date"),
            ],
        ),
    ]

    demographic_attribute_field_filter_lookml_fields: List[LookMLViewField] = []
    demographic_attribute_field_dimension_lookml_fields: List[
        DimensionLookMLViewField
    ] = []
    for (
        field,
        suggestions,
    ) in demographic_attribute_field_filters_with_suggestions.items():

        if field in ["is_female", "is_nonwhite"]:
            field_type = LookMLFieldType.YESNO
            dimension_sql = f"${{TABLE}}.{field}"
            if field == "is_female":
                dimension_description = (
                    "Yes if the gender of the person is like 'FEMALE'"
                )
            else:
                dimension_description = "Yes if the race of the person is not 'WHITE'"
        else:
            field_type = LookMLFieldType.STRING
            dimension_description = f"{field} of the person".capitalize()
            dimension_sql = f"INITCAP(REPLACE(${{TABLE}}.{field}, '_', ' '))"

        demographic_attribute_filter = FilterLookMLViewField(
            field_name=f"{field}_filter",
            parameters=[
                LookMLFieldParameter.description(
                    f"Filter that restricts {snake_to_title(field).lower()}"
                ),
                LookMLFieldParameter.type(field_type),
                LookMLFieldParameter.view_label("Custom Filters"),
                LookMLFieldParameter.sql(
                    f"{{% condition {field}_filter %}} ${{{field}}} {{% endcondition %}}"
                ),
                LookMLFieldParameter.suggestions(suggestions),
            ],
        )
        demographic_attribute_field_filter_lookml_fields.append(
            demographic_attribute_filter
        )

        demographic_attribute_field_dimension_lookml_field = DimensionLookMLViewField(
            field_name=f"{field}",
            parameters=[
                LookMLFieldParameter.description(dimension_description),
                LookMLFieldParameter.type(field_type),
                LookMLFieldParameter.view_label("Attributes"),
                LookMLFieldParameter.group_label("Person Attributes"),
                LookMLFieldParameter.sql(dimension_sql),
            ],
        )
        demographic_attribute_field_dimension_lookml_fields.append(
            demographic_attribute_field_dimension_lookml_field
        )

    return LookMLView(
        view_name=f"person_assignments_with_attributes_{view_name}",
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[
            *time_dependent_attribute_dimensions,
            *experiment_attribute_dimensions,
            *demographic_attribute_field_dimension_lookml_fields,
            *demographic_attribute_field_filter_lookml_fields,
        ],
    )


def generate_assignments_with_attributes_and_time_periods_view(
    view_name: str, unit_of_observation: MetricUnitOfObservation
) -> LookMLView:
    """Generates LookMLView that joins assignment + attributes views to customizable time periods"""
    assignments_view_for_unit_of_observation = (
        f"${{person_assignments_with_attributes_{view_name}.SQL_TABLE_NAME}}"
        if unit_of_observation.type == MetricUnitOfObservationType.PERSON_ID
        else f"""(
                SELECT
                    *, 
                    # Since there are no attributes for non-person units of observations, we add in a dummy field
                    TO_JSON_STRING(STRUCT(TRUE AS dummy_attribute)) AS all_attributes,
                FROM
                ${{{unit_of_observation.type.short_name}_assignments_{view_name}.SQL_TABLE_NAME}}
            )"""
    )
    unit_of_observation_query_fragment = list_to_query_string(
        sorted({*unit_of_observation.primary_key_columns}),
        table_prefix="assignments",
    )
    derived_table_query = f"""
    /*
    Goal: support either calendar time or event time.

    Calendar time: group by day, week, month, quarter, or yearly granularity with custom
    index dates.

    Event time: group by days, weeks, months, etc. since assignment, including potentially negative values.

    Calendar timer example:
    population_start_date = March 23
    period_param = Quarter
    period_duration = 1
    date spans: March 23 - June 23, June 23 - Sept 23, Sept 23 - Dec 23, etc.

    With event time, there are two kinds of assignment:
    1) client assignment: only nonnegative values
    2) officer/district/etc. assignment to an experiment variant: can be negative
    */

    {{% if {view_name}.event_time_toggle._parameter_value == "true" %}}
    -- event time

    WITH period_cte AS (
        SELECT
            periods_since_assignment
        FROM
        -- max possible periods between variant and current date
        UNNEST(GENERATE_ARRAY(
            DATE_DIFF(
                DATE({{% parameter {view_name}.population_start_date %}}),
                DATE({{% parameter {view_name}.population_end_date %}}),
                {{% parameter {view_name}.period_param %}}
            ),
            DATE_DIFF(
                DATE({{% parameter {view_name}.population_end_date %}}),
                DATE({{% parameter {view_name}.population_start_date %}}),
                {{% parameter {view_name}.period_param %}}
            ), 
            {{% parameter {view_name}.period_duration %}}
        )) periods_since_assignment
    )

    SELECT
        *,
        DATE_DIFF(end_date, start_date, DAY) AS days_in_period,
        GREATEST(assignment_date, start_date) AS span_start_date,
        LEAST(
            IFNULL(assignment_end_date, DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY)),
            DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY),
            end_date
        ) AS span_end_date,
    FROM (
        SELECT
            {unit_of_observation_query_fragment},
            unit_of_analysis,
            all_attributes,
            assignment_date,
            assignment_end_date,

            -- start and end dates generated relative to a fixed unit of analysis and base date
            DATE_ADD(
                event_time_base_date,
                INTERVAL periods_since_assignment * {{% parameter {view_name}.period_duration %}}
                {{% parameter {view_name}.period_param %}}
            ) AS start_date,
            DATE_ADD(
                event_time_base_date,
                INTERVAL (periods_since_assignment + 1) * {{% parameter {view_name}.period_duration %}}
                {{% parameter {view_name}.period_param %}}
            ) AS end_date,
            "{{% parameter {view_name}.period_param %}}" AS period,
            periods_since_assignment,
        FROM
            {assignments_view_for_unit_of_observation} assignments
        CROSS JOIN
            period_cte
    )
    WHERE
    -- overlap in period and assignment span
    (
        assignment_date BETWEEN start_date AND DATE_SUB(end_date, INTERVAL 1 DAY)
        OR start_date BETWEEN assignment_date AND IFNULL(DATE_SUB(assignment_end_date, INTERVAL 1 DAY), "9999-12-31")
    )

    -- period within specified range
    AND start_date >= DATE({{% parameter {view_name}.population_start_date %}})
    AND end_date <= DATE({{% parameter {view_name}.population_end_date %}})

    {{% if {view_name}.complete_periods._parameter_value == "true" %}}
    -- keep completed periods only
    -- OK if = current date since exclusive
    AND end_date <= CURRENT_DATE("US/Eastern")
    {{% endif %}}

    -- periods since assignment within specified range
    AND {{% condition {view_name}.periods_since_assignment %}}periods_since_assignment{{% endcondition %}}

    {{% else %}}
    -- calendar time
    SELECT
        {unit_of_observation_query_fragment},
        assignments.unit_of_analysis,
        assignments.all_attributes,
        assignments.assignment_date,
        assignments.assignment_end_date,
        time_period.start_date,
        time_period.end_date,
        time_period.period,
        {{% if {view_name}.period_param._parameter_value == "NONE" %}}
        0
        {{% else %}}
        DATE_DIFF(time_period.start_date, assignments.assignment_date, {{% parameter {view_name}.period_param %}})
        {{% endif %}} AS periods_since_assignment,
        DATE_DIFF(time_period.end_date, time_period.start_date, DAY) AS days_in_period,
        GREATEST(assignments.assignment_date, time_period.start_date) AS span_start_date,
        LEAST(
            IFNULL(assignments.assignment_end_date, DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY)),
            DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY),
            time_period.end_date
        ) AS span_end_date,
    FROM (
        SELECT
            "{{% parameter {view_name}.period_param %}}" AS period,
            end_date,
            -- exclusive end date
            {{% if {view_name}.period_param._parameter_value == "NONE" %}}
            DATE({{% parameter {view_name}.population_start_date %}}) AS start_date,
            {{% else %}}
            DATE_SUB(
                end_date, 
                INTERVAL {{% parameter {view_name}.period_duration %}} {{% parameter {view_name}.period_param %}}
            ) AS start_date,
            {{% endif %}}
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            DATE({{% parameter {view_name}.population_end_date %}}),
            {{% if {view_name}.period_param._parameter_value == "NONE" %}}
            DATE({{% parameter {view_name}.population_end_date %}})
            {{% else %}}
            DATE(DATE_ADD({{% parameter {view_name}.population_start_date %}}, INTERVAL 1 DAY)),

            # If period interval is NONE, use the length of the analysis period as the interval.
            # Otherwise, use the specified period interval
            {{% if {view_name}.period_interval_param._parameter_value == "NONE" %}}
            INTERVAL -{{% parameter {view_name}.period_duration %}} {{% parameter {view_name}.period_param %}}
            {{% else %}}
            INTERVAL -{{% parameter {view_name}.period_interval_duration %}} {{% parameter {view_name}.period_interval_param %}}
            {{% endif %}}

            {{% endif %}}
        )) AS end_date
    ) time_period

    -- join assignments/attributes
    LEFT JOIN
        {assignments_view_for_unit_of_observation} assignments
    ON
        assignments.assignment_date BETWEEN time_period.start_date AND DATE_SUB(time_period.end_date, INTERVAL 1 DAY)
        OR time_period.start_date BETWEEN assignments.assignment_date AND
        IFNULL(DATE_SUB(assignments.assignment_end_date, INTERVAL 1 DAY), "9999-12-31")

    {{% if {view_name}.complete_periods._parameter_value == "true" %}}
    -- keep completed periods only
    WHERE
        -- OK if = current date since exclusive
        end_date <= CURRENT_DATE("US/Eastern")
    {{% endif %}}
    {{% endif %}}
    """

    time_period_parameters = [
        ParameterLookMLViewField(
            field_name="population_start_date",
            parameters=[
                LookMLFieldParameter.description("Start date of the metric range"),
                LookMLFieldParameter.type(LookMLFieldType.DATE),
                LookMLFieldParameter.view_label("Time Periods"),
                LookMLFieldParameter.default_value("2017-01-01"),
            ],
        ),
        ParameterLookMLViewField(
            field_name="population_end_date",
            parameters=[
                LookMLFieldParameter.description("End date of the metric range"),
                LookMLFieldParameter.type(LookMLFieldType.DATE),
                LookMLFieldParameter.view_label("Time Periods"),
                LookMLFieldParameter.default_value("2024-01-01"),
            ],
        ),
        ParameterLookMLViewField(
            field_name="period_param",
            parameters=[
                LookMLFieldParameter.label("Period"),
                LookMLFieldParameter.description(
                    "For setting the time granularity of the metric calculation"
                ),
                LookMLFieldParameter.type(LookMLFieldType.UNQUOTED),
                LookMLFieldParameter.view_label("Time Periods"),
                LookMLFieldParameter.default_value("MONTH"),
                *[
                    LookMLFieldParameter.allowed_value(snake_to_title(x.value), x.value)
                    for x in MetricTimePeriod
                    if x != MetricTimePeriod.CUSTOM
                ],
                LookMLFieldParameter.allowed_value("None", "NONE"),
            ],
        ),
        ParameterLookMLViewField(
            field_name="period_duration",
            parameters=[
                LookMLFieldParameter.description(
                    "Sets the number of date units specified by `period_param` that make up each period of analysis"
                ),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.view_label("Time Periods"),
                LookMLFieldParameter.default_value("1"),
            ],
        ),
        ParameterLookMLViewField(
            field_name="period_interval_param",
            parameters=[
                LookMLFieldParameter.label("Period Interval"),
                LookMLFieldParameter.description(
                    "For setting the date unit of intervals at which a new period is generated"
                ),
                LookMLFieldParameter.type(LookMLFieldType.UNQUOTED),
                LookMLFieldParameter.view_label("Time Periods"),
                LookMLFieldParameter.default_value("NONE"),
                *[
                    LookMLFieldParameter.allowed_value(snake_to_title(x.value), x.value)
                    for x in MetricTimePeriod
                    if x != MetricTimePeriod.CUSTOM
                ],
                LookMLFieldParameter.allowed_value("None", "NONE"),
            ],
        ),
        ParameterLookMLViewField(
            field_name="period_interval_duration",
            parameters=[
                LookMLFieldParameter.description(
                    "Sets the number of date units specified by `period_interval_param` between each analysis period"
                ),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.view_label("Time Periods"),
                LookMLFieldParameter.default_value("1"),
            ],
        ),
        ParameterLookMLViewField(
            field_name="complete_periods",
            parameters=[
                LookMLFieldParameter.description(
                    "If 'Yes' (default), only includes complete periods when calculating metrics"
                ),
                LookMLFieldParameter.type(LookMLFieldType.YESNO),
                LookMLFieldParameter.view_label("Time Periods"),
                LookMLFieldParameter.default_value("yes"),
            ],
        ),
        ParameterLookMLViewField(
            field_name="event_time_toggle",
            parameters=[
                LookMLFieldParameter.label("Event-Time Toggle"),
                LookMLFieldParameter.description(
                    "If 'Yes', use event-time in place of calendar time (default). Currently "
                    "event-time is relative to variant assignment date and is compatible only with "
                    "supervision officer, supervision state, and incarceration state assignment types."
                ),
                LookMLFieldParameter.type(LookMLFieldType.YESNO),
                LookMLFieldParameter.view_label("Time Periods"),
                LookMLFieldParameter.group_label("Event Time"),
                LookMLFieldParameter.default_value("no"),
            ],
        ),
    ]

    time_period_dimensions = [
        DimensionLookMLViewField(
            field_name="period",
            parameters=[
                LookMLFieldParameter.description(
                    "For adding a period string to a table. Use `Period` parameter to filter."
                ),
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.view_label("Time Periods"),
                LookMLFieldParameter.sql("${TABLE}.period"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="period_duration_dimension",
            parameters=[
                LookMLFieldParameter.label("Period Duration"),
                LookMLFieldParameter.description(
                    "Dimension set by the parameter `period_duration`"
                ),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.view_label("Time Periods"),
                LookMLFieldParameter.sql("${TABLE}.period_duration"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="start_date",
            parameters=[
                LookMLFieldParameter.description("Start date of the metric"),
                LookMLFieldParameter.type(LookMLFieldType.DATE),
                LookMLFieldParameter.datatype(LookMLFieldDatatype.DATE),
                LookMLFieldParameter.view_label("Time Periods"),
                LookMLFieldParameter.group_label("Calendar Time"),
                LookMLFieldParameter.sql("${TABLE}.start_date"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="end_date",
            parameters=[
                LookMLFieldParameter.description("End date of the metric"),
                LookMLFieldParameter.type(LookMLFieldType.DATE),
                LookMLFieldParameter.datatype(LookMLFieldDatatype.DATE),
                LookMLFieldParameter.view_label("Time Periods"),
                LookMLFieldParameter.group_label("Calendar Time"),
                LookMLFieldParameter.sql("${TABLE}.end_date"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="days_in_period",
            parameters=[
                LookMLFieldParameter.description(
                    "Number of days between start and end date of the metric"
                ),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.view_label("Time Periods"),
                LookMLFieldParameter.sql("${TABLE}.days_in_period"),
            ],
        ),
        DimensionLookMLViewField(
            field_name="periods_since_assignment",
            parameters=[
                LookMLFieldParameter.description(
                    "Number of time periods elapsed since assignment. May be negative"
                ),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.view_label("Time Periods"),
                LookMLFieldParameter.group_label("Event Time"),
                LookMLFieldParameter.sql("${TABLE}.periods_since_assignment"),
            ],
        ),
    ]

    return LookMLView(
        view_name=f"{unit_of_observation.type.short_name}_assignments_with_attributes_and_time_periods_{view_name}",
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[
            *time_period_dimensions,
            *time_period_parameters,
        ],
    )


def custom_metrics_view_query_template(
    view_name: str,
    unit_of_observation: MetricUnitOfObservation,
    json_field_filters: List[str],
    include_column_mapping: bool = True,
) -> str:
    """Returns query template that unions together all LookML view dependencies to generate a custom metrics table.
    `include_column_mapping` indicates whether we want to pull index columns into the final query template.
    """

    assignments_and_attributes_view = (
        f"${{person_assignments_with_attributes_{view_name}.SQL_TABLE_NAME}}"
        if MetricUnitOfObservationType.PERSON_ID
        else f"${{{unit_of_observation.type.short_name}_assignments_with_attributes_{view_name}.SQL_TABLE_NAME}}"
    )
    field_filters_query_fragment = "".join(
        [
            liquid_wrap_json_field(f", {field}", field, view_name)
            for field in json_field_filters
        ]
    )

    first_join_fragment = "period_span_metrics"
    if include_column_mapping:
        first_join_fragment = """column_mapping
    INNER JOIN
        period_span_metrics
    USING
        (unit_of_analysis, all_attributes)"""

    derived_table_query = f"""
    WITH time_period_cte AS (
        SELECT
            *
        FROM
            ${{{unit_of_observation.type.short_name}_assignments_with_attributes_and_time_periods_{view_name}.SQL_TABLE_NAME}}
    )
    , assignments_and_attributes_cte AS (
        SELECT
          *
        FROM
          {assignments_and_attributes_view}
    )

    -- map all_attributes to original columns
    , column_mapping AS (
        SELECT DISTINCT
            * EXCEPT (
                assignment_date, assignment_end_date, event_time_base_date, person_id
            )
        FROM
            assignments_and_attributes_cte
    )

    -- period_span metrics
    , period_span_metrics AS (
        SELECT
            *
        FROM
            ${{{unit_of_observation.type.short_name}_period_span_aggregated_metrics_{view_name}.SQL_TABLE_NAME}}
    )

    -- period_event metrics
    , period_event_metrics AS (
        SELECT
            *
        FROM
            ${{{unit_of_observation.type.short_name}_period_event_aggregated_metrics_{view_name}.SQL_TABLE_NAME}}
    )

    -- assignment_span metrics
    , assignment_span_metrics AS (
        SELECT
            *
        FROM
            ${{{unit_of_observation.type.short_name}_assignment_span_aggregated_metrics_{view_name}.SQL_TABLE_NAME}}
    )

    -- assignment_event metrics
    , assignment_event_metrics AS (
        SELECT
            *
        FROM
            ${{{unit_of_observation.type.short_name}_assignment_event_aggregated_metrics_{view_name}.SQL_TABLE_NAME}}
    )

    -- join all metrics on unit-of-analysis and attribute struct to return original columns
    SELECT
        * EXCEPT(start_date, end_date),
        {{% parameter {view_name}.period_duration %}} AS period_duration,

        -- if event_time, put back periods_since_assignment, otherwise add dates
        {{% if {view_name}.event_time_toggle._parameter_value == "true" %}}
        start_date AS periods_since_assignment,
        DATE_DIFF(
        DATE_ADD("2000-01-01", INTERVAL {{% parameter {view_name}.period_duration %}} {{% parameter {view_name}.period_param %}}),
        "2000-01-01",
        DAY
        ) AS days_in_period,
        {{% else %}}
        start_date,
        end_date,
        DATE_DIFF(end_date, start_date, DAY) AS days_in_period,
        {{% endif %}}
    FROM
        {first_join_fragment}
    LEFT JOIN
        period_event_metrics
    USING
        (unit_of_analysis, all_attributes, period, start_date, end_date{field_filters_query_fragment})
    LEFT JOIN
        assignment_span_metrics
    USING
        (unit_of_analysis, all_attributes, period, start_date, end_date{field_filters_query_fragment})
    LEFT JOIN
        assignment_event_metrics
    USING
        (unit_of_analysis, all_attributes, period, start_date, end_date{field_filters_query_fragment})
"""
    return derived_table_query


def generate_custom_metrics_view(
    metrics: List[AggregatedMetric],
    view_name: str,
    json_field_filters_with_suggestions: Dict[str, List[str]],
    additional_view_fields: Optional[List[LookMLViewField]],
) -> LookMLView:
    """Generates LookMLView with derived table that joins together metric view
    builders, analysis periods, and assignments to dynamically calculate metrics,
    referencing the provided view name."""
    unit_of_observation_types = sorted(
        set(
            metric.unit_of_observation_type
            for metric in metrics
            if hasattr(metric, "unit_of_observation_type")
        ),
        key=lambda x: x.name,
    )
    derived_table_subqueries: List[str] = []
    for unit_of_observation in unit_of_observation_types:
        derived_table_subqueries = derived_table_subqueries + [
            custom_metrics_view_query_template(
                view_name=view_name,
                unit_of_observation=MetricUnitOfObservation(type=unit_of_observation),
                json_field_filters=list(json_field_filters_with_suggestions),
                # Only include full index column mappings for the first unit of observation cte
                # to avoid having duplicate index columns in the final query
                include_column_mapping=(
                    unit_of_observation == unit_of_observation_types[0]
                ),
            )
        ]

    field_filters_query_fragment = "".join(
        [
            liquid_wrap_json_field(f", {field}", field, view_name)
            for field in json_field_filters_with_suggestions
        ]
    )

    derived_table_query = f"""
SELECT *
FROM ({derived_table_subqueries[0]})
"""
    if len(derived_table_subqueries) > 0:
        derived_table_query = derived_table_query + "\n".join(
            [
                f"""FULL OUTER JOIN
({subquery})
USING (unit_of_analysis, all_attributes, period, start_date, end_date, days_in_period{field_filters_query_fragment})
"""
                for subquery in derived_table_subqueries[1:]
            ]
        )

    extended_views = []
    for unit_of_observation_type in unit_of_observation_types:
        if unit_of_observation_type == MetricUnitOfObservationType.PERSON_ID:
            extended_views.append(f"person_assignments_with_attributes_{view_name}")
        extended_views = extended_views + [
            f"{unit_of_observation_type.short_name}_assignments_{view_name}",
            f"{unit_of_observation_type.short_name}_assignments_with_attributes_and_time_periods_{view_name}",
            f"{unit_of_observation_type.short_name}_period_span_aggregated_metrics_{view_name}",
            f"{unit_of_observation_type.short_name}_period_event_aggregated_metrics_{view_name}",
            f"{unit_of_observation_type.short_name}_assignment_span_aggregated_metrics_{view_name}",
            f"{unit_of_observation_type.short_name}_assignment_event_aggregated_metrics_{view_name}",
        ]

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
    all_attributes_dimension = DimensionLookMLViewField(
        field_name="all_attributes",
        parameters=[
            LookMLFieldParameter.description("Collection of all custom attributes"),
            LookMLFieldParameter.type(LookMLFieldType.STRING),
            LookMLFieldParameter.view_label("Person Attributes"),
            LookMLFieldParameter.sql("${TABLE}.all_attributes"),
            LookMLFieldParameter.hidden(is_hidden=True),
        ],
    )
    count_units_measure = MeasureLookMLViewField(
        field_name="count_units_of_analysis",
        parameters=[
            LookMLFieldParameter.label("Count Units"),
            LookMLFieldParameter.description(
                "Counts distinct units of analysis and included attributes"
            ),
            LookMLFieldParameter.type(LookMLFieldType.NUMBER),
            LookMLFieldParameter.view_label("Metric Menu"),
            LookMLFieldParameter.sql(
                "COUNT(CONCAT(${assignment_unit_of_analysis}, ${all_attributes}))"
            ),
        ],
    )

    # Generate a dimension and filter field for every inputted filter field name
    json_field_filter_lookml_fields: List[LookMLViewField] = []
    for field, suggestions in json_field_filters_with_suggestions.items():
        json_dimension = DimensionLookMLViewField(
            field_name=field,
            parameters=[
                LookMLFieldParameter.description(f"{snake_to_title(field)}"),
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.view_label("Custom Filters"),
                LookMLFieldParameter.sql(
                    f"INITCAP(REPLACE(${{TABLE}}.{field}, '_', ' '))"
                ),
            ],
        )
        json_field_filter_lookml_fields.append(json_dimension)
        json_filter = FilterLookMLViewField(
            field_name=f"{field}_filter",
            parameters=[
                LookMLFieldParameter.description(
                    f"Filter that restricts {snake_to_title(field).lower()}"
                ),
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.view_label("Custom Filters"),
                LookMLFieldParameter.sql(
                    f"{{% condition {field}_filter %}} ${{{field}}} {{% endcondition %}}"
                ),
                LookMLFieldParameter.suggestions(suggestions),
            ],
        )
        json_field_filter_lookml_fields.append(json_filter)

    return LookMLView(
        view_name=view_name,
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[
            measure_type_parameter,
            metric_filter_parameter,
            metric_value_measure,
            *(additional_view_fields or []),
            all_attributes_dimension,
            count_units_measure,
            *json_field_filter_lookml_fields,
        ],
        included_paths=[
            f"/views/aggregated_metrics/generated/{view_name}/subqueries/*",
        ],
        extended_views=extended_views,
    )
