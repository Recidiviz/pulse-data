# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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

from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from recidiviz.aggregated_metrics.aggregated_metrics_view_collector import (
    METRIC_CLASSES,
)
from recidiviz.aggregated_metrics.assignment_sessions_view_builder import (
    get_metric_assignment_sessions_materialized_table_address,
    has_configured_assignment_query,
)
from recidiviz.aggregated_metrics.assignments_by_time_period_view_builder import (
    AssignmentsByTimePeriodViewBuilder,
    MetricTimePeriodToAssignmentJoinType,
)
from recidiviz.aggregated_metrics.metric_time_period_config import MetricTimePeriod
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
    get_static_attributes_query_for_unit_of_analysis,
)
from recidiviz.aggregated_metrics.query_building.aggregated_metric_query_utils import (
    AggregatedMetricClassType,
)
from recidiviz.aggregated_metrics.query_building.build_aggregated_metric_query import (
    metric_output_column_clause,
)
from recidiviz.aggregated_metrics.query_building.build_single_observation_type_aggregated_metric_query import (
    OBSERVATIONS_BY_ASSIGNMENTS_CTE_NAME,
    OBSERVATIONS_CTE_NAME,
    _aggregation_clause_for_metric,
    build_observations_by_assignments_query_template,
    build_observations_query_template_for_metrics,
)
from recidiviz.calculator.query.bq_utils import MAGIC_START_DATE, list_to_query_string
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
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.observation_selector import ObservationTypeT
from recidiviz.observations.observation_type_utils import (
    ObservationType,
    attribute_cols_for_observation_type,
)
from recidiviz.tools.looker.aggregated_metrics.aggregated_metrics_lookml_utils import (
    get_metric_explore_parameter,
    get_metric_value_measure,
    measure_for_metric,
)
from recidiviz.utils.string_formatting import fix_indent


def _liquid_wrap_metrics(
    query_fragment: str,
    metrics: list[AggregatedMetric],
    lookml_views_package_name: str,
    include_comma: bool = True,
) -> str:
    """
    Outputs a conditional liquid fragment that will display the provided query fragment
    if any of a set of metrics is selected in the Explore.
    """
    metric_liquid_fragments: list[str] = []
    for metric in metrics:
        metric_liquid_fragments.append(
            f'{lookml_views_package_name}.{metric.name}_measure._in_query or {lookml_views_package_name}.metric_filter._parameter_value contains "{metric.name}"'
        )
    metric_liquid_fragment = " or ".join(metric_liquid_fragments)
    return f"""{{% if {metric_liquid_fragment} %}}
{query_fragment}{"," if include_comma else ""}
{{% endif %}}"""


def liquid_wrap_json_field(
    query_fragment: str, field_name: str, lookml_views_package_name: str
) -> str:
    """
    Outputs a conditional liquid fragment that will display the provided query fragment
    if the json field is selected in the Explore.
    """
    return f"""{{% if {lookml_views_package_name}.{field_name}._in_query or {lookml_views_package_name}.{field_name}._is_filtered %}}{query_fragment}{{% endif %}}"""


def build_time_periods_lookml_view(lookml_views_package_name: str) -> LookMLView:
    """Returns a LookMLView that constructs time periods using params for
    start and end dates, and period duration and interval values."""

    derived_table_query = f"""
SELECT
    "{{% parameter {lookml_views_package_name}.period_param %}}" AS period,
    end_date AS metric_period_end_date_exclusive,
    -- exclusive end date
    {{% if {lookml_views_package_name}.period_param._parameter_value == "NONE" %}}
    DATE({{% parameter {lookml_views_package_name}.population_start_date %}}) AS metric_period_start_date,
    {{% else %}}
    DATE_SUB(
        end_date, 
        INTERVAL {{% parameter {lookml_views_package_name}.period_duration %}} {{% parameter {lookml_views_package_name}.period_param %}}
    ) AS metric_period_start_date,
    {{% endif %}}
FROM
    UNNEST(GENERATE_DATE_ARRAY(
        DATE({{% parameter {lookml_views_package_name}.population_end_date %}}),
        {{% if {lookml_views_package_name}.period_param._parameter_value == "NONE" %}}
        DATE({{% parameter {lookml_views_package_name}.population_end_date %}})
        {{% else %}}
        DATE(DATE_ADD({{% parameter {lookml_views_package_name}.population_start_date %}}, INTERVAL 1 DAY)),

        # If period interval is NONE, use the length of the analysis period as the interval.
        # Otherwise, use the specified period interval
        {{% if {lookml_views_package_name}.period_interval_param._parameter_value == "NONE" %}}
        INTERVAL -{{% parameter {lookml_views_package_name}.period_duration %}} {{% parameter {lookml_views_package_name}.period_param %}}
        {{% else %}}
        INTERVAL -{{% parameter {lookml_views_package_name}.period_interval_duration %}} {{% parameter {lookml_views_package_name}.period_interval_param %}}
        {{% endif %}}

        {{% endif %}}
    )) AS end_date
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
                LookMLFieldParameter.sql(
                    "DATE_DIFF(${TABLE}.end_date, ${TABLE}.start_date, DAY)"
                ),
            ],
        ),
    ]

    return LookMLView(
        view_name=_get_metric_time_periods_cte_name(lookml_views_package_name),
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[
            *time_period_dimensions,
            *time_period_parameters,
        ],
    )


def _generate_assignment_query_fragment_for_unit_of_analysis(
    lookml_views_package_name: str,
    assignment_types_dict: Dict[
        str, Tuple[MetricPopulationType, MetricUnitOfAnalysisType]
    ],
    unit_of_observation: MetricUnitOfObservation,
    assignment_type: str,
) -> str:
    """Returns a query fragment that queries the relevant assignment table between
    the unit of analysis associated with the inputted `assignment_type`, and the
    specified unit of observation, along with a join to the associated static
    attributes query using liquid to reference the assignment type parameter."""

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
    static_attributes_source_table = get_static_attributes_query_for_unit_of_analysis(
        unit_of_analysis_type, bq_view=False
    )
    static_attributes_join_query_fragment = (
        f"""
            LEFT JOIN 
                (
{fix_indent(static_attributes_source_table, indent_level=16)}
                )
            USING
                ({primary_columns_str})
            """
        if static_attributes_source_table
        else ""
    )

    if has_configured_assignment_query(unit_of_analysis_type, unit_of_observation.type):
        source_table_address = (
            get_metric_assignment_sessions_materialized_table_address(
                unit_of_observation_type=unit_of_observation.type,
                unit_of_analysis_type=unit_of_analysis_type,
                population_type=population_type,
            ).to_str()
        )
        source_table = f"{source_table_address}{static_attributes_join_query_fragment}"
    else:
        # If no assignment table has been defined, create a dummy assignment table with all NULL fields
        placeholder_columns_query_string = ", ".join(
            [
                (
                    f"NULL AS {col}"
                    if col in ["person_id", "facility_counselor_id", "unit_supervisor"]
                    else (
                        f"CAST(NULL AS DATE) AS {col}"
                        if col == "cohort_month_end_date"
                        else f"CAST(NULL AS STRING) AS {col}"
                    )
                )
                for col in shared_columns
            ]
        )
        source_table = f"""(
            SELECT
                {placeholder_columns_query_string},
                DATE("{MAGIC_START_DATE}") AS assignment_date,
                CAST(NULL AS DATE) AS end_date_exclusive,
                FALSE AS assignment_is_first_day_in_population,
        )"""

    return f"""
{{% elsif {lookml_views_package_name}.assignment_type._parameter_value == "{assignment_type}" %}}
    SELECT
        {shared_columns_string},
        assignment_date,
        end_date_exclusive AS end_date,
        assignment_is_first_day_in_population,
        CONCAT({primary_columns_str}) AS unit_of_analysis,
    FROM
        {source_table}
"""


def build_assignments_lookml_view(
    lookml_views_package_name: str,
    assignment_types_dict: Dict[
        str, Tuple[MetricPopulationType, MetricUnitOfAnalysisType]
    ],
    unit_of_observation: MetricUnitOfObservation,
) -> LookMLView:
    """Generates LookMLView for all possible assignment types (combinations of
    population & unit of analysis types)"""

    all_assignment_type_query_fragments = "".join(
        [
            _generate_assignment_query_fragment_for_unit_of_analysis(
                lookml_views_package_name,
                assignment_types_dict,
                unit_of_observation,
                assignment_type,
            )
            for assignment_type in assignment_types_dict
            if assignment_type != "PERSON"
        ]
    )

    # Assembles a query fragment for filtering over the set of index columns across all units of analysis
    all_attribute_columns = sorted(
        set(
            col
            for (_, unit_of_analysis_type) in assignment_types_dict.values()
            for col in MetricUnitOfAnalysis.for_type(
                unit_of_analysis_type
            ).index_columns
        )
    )
    attribute_columns_conditional = "\nAND ".join(
        [
            f"{{% condition {lookml_views_package_name}.{attr} %}}{attr}{{% endcondition %}}"
            for attr in all_attribute_columns
        ]
    )

    derived_table_query = f"""
-- Pull in {{assignment_type}}_metrics_{{unit_of_observation}}_assignment_sessions, which
-- contains spans of assignment of units of observation to the unit of analysis.
SELECT
assignment_sessions.* EXCEPT(end_date),
assignment_sessions.assignment_date AS assignment_start_date,
assignment_sessions.end_date AS assignment_end_date,
COALESCE(assignment_sessions.end_date, "9999-01-01") AS assignment_end_date_exclusive_nonnull,

FROM (
{{% if {lookml_views_package_name}.assignment_type._parameter_value == "PERSON" %}}
    SELECT
        state_code, person_id,
        start_date AS assignment_date,
        end_date_exclusive AS end_date,
        assignment_is_first_day_in_population,
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
        view_name=_get_assignments_cte_name(
            lookml_views_package_name, unit_of_observation.type
        ),
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[
            assignment_type_parameter,
            unit_of_analysis_dimension,
            *all_unit_of_analysis_dimensions,
        ],
    )


def _get_assignments_cte_name(
    lookml_views_package_name: str,
    unit_of_observation_type: MetricUnitOfObservationType,
) -> str:
    return (
        f"{unit_of_observation_type.short_name}_assignments_{lookml_views_package_name}"
    )


def _get_metric_time_periods_cte_name(lookml_views_package_name: str) -> str:
    return f"metric_time_periods_{lookml_views_package_name}"


def build_assignments_by_time_period_lookml_view(
    lookml_views_package_name: str,
    unit_of_observation_type: MetricUnitOfObservationType,
    metric_time_period_to_assignment_join_type: MetricTimePeriodToAssignmentJoinType,
) -> LookMLView:
    """Builds a query template (with project_id format argument) that joins a collection
    of metric time periods with assignment periods, returning one result row for every
    metric time period where there is some overlap with an assignment span. If there are
    multiple assignments that overlap with a metric period, multiple rows will be
    returned.
    """
    join_clause = AssignmentsByTimePeriodViewBuilder.get_join_condition_clause(
        metric_time_period_to_assignment_join_type
    )
    assignment_sessions_table_name = _get_assignments_cte_name(
        lookml_views_package_name, unit_of_observation_type
    )
    metric_time_periods_table_name = _get_metric_time_periods_cte_name(
        lookml_views_package_name
    )
    output_columns = (
        AssignmentsByTimePeriodViewBuilder.additional_output_columns_for_join_type(
            metric_time_period_to_assignment_join_type
        )
    )
    output_column_clauses = (
        AssignmentsByTimePeriodViewBuilder.get_date_output_column_clauses(
            output_columns
        )
    )
    output_column_clauses_query_fragment = (
        fix_indent(",\n".join(output_column_clauses), indent_level=4) + "\n"
    )

    query_template = f"""
WITH
time_periods AS (
    SELECT * FROM ${{{metric_time_periods_table_name}.SQL_TABLE_NAME}}
),
assignment_sessions AS (
    SELECT * FROM ${{{assignment_sessions_table_name}.SQL_TABLE_NAME}}
)
SELECT
    * EXCEPT(assignment_is_first_day_in_population),
{output_column_clauses_query_fragment}FROM
    time_periods
JOIN
    assignment_sessions
ON
{fix_indent(join_clause, indent_level=4)}
"""
    derived_table_query = fix_indent(query_template, indent_level=0)

    extended_views = [
        assignment_sessions_table_name,
        metric_time_periods_table_name,
    ]

    periods_since_assignment_dimension = DimensionLookMLViewField(
        field_name="periods_since_assignment",
        parameters=[
            LookMLFieldParameter.description("Periods Since Assignment"),
            LookMLFieldParameter.type(LookMLFieldType.NUMBER),
            LookMLFieldParameter.view_label("Time Periods"),
            LookMLFieldParameter.sql(
                "DATE_DIFF(${TABLE}.start_date, ${TABLE}.assignment_start_date, INTERVAL ${period_duration} ${period})"
            ),
        ],
    )

    return LookMLView(
        view_name=_get_assignments_by_time_period_cte_name(
            lookml_views_package_name,
            join_type=metric_time_period_to_assignment_join_type,
            unit_of_observation_type=unit_of_observation_type,
        ),
        table=LookMLViewSourceTable.derived_table(derived_table_query),
        fields=[periods_since_assignment_dimension],
        included_paths=[
            f"/__generated__/views/aggregated_metrics/{lookml_views_package_name}/subqueries/*",
        ],
        extended_views=extended_views,
    )


def _get_assignments_by_time_period_cte_name(
    view_id: str,
    join_type: MetricTimePeriodToAssignmentJoinType,
    unit_of_observation_type: MetricUnitOfObservationType,
) -> str:
    return f"{unit_of_observation_type.short_name}_assignments_by_time_period_{join_type.value.lower()}_{view_id}"


def _generate_all_assignments_by_time_period_ctes(
    lookml_views_package_name: str,
    unit_of_observation_types: list[MetricUnitOfObservationType],
) -> str:
    all_assignments_by_time_period_ctes: list[str] = []
    for metric_class in METRIC_CLASSES:
        for unit_of_observation_type in unit_of_observation_types:
            join_type_lookml_views_package_name = (
                _get_assignments_by_time_period_cte_name(
                    lookml_views_package_name,
                    join_type=metric_class.metric_time_period_to_assignment_join_type(),
                    unit_of_observation_type=unit_of_observation_type,
                )
            )
            cte_for_join_type = f"""{_get_assignments_by_time_period_cte_name(lookml_views_package_name, metric_class.metric_time_period_to_assignment_join_type(), unit_of_observation_type)} AS (
    SELECT * FROM ${{{join_type_lookml_views_package_name}.SQL_TABLE_NAME}}
)"""
            all_assignments_by_time_period_ctes.append(cte_for_join_type)
    return ",\n".join(sorted(set(all_assignments_by_time_period_ctes)))


def get_metrics_by_observation_type_and_metric_class(
    metrics: List[AggregatedMetric],
) -> defaultdict[
    tuple[ObservationType, AggregatedMetricClassType],
    list[AggregatedMetric],
]:
    metrics_by_observation_type_and_metric_class = defaultdict(list)
    for metric_class in METRIC_CLASSES:
        metrics_with_class = [
            metric for metric in metrics if isinstance(metric, metric_class)
        ]
        for metric in metrics_with_class:
            metrics_by_observation_type_and_metric_class[
                (metric.observation_type, metric_class)
            ].append(metric)
    return metrics_by_observation_type_and_metric_class


def _generate_single_observation_type_aggregated_metric_lookml_query_template(
    lookml_views_package_name: str,
    observation_type: ObservationTypeT,
    metric_class: AggregatedMetricClassType,
    single_observation_type_metrics: list[AggregatedMetric[ObservationTypeT]],
    json_field_filters: list[str],
    assignment_types_dict: dict[
        str, tuple[MetricPopulationType, MetricUnitOfAnalysisType]
    ],
) -> str:
    """Returns a query template (with a project_id format arg) that can be used to
    calculate aggregated metrics for metrics matching a single observation type and
    metric class type.
    """

    assignments_by_time_period_cte_name = _get_assignments_by_time_period_cte_name(
        lookml_views_package_name,
        metric_class.metric_time_period_to_assignment_join_type(),
        observation_type.unit_of_observation_type,
    )

    if not single_observation_type_metrics:
        raise ValueError(
            f"Must provide at least one metric. Found no metrics for "
            f"[{observation_type}] and [{metric_class}]"
        )

    for metric in single_observation_type_metrics:
        if not isinstance(metric, metric_class):
            raise ValueError(
                f"Found metric [{metric.name}] which has type [{type(metric)}] which "
                f"is not the expected type [{metric_class}]"
            )

        if metric.observation_type != observation_type:
            raise ValueError(
                f"Found metric [{metric.name}] which has observation_type "
                f"[{metric.observation_type}] which is not the expected type "
                f"[{observation_type}]"
            )
    compatible_json_field_filters = [
        (
            field
            if field in attribute_cols_for_observation_type(observation_type)
            else f"CAST(NULL AS STRING) AS {field}"
        )
        for field in json_field_filters
    ]

    observations_cte = build_observations_query_template_for_metrics(
        observation_type,
        single_observation_type_metrics,
        additional_attribute_column_clauses=compatible_json_field_filters,
        include_project_id_format_arg=False,
    )
    all_observations_by_assignments_query_fragments: list[str] = []
    for assignment_type, (_, unit_of_analysis_type) in assignment_types_dict.items():
        observations_by_assignments_cte = (
            build_observations_by_assignments_query_template(
                observation_type,
                unit_of_analysis_type,
                metric_class,
                single_observation_type_metrics,
                assignments_by_time_period_cte_name,
                additional_assignment_attribute_columns=["unit_of_analysis"],
                additional_observation_attribute_columns=json_field_filters,
            )
        )
        observations_by_assignments_cte_liquid_wrap = f"""\
{{% if {lookml_views_package_name}.assignment_type._parameter_value == "{assignment_type}" %}}{observations_by_assignments_cte}{{% endif %}}
"""
        all_observations_by_assignments_query_fragments.append(
            observations_by_assignments_cte_liquid_wrap
        )
    observations_by_assignments_liquid_wrap = "\n".join(
        all_observations_by_assignments_query_fragments
    )

    json_field_filters_with_liquid_wrap = fix_indent(
        "\n".join(
            [
                liquid_wrap_json_field(
                    query_fragment=f"{json_field}, ",
                    field_name=json_field,
                    lookml_views_package_name=lookml_views_package_name,
                )
                for json_field in json_field_filters
            ]
        ),
        indent_level=4,
    )

    group_by_cols: list[str] = [
        "unit_of_analysis",
        "period",
        "metric_period_start_date",
        "metric_period_end_date_exclusive",
    ]
    output_column_strs = [
        "unit_of_analysis",
        "period",
        "metric_period_start_date AS start_date",
        "metric_period_end_date_exclusive AS end_date",
        *[
            fix_indent(_aggregation_clause_for_metric(metric), indent_level=0)
            for metric in single_observation_type_metrics
        ],
    ]
    output_columns_str = ",\n".join(output_column_strs)
    return f"""
WITH
{OBSERVATIONS_CTE_NAME} AS (
{fix_indent(observations_cte, indent_level=4)}
),
{OBSERVATIONS_BY_ASSIGNMENTS_CTE_NAME} AS (
{fix_indent(observations_by_assignments_liquid_wrap, indent_level=4)}
)
SELECT
{json_field_filters_with_liquid_wrap}
{fix_indent(output_columns_str, indent_level=4)}
FROM {OBSERVATIONS_BY_ASSIGNMENTS_CTE_NAME}
GROUP BY
{json_field_filters_with_liquid_wrap}{list_to_query_string(group_by_cols)}
"""


def _get_index_columns_to_exclude_for_assignment_type(
    lookml_views_package_name: str,
    assignment_type: str,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    unit_of_observation_type: MetricUnitOfObservationType,
) -> str:
    columns_to_exclude = [
        col
        for col in MetricUnitOfObservation(
            type=unit_of_observation_type
        ).primary_key_columns_ordered
        if col
        not in MetricUnitOfAnalysis.for_type(unit_of_analysis_type).primary_key_columns
    ]
    # Add a comma if there is at least one column here, otherwise don't
    columns_to_exclude_query_fragment = (
        ",\n" if columns_to_exclude else ""
    ) + list_to_query_string(columns_to_exclude)
    return f"""{{% if {lookml_views_package_name}.assignment_type._parameter_value == "{assignment_type}" %}}{columns_to_exclude_query_fragment}{{% endif %}}"""


def _generate_output_row_keys_cte_for_unit_of_observation(
    lookml_views_package_name: str,
    metric_class: AggregatedMetricClassType,
    unit_of_observation_type: MetricUnitOfObservationType,
    assignment_types_dict: Dict[
        str, Tuple[MetricPopulationType, MetricUnitOfAnalysisType]
    ],
) -> str:
    """Returns a cte that combines all distinct keys across all assignment X time period
    views. This cte will be used as the base of the final join across metrics to ensure
    that all grouping variables are represented in the final output."""

    date_fields_to_exclude = [
        "assignment_date",
        "assignment_end_date",
        AssignmentsByTimePeriodViewBuilder.ASSIGNMENT_START_DATE_COLUMN_NAME,
        AssignmentsByTimePeriodViewBuilder.ASSIGNMENT_END_DATE_EXCLUSIVE_COLUMN_NAME,
        "metric_period_start_date",
        "metric_period_end_date_exclusive",
    ] + AssignmentsByTimePeriodViewBuilder.additional_output_columns_for_join_type(
        metric_class.metric_time_period_to_assignment_join_type()
    )
    index_columns_to_exclude = "".join(
        [
            _get_index_columns_to_exclude_for_assignment_type(
                lookml_views_package_name,
                assignment_type_name,
                configuration[1],
                unit_of_observation_type,
            )
            for assignment_type_name, configuration in assignment_types_dict.items()
        ]
    )

    column_attributes_to_exclude_query_fragment = fix_indent(
        list_to_query_string(date_fields_to_exclude) + index_columns_to_exclude,
        indent_level=8,
    )

    return f"""
SELECT DISTINCT * EXCEPT(
{column_attributes_to_exclude_query_fragment}
    ),
    metric_period_start_date AS start_date,
    metric_period_end_date_exclusive AS end_date,
FROM {_get_assignments_by_time_period_cte_name(lookml_views_package_name, metric_class.metric_time_period_to_assignment_join_type(), unit_of_observation_type)}"""


def build_custom_metrics_lookml_view(
    lookml_views_package_name: str,
    metrics: list[AggregatedMetric],
    assignment_types_dict: Dict[
        str, Tuple[MetricPopulationType, MetricUnitOfAnalysisType]
    ],
    json_field_filters_with_suggestions: Optional[dict[str, list[str]]] = None,
) -> LookMLView:
    """Generates a LookML view that contains all metrics across all units of observation,
    supporting a provided set of assignment types and json field filters."""

    metrics_by_unit_of_observation_type_and_metric_class = (
        get_metrics_by_observation_type_and_metric_class(metrics)
    )

    all_metric_ctes = [
        f"""
{observation_type.value.lower()}_{metrics[0].metric_time_period_to_assignment_join_type().value.lower()}_metrics AS (
    {fix_indent(
        _generate_single_observation_type_aggregated_metric_lookml_query_template(
            lookml_views_package_name,
            observation_type,
            metric_class,
            metrics,
            list(json_field_filters_with_suggestions.keys()) if json_field_filters_with_suggestions else [],
            assignment_types_dict
        ),
        indent_level=4
    )}
)
        """
        for (observation_type, metric_class,), metrics in sorted(
            metrics_by_unit_of_observation_type_and_metric_class.items(),
            key=lambda item: (item[0][0].value, item[0][1].__name__),
        )
    ]
    all_metric_ctes_query_fragment = "," + ",\n".join(all_metric_ctes)

    final_select_metrics_query_fragment = "".join(
        [
            _liquid_wrap_metrics(
                metric_output_column_clause(metric),
                metrics=[metric],
                lookml_views_package_name=lookml_views_package_name,
            )
            for metric in metrics
        ]
    )

    json_field_filters_with_liquid_wrap = (
        fix_indent(
            "".join(
                [
                    liquid_wrap_json_field(
                        query_fragment=f"{json_field},\n",
                        field_name=json_field,
                        lookml_views_package_name=lookml_views_package_name,
                    )
                    for json_field in json_field_filters_with_suggestions
                ]
            ),
            indent_level=8,
        )
        if json_field_filters_with_suggestions
        else ""
    )

    all_json_field_key_subqueries = [
        f"""SELECT DISTINCT
    -- dummy in case there are no json field filters
    NULL AS dummy,
{fix_indent(json_field_filters_with_liquid_wrap, indent_level=4)}
FROM
    {unit_of_observation_type.value.lower()}_{metrics[0].metric_time_period_to_assignment_join_type().value.lower()}_metrics"""
        for _, ((unit_of_observation_type, _), metrics) in enumerate(
            metrics_by_unit_of_observation_type_and_metric_class.items()
        )
    ]

    all_json_field_key_cte = fix_indent(
        "\nUNION DISTINCT\n".join(all_json_field_key_subqueries), indent_level=4
    )

    join_fragment = f"""{json_field_filters_with_liquid_wrap}
        unit_of_analysis, period, start_date, end_date"""

    final_join_queries = [
        _liquid_wrap_metrics(
            f"""FULL OUTER JOIN
    {unit_of_observation_type.value.lower()}_{metrics[0].metric_time_period_to_assignment_join_type().value.lower()}_metrics
USING
    (
{join_fragment}
    )""",
            metrics,
            lookml_views_package_name,
            include_comma=False,
        )
        for _, ((unit_of_observation_type, _), metrics) in enumerate(
            metrics_by_unit_of_observation_type_and_metric_class.items()
        )
    ]

    final_join_query_fragment = "".join(final_join_queries)

    all_unit_of_observation_types = list(
        set(metric.unit_of_observation_type for metric in metrics)
    )

    all_output_row_keys_queries = []
    for metric_class in METRIC_CLASSES:
        for unit_of_observation_type in all_unit_of_observation_types:
            all_output_row_keys_queries.append(
                _generate_output_row_keys_cte_for_unit_of_observation(
                    lookml_views_package_name,
                    metric_class,
                    unit_of_observation_type,
                    assignment_types_dict,
                )
            )
    output_row_keys_unioned_query_fragment = fix_indent(
        "\nUNION DISTINCT\n".join(sorted(set(all_output_row_keys_queries))),
        indent_level=4,
    )

    metric_filter_parameter = get_metric_explore_parameter(
        metrics, "metric_filter"
    ).extend(
        additional_parameters=[
            LookMLFieldParameter.label("Metric Filter"),
            LookMLFieldParameter.description("Used to select one metric for a Look."),
        ]
    )
    metric_value_measure = get_metric_value_measure(
        lookml_views_package_name, metric_filter_parameter
    )
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
    count_units_measure = MeasureLookMLViewField(
        field_name="count_units_of_analysis",
        parameters=[
            LookMLFieldParameter.label("Count Units"),
            LookMLFieldParameter.description(
                "Counts distinct units of analysis and included attributes"
            ),
            LookMLFieldParameter.type(LookMLFieldType.NUMBER),
            LookMLFieldParameter.view_label("Metric Menu"),
            LookMLFieldParameter.sql("COUNT(CONCAT(${assignment_unit_of_analysis}))"),
        ],
    )

    # Generate a dimension and filter field for every inputted filter field name
    json_field_filter_lookml_fields: List[LookMLViewField] = []
    if json_field_filters_with_suggestions:
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

    query_template = f"""
WITH {_generate_all_assignments_by_time_period_ctes(lookml_views_package_name, all_unit_of_observation_types)},
output_row_keys AS (
{output_row_keys_unioned_query_fragment}
)
{all_metric_ctes_query_fragment}
,
json_field_filter_keys AS (
    {all_json_field_key_cte}
)
SELECT
    output_row_keys.*,
{fix_indent(json_field_filters_with_liquid_wrap, indent_level=4)}
{fix_indent(final_select_metrics_query_fragment, indent_level=4)}
FROM
    output_row_keys
CROSS JOIN
    json_field_filter_keys
{final_join_query_fragment}
"""
    metric_measures = [
        measure_for_metric(
            metric, days_in_period_source=LookMLSqlReferenceType.DIMENSION
        )
        for metric in metrics
    ]
    extended_views: list[str] = []
    for unit_of_observation_type in all_unit_of_observation_types:
        for metric_class in METRIC_CLASSES:
            extended_views = extended_views + [
                _get_assignments_by_time_period_cte_name(
                    view_id=lookml_views_package_name,
                    join_type=metric_class.metric_time_period_to_assignment_join_type(),
                    unit_of_observation_type=unit_of_observation_type,
                ),
            ]

    return LookMLView(
        view_name=lookml_views_package_name,
        fields=[
            *metric_measures,
            measure_type_parameter,
            metric_filter_parameter,
            metric_value_measure,
            count_units_measure,
            *json_field_filter_lookml_fields,
        ],
        table=LookMLViewSourceTable.derived_table(query_template),
        included_paths=[
            f"/__generated__/views/aggregated_metrics/{lookml_views_package_name}/subqueries/*",
        ],
        extended_views=sorted(set(extended_views)),
    )
