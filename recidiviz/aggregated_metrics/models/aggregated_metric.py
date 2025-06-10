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
"""Creates AggregatedMetric objects with properties of spans/events required to
calculate a metric
"""
import abc
import re
from typing import Generic, List, Optional

import attr

from recidiviz.aggregated_metrics.assignments_by_time_period_view_builder import (
    MetricTimePeriodToAssignmentJoinType,
)
from recidiviz.calculator.query.bq_utils import nonnull_current_date_exclusive_clause
from recidiviz.common import attr_validators
from recidiviz.observations.event_selector import EventSelector
from recidiviz.observations.event_type import EventType
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.observation_selector import ObservationSelector
from recidiviz.observations.observation_type_utils import (
    ObservationTypeT,
    observation_attribute_value_clause,
)
from recidiviz.observations.span_selector import SpanSelector
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.string_formatting import fix_indent


@attr.define(frozen=True, kw_only=True)
class AggregatedMetric(Generic[ObservationTypeT]):
    """Class that stores information about an aggregated metric."""

    # The name of the metric as found in GBQ tables (must be lowercase and without spaces)
    name: str = attr.field(validator=attr_validators.is_non_empty_str)

    # A human-readable display name for the metric, for use in Looker and other surfaces.
    display_name: str = attr.field(validator=attr_validators.is_non_empty_str)

    # A description of what this metric computes, which can be displayed in various surfaces, such as Looker or Gitbook.
    description: str = attr.field(validator=attr_validators.is_non_empty_str)

    @abc.abstractmethod
    def referenced_observation_attributes(self) -> list[str]:
        """Returns the list of event/span attributes referenced by this metric."""

    @abc.abstractmethod
    def generate_aggregate_time_periods_query_fragment(self) -> str:
        """
        Returns a query fragment used to aggregate a metric over multiple time periods,
        e.g. from monthly to quarterly and yearly granularity
        """

    @classmethod
    def pretty_name(cls) -> str:
        short_cls_name = cls.__mro__[1].__name__.replace("Aggregated", "")
        # Solution taken from here: https://stackoverflow.com/questions/199059/a-pythonic-way-to-insert-a-space-before-capital-letters
        return re.sub(r"(\w)([A-Z])", r"\1 \2", short_cls_name)

    @classmethod
    @abc.abstractmethod
    def metric_time_period_to_assignment_join_type(
        cls,
    ) -> MetricTimePeriodToAssignmentJoinType:
        """Returns the type of join logic we'll use to associate metric time periods
        with unit of observation to unit of assignment periods.
        """

    @classmethod
    @abc.abstractmethod
    def metric_class_name_lower(cls) -> str:
        """Subclasses should return a lowercase name that can be used in view_ids that
        represents the metric class for this metric.
        """

    @property
    @abc.abstractmethod
    def observation_selector(self) -> ObservationSelector[ObservationTypeT]:
        """Returns the ObservationSelector that should be used to select the
        observations referenced by this aggregated metric.
        """

    @property
    def unit_of_observation(self) -> MetricUnitOfObservation:
        return self.observation_selector.unit_of_observation

    @property
    def observation_type(self) -> ObservationTypeT:
        return self.observation_selector.observation_type

    @property
    def unit_of_observation_type(self) -> MetricUnitOfObservationType:
        return self.unit_of_observation.type

    def get_observation_conditions_string(
        self,
        filter_by_observation_type: bool,
        read_observation_attributes_from_json: bool,
    ) -> str:
        """Returns a query fragment that filters a rows that contain observation data
        based on configured observation conditions for this metric.
        """
        fragment = (
            self.observation_selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=filter_by_observation_type,
                read_attributes_from_json=read_observation_attributes_from_json,
                strip_newlines=False,
            )
        )
        return f"({fragment})"

    def get_observation_conditions_string_no_newline(
        self,
        filter_by_observation_type: bool,
        read_observation_attributes_from_json: bool,
    ) -> str:
        """Returns a query fragment that filters a rows that contain observation data
        based on configured observation conditions for this metric. All newlines are
        stripped from the condition string so this can be used in places where we want
        more succinct output.
        """
        fragment = (
            self.observation_selector.generate_observation_conditions_query_fragment(
                filter_by_observation_type=filter_by_observation_type,
                read_attributes_from_json=read_observation_attributes_from_json,
                strip_newlines=True,
            )
        )
        return f"({fragment})"


@attr.define(frozen=True, kw_only=True, slots=False)
class SpanMetricConditionsMixin:
    """Attributes and functions to derive query snippets applied to spans"""

    # The SpanSelector specifying the spans to include in this metric
    span_selector: SpanSelector

    @property
    def span_type(self) -> SpanType:
        return self.span_selector.span_type


@attr.define(frozen=True, kw_only=True, slots=False)
class EventMetricConditionsMixin:
    """Attributes and functions to derive query snippets applied to events"""

    # The EventSelector specifying the events to include in this metric
    event_selector: EventSelector

    @property
    def observation_selector(self) -> EventSelector:
        return self.event_selector

    @property
    def event_type(self) -> EventType:
        return self.event_selector.event_type


@attr.define(frozen=True, kw_only=True)
class PeriodSpanAggregatedMetric(AggregatedMetric[SpanType], SpanMetricConditionsMixin):
    """
    Class that stores information about metrics that involve spans and calculate
    aggregations across an entire analysis period.
    """

    @classmethod
    def metric_class_name_lower(cls) -> str:
        return "period_span"

    @classmethod
    def metric_time_period_to_assignment_join_type(
        cls,
    ) -> MetricTimePeriodToAssignmentJoinType:
        return MetricTimePeriodToAssignmentJoinType.INTERSECTION

    @property
    def observation_selector(self) -> SpanSelector:
        return self.span_selector

    @abc.abstractmethod
    def generate_aggregation_query_fragment(
        self,
        *,
        observations_by_assignments_cte_name: str,
        span_start_date_col: str,
        span_end_date_col: str,
        period_start_date_col: str,
        period_end_date_col: str,
        original_span_start_date: str,
    ) -> str:
        """Returns a query fragment that calculates an aggregation corresponding to the PeriodSpan metric type."""

    def referenced_observation_attributes(self) -> list[str]:
        return list(self.span_selector.span_conditions_dict.keys())


@attr.define(frozen=True, kw_only=True)
class AssignmentSpanAggregatedMetric(
    AggregatedMetric[SpanType], SpanMetricConditionsMixin
):
    """
    Class that stores information about metrics that involve spans and calculate
    aggregations over some window following assignment, for all assignments during an analysis period.
    """

    @classmethod
    def metric_class_name_lower(cls) -> str:
        return "assignment_span"

    @classmethod
    def metric_time_period_to_assignment_join_type(
        cls,
    ) -> MetricTimePeriodToAssignmentJoinType:
        return MetricTimePeriodToAssignmentJoinType.ASSIGNMENT

    # Length (in days) of the window following assignment date over which to calculate metric
    window_length_days: int = 365

    @property
    def observation_selector(self) -> SpanSelector:
        return self.span_selector

    @abc.abstractmethod
    def generate_aggregation_query_fragment(
        self,
        span_start_date_col: str,
        span_end_date_col: str,
        assignment_date_col: str,
    ) -> str:
        """Returns a query fragment that calculates an aggregation corresponding to the
        AssignmentSpan metric type.
        """

    def referenced_observation_attributes(self) -> list[str]:
        return list(self.span_selector.span_conditions_dict.keys())


@attr.define(frozen=True, kw_only=True)
class PeriodEventAggregatedMetric(
    AggregatedMetric[EventType], EventMetricConditionsMixin
):
    """
    Class that stores information about metrics that involve `events` and calculate
    aggregations across an entire analysis period.
    """

    @classmethod
    def metric_class_name_lower(cls) -> str:
        return "period_event"

    @classmethod
    def metric_time_period_to_assignment_join_type(
        cls,
    ) -> MetricTimePeriodToAssignmentJoinType:
        return MetricTimePeriodToAssignmentJoinType.INTERSECTION_EVENT_ATTRIBUTION

    @property
    def observation_selector(self) -> EventSelector:
        return self.event_selector

    @abc.abstractmethod
    def generate_aggregation_query_fragment(
        self,
        *,
        observations_by_assignments_cte_name: str,
        event_date_col: str,
    ) -> str:
        """Returns a query fragment that calculates an aggregation corresponding to the PeriodEvent metric type."""

    def referenced_observation_attributes(self) -> list[str]:
        return list(self.event_selector.event_conditions_dict.keys())


@attr.define(frozen=True, kw_only=True)
class AssignmentEventAggregatedMetric(
    AggregatedMetric[EventType], EventMetricConditionsMixin
):
    """
    Class that stores information about metrics that involve `events` and calculate
    aggregations over some window following assignment, for all assignments during an analysis period.
    """

    @classmethod
    def metric_class_name_lower(cls) -> str:
        return "assignment_event"

    @classmethod
    def metric_time_period_to_assignment_join_type(
        cls,
    ) -> MetricTimePeriodToAssignmentJoinType:
        return MetricTimePeriodToAssignmentJoinType.ASSIGNMENT

    # Length (in days) of the window following assignment date over which to calculate metric
    window_length_days: int = 365

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"

    @property
    def observation_selector(self) -> EventSelector:
        return self.event_selector

    @abc.abstractmethod
    def generate_aggregation_query_fragment(
        self,
        observations_by_assignments_cte_name: str,
        event_date_col: str,
        assignment_date_col: str,
    ) -> str:
        """Returns a query fragment that calculates an aggregation corresponding to the
        AssignmentEvent metric type.
        """

    def referenced_observation_attributes(self) -> list[str]:
        return list(self.event_selector.event_conditions_dict.keys())


@attr.define(frozen=True, kw_only=True)
class DailyAvgSpanCountMetric(PeriodSpanAggregatedMetric):
    """
    Class that stores information about a metric that calculates average daily population
    for a specified set of span rows. All end_date_cols should be end date exclusive.

    Example metric: Average daily female population.
    """

    def generate_aggregation_query_fragment(
        self,
        *,
        observations_by_assignments_cte_name: str,
        span_start_date_col: str,
        span_end_date_col: str,
        period_start_date_col: str,
        period_end_date_col: str,
        original_span_start_date: Optional[str] = None,
    ) -> str:
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        return f"""
            SUM(
            (
                DATE_DIFF(
                    LEAST({period_end_date_col}, {nonnull_current_date_exclusive_clause(span_end_date_col)}),
                    GREATEST({period_start_date_col}, {span_start_date_col}),
                    DAY)
                ) * (IF({observation_conditions}, 1, 0))
            ) / DATE_DIFF(
                    {period_end_date_col},
                    {period_start_date_col},
                    DAY
            ) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return (
            f"SUM(DATE_DIFF(end_date, start_date, DAY) * {self.name}) /\n\t"
            f"SUM(DATE_DIFF(end_date, start_date, DAY)) AS {self.name}"
        )


@attr.define(frozen=True, kw_only=True)
class DailyAvgSpanValueMetric(PeriodSpanAggregatedMetric):
    """
    Class that stores information about a metric that calculates average daily value
    for a specified set of span rows intersecting with the analysis period.
    All end_date_cols should be end date exclusive.

    Example: Average daily LSI-R score.
    """

    # Name of the field in span_attributes JSON containing the numeric attribute of the span.
    span_value_numeric: str = attr.field(validator=attr_validators.is_non_empty_str)

    def referenced_observation_attributes(self) -> list[str]:
        return super().referenced_observation_attributes() + [self.span_value_numeric]

    def generate_aggregation_query_fragment(
        self,
        *,
        observations_by_assignments_cte_name: str,
        span_start_date_col: str,
        span_end_date_col: str,
        period_start_date_col: str,
        period_end_date_col: str,
        original_span_start_date: Optional[str] = None,
    ) -> str:
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        span_value_numeric_clause = observation_attribute_value_clause(
            observation_type=self.observation_selector.observation_type,
            attribute=self.span_value_numeric,
            read_attributes_from_json=False,
        )
        return f"""
            SAFE_DIVIDE(
                SUM(
                    DATE_DIFF(
                        LEAST({period_end_date_col}, {nonnull_current_date_exclusive_clause(span_end_date_col)}),
                        GREATEST({period_start_date_col}, {span_start_date_col}),
                        DAY
                    ) * IF(
                        {observation_conditions},
                        CAST({span_value_numeric_clause} AS FLOAT64),
                        0
                    )
                ),
                SUM(
                    DATE_DIFF(
                        LEAST({period_end_date_col}, {nonnull_current_date_exclusive_clause(span_end_date_col)}),
                        GREATEST({period_start_date_col}, {span_start_date_col}),
                        DAY
                    ) * IF({observation_conditions}, 1, 0)
                )
            ) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return (
            f"SUM(avg_daily_population * DATE_DIFF(end_date, start_date, DAY) * {self.name}) /\n\t"
            f"SUM(avg_daily_population * DATE_DIFF(end_date, start_date, DAY)) AS {self.name}"
        )


@attr.define(frozen=True, kw_only=True)
class DailyAvgTimeSinceSpanStartMetric(PeriodSpanAggregatedMetric):
    """
    Class that stores information about a metric that calculates the average days since the start of the span,
    for the daily population over a specified set of span rows.
    All end_date_cols should be end date exclusive.

    Example metrics: Average age.
    """

    # Indicates whether to scale metric by 365.25 to provide year values instead of day values
    scale_to_year: bool = False

    def generate_aggregation_query_fragment(
        self,
        *,
        observations_by_assignments_cte_name: str,
        span_start_date_col: str,
        span_end_date_col: str,
        period_start_date_col: str,
        period_end_date_col: str,
        original_span_start_date: str,
    ) -> str:
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        return f"""
            SAFE_DIVIDE(
                SUM(
                    DATE_DIFF(
                        LEAST({period_end_date_col}, {nonnull_current_date_exclusive_clause(span_end_date_col)}),
                        GREATEST({period_start_date_col}, {span_start_date_col}),
                        DAY
                    ) * IF(
                        {observation_conditions},
                        (
                            # Average of LoS on last day (inclusive) of period/span and LoS on first day of period/span
                            (DATE_DIFF(
                                DATE_SUB(LEAST({period_end_date_col}, {nonnull_current_date_exclusive_clause(span_end_date_col)}), INTERVAL 1 DAY),
                                {original_span_start_date}, DAY
                            ) + DATE_DIFF(
                                GREATEST({period_start_date_col}, {span_start_date_col}),
                                {original_span_start_date}, DAY
                            )
                        ) / 2) {"/ 365.25" if self.scale_to_year else ""},
                        NULL
                    )
                ),
                SUM(
                    DATE_DIFF(
                        LEAST({period_end_date_col}, {nonnull_current_date_exclusive_clause(span_end_date_col)}),
                        GREATEST({period_start_date_col}, {span_start_date_col}),
                        DAY
                    ) * IF({observation_conditions}, 1, 0)
                )
            ) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return (
            f"SUM(avg_daily_population * DATE_DIFF(end_date, start_date, DAY) * {self.name}) /\n\t"
            f"SUM(avg_daily_population * DATE_DIFF(end_date, start_date, DAY)) AS {self.name}"
        )


@attr.define(frozen=True, kw_only=True)
class SumSpanDaysMetric(PeriodSpanAggregatedMetric):
    """
    Class that stores information about a metric that calculates the average days spent in span
    for the daily population over a specified set of span rows over the analysis period.
    All end_date_cols should be end date exclusive.

    Example metrics: Person days eligible for early discharge opportunity.
    """

    # optional column by which to weight person-days, e.g. for
    # person_days_weighted_justice_impact
    weight_col: Optional[str] = None

    def referenced_observation_attributes(self) -> list[str]:
        return super().referenced_observation_attributes() + (
            [self.weight_col] if self.weight_col else []
        )

    def generate_aggregation_query_fragment(
        self,
        *,
        observations_by_assignments_cte_name: str,
        span_start_date_col: str,
        span_end_date_col: str,
        period_start_date_col: str,
        period_end_date_col: str,
        original_span_start_date: Optional[str] = None,
    ) -> str:
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )

        if self.weight_col:
            weight_col_clause = observation_attribute_value_clause(
                observation_type=self.observation_selector.observation_type,
                attribute=self.weight_col,
                read_attributes_from_json=False,
            )
            weight_snippet = f"CAST({weight_col_clause} AS FLOAT64) * "
        else:
            weight_snippet = ""

        return f"""
            SUM(
            (
                {weight_snippet}DATE_DIFF(
                    LEAST({period_end_date_col}, {nonnull_current_date_exclusive_clause(span_end_date_col)}),
                    GREATEST({period_start_date_col}, {span_start_date_col}),
                    DAY)
                ) * (IF({observation_conditions}, 1, 0))
            ) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class SpanDistinctUnitCountMetric(PeriodSpanAggregatedMetric):
    """
    Class that stores information about a metric that counts the distinct
    number of unit of observations among the observed spans.

    Example metric: total registered users
    """

    def generate_aggregation_query_fragment(
        self,
        *,
        observations_by_assignments_cte_name: str,
        span_start_date_col: str,
        span_end_date_col: str,
        period_start_date_col: str,
        period_end_date_col: str,
        original_span_start_date: str,
    ) -> str:
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        return f"""
            COUNT(DISTINCT IF(
                {observation_conditions},
                CONCAT({self.unit_of_observation.get_primary_key_columns_query_string(prefix=observations_by_assignments_cte_name)}),
                NULL
            )) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"AVG({self.name}) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class AssignmentSpanDaysMetric(AssignmentSpanAggregatedMetric):
    """
    Class that stores information about a metric that counts total length in days of intersection
    between span and {window_length_days} window following assignment date
    (includes when person has left the eligible population).

    Example metric: Days incarcerated within 365 days of assignment.
    """

    def generate_aggregation_query_fragment(
        self,
        span_start_date_col: str,
        span_end_date_col: str,
        assignment_date_col: str,
    ) -> str:
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        return f"""
            SUM(
                IF({observation_conditions}, DATE_DIFF(
                    LEAST(
                        DATE_ADD({assignment_date_col}, INTERVAL {self.window_length_days} DAY),
                        {nonnull_current_date_exclusive_clause(span_end_date_col)}
                    ),
                    GREATEST(
                        {assignment_date_col},
                        IF({span_start_date_col} <= DATE_ADD({assignment_date_col}, INTERVAL {self.window_length_days} DAY), {span_start_date_col}, NULL)
                    ),
                    DAY
                ), 0)
            ) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class AssignmentSpanMaxDaysMetric(AssignmentSpanAggregatedMetric):
    """
    Class that stores information about a metric that takes the longest contiguous intersection
    between span and {window_length_days} window following assignment date
    (includes when person has left the eligible population).

    Example metric: Maximum days with consistent employer within 365 days of assignment.
    """

    def _days_span_overlaps_with_post_assignment_window_clause(
        self,
        span_start_date_col: str,
        span_end_date_col: str,
        assignment_date_col: str,
    ) -> str:
        """Returns a SQL clause that will calculate the number of days that a span
        observation overlaps with this metric's post-assignment time window (e.g. days
        overlapping with the 365 days after assignment).
        """
        window_end_exclusive_clause = (
            f"DATE_ADD({assignment_date_col}, INTERVAL {self.window_length_days} DAY)"
        )
        span_observation_end_clause = nonnull_current_date_exclusive_clause(
            span_end_date_col
        )
        return fix_indent(
            f"""
            GREATEST (
                DATE_DIFF(
                    LEAST(
                        {window_end_exclusive_clause},
                        {span_observation_end_clause}
                    ),
                    GREATEST({assignment_date_col}, {span_start_date_col}),
                    DAY
                ),
                0
            )
            """,
            indent_level=0,
        )

    @property
    def _is_max_days_overlap_in_window_col_name(self) -> str:
        return f"is_max_days_overlap_in_window__{self.name}"

    def generate_is_max_days_overlap_in_window_clause(
        self,
        span_start_date_col: str,
        span_end_date_col: str,
        assignment_date_col: str,
        qualified_assignment_cols: list[str],
    ) -> str:
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        assignment_cols_str = ",\n                    ".join(qualified_assignment_cols)
        days_overlap_clause = (
            self._days_span_overlaps_with_post_assignment_window_clause(
                span_start_date_col=span_start_date_col,
                span_end_date_col=span_end_date_col,
                assignment_date_col=assignment_date_col,
            )
        )
        return fix_indent(
            f"""
            ROW_NUMBER() OVER (
                PARTITION BY 
                    {assignment_cols_str},
                    -- Partition by observations filter so we only order among relevant
                    -- events.
                    {observation_conditions}
                ORDER BY\n{fix_indent(days_overlap_clause, indent_level=20)} DESC
            ) = 1 AS {self._is_max_days_overlap_in_window_col_name}
            """,
            indent_level=0,
        )

    def generate_aggregation_query_fragment(
        self,
        span_start_date_col: str,
        span_end_date_col: str,
        assignment_date_col: str,
    ) -> str:
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        days_overlap_clause = (
            self._days_span_overlaps_with_post_assignment_window_clause(
                span_start_date_col=span_start_date_col,
                span_end_date_col=span_end_date_col,
                assignment_date_col=assignment_date_col,
            )
        )
        return f"""
        SUM(
            IF(
                {observation_conditions} AND {self._is_max_days_overlap_in_window_col_name},\n{fix_indent(days_overlap_clause, indent_level=16)}, 
                0
            )
        ) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class AssignmentSpanValueAtStartMetric(AssignmentSpanAggregatedMetric):
    """
    Class that stores information about a metric that calculates average value
    for a specified set of span rows intersecting with the assignment date

    Example metric: Average LSI-R score at assignment
    """

    # Name of the field in span_attributes JSON containing the numeric attribute of the span.
    span_value_numeric: str = attr.field(validator=attr_validators.is_str)

    # Metric counting the number of assignments satisfying the span condition
    span_count_metric: AssignmentSpanDaysMetric

    def referenced_observation_attributes(self) -> list[str]:
        return super().referenced_observation_attributes() + [self.span_value_numeric]

    def generate_aggregation_query_fragment(
        self,
        span_start_date_col: str,
        span_end_date_col: str,
        assignment_date_col: str,
    ) -> str:
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        span_value_numeric_clause = observation_attribute_value_clause(
            observation_type=self.observation_selector.observation_type,
            attribute=self.span_value_numeric,
            read_attributes_from_json=False,
        )
        return f"""
            AVG(
                IF(
                    {observation_conditions}
                    AND {assignment_date_col} BETWEEN {span_start_date_col} AND {nonnull_current_date_exclusive_clause(span_end_date_col)},
                    CAST({span_value_numeric_clause} AS FLOAT64),
                    NULL
                )
            ) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SAFE_DIVIDE(SUM({self.span_count_metric.name} * {self.name}), SUM({self.span_count_metric.name})) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class AssignmentCountMetric(AssignmentSpanAggregatedMetric):
    """
    Class used specifically for calculating number of assignments in a period.

    This is used only for the metric "Assignments".
    """

    def generate_aggregation_query_fragment(
        self,
        span_start_date_col: str,
        span_end_date_col: str,
        assignment_date_col: str,
    ) -> str:
        return f"COUNT(*) AS {self.name}"

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class EventCountMetric(PeriodEventAggregatedMetric):
    """
    Class that stores information about a metric that counts the number of events
    for a specified set of event rows occurring during the analysis period.
    Events are deduplicated to one person-event per day.

    Example metric: Number of technical violations.
    """

    # When two (or more) event rows for the same unit of observation (e.g. the same person) are present on the
    # same day, we will count two (ore more) distinct events when the values in these columns are different.
    # Otherwise, we treat those rows as the same event.
    event_segmentation_columns: Optional[List[str]] = None

    def referenced_observation_attributes(self) -> list[str]:
        return super().referenced_observation_attributes() + (
            self.event_segmentation_columns or []
        )

    def generate_aggregation_query_fragment(
        self,
        *,
        observations_by_assignments_cte_name: str,
        event_date_col: str,
    ) -> str:

        # If `event_segmentation_columns` are provided, add to the set of fields used to calculate the
        # COUNT DISTINCT.
        event_segmentation_columns = []
        if self.event_segmentation_columns:
            event_segmentation_columns = self.event_segmentation_columns

        event_segmentation_columns_json = [
            observation_attribute_value_clause(
                observation_type=self.observation_selector.observation_type,
                attribute=col,
                read_attributes_from_json=False,
            )
            for col in event_segmentation_columns
        ]
        event_segmentation_columns_str = (
            ",\n                    " + ", ".join(event_segmentation_columns_json)
            if len(event_segmentation_columns_json) > 0
            else ""
        )
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        return f"""
            COUNT(DISTINCT IF(
                {observation_conditions},
                CONCAT(
                    {self.unit_of_observation.get_primary_key_columns_query_string(prefix=observations_by_assignments_cte_name)}, 
                    {event_date_col}{event_segmentation_columns_str}
                ), NULL
            )) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class EventValueMetric(PeriodEventAggregatedMetric):
    """
    Class that stores information about a metric that takes the average value over events
    for a specified set of event rows occurring during the analysis period.

    Example metric: Average LSI-R score across all assessments.
    """

    # Name of the field in event_attributes JSON containing the numeric attribute of the event.
    event_value_numeric: str

    # EventCount metric counting the number of events contributing to the event value metric
    event_count_metric: EventCountMetric

    def referenced_observation_attributes(self) -> list[str]:
        return super().referenced_observation_attributes() + [self.event_value_numeric]

    def generate_aggregation_query_fragment(
        self,
        *,
        observations_by_assignments_cte_name: str,
        event_date_col: str,
    ) -> str:
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        event_value_numeric_clause = observation_attribute_value_clause(
            observation_type=self.observation_selector.observation_type,
            attribute=self.event_value_numeric,
            read_attributes_from_json=False,
        )
        return f"""
            AVG(IF(
                {observation_conditions},
                CAST({event_value_numeric_clause} AS FLOAT64),
                NULL
            )) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SAFE_DIVIDE(SUM({self.event_count_metric.name} * {self.name}), SUM({self.event_count_metric.name})) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class EventDistinctUnitCountMetric(PeriodEventAggregatedMetric):
    """
    Class that stores information about a metric that counts the distinct
    number of unit of observations among the observed events.

    Example metric: distinct active users.
    """

    def generate_aggregation_query_fragment(
        self,
        *,
        observations_by_assignments_cte_name: str,
        event_date_col: str,
    ) -> str:
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        return f"""
            COUNT(DISTINCT IF(
                {observation_conditions},
                CONCAT({self.unit_of_observation.get_primary_key_columns_query_string(prefix=observations_by_assignments_cte_name)}),
                NULL
            )) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"AVG({self.name}) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class AssignmentDaysToFirstEventMetric(AssignmentEventAggregatedMetric):
    """
    Class that stores information about a metric that calculates the number of days from
    assignment to the first instance of the event specified in `events` occurring within
    {window_length_days} of assignment, for all assignments occurring during the analysis period.

    Example metric: Days to first absconsion within 365 days of assignment.
    """

    @property
    def _event_seq_num_col_name(self) -> str:
        return f"event_seq_num__{self.name}"

    def generate_event_seq_num_col_clause(
        self, event_date_col: str, qualified_assignment_cols: list[str]
    ) -> str:
        """Returns a column clause that, for each event associated with the metric
        period x assignment period pair, assigns a sequence number to that event. This
        will allow us to select the first occurrence of an event after assignment.
        """
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        assignment_cols_str = ",\n                    ".join(qualified_assignment_cols)
        return fix_indent(
            f"""
            ROW_NUMBER() OVER (
                PARTITION BY 
                    {assignment_cols_str},
                    -- Partition by observations filter so we only order among relevant
                    -- events.
                    event_date IS NOT NULL AND {observation_conditions}
                ORDER BY
                    {event_date_col}
            ) AS {self._event_seq_num_col_name}
            """,
            indent_level=0,
        )

    @property
    def _num_matching_events_col_name(self) -> str:
        return f"num_matching_events__{self.name}"

    def generate_num_matching_events_clause(
        self, qualified_assignment_cols: list[str]
    ) -> str:
        """Returns a column clause that returns the number of events associated with
        the metric period x assignment period pair that would qualify a valid event
        for this metric.
        """
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        assignment_cols_str = ",\n                    ".join(qualified_assignment_cols)
        return fix_indent(
            f"""
            COUNTIF(event_date IS NOT NULL AND {observation_conditions}) OVER (
                PARTITION BY 
                    {assignment_cols_str}
            ) AS {self._num_matching_events_col_name}
            """,
            indent_level=0,
        )

    def generate_aggregation_query_fragment(
        self,
        observations_by_assignments_cte_name: str,
        event_date_col: str,
        assignment_date_col: str,
    ) -> str:
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        return f"""
        SUM(
            IF(
                {observations_by_assignments_cte_name}.{self._num_matching_events_col_name} = 0 AND {observations_by_assignments_cte_name}.{self._event_seq_num_col_name} = 1,
                -- There were no events associated with this assignment - return full 
                -- window length.
                {self.window_length_days},
                -- Otherwise, if this is a valid first event, get the time since 
                -- assignment or window length, whichever is less
                IF(
                    {observations_by_assignments_cte_name}.{self._event_seq_num_col_name} = 1 AND {observation_conditions},
                    DATE_DIFF(
                        LEAST(
                            {event_date_col}, 
                            DATE_ADD({assignment_date_col}, INTERVAL {self.window_length_days} DAY)
                        ),
                        {assignment_date_col},
                        DAY
                    ),
                    0
                )
            )
        ) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class AssignmentEventCountMetric(AssignmentEventAggregatedMetric):
    """
    Class that stores information about a metric that counts the number of events
    specified in `events` occurring within {window_length_days} of assignment,
    for all assignments occurring during the analysis period.

    Example metric: Number of contacts within 30 days of assignment.
    """

    def generate_aggregation_query_fragment(
        self,
        observations_by_assignments_cte_name: str,
        event_date_col: str,
        assignment_date_col: str,
    ) -> str:
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        return f"""
            COUNT(
                IF(
                    {observation_conditions} AND {event_date_col} <= DATE_ADD({assignment_date_col}, INTERVAL {self.window_length_days} DAY),
                    CONCAT({self.unit_of_observation.get_primary_key_columns_query_string(prefix=observations_by_assignments_cte_name)}, "#", {assignment_date_col}),
                    NULL
                )
            ) AS {self.name}"""

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class AssignmentEventBinaryMetric(AssignmentEventAggregatedMetric):
    """
    Class that stores information about a metric that counts one event per person
    specified in `events` occurring within {window_length_days} of assignment,
    for all assignments occurring during the analysis period.

    Example metric: Any Incarceration Start Within 1 Year of Assignment
    """

    def generate_aggregation_query_fragment(
        self,
        observations_by_assignments_cte_name: str,
        event_date_col: str,
        assignment_date_col: str,
    ) -> str:
        observation_conditions = self.get_observation_conditions_string(
            filter_by_observation_type=False,
            read_observation_attributes_from_json=False,
        )
        return f"""
            COUNT(
                DISTINCT IF(
                    {observation_conditions} AND {event_date_col} <= DATE_ADD({assignment_date_col}, INTERVAL {self.window_length_days} DAY),
                    CONCAT({self.unit_of_observation.get_primary_key_columns_query_string(prefix=observations_by_assignments_cte_name)}, "#", {assignment_date_col}),
                    NULL
                )
            ) AS {self.name}"""

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"
