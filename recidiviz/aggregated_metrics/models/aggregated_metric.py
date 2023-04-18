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
"""Creates AggregatedMetric objects with properties of spans/events required to calculate a metric"""
import abc
import re
from typing import Dict, List, Optional, Sequence, Union

import attr

from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_current_date_exclusive_clause,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.common import attr_validators


@attr.define(frozen=True, kw_only=True)
class AggregatedMetric:
    """Class that stores information about an aggregated metric."""

    # The name of the metric as found in GBQ tables (must be lowercase and without spaces)
    name: str = attr.field(validator=attr_validators.is_non_empty_str)

    # A human-readable display name for the metric, for use in Looker and other surfaces.
    display_name: str = attr.field(validator=attr_validators.is_non_empty_str)

    # A description of what this metric computes, which can be displayed in various surfaces, such as Looker or Gitbook.
    description: str = attr.field(validator=attr_validators.is_non_empty_str)

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


@attr.define(frozen=True, kw_only=True)
class MiscAggregatedMetric(AggregatedMetric):
    """
    Class that stores information about metrics that are calculated in a separate user-defined query
    for specific populations and aggregation levels, without using person_events or person_spans logic
    """

    # Populations compatible with metric
    populations: List[MetricPopulationType]

    # Aggregation levels compatible with metric
    aggregation_levels: List[MetricUnitOfAnalysisType]

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"ARRAY_AGG({self.name}) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class PeriodSpanAggregatedMetric(AggregatedMetric):
    """
    Class that stores information about metrics that involve `person_spans` and calculate
    aggregations across an entire analysis period.
    """

    @abc.abstractmethod
    def generate_aggregation_query_fragment(
        self,
        span_start_date_col: str,
        span_end_date_col: str,
        period_start_date_col: str,
        period_end_date_col: str,
        original_span_start_date: str,
    ) -> str:
        """Returns a query fragment that calculates an aggregation corresponding to the PeriodSpan metric type."""


@attr.define(frozen=True, kw_only=True)
class AssignmentSpanAggregatedMetric(AggregatedMetric):
    """
    Class that stores information about metrics that involve `person_spans` and calculate
    aggregations over some window following assignment, for all assignments during an analysis period.
    """

    # Length (in days) of the window following assignment date over which to calculate metric
    window_length_days: int = 365

    @abc.abstractmethod
    def generate_aggregation_query_fragment(
        self, span_start_date_col: str, span_end_date_col: str, assignment_date_col: str
    ) -> str:
        """Returns a query fragment that calculates an aggregation corresponding to the AssignmentSpan metric type."""


@attr.define(frozen=True, kw_only=True)
class PeriodEventAggregatedMetric(AggregatedMetric):
    """
    Class that stores information about metrics that involve `person_events` and calculate
    aggregations across an entire analysis period.
    """

    @abc.abstractmethod
    def generate_aggregation_query_fragment(self, event_date_col: str) -> str:
        """Returns a query fragment that calculates an aggregation corresponding to the PeriodEvent metric type."""


@attr.define(frozen=True, kw_only=True)
class AssignmentEventAggregatedMetric(AggregatedMetric):
    """
    Class that stores information about metrics that involve `person_events` and calculate
    aggregations over some window following assignment, for all assignments during an analysis period.
    """

    # Length (in days) of the window following assignment date over which to calculate metric
    window_length_days: int = 365

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"

    @abc.abstractmethod
    def generate_aggregation_query_fragment(
        self, event_date_col: str, assignment_date_col: str
    ) -> str:
        """Returns a query fragment that calculates an aggregation corresponding to the AssignmentEvent metric type."""


class MetricConditionsMixin:
    """Attributes and functions to derive query snippets for defining a metric"""

    def get_metric_conditions_string(self) -> str:
        """Returns a query fragment string that joins SQL conditional statements with `AND`."""
        return "\n\t\t\t\tAND\n".join(self.get_metric_conditions())

    def get_metric_conditions_string_no_newline(self) -> str:
        """
        Returns a query fragment string that joins SQL conditional statements with `AND` without line breaks
        or extra spaces, for more succinct print output.
        """
        return re.sub(r" +|\n+", " ", " AND ".join(self.get_metric_conditions()))

    @abc.abstractmethod
    def get_metric_conditions(self) -> List[str]:
        """Returns a list of conditional query fragments filtering person_spans or person_events."""


@attr.define(frozen=True, kw_only=True, slots=False)
class SpanMetricConditionsMixin(MetricConditionsMixin):
    """Attributes and functions to derive query snippets applied to `person_spans`"""

    # The list of strings corresponding with the `span` field in `person_spans` specifying the type of span.
    # TODO(#16785): Replace List[str] with List[SpanType] or List[EventType], and add tests for attributes
    span_types: List[str] = attr.field(validator=attr_validators.is_list)

    # A dictionary mapping fields from the JSON object `span_attributes` in `person_spans` to either
    # a list of accepted values or a query string for a custom condition.
    span_attribute_filters: Dict[str, Union[Sequence[str], str]] = attr.field(
        validator=attr_validators.is_dict
    )

    def get_metric_conditions(self) -> List[str]:
        condition_strings = [
            f"span IN ({list_to_query_string(self.span_types, quoted=True)})"
        ]

        for attribute in self.span_attribute_filters.keys():
            conditions = self.span_attribute_filters[attribute]
            if isinstance(conditions, str):
                attribute_condition_string = conditions
            elif isinstance(conditions, Sequence):
                attribute_condition_string = (
                    f" IN ({list_to_query_string(conditions, quoted=True)})"
                )
            else:
                raise TypeError(
                    "All values in `span_attribute_filters` dictionary must have type str or Sequence[str]"
                )
            condition_strings.append(
                f"""
                 JSON_EXTRACT_SCALAR(span_attributes, "$.{attribute}") {attribute_condition_string}
                 """
            )
        return condition_strings


@attr.define(frozen=True, kw_only=True, slots=False)
class EventMetricConditionsMixin(MetricConditionsMixin):
    """Attributes and functions to derive query snippets applied to `person_events`"""

    # The list of strings corresponding with the `event` field in `person_events` specifying the type of event.
    event_types: List[str] = attr.field(validator=attr_validators.is_list)

    # A dictionary mapping fields from the JSON object `event_attributes` in `person_events` to either
    # a list of accepted values or a query string for a custom condition.
    event_attribute_filters: Dict[str, Union[Sequence[str], str]] = attr.field(
        validator=attr_validators.is_dict
    )

    def get_metric_conditions(self) -> List[str]:
        condition_strings = [
            f"event IN ({list_to_query_string(self.event_types, quoted=True)})"
        ]
        for attribute in self.event_attribute_filters.keys():
            conditions = self.event_attribute_filters[attribute]
            if isinstance(conditions, str):
                attribute_condition_string = conditions
            elif isinstance(conditions, Sequence):
                attribute_condition_string = f"""
                        IN ({list_to_query_string(conditions, quoted=True)})
                    """
            else:
                raise TypeError(
                    "All values in `event_attribute_filters` dictionary must have type str or Sequence[str]"
                )
            condition_strings.append(
                f"""
                    JSON_EXTRACT_SCALAR(event_attributes, "$.{attribute}") {attribute_condition_string}"""
            )
        return condition_strings


@attr.define(frozen=True, kw_only=True)
class DailyAvgSpanCountMetric(PeriodSpanAggregatedMetric, SpanMetricConditionsMixin):
    """
    Class that stores information about a metric that calculates average daily population
    for a specified set of `person_span` rows. All end_date_cols should be end date exclusive.

    Example metric: Average daily female population.
    """

    def generate_aggregation_query_fragment(
        self,
        span_start_date_col: str,
        span_end_date_col: str,
        period_start_date_col: str,
        period_end_date_col: str,
        original_span_start_date: Optional[str] = None,
    ) -> str:
        return f"""
            SUM(
            (
                DATE_DIFF(
                    LEAST({period_end_date_col}, {nonnull_current_date_exclusive_clause(span_end_date_col)}),
                    GREATEST({period_start_date_col}, {span_start_date_col}),
                    DAY)
                ) * (IF({self.get_metric_conditions_string()}, 1, 0)) / DATE_DIFF({period_end_date_col}, {period_start_date_col}, DAY)
            ) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return (
            f"SUM(DATE_DIFF(end_date, start_date, DAY) * {self.name}) /\n\t"
            f"SUM(DATE_DIFF(end_date, start_date, DAY)) AS {self.name}"
        )


@attr.define(frozen=True, kw_only=True)
class DailyAvgSpanValueMetric(PeriodSpanAggregatedMetric, SpanMetricConditionsMixin):
    """
    Class that stores information about a metric that calculates average daily value
    for a specified set of `person_span` rows intersecting with the analysis period.
    All end_date_cols should be end date exclusive.

    Example: Average daily LSI-R score.
    """

    # Name of the field in span_attributes JSON containing the numeric attribute of the span.
    span_value_numeric: str = attr.field(validator=attr_validators.is_str)

    def generate_aggregation_query_fragment(
        self,
        span_start_date_col: str,
        span_end_date_col: str,
        period_start_date_col: str,
        period_end_date_col: str,
        original_span_start_date: Optional[str] = None,
    ) -> str:
        return f"""
            SAFE_DIVIDE(
                SUM(
                    DATE_DIFF(
                        LEAST({period_end_date_col}, {nonnull_current_date_exclusive_clause(span_end_date_col)}),
                        GREATEST({period_start_date_col}, {span_start_date_col}),
                        DAY
                    ) * IF(
                        {self.get_metric_conditions_string()},
                        CAST(JSON_EXTRACT_SCALAR(span_attributes, "$.{self.span_value_numeric}") AS FLOAT64),
                        0
                    )
                ),
                SUM(
                    DATE_DIFF(
                        LEAST({period_end_date_col}, {nonnull_current_date_exclusive_clause(span_end_date_col)}),
                        GREATEST({period_start_date_col}, {span_start_date_col}),
                        DAY
                    ) * IF({self.get_metric_conditions_string()}, 1, 0)
                )
            ) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return (
            f"SUM(avg_daily_population * DATE_DIFF(end_date, start_date, DAY) * {self.name}) /\n\t"
            f"SUM(avg_daily_population * DATE_DIFF(end_date, start_date, DAY)) AS {self.name}"
        )


@attr.define(frozen=True, kw_only=True)
class DailyAvgTimeSinceSpanStartMetric(
    PeriodSpanAggregatedMetric, SpanMetricConditionsMixin
):
    """
    Class that stores information about a metric that calculates the average days since the start of the span,
    for the daily population over a specified set of `person_span` rows.
    All end_date_cols should be end date exclusive.

    Example metrics: Average age.
    """

    # Indicates whether to scale metric by 365.25 to provide year values instead of day values
    scale_to_year: bool = False

    def generate_aggregation_query_fragment(
        self,
        span_start_date_col: str,
        span_end_date_col: str,
        period_start_date_col: str,
        period_end_date_col: str,
        original_span_start_date: str,
    ) -> str:
        return f"""
            SAFE_DIVIDE(
                SUM(
                    DATE_DIFF(
                        LEAST({period_end_date_col}, {nonnull_current_date_exclusive_clause(span_end_date_col)}),
                        GREATEST({period_start_date_col}, {span_start_date_col}),
                        DAY
                    ) * IF(
                        {self.get_metric_conditions_string()},
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
                    ) * IF({self.get_metric_conditions_string()}, 1, 0)
                )  
            ) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return (
            f"SUM(avg_daily_population * DATE_DIFF(end_date, start_date, DAY) * {self.name}) /\n\t"
            f"SUM(avg_daily_population * DATE_DIFF(end_date, start_date, DAY)) AS {self.name}"
        )


@attr.define(frozen=True, kw_only=True)
class SumSpanDaysMetric(PeriodSpanAggregatedMetric, SpanMetricConditionsMixin):
    """
    Class that stores information about a metric that calculates the average days spent in span
    for the daily population over a specified set of `person_span` rows over the analysis period.
    All end_date_cols should be end date exclusive.

    Example metrics: Person days eligible for early discharge opportunity.
    """

    span_types: List[str] = attr.field(validator=attr_validators.is_list)

    def generate_aggregation_query_fragment(
        self,
        span_start_date_col: str,
        span_end_date_col: str,
        period_start_date_col: str,
        period_end_date_col: str,
        original_span_start_date: Optional[str] = None,
    ) -> str:
        return f"""
            SUM(
            (
                DATE_DIFF(
                    LEAST({period_end_date_col}, {nonnull_current_date_exclusive_clause(span_end_date_col)}),
                    GREATEST({period_start_date_col}, {span_start_date_col}),
                    DAY)
                ) * (IF({self.get_metric_conditions_string()}, 1, 0))
            ) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class AssignmentSpanDaysMetric(
    AssignmentSpanAggregatedMetric, SpanMetricConditionsMixin
):
    """
    Class that stores information about a metric that counts total length in days of intersection
    between span and {window_length_days} window following assignment date
    (includes when person has left the eligible population).

    Example metric: Days incarcerated within 365 days of assignment.
    """

    def generate_aggregation_query_fragment(
        self, span_start_date_col: str, span_end_date_col: str, assignment_date_col: str
    ) -> str:
        return f"""
            SUM(
                IF({self.get_metric_conditions_string()}, DATE_DIFF(
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
class AssignmentSpanMaxDaysMetric(
    AssignmentSpanAggregatedMetric, SpanMetricConditionsMixin
):
    """
    Class that stores information about a metric that takes the longest contiguous intersection
    between span and {window_length_days} window following assignment date
    (includes when person has left the eligible population).

    Example metric: Maximum days with consistent employer within 365 days of assignment.
    """

    def generate_aggregation_query_fragment(
        self, span_start_date_col: str, span_end_date_col: str, assignment_date_col: str
    ) -> str:
        return f"""
            MAX(
                IF(
                    {self.get_metric_conditions_string()}
                    AND {span_start_date_col} <= DATE_ADD({assignment_date_col}, INTERVAL {self.window_length_days} DAY), 
                    DATE_DIFF(
                        LEAST(
                            DATE_ADD({assignment_date_col}, INTERVAL {self.window_length_days} DAY),
                            {nonnull_current_date_exclusive_clause(span_end_date_col)}
                        ),
                        GREATEST({assignment_date_col}, {span_start_date_col}),
                        DAY
                    ), 0
                )
            ) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class AssignmentSpanValueAtStartMetric(
    AssignmentSpanAggregatedMetric, SpanMetricConditionsMixin
):
    """
    Class that stores information about a metric that calculates average value
    for a specified set of `person_span` rows intersecting with the assignment date

    Example metric: Average LSI-R score at assignment
    """

    # Name of the field in span_attributes JSON containing the numeric attribute of the span.
    span_value_numeric: str

    # Metric counting the number of assignments satisfying the span condition
    span_count_metric: AssignmentSpanDaysMetric

    def generate_aggregation_query_fragment(
        self, span_start_date_col: str, span_end_date_col: str, assignment_date_col: str
    ) -> str:
        return f"""
            AVG(
                IF(
                    {self.get_metric_conditions_string()}
                    AND {assignment_date_col} BETWEEN {span_start_date_col} AND {nonnull_current_date_exclusive_clause(span_end_date_col)}, 
                    CAST(JSON_EXTRACT_SCALAR(span_attributes, "$.{self.span_value_numeric}") AS FLOAT64), 
                    NULL
                )
            ) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SAFE_DIVIDE(SUM({self.span_count_metric.name} * {self.name}), SUM({self.span_count_metric.name})) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class EventCountMetric(PeriodEventAggregatedMetric, EventMetricConditionsMixin):
    """
    Class that stores information about a metric that counts the number of events
    for a specified set of `person_event` rows occurring during the analysis period.
    Events are deduplicated to one person-event per day.

    Example metric: Number of technical violations.
    """

    def generate_aggregation_query_fragment(self, event_date_col: str) -> str:
        return f"""
            COUNT(DISTINCT IF(
                {self.get_metric_conditions_string()},
                CONCAT(events.person_id, {event_date_col}), NULL
            )) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class EventValueMetric(PeriodEventAggregatedMetric, EventMetricConditionsMixin):
    """
    Class that stores information about a metric that takes the average value over events
    for a specified set of `person_event` rows occurring during the analysis period.

    Example metric: Average LSI-R score across all assessments.
    """

    # Name of the field in event_attributes JSON containing the numeric attribute of the event.
    event_value_numeric: str

    # EventCount metric counting the number of events contributing to the event value metric
    event_count_metric: EventCountMetric

    def generate_aggregation_query_fragment(self, event_date_col: str) -> str:
        return f"""
            AVG(IF(
                {self.get_metric_conditions_string()},
                CAST(JSON_EXTRACT_SCALAR(event_attributes, "$.{self.event_value_numeric}") AS FLOAT64),
                NULL
            )) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SAFE_DIVIDE(SUM({self.event_count_metric.name} * {self.name}), SUM({self.event_count_metric.name}))"


@attr.define(frozen=True, kw_only=True)
class AssignmentDaysToFirstEventMetric(
    AssignmentEventAggregatedMetric, EventMetricConditionsMixin
):
    """
    Class that stores information about a metric that calculates the number of days from
    assignment to the first instance of the event specified in `person_events` occurring within
    {window_length_days} of assignment, for all assignments occurring during the analysis period.

    Example metric: Days to first absconsion within 365 days of assignment.
    """

    def generate_aggregation_query_fragment(
        self, event_date_col: str, assignment_date_col: str
    ) -> str:
        return f"""
            MIN(DATE_DIFF(
                IFNULL(
                    IF(
                        {self.get_metric_conditions_string()},
                        LEAST({event_date_col}, DATE_ADD({assignment_date_col}, INTERVAL {self.window_length_days} DAY)),
                        NULL
                    ), DATE_ADD({assignment_date_col}, INTERVAL {self.window_length_days} DAY)),
                {assignment_date_col}, DAY
            )) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"


@attr.define(frozen=True, kw_only=True)
class AssignmentEventCountMetric(
    AssignmentEventAggregatedMetric, EventMetricConditionsMixin
):
    """
    Class that stores information about a metric that counts the number of events
    specified in `person_events` occurring within {window_length_days} of assignment,
    for all assignments occurring during the analysis period.

    Example metric: Number of contacts within 30 days of assignment.
    """

    def generate_aggregation_query_fragment(
        self, event_date_col: str, assignment_date_col: str
    ) -> str:
        return f"""
            COUNT( 
                DISTINCT IF(
                    {self.get_metric_conditions_string()}
                    AND {event_date_col} <= DATE_ADD({assignment_date_col}, INTERVAL {self.window_length_days} DAY),
                    CONCAT(events.person_id, {event_date_col}), 
                    NULL
                )
            ) AS {self.name}
        """

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"
