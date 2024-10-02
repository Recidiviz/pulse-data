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
from typing import List, Optional

import attr
from more_itertools import one

from recidiviz.calculator.query.bq_utils import nonnull_current_date_exclusive_clause
from recidiviz.calculator.query.state.views.analyst_data.models.event_selector import (
    EventSelector,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.span_selector import (
    SpanSelector,
)
from recidiviz.common import attr_validators
from recidiviz.observations.event_type import EventType
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.span_type import SpanType


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


class MetricConditionsMixin:
    """Attributes and functions to derive query snippets for defining a metric"""

    def get_metric_conditions_string(self) -> str:
        """Returns a query fragment string that joins SQL conditional statements with `AND`."""
        return "\n\t\t\t\tOR\n".join(self.get_metric_conditions())

    def get_metric_conditions_string_no_newline(self) -> str:
        """
        Returns a query fragment string that joins SQL conditional statements with `AND` without line breaks
        or extra spaces, for more succinct print output.
        """
        return re.sub(r" +|\n+", " ", " OR ".join(self.get_metric_conditions()))

    @abc.abstractmethod
    def get_metric_conditions(self) -> List[str]:
        """Returns a list of conditional query fragments filtering spans or events."""


@attr.define(frozen=True, kw_only=True, slots=False)
class SpanMetricConditionsMixin(MetricConditionsMixin):
    """Attributes and functions to derive query snippets applied to spans"""

    # SpanSelectors specifying conditions on a spans table
    span_selectors: List[SpanSelector]

    def get_metric_conditions(self) -> List[str]:
        return [
            f"({s.generate_span_conditions_query_fragment(filter_by_span_type=True)})"
            for s in self.span_selectors
        ]

    @property
    def unit_of_observation(self) -> MetricUnitOfObservation:
        return one({s.unit_of_observation for s in self.span_selectors})

    @property
    def unit_of_observation_type(self) -> MetricUnitOfObservationType:
        return self.unit_of_observation.type

    @property
    def span_types(self) -> List[SpanType]:
        return [s.span_type for s in self.span_selectors]


@attr.define(frozen=True, kw_only=True, slots=False)
class EventMetricConditionsMixin(MetricConditionsMixin):
    """Attributes and functions to derive query snippets applied to events"""

    # The list of EventSelectors specifying conditions on a spans table
    event_selectors: List[EventSelector]

    def get_metric_conditions(self) -> List[str]:
        return [
            f"({s.generate_event_conditions_query_fragment(filter_by_event_type=True)})"
            for s in self.event_selectors
        ]

    @property
    def unit_of_observation(self) -> MetricUnitOfObservation:
        return one({s.unit_of_observation for s in self.event_selectors})

    @property
    def unit_of_observation_type(self) -> MetricUnitOfObservationType:
        return self.unit_of_observation.type

    @property
    def event_types(self) -> List[EventType]:
        return [s.event_type for s in self.event_selectors]


@attr.define(frozen=True, kw_only=True)
class MiscAggregatedMetric(AggregatedMetric):
    """
    Class that stores information about metrics that are calculated in a separate user-defined query
    for specific populations and units of analysis, without using events or spans logic
    """

    # Populations compatible with metric
    populations: List[MetricPopulationType]

    # Units of analysis at which the metric can be aggregated
    unit_of_analysis_types: List[MetricUnitOfAnalysisType]

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"ARRAY_AGG({self.name}) AS {self.name}"

    @classmethod
    def pretty_name(cls) -> str:
        return "Misc. Metric"


@attr.define(frozen=True, kw_only=True)
class PeriodSpanAggregatedMetric(AggregatedMetric, SpanMetricConditionsMixin):
    """
    Class that stores information about metrics that involve spans and calculate
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
class AssignmentSpanAggregatedMetric(AggregatedMetric, SpanMetricConditionsMixin):
    """
    Class that stores information about metrics that involve spans and calculate
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
class PeriodEventAggregatedMetric(AggregatedMetric, EventMetricConditionsMixin):
    """
    Class that stores information about metrics that involve `events` and calculate
    aggregations across an entire analysis period.
    """

    @abc.abstractmethod
    def generate_aggregation_query_fragment(self, event_date_col: str) -> str:
        """Returns a query fragment that calculates an aggregation corresponding to the PeriodEvent metric type."""


@attr.define(frozen=True, kw_only=True)
class AssignmentEventAggregatedMetric(AggregatedMetric, EventMetricConditionsMixin):
    """
    Class that stores information about metrics that involve `events` and calculate
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


@attr.define(frozen=True, kw_only=True)
class DailyAvgSpanCountMetric(PeriodSpanAggregatedMetric):
    """
    Class that stores information about a metric that calculates average daily population
    for a specified set of span rows. All end_date_cols should be end date exclusive.

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
class DailyAvgSpanValueMetric(PeriodSpanAggregatedMetric):
    """
    Class that stores information about a metric that calculates average daily value
    for a specified set of span rows intersecting with the analysis period.
    All end_date_cols should be end date exclusive.

    Example: Average daily LSI-R score.
    """

    # Name of the field in span_attributes JSON containing the numeric attribute of the span.
    span_value_numeric: str = attr.field(validator=attr_validators.is_non_empty_str)

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

    def generate_aggregation_query_fragment(
        self,
        span_start_date_col: str,
        span_end_date_col: str,
        period_start_date_col: str,
        period_end_date_col: str,
        original_span_start_date: Optional[str] = None,
    ) -> str:
        weight_snippet = (
            (
                f'CAST(JSON_EXTRACT_SCALAR(span_attributes, "$.{self.weight_col}") AS FLOAT64) * '
            )
            if self.weight_col
            else ""
        )
        return f"""
            SUM(
            (
                {weight_snippet}DATE_DIFF(
                    LEAST({period_end_date_col}, {nonnull_current_date_exclusive_clause(span_end_date_col)}),
                    GREATEST({period_start_date_col}, {span_start_date_col}),
                    DAY)
                ) * (IF({self.get_metric_conditions_string()}, 1, 0))
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
        span_start_date_col: str,
        span_end_date_col: str,
        period_start_date_col: str,
        period_end_date_col: str,
        original_span_start_date: str,
    ) -> str:
        return f"""
            COUNT(DISTINCT IF(
                {self.get_metric_conditions_string()},
                CONCAT({self.unit_of_observation.get_primary_key_columns_query_string(prefix="ses")}),
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
class AssignmentSpanMaxDaysMetric(AssignmentSpanAggregatedMetric):
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
class AssignmentCountMetric(AssignmentSpanAggregatedMetric):
    """
    Class used specifically for calculating number of assignments in a period.

    This is used only for the metric "Assignments".
    """

    def generate_aggregation_query_fragment(
        self, span_start_date_col: str, span_end_date_col: str, assignment_date_col: str
    ) -> str:
        return f"1 AS {self.name}"

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

    def generate_aggregation_query_fragment(self, event_date_col: str) -> str:

        # If `event_segmentation_columns` are provided, add to the set of fields used to calculate the
        # COUNT DISTINCT.
        event_segmentation_columns = []
        if self.event_segmentation_columns:
            event_segmentation_columns = self.event_segmentation_columns
        event_segmentation_columns_json = [
            f'JSON_EXTRACT_SCALAR(event_attributes, "$.{col}")'
            for col in event_segmentation_columns
        ]
        event_segmentation_columns_str = (
            ",\n                    " + ", ".join(event_segmentation_columns_json)
            if len(event_segmentation_columns_json) > 0
            else ""
        )
        return f"""
            COUNT(DISTINCT IF(
                {self.get_metric_conditions_string()},
                CONCAT(
                    {self.unit_of_observation.get_primary_key_columns_query_string(prefix="events")}, 
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

    def generate_aggregation_query_fragment(self, event_date_col: str) -> str:
        return f"""
            AVG(IF(
                {self.get_metric_conditions_string()},
                CAST(JSON_EXTRACT_SCALAR(event_attributes, "$.{self.event_value_numeric}") AS FLOAT64),
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

    def generate_aggregation_query_fragment(self, event_date_col: str) -> str:
        return f"""
            COUNT(DISTINCT IF(
                {self.get_metric_conditions_string()},
                CONCAT({self.unit_of_observation.get_primary_key_columns_query_string(prefix="events")}),
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
class AssignmentEventCountMetric(AssignmentEventAggregatedMetric):
    """
    Class that stores information about a metric that counts the number of events
    specified in `events` occurring within {window_length_days} of assignment,
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
                    CONCAT({self.unit_of_observation.get_primary_key_columns_query_string(prefix="events")}, {event_date_col}),
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
        self, event_date_col: str, assignment_date_col: str
    ) -> str:
        return f"""
            CAST(LOGICAL_OR(
                {self.get_metric_conditions_string()}
                AND {event_date_col} <= DATE_ADD({assignment_date_col}, INTERVAL {self.window_length_days} DAY)
            ) AS INT64) AS {self.name}"""

    def generate_aggregate_time_periods_query_fragment(self) -> str:
        return f"SUM({self.name}) AS {self.name}"
