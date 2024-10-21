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
"""Configured metrics for custom workflows impact metrics displayable in looker and responsive to task_type parameter"""

from recidiviz.aggregated_metrics.models.aggregated_metric import (
    DailyAvgSpanCountMetric,
    EventCountMetric,
    EventDistinctUnitCountMetric,
    EventValueMetric,
    SpanDistinctUnitCountMetric,
    SumSpanDaysMetric,
)
from recidiviz.calculator.query.state.views.analyst_data.models.event_selector import (
    EventSelector,
)
from recidiviz.calculator.query.state.views.analyst_data.models.span_selector import (
    SpanSelector,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_person_events import (
    USAGE_EVENTS_DICT,
)
from recidiviz.common.str_field_utils import snake_to_title
from recidiviz.observations.event_type import EventType
from recidiviz.observations.span_type import SpanType

AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_LOOKER = DailyAvgSpanCountMetric(
    name="avg_population_task_almost_eligible",
    display_name="Average Population: Task Almost Eligible",
    description="Average daily count of clients almost eligible for selected task type",
    span_selectors=[
        SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_almost_eligible": ["true"],
            },
        )
    ],
)
AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_LOOKER_FUNNEL_METRICS = [
    DailyAvgSpanCountMetric(
        name=f"avg_population_task_almost_eligible_{k.lower()}",
        display_name=f"Average Population: Task Almost Eligible And {snake_to_title(k)}",
        description=f"Average daily count of clients almost eligible for selected task type with funnel status "
        f"{snake_to_title(k).lower()}",
        span_selectors=[
            SpanSelector(
                span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
                span_conditions_dict={
                    "is_almost_eligible": ["true"],
                    k.lower(): ["true"],
                },
            )
        ],
    )
    for k in USAGE_EVENTS_DICT
]
AVG_DAILY_POPULATION_TASK_ELIGIBLE_LOOKER = DailyAvgSpanCountMetric(
    name="avg_population_task_eligible",
    display_name="Average Population: Task Eligible",
    description="Average daily count of clients eligible for selected task type",
    span_selectors=[
        SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
            },
        )
    ],
)
AVG_DAILY_POPULATION_TASK_ELIGIBLE_LOOKER_FUNNEL_METRICS = [
    DailyAvgSpanCountMetric(
        name=f"avg_population_task_eligible_{k.lower()}",
        display_name=f"Average Population: Task Eligible And {snake_to_title(k)}",
        description=f"Average daily count of clients eligible for selected task type with funnel status "
        f"{snake_to_title(k).lower()}",
        span_selectors=[
            SpanSelector(
                span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
                span_conditions_dict={
                    "is_eligible": ["true"],
                    k.lower(): ["true"],
                },
            )
        ],
    )
    for k in USAGE_EVENTS_DICT
]
DISTINCT_ACTIVE_USERS_LOOKER = EventDistinctUnitCountMetric(
    name="distinct_active_users",
    display_name="Distinct Active Primary Users",
    description="Number of distinct primary (line staff) Workflows users having at least one usage event for the "
    "task type during the time period",
    event_selectors=[
        EventSelector(
            event_type=EventType.WORKFLOWS_USER_ACTION,
            event_conditions_dict={},
        ),
        EventSelector(
            event_type=EventType.WORKFLOWS_USER_CLIENT_STATUS_UPDATE,
            event_conditions_dict={},
        ),
        EventSelector(
            event_type=EventType.WORKFLOWS_USER_PAGE,
            event_conditions_dict={},
        ),
    ],
)
DISTINCT_REGISTERED_USERS_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_registered_users",
    display_name="Distinct Total Registered Primary Users",
    description="Number of distinct primary (line staff) Workflows users who have signed up/logged into Workflows at least once",
    span_selectors=[
        SpanSelector(
            span_type=SpanType.WORKFLOWS_USER_REGISTRATION_SESSION,
            span_conditions_dict={},
        ),
    ],
)
DISTINCT_LOGGED_IN_USERS_LOOKER = EventDistinctUnitCountMetric(
    name="distinct_logged_in_users",
    display_name="Distinct Primary Users Logging In",
    description="Number of distinct primary (line staff) Workflows users who logged into Workflows",
    event_selectors=[
        EventSelector(
            event_type=EventType.WORKFLOWS_USER_LOGIN,
            event_conditions_dict={},
        ),
    ],
)
LOGINS_LOOKER = EventCountMetric(
    name="logins",
    display_name="Logins",
    description="Number of logins performed by primary Workflows users",
    event_selectors=[
        EventSelector(
            event_type=EventType.WORKFLOWS_USER_LOGIN,
            event_conditions_dict={},
        ),
    ],
)
PERSON_DAYS_TASK_ELIGIBLE_LOOKER = SumSpanDaysMetric(
    name="person_days_task_eligible",
    display_name="Person-Days Eligible for Opportunity",
    description="Total number of person-days spent eligible for opportunities of selected task type",
    span_selectors=[
        SpanSelector(
            span_type=SpanType.TASK_ELIGIBILITY_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
            },
        )
    ],
)
TASK_COMPLETIONS_LOOKER = EventCountMetric(
    name="task_completions",
    display_name="Task Completions",
    description="Number of task completions of selected task type",
    event_selectors=[
        EventSelector(
            event_type=EventType.TASK_COMPLETED,
            event_conditions_dict={},
        ),
    ],
    event_segmentation_columns=["task_type"],
)
TASK_COMPLETIONS_AFTER_TOOL_ACTION_LOOKER = EventCountMetric(
    name="task_completions_after_tool_action",
    display_name="Task Completions After Tool Action",
    description="Number of task completions for selected task type occurring after an action was taken in the tool",
    event_selectors=[
        EventSelector(
            event_type=EventType.TASK_COMPLETED,
            event_conditions_dict={
                "after_tool_action": ["true"],
            },
        ),
    ],
    event_segmentation_columns=["task_type"],
)
TASK_COMPLETIONS_WHILE_ALMOST_ELIGIBLE_AFTER_TOOL_ACTION_LOOKER = EventCountMetric(
    name="task_completions_while_almost_eligible_after_tool_action",
    display_name="Task Completions While Almost Eligible After Tool Action",
    description="Number of task completions occurring while client is almost eligible for selected task type, "
    "occurring after an action was taken in the tool",
    event_selectors=[
        EventSelector(
            event_type=EventType.TASK_COMPLETED,
            event_conditions_dict={
                "after_tool_action": ["true"],
                "is_almost_eligible": ["true"],
            },
        ),
    ],
    event_segmentation_columns=["task_type"],
)
TASK_COMPLETIONS_WHILE_ELIGIBLE_LOOKER = EventCountMetric(
    name="task_completions_while_eligible",
    display_name="Task Completions While Eligible",
    description="Number of task completions for selected task type occurring while eligible for opportunity",
    event_selectors=[
        EventSelector(
            event_type=EventType.TASK_COMPLETED,
            event_conditions_dict={
                "is_eligible": ["true"],
            },
        ),
    ],
    event_segmentation_columns=["task_type"],
)
FIRST_TOOL_ACTIONS_LOOKER = EventCountMetric(
    name="first_tool_actions",
    display_name="First Tool Actions",
    description="Number of unique instances of the first action taken in the workflows tool after a client is "
    "newly surfaced for the selected task type",
    event_selectors=[
        EventSelector(
            event_type=EventType.WORKFLOWS_PERSON_USAGE_EVENT,
            event_conditions_dict={
                "is_first_tool_action": ["true"],
            },
        ),
    ],
    event_segmentation_columns=["task_type"],
)
DAYS_ELIGIBLE_AT_FIRST_TOOL_ACTION_LOOKER = EventValueMetric(
    name="days_eligible_at_first_tool_action",
    display_name="Days Eligible At First Workflows Tool Action",
    description="Number of days spent eligible for selected opportunity at time of first action in Workflows tool",
    event_selectors=[
        EventSelector(
            event_type=EventType.WORKFLOWS_PERSON_USAGE_EVENT,
            event_conditions_dict={
                "is_first_tool_action": ["true"],
            },
        ),
    ],
    event_value_numeric="days_eligible",
    event_count_metric=FIRST_TOOL_ACTIONS_LOOKER,
)
DAYS_ELIGIBLE_AT_TASK_COMPLETION_LOOKER = EventValueMetric(
    name="days_eligible_at_task_completion",
    display_name="Days Eligible At Task Completion",
    description="Number of days spent eligible for selected opportunity at task completion",
    event_selectors=[
        EventSelector(
            event_type=EventType.TASK_COMPLETED,
            event_conditions_dict={},
        ),
    ],
    event_value_numeric="days_eligible",
    event_count_metric=TASK_COMPLETIONS_LOOKER,
)
TASK_ELIGIBILITY_STARTS_WHILE_ALMOST_ELIGIBLE_AFTER_TOOL_ACTION_LOOKER = EventCountMetric(
    name="task_eligibility_starts_while_almost_eligible_after_tool_action",
    display_name="Task Eligibility Starts While Almost Eligible After Tool Action",
    description="Number of task eligibility starts occurring while client is almost eligible for selected task type, "
    "occurring after an action was taken in the tool",
    event_selectors=[
        EventSelector(
            event_type=EventType.TASK_COMPLETED,
            event_conditions_dict={
                "after_tool_action": ["true"],
                "after_almost_eligible": ["true"],
            },
        ),
    ],
    event_segmentation_columns=["task_type"],
)
