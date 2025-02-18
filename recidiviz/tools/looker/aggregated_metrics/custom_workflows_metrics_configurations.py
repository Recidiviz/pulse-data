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
from recidiviz.calculator.query.state.views.analyst_data.workflows_person_events import (
    USAGE_EVENTS_DICT,
)
from recidiviz.common.str_field_utils import snake_to_title
from recidiviz.observations.event_selector import EventSelector
from recidiviz.observations.event_type import EventType
from recidiviz.observations.span_selector import SpanSelector
from recidiviz.observations.span_type import SpanType

# Adoption and usage metrics
DISTINCT_PROVISIONED_WORKFLOWS_USERS_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_provisioned_workflows_users",
    display_name="Distinct Provisioned Workflows Users",
    description="Number of distinct Workflows users who are provisioned to have tool access (regardless of role type)",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PROVISIONED_USER_SESSION,
        span_conditions_dict={},
    ),
)
DISTINCT_REGISTERED_PROVISIONED_WORKFLOWS_USERS_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_registered_provisioned_workflows_users",
    display_name="Distinct Registered Provisioned Workflows Users",
    description=(
        "Number of distinct Workflows users who are provisioned to have tool access (regardless of role type) "
        "who have signed up/logged into Workflows at least once"
    ),
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PROVISIONED_USER_SESSION,
        span_conditions_dict={"is_registered": ["true"]},
    ),
)
DISTINCT_PROVISIONED_PRIMARY_WORKFLOWS_USERS_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_provisioned_primary_workflows_users",
    display_name="Distinct Provisioned Primary Workflows Users",
    description="Number of distinct primary Workflows users who are provisioned to have tool access",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PROVISIONED_USER_SESSION,
        span_conditions_dict={"is_primary_user": ["true"]},
    ),
)
DISTINCT_REGISTERED_PRIMARY_WORKFLOWS_USERS_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_registered_primary_workflows_users",
    display_name="Distinct Registered Primary Workflows Users",
    description="Number of distinct primary (line staff) Workflows users who have signed up/logged into Workflows at least once",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION,
        span_conditions_dict={},
    ),
)
DISTINCT_LOGGED_IN_PRIMARY_WORKFLOWS_USERS_LOOKER = EventDistinctUnitCountMetric(
    name="distinct_logged_in_primary_workflows_users",
    display_name="Distinct Logged In Primary Workflows Users",
    description="Number of distinct primary (line staff) Workflows users who logged into Workflows",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_USER_LOGIN,
        event_conditions_dict={},
    ),
)
DISTINCT_ACTIVE_PRIMARY_WORKFLOWS_USERS_LOOKER = EventDistinctUnitCountMetric(
    name="distinct_active_primary_workflows_users",
    display_name="Distinct Active Primary Workflows Users",
    description="Number of distinct primary (line staff) Workflows users having at least one usage event for the "
    "task type during the time period",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={},
    ),
)
LOGINS_BY_PRIMARY_WORKFLOWS_USER_LOOKER = EventCountMetric(
    name="logins_primary_workflows_user",
    display_name="Logins, Primary Workflows Users",
    description="Number of logins performed by primary Workflows users",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_USER_LOGIN,
        event_conditions_dict={},
    ),
)
FIRST_TOOL_ACTIONS_LOOKER = EventCountMetric(
    name="first_tool_actions_workflows",
    display_name="First Tool Actions, Workflows",
    description="Number of unique instances of the first action taken in the workflows tool after a client is "
    "newly surfaced for the selected task type",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_PERSON_USAGE_EVENT,
        event_conditions_dict={
            "is_first_tool_action": ["true"],
        },
    ),
    event_segmentation_columns=["task_type"],
)

# Outcome metrics
AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_LOOKER = DailyAvgSpanCountMetric(
    name="avg_population_task_almost_eligible",
    display_name="Average Population: Task Almost Eligible",
    description="Average daily count of clients almost eligible for selected task type",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_almost_eligible": ["true"],
        },
    ),
)
AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_LOOKER_FUNNEL_METRICS = [
    DailyAvgSpanCountMetric(
        name=f"avg_population_task_almost_eligible_{k.lower()}",
        display_name=f"Average Population: Task Almost Eligible And {snake_to_title(k)}",
        description=f"Average daily count of clients almost eligible for selected task type with funnel status "
        f"{snake_to_title(k).lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_almost_eligible": ["true"],
                k.lower(): ["true"],
            },
        ),
    )
    for k in USAGE_EVENTS_DICT
]
AVG_DAILY_POPULATION_TASK_ELIGIBLE_LOOKER = DailyAvgSpanCountMetric(
    name="avg_population_task_eligible",
    display_name="Average Population: Task Eligible",
    description="Average daily count of clients eligible for selected task type",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_eligible": ["true"],
        },
    ),
)
AVG_DAILY_POPULATION_TASK_ELIGIBLE_LOOKER_FUNNEL_METRICS = [
    DailyAvgSpanCountMetric(
        name=f"avg_population_task_eligible_{k.lower()}",
        display_name=f"Average Population: Task Eligible And {snake_to_title(k)}",
        description=f"Average daily count of clients eligible for selected task type with funnel status "
        f"{snake_to_title(k).lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
                k.lower(): ["true"],
            },
        ),
    )
    for k in USAGE_EVENTS_DICT
]
PERSON_DAYS_TASK_ELIGIBLE_LOOKER = SumSpanDaysMetric(
    name="person_days_task_eligible",
    display_name="Person-Days Eligible for Opportunity",
    description="Total number of person-days spent eligible for opportunities of selected task type",
    span_selector=SpanSelector(
        span_type=SpanType.TASK_ELIGIBILITY_SESSION,
        span_conditions_dict={
            "is_eligible": ["true"],
        },
    ),
)
TASK_COMPLETIONS_LOOKER = EventCountMetric(
    name="task_completions",
    display_name="Task Completions",
    description="Number of task completions of selected task type",
    event_selector=EventSelector(
        event_type=EventType.TASK_COMPLETED,
        event_conditions_dict={},
    ),
    event_segmentation_columns=["task_type"],
)
TASK_COMPLETIONS_AFTER_TOOL_ACTION_LOOKER = EventCountMetric(
    name="task_completions_after_tool_action",
    display_name="Task Completions After Tool Action",
    description="Number of task completions for selected task type occurring after an action was taken in the tool",
    event_selector=EventSelector(
        event_type=EventType.TASK_COMPLETED,
        event_conditions_dict={
            "after_tool_action": ["true"],
        },
    ),
    event_segmentation_columns=["task_type"],
)
TASK_COMPLETIONS_WHILE_ALMOST_ELIGIBLE_LOOKER = EventCountMetric(
    name="task_completions_while_almost_eligible",
    display_name="Task Completions While Almost Eligible",
    description="Number of task completions for selected task type occurring while almost eligible for opportunity",
    event_selector=EventSelector(
        event_type=EventType.TASK_COMPLETED,
        event_conditions_dict={
            "is_almost_eligible": ["true"],
        },
    ),
    event_segmentation_columns=["task_type"],
)
TASK_COMPLETIONS_WHILE_ALMOST_ELIGIBLE_AFTER_TOOL_ACTION_LOOKER = EventCountMetric(
    name="task_completions_while_almost_eligible_after_tool_action",
    display_name="Task Completions While Almost Eligible After Tool Action",
    description="Number of task completions occurring while client is almost eligible for selected task type, "
    "occurring after an action was taken in the tool",
    event_selector=EventSelector(
        event_type=EventType.TASK_COMPLETED,
        event_conditions_dict={
            "after_tool_action": ["true"],
            "is_almost_eligible": ["true"],
        },
    ),
    event_segmentation_columns=["task_type"],
)
TASK_COMPLETIONS_WHILE_ELIGIBLE_LOOKER = EventCountMetric(
    name="task_completions_while_eligible",
    display_name="Task Completions While Eligible",
    description="Number of task completions for selected task type occurring while eligible for opportunity",
    event_selector=EventSelector(
        event_type=EventType.TASK_COMPLETED,
        event_conditions_dict={
            "is_eligible": ["true"],
        },
    ),
    event_segmentation_columns=["task_type"],
)
DAYS_ELIGIBLE_AT_FIRST_TOOL_ACTION_LOOKER = EventValueMetric(
    name="days_eligible_at_first_tool_action",
    display_name="Days Eligible At First Workflows Tool Action",
    description="Number of days spent eligible for selected opportunity at time of first action in Workflows tool",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_PERSON_USAGE_EVENT,
        event_conditions_dict={
            "is_first_tool_action": ["true"],
        },
    ),
    event_value_numeric="days_eligible",
    event_count_metric=FIRST_TOOL_ACTIONS_LOOKER,
)
DAYS_ELIGIBLE_AT_TASK_COMPLETION_LOOKER = EventValueMetric(
    name="days_eligible_at_task_completion",
    display_name="Days Eligible At Task Completion",
    description="Number of days spent eligible for selected opportunity at task completion",
    event_selector=EventSelector(
        event_type=EventType.TASK_COMPLETED,
        event_conditions_dict={},
    ),
    event_value_numeric="days_eligible",
    event_count_metric=TASK_COMPLETIONS_LOOKER,
)
TASK_ELIGIBILITY_STARTS_WHILE_ALMOST_ELIGIBLE_AFTER_TOOL_ACTION_LOOKER = EventCountMetric(
    name="task_eligibility_starts_while_almost_eligible_after_tool_action",
    display_name="Task Eligibility Starts While Almost Eligible After Tool Action",
    description="Number of task eligibility starts occurring while client is almost eligible for selected task type, "
    "occurring after an action was taken in the tool",
    event_selector=EventSelector(
        event_type=EventType.TASK_ELIGIBILITY_START,
        event_conditions_dict={
            "after_tool_action": ["true"],
            "after_almost_eligible": ["true"],
        },
    ),
    event_segmentation_columns=["task_type"],
)

# Officer Opportunities metrics
DISTINCT_OFFICERS_WITH_CANDIDATE_CASELOAD = SpanDistinctUnitCountMetric(
    name="distinct_officers_with_candidate_caseload",
    display_name="Distinct Officers With Candidate Caseload",
    description="Number of distinct officers with a client/resident considered a potential candidate for an opportunity",
    span_selector=SpanSelector(
        span_type=SpanType.SUPERVISION_OFFICER_ELIGIBILITY_SESSIONS,
        span_conditions_dict={},
    ),
)
DISTINCT_OFFICERS_WITH_ELIGIBLE_OR_ALMOST_ELIGIBLE_CASELOAD = SpanDistinctUnitCountMetric(
    name="distinct_officers_with_eligible_or_almost_eligible_caseload",
    display_name="Distinct Officers With Eligible Or Almost Eligible Caseload",
    description="Number of distinct officers with a client/resident eligible or almost eligible for an opportunity",
    span_selector=SpanSelector(
        span_type=SpanType.SUPERVISION_OFFICER_ELIGIBILITY_SESSIONS,
        span_conditions_dict={
            "is_eligible_or_almost_eligible": ["true"],
        },
    ),
)
DISTINCT_OFFICERS_WITH_ELIGIBLE_CASELOAD = SpanDistinctUnitCountMetric(
    name="distinct_officers_with_eligible_caseload",
    display_name="Distinct Officers With Eligible Caseload",
    description="Number of distinct officers with a client/resident eligible for an opportunity",
    span_selector=SpanSelector(
        span_type=SpanType.SUPERVISION_OFFICER_ELIGIBILITY_SESSIONS,
        span_conditions_dict={
            "is_eligible": ["true"],
        },
    ),
)

DISTINCT_OFFICERS_WITH_ALMOST_ELIGIBLE_CASELOAD = SpanDistinctUnitCountMetric(
    name="distinct_officers_with_almost_eligible_caseload",
    display_name="Distinct Officers With Almost Eligible Caseload",
    description="Number of distinct officers with a client/resident almost eligible for an opportunity",
    span_selector=SpanSelector(
        span_type=SpanType.SUPERVISION_OFFICER_ELIGIBILITY_SESSIONS,
        span_conditions_dict={
            "is_almost_eligible": ["true"],
        },
    ),
)

DISTINCT_OFFICERS_WITH_TASKS_COMPLETED = EventDistinctUnitCountMetric(
    name="distinct_officers_with_tasks_completed",
    display_name="Distinct Officers With Tasks Completed",
    description="Number of distinct officers that completed at least one task for an opportunity",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_OFFICER_TASK_COMPLETED,
        event_conditions_dict={},
    ),
)

DISTINCT_OFFICERS_WITH_TASKS_COMPLETED_WHILE_ELIGIBLE_OR_ALMOST_ELIGIBLE = EventDistinctUnitCountMetric(
    name="distinct_officers_with_tasks_completed_while_eligible_or_almost_eligible",
    display_name="Distinct Officers With Tasks Completed While Eligible Or Almost Eligible",
    description="Number of distinct officers that completed at least one task for an opportunity while the client was eligible or almost eligible",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_OFFICER_TASK_COMPLETED,
        event_conditions_dict={
            "is_eligible_or_almost_eligible": ["true"],
        },
    ),
)

DISTINCT_OFFICERS_WITH_TASKS_COMPLETED_WHILE_ELIGIBLE = EventDistinctUnitCountMetric(
    name="distinct_officers_with_tasks_completed_while_eligible",
    display_name="Distinct Officers With Tasks Completed While Eligible",
    description="Number of distinct officers that completed at least one task for an opportunity while the client was eligible",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_OFFICER_TASK_COMPLETED,
        event_conditions_dict={
            "is_eligible": ["true"],
        },
    ),
)

DISTINCT_OFFICERS_WITH_TASKS_COMPLETED_WHILE_ALMOST_ELIGIBLE = EventDistinctUnitCountMetric(
    name="distinct_officers_with_tasks_completed_while_almost_eligible",
    display_name="Distinct Officers With Tasks Completed While Almost Eligible",
    description="Number of distinct officers that completed at least one task for an opportunity while the client was almost eligible",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_OFFICER_TASK_COMPLETED,
        event_conditions_dict={
            "is_almost_eligible": ["true"],
        },
    ),
)

DISTINCT_OFFICERS_WITH_TASKS_COMPLETED_AFTER_TOOL_ACTION = EventDistinctUnitCountMetric(
    name="distinct_officers_with_tasks_completed_after_tool_action",
    display_name="Distinct Officers With Tasks Completed After Tool Action",
    description="Number of distinct officers that completed at least one task after a corresponding tool action",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_OFFICER_TASK_COMPLETED,
        event_conditions_dict={
            "after_tool_action": ["true"],
        },
    ),
)
