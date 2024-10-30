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
"""Defines AggregatedMetric objects for impact reports"""

from recidiviz.aggregated_metrics.models.aggregated_metric import (
    DailyAvgSpanCountMetric,
    EventDistinctUnitCountMetric,
    SpanDistinctUnitCountMetric,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    DEDUPED_TASK_COMPLETION_EVENT_VB,
)
from recidiviz.calculator.query.state.views.analyst_data.models.event_selector import (
    EventSelector,
)
from recidiviz.calculator.query.state.views.analyst_data.models.span_selector import (
    SpanSelector,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.span_type import SpanType
from recidiviz.workflows.types import WorkflowsSystemType

AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_SUPERVISION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_marked_ineligible_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Marked Ineligible, {b.task_title}",
        description=f"Average daily count of residents marked ineligible for task of type: {b.task_title.lower()}",
        span_selectors=[
            SpanSelector(
                span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
                span_conditions_dict={
                    "marked_ineligible": ["true"],
                    "is_eligible": ["true"],
                    "task_type": [b.task_type_name],
                },
            )
        ],
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_INCARCERATION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_marked_ineligible_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Marked Ineligible, {b.task_title}",
        description=f"Average daily count of residents marked ineligible for task of type: {b.task_title.lower()}",
        span_selectors=[
            SpanSelector(
                span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
                span_conditions_dict={
                    "marked_ineligible": ["true"],
                    "is_eligible": ["true"],
                    "task_type": [b.task_type_name],
                },
            )
        ],
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_INCARCERATION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_almost_eligible_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Almost Eligible, {b.task_title}",
        description=f"Average daily count of residents almost eligible for task of type: {b.task_title.lower()}",
        span_selectors=[
            SpanSelector(
                span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
                span_conditions_dict={
                    "is_almost_eligible": ["true"],
                    "task_type": [b.task_type_name],
                },
            )
        ],
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_INCARCERATION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_eligible_and_viewed_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Eligible And Viewed, {b.task_title}",
        description=f"Average daily count of residents eligible and viewed for task of type: {b.task_title.lower()}",
        span_selectors=[
            SpanSelector(
                span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
                span_conditions_dict={
                    "is_eligible": ["true"],
                    "viewed": ["true"],
                    "marked_ineligible": ["false"],
                    "task_type": [b.task_type_name],
                },
            ),
        ],
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_SUPERVISION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_eligible_and_viewed_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Eligible And Viewed, {b.task_title}",
        description=f"Average daily count of residents eligible and viewed for task of type: {b.task_title.lower()}",
        span_selectors=[
            SpanSelector(
                span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
                span_conditions_dict={
                    "is_eligible": ["true"],
                    "viewed": ["true"],
                    "marked_ineligible": ["false"],
                    "task_type": [b.task_type_name],
                },
            ),
        ],
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_SUPERVISION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_eligible_and_not_viewed_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Eligible And Not Viewed, {b.task_title}",
        description=f"Average daily count of residents eligible and not viewed for task of type: {b.task_title.lower()}",
        span_selectors=[
            SpanSelector(
                span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
                span_conditions_dict={
                    "is_eligible": ["true"],
                    "viewed": ["false"],
                    "marked_ineligible": ["false"],
                    "task_type": [b.task_type_name],
                },
            ),
        ],
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_INCARCERATION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_eligible_and_not_viewed_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Eligible And Not Viewed, {b.task_title}",
        description=f"Average daily count of residents eligible and not viewed for task of type: {b.task_title.lower()}",
        span_selectors=[
            SpanSelector(
                span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
                span_conditions_dict={
                    "is_eligible": ["true"],
                    "viewed": ["false"],
                    "marked_ineligible": ["false"],
                    "task_type": [b.task_type_name],
                },
            ),
        ],
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]


AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_SUPERVISION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_almost_eligible_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Almost Eligible, {b.task_title}",
        description=f"Average daily count of residents almost eligible for task of type: {b.task_title.lower()}",
        span_selectors=[
            SpanSelector(
                span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
                span_conditions_dict={
                    "is_almost_eligible": ["true"],
                    "task_type": [b.task_type_name],
                },
            )
        ],
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

DISTINCT_ACTIVE_USERS_INCARCERATION = [
    EventDistinctUnitCountMetric(
        name=f"distinct_active_users_{b.task_type_name.lower()}",
        display_name="Distinct Active Users",
        description="Number of distinct Workflows users having at least one usage event for the "
        f"task of type {b.task_title.lower()} during the time period",
        event_selector=EventSelector(
            event_type=EventType.WORKFLOWS_ACTIVE_USAGE_EVENT,
            event_conditions_dict={
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

DISTINCT_ACTIVE_USERS_SUPERVISION = [
    EventDistinctUnitCountMetric(
        name=f"distinct_active_users_{b.task_type_name.lower()}",
        display_name="Distinct Active Users",
        description="Number of distinct Workflows users having at least one usage event for the "
        f"task of type {b.task_title.lower()} during the time period",
        event_selector=EventSelector(
            event_type=EventType.WORKFLOWS_ACTIVE_USAGE_EVENT,
            event_conditions_dict={
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

DISTINCT_REGISTERED_USERS_SUPERVISION = SpanDistinctUnitCountMetric(
    name="distinct_registered_users_supervision",
    display_name="Distinct Total Registered Users",
    description="Number of distinct Workflows users who have signed up/logged into Workflows at least once",
    span_selectors=[
        SpanSelector(
            span_type=SpanType.WORKFLOWS_USER_REGISTRATION_SESSION,
            span_conditions_dict={"system_type": ["SUPERVISION"]},
        ),
    ],
)

DISTINCT_REGISTERED_USERS_INCARCERATION = SpanDistinctUnitCountMetric(
    name="distinct_registered_users_incarceration",
    display_name="Distinct Total Registered Users",
    description="Number of distinct Workflows users who have signed up/logged into Workflows at least once",
    span_selectors=[
        SpanSelector(
            span_type=SpanType.WORKFLOWS_USER_REGISTRATION_SESSION,
            span_conditions_dict={"system_type": ["INCARCERATION"]},
        ),
    ],
)
