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
"""Configured metrics for custom insights impact metrics displayable in Looker"""

import recidiviz.aggregated_metrics.models.aggregated_metric_configurations as metric_config
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    EventCountMetric,
    EventDistinctUnitCountMetric,
    SpanDistinctUnitCountMetric,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.observations.event_selector import EventSelector
from recidiviz.observations.event_type import EventType
from recidiviz.observations.span_selector import SpanSelector
from recidiviz.observations.span_type import SpanType

INSIGHTS_ASSIGNMENT_NAMES_TO_TYPES = {
    "ALL_SUPERVISION_STATES": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.ALL_STATES,
    ),
    "SUPERVISION_STATE": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.STATE_CODE,
    ),
    "SUPERVISION_DISTRICT": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ),
    "SUPERVISION_OFFICE": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
    ),
    "SUPERVISION_UNIT_SUPERVISOR": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.SUPERVISION_UNIT,
    ),
    "SUPERVISION_OFFICER": (
        MetricPopulationType.SUPERVISION,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
    ),
}


# Adoption and usage metrics
DISTINCT_PROVISIONED_INSIGHTS_USERS_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_provisioned_insights_users",
    display_name="Distinct Provisioned Supervisor Homepage Users",
    description="Number of distinct Supervisor Homepage users who are provisioned to have tool access (regardless of role type)",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PROVISIONED_USER_SESSION,
        span_conditions_dict={},
    ),
)
DISTINCT_REGISTERED_PROVISIONED_INSIGHTS_USERS_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_registered_provisioned_insights_users",
    display_name="Distinct Registered Provisioned Supervisor Homepage Users",
    description=(
        "Number of distinct Supervisor Homepage users who are provisioned to have tool access (regardless of role type) "
        "who have signed up/logged into Supervisor Homepage at least once"
    ),
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PROVISIONED_USER_SESSION,
        span_conditions_dict={"is_registered": ["true"]},
    ),
)
DISTINCT_PROVISIONED_PRIMARY_INSIGHTS_USERS_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_provisioned_primary_insights_users",
    display_name="Distinct Provisioned Primary Supervisor Homepage Users",
    description="Number of distinct primary Supervisor Homepage users who are provisioned to have tool access",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PROVISIONED_USER_SESSION,
        span_conditions_dict={"is_primary_user": ["true"]},
    ),
)
DISTINCT_REGISTERED_PRIMARY_INSIGHTS_USERS_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_registered_primary_insights_users",
    display_name="Distinct Total Registered Primary Supervisor Homepage Users",
    description="Number of distinct primary (supervisor) Supervisor Homepage users who have signed up/logged into Supervisor Homepage at least once",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PRIMARY_USER_REGISTRATION_SESSION,
        span_conditions_dict={},
    ),
)
DISTINCT_LOGGED_IN_PRIMARY_INSIGHTS_USERS_LOOKER = EventDistinctUnitCountMetric(
    name="distinct_logged_in_primary_insights_users",
    display_name="Distinct Logged In Primary Supervisor Homepage Users",
    description="Number of distinct primary (supervisor) Supervisor Homepage users who logged into Supervisor Homepage",
    event_selector=EventSelector(
        event_type=EventType.INSIGHTS_USER_LOGIN,
        event_conditions_dict={},
    ),
)
DISTINCT_ACTIVE_PRIMARY_INSIGHTS_USERS_LOOKER = EventDistinctUnitCountMetric(
    name="distinct_active_primary_insights_users",
    display_name="Distinct Active Primary Supervisor Homepage Users",
    description="Number of distinct primary (supervisor) Supervisor Homepage users having at least one active usage event "
    "during the time period",
    event_selector=EventSelector(
        event_type=EventType.INSIGHTS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={},
    ),
)
DISTINCT_ACTIVE_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_LOOKER = EventDistinctUnitCountMetric(
    name="distinct_active_primary_insights_users_with_outliers_visible_in_tool",
    display_name="Distinct Active Primary Supervisor Homepage Users with Outliers Visible in Tool",
    description="Number of distinct primary (supervisor) Supervisor Homepage users who had outliers and had at least one active usage event "
    "during the time period",
    event_selector=EventSelector(
        event_type=EventType.INSIGHTS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={"has_outlier_officers": ["true"]},
    ),
)
DISTINCT_ACTIVE_PRIMARY_INSIGHTS_USERS_WITHOUT_OUTLIERS_VISIBLE_IN_TOOL_LOOKER = EventDistinctUnitCountMetric(
    name="distinct_active_primary_insights_users_without_outliers_visible_in_tool",
    display_name="Distinct Active Primary Supervisor Homepage Users without Outliers Visible in Tool",
    description="Number of distinct primary (supervisor) Supervisor Homepage users who did not have outliers and had at least one active usage event "
    "during the time period",
    event_selector=EventSelector(
        event_type=EventType.INSIGHTS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={"has_outlier_officers": ["false"]},
    ),
)
LOGINS_PRIMARY_INSIGHTS_USERS_LOOKER = EventCountMetric(
    name="logins_primary_insights_user",
    display_name="Logins, Primary Supervisor Homepage Users",
    description="Number of logins performed by primary Supervisor Homepage users",
    event_selector=EventSelector(
        event_type=EventType.INSIGHTS_USER_LOGIN,
        event_conditions_dict={},
    ),
)
DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_LOGGED_IN_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_primary_insights_users_with_outliers_visible_in_tool_logged_in",
    display_name="Distinct Primary Supervisor Homepage Users with Outliers Visible in Tool - Logged In",
    description="Number of primary supervisor homepage users who had outliers visible in the tool and logged in",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PRIMARY_USER_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "has_outlier_officers": ["true"],
            "viewed_supervisor_page": ["true"],
        },
    ),
)
DISTINCT_PRIMARY_INSIGHTS_USERS_WITHOUT_OUTLIERS_VISIBLE_IN_TOOL_LOGGED_IN_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_primary_insights_users_without_outliers_visible_in_tool_logged_in",
    display_name="Distinct Primary Supervisor Homepage Users without Outliers Visible in Tool - Logged In",
    description="Number of primary supervisor homepage users who did not have outliers visible in the tool and logged in",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PRIMARY_USER_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "has_outlier_officers": ["false"],
            "viewed_supervisor_page": ["true"],
        },
    ),
)
DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_primary_insights_users_with_outliers_visible_in_tool",
    display_name="Distinct Primary Supervisor Homepage Users with Outliers Visible in Tool",
    description="Number of primary supervisor homepage users who had outliers visible in the tool",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PRIMARY_USER_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={"has_outlier_officers": ["true"]},
    ),
)
DISTINCT_PRIMARY_INSIGHTS_USERS_WITHOUT_OUTLIERS_VISIBLE_IN_TOOL_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_primary_insights_users_without_outliers_visible_in_tool",
    display_name="Distinct Primary Supervisor Homepage Users without Outliers Visible in Tool",
    description="Number of primary supervisor homepage users who did not have outliers visible in the tool",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PRIMARY_USER_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={"has_outlier_officers": ["false"]},
    ),
)
DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_VIEWED_STAFF_MEMBER_PAGE_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_primary_insights_users_with_outliers_visible_in_tool_viewed_staff_member_page",
    display_name="Distinct Primary Supervisor Homepage Users with Outliers Visible in Tool - Viewed a Staff Member Page",
    description="Number of primary supervisor homepage users who had outliers visible in the tool and viewed a staff member page",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PRIMARY_USER_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "has_outlier_officers": ["true"],
            "viewed_staff_page": ["true"],
        },
    ),
)
DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_VIEWED_STAFF_MEMBER_METRIC_PAGE_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_primary_insights_users_with_outliers_visible_in_tool_viewed_staff_member_metric_page",
    display_name="Distinct Primary Supervisor Homepage Users with Outliers Visible in Tool - Viewed a Staff Member Metric Page",
    description="Number of primary supervisor homepage users who had outliers visible in the tool and viewed a staff member metric page",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PRIMARY_USER_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "has_outlier_officers": ["true"],
            "viewed_staff_metric": ["true"],
        },
    ),
)
DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_VIEWED_CLIENT_PAGE_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_primary_insights_users_with_outliers_visible_in_tool_viewed_client_page",
    display_name="Distinct Primary Supervisor Homepage Users with Outliers Visible in Tool - Viewed a Client Page",
    description="Number of primary supervisor homepage users who had outliers visible in the tool and viewed a client page from the "
    "list of revocations or the list of incarcerations",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PRIMARY_USER_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "has_outlier_officers": ["true"],
            "viewed_client_page": ["true"],
        },
    ),
)
DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_VIEWED_ACTION_STRATEGY_POP_UP_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_primary_insights_users_with_outliers_visible_in_tool_viewed_action_strategy_pop_up",
    display_name="Distinct Primary Supervisor Homepage Users with Outliers Visible in Tool - Viewed Action Strategy Pop-up",
    description="Number of primary supervisor homepage users who had outliers visible in the tool and viewed the action strategy pop-up",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PRIMARY_USER_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "has_outlier_officers": ["true"],
            "viewed_staff_action_strategy_popup": ["true"],
        },
    ),
)
DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_VIEWED_ANY_PAGE_FOR_30_SECONDS_LOOKER = EventDistinctUnitCountMetric(
    name="distinct_primary_insights_users_with_outliers_visible_in_tool_viewed_any_page_for_30_seconds",
    display_name="Distinct Primary Supervisor Homepage Users with Outliers Visible in Tool - Viewed Any Page for 30 Seconds",
    description="Number of primary supervisor homepage users who had outliers visible in the tool and viewed any page for 30 seconds",
    event_selector=EventSelector(
        event_type=EventType.INSIGHTS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={
            "has_outlier_officers": ["true"],
            "event": ["VIEWED_PAGE_30_SECONDS"],
        },
    ),
)
DISTINCT_PRIMARY_INSIGHTS_USERS_VIEWED_ANY_PAGE_FOR_30_SECONDS_LOOKER = EventDistinctUnitCountMetric(
    name="distinct_primary_insights_users_viewed_any_page_for_30_seconds",
    display_name="Distinct Primary Supervisor Homepage Users Who Viewed Any Page for 30 Seconds",
    description="Number of primary supervisor homepage users who viewed any page for 30 seconds",
    event_selector=EventSelector(
        event_type=EventType.INSIGHTS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={
            "event": ["VIEWED_PAGE_30_SECONDS"],
        },
    ),
)

# Outcome metrics
ABSCONSIONS_AND_BENCH_WARRANTS_LOOKER = metric_config.ABSCONSIONS_BENCH_WARRANTS
AVG_DAILY_POPULATION_LOOKER = metric_config.AVG_DAILY_POPULATION
INCARCERATION_STARTS_LOOKER = metric_config.INCARCERATION_STARTS
INCARCERATION_STARTS_AND_INFERRED_LOOKER = (
    metric_config.INCARCERATION_STARTS_AND_INFERRED
)
VIOLATIONS_ABSCONSION = next(
    metric
    for metric in metric_config.VIOLATIONS_BY_TYPE_METRICS
    if metric.name == "violations_absconsion"
)


INSIGHTS_IMPACT_LOOKER_METRICS: list[AggregatedMetric] = [
    ABSCONSIONS_AND_BENCH_WARRANTS_LOOKER,
    AVG_DAILY_POPULATION_LOOKER,
    DISTINCT_PROVISIONED_INSIGHTS_USERS_LOOKER,
    DISTINCT_REGISTERED_PROVISIONED_INSIGHTS_USERS_LOOKER,
    DISTINCT_PROVISIONED_PRIMARY_INSIGHTS_USERS_LOOKER,
    DISTINCT_REGISTERED_PRIMARY_INSIGHTS_USERS_LOOKER,
    DISTINCT_LOGGED_IN_PRIMARY_INSIGHTS_USERS_LOOKER,
    DISTINCT_ACTIVE_PRIMARY_INSIGHTS_USERS_LOOKER,
    DISTINCT_ACTIVE_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_LOOKER,
    DISTINCT_ACTIVE_PRIMARY_INSIGHTS_USERS_WITHOUT_OUTLIERS_VISIBLE_IN_TOOL_LOOKER,
    LOGINS_PRIMARY_INSIGHTS_USERS_LOOKER,
    DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_LOGGED_IN_LOOKER,
    DISTINCT_PRIMARY_INSIGHTS_USERS_WITHOUT_OUTLIERS_VISIBLE_IN_TOOL_LOGGED_IN_LOOKER,
    DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_LOOKER,
    DISTINCT_PRIMARY_INSIGHTS_USERS_WITHOUT_OUTLIERS_VISIBLE_IN_TOOL_LOOKER,
    DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_VIEWED_STAFF_MEMBER_PAGE_LOOKER,
    DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_VIEWED_STAFF_MEMBER_METRIC_PAGE_LOOKER,
    DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_VIEWED_CLIENT_PAGE_LOOKER,
    DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_VIEWED_ACTION_STRATEGY_POP_UP_LOOKER,
    DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_VIEWED_ANY_PAGE_FOR_30_SECONDS_LOOKER,
    DISTINCT_PRIMARY_INSIGHTS_USERS_VIEWED_ANY_PAGE_FOR_30_SECONDS_LOOKER,
    INCARCERATION_STARTS_LOOKER,
    INCARCERATION_STARTS_AND_INFERRED_LOOKER,
    VIOLATIONS_ABSCONSION,
]
