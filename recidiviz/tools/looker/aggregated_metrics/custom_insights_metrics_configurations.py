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
    EventCountMetric,
    EventDistinctUnitCountMetric,
    SpanDistinctUnitCountMetric,
)
from recidiviz.observations.event_selector import EventSelector
from recidiviz.observations.event_type import EventType
from recidiviz.observations.span_selector import SpanSelector
from recidiviz.observations.span_type import SpanType

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
    description="Number of distinct primary (supervisor) Supervisor Homepage users having at least one active usage event for the "
    "during the time period",
    event_selector=EventSelector(
        event_type=EventType.INSIGHTS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={},
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
