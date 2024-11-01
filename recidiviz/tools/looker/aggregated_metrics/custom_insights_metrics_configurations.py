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
from recidiviz.calculator.query.state.views.analyst_data.models.event_selector import (
    EventSelector,
)
from recidiviz.calculator.query.state.views.analyst_data.models.span_selector import (
    SpanSelector,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.span_type import SpanType

ABSCONSIONS_AND_BENCH_WARRANTS_LOOKER = metric_config.ABSCONSIONS_BENCH_WARRANTS
AVG_DAILY_POPULATION_LOOKER = metric_config.AVG_DAILY_POPULATION
DISTINCT_REGISTERED_USERS_LOOKER = SpanDistinctUnitCountMetric(
    name="distinct_registered_users",
    display_name="Distinct Total Registered Primary Users (Insights)",
    description="Number of distinct primary (supervisor) Insights users who have signed up/logged into Insights at least once",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_USER_REGISTRATION_SESSION,
        span_conditions_dict={},
    ),
)
DISTINCT_LOGGED_IN_USERS_LOOKER = EventDistinctUnitCountMetric(
    name="distinct_logged_in_users",
    display_name="Distinct Primary Users Logging In (Insights)",
    description="Number of distinct primary (supervisor) Insights users who logged into Insights",
    event_selector=EventSelector(
        event_type=EventType.INSIGHTS_USER_LOGIN,
        event_conditions_dict={},
    ),
)
LOGINS_LOOKER = EventCountMetric(
    name="logins",
    display_name="Logins (Insights)",
    description="Number of logins performed by primary Insights users",
    event_selector=EventSelector(
        event_type=EventType.INSIGHTS_USER_LOGIN,
        event_conditions_dict={},
    ),
)
INCARCERATION_STARTS_LOOKER = metric_config.INCARCERATION_STARTS
INCARCERATION_STARTS_AND_INFERRED_LOOKER = (
    metric_config.INCARCERATION_STARTS_AND_INFERRED
)
VIOLATIONS_ABSCONSION = next(
    metric
    for metric in metric_config.VIOLATIONS_BY_TYPE_METRICS
    if metric.name == "violations_absconsion"
)
