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
"""Aggregated metric definitions used in tests"""
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentDaysToFirstEventMetric,
    AssignmentEventBinaryMetric,
    AssignmentEventCountMetric,
    AssignmentSpanDaysMetric,
    AssignmentSpanMaxDaysMetric,
    DailyAvgSpanCountMetric,
    DailyAvgSpanValueMetric,
    EventCountMetric,
)
from recidiviz.observations.event_selector import EventSelector
from recidiviz.observations.event_type import EventType
from recidiviz.observations.span_selector import SpanSelector
from recidiviz.observations.span_type import SpanType

MY_DRUG_SCREENS_METRIC = EventCountMetric(
    name="my_drug_screens",
    display_name="My Drug Screens",
    description="Number of my drug screens",
    event_selector=EventSelector(
        event_type=EventType.DRUG_SCREEN,
        event_conditions_dict={},
    ),
)

MY_CONTACTS_ATTEMPTED_METRIC = EventCountMetric(
    name="my_contacts_attempted",
    display_name="Contacts: Attempted",
    description="Number of attempted contacts",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_CONTACT,
        event_conditions_dict={"status": ["ATTEMPTED"]},
    ),
)

MY_CONTACTS_COMPLETED_METRIC = EventCountMetric(
    name="my_contacts_completed",
    display_name="Contacts: Completed",
    description="Number of completed contacts",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_CONTACT,
        event_conditions_dict={"status": ["COMPLETED"]},
    ),
)

MY_LOGINS_BY_PRIMARY_WORKFLOWS = EventCountMetric(
    name="my_logins_primary_workflows_user",
    display_name="My Logins, Primary Workflows Users",
    description="Number of logins performed by primary Workflows users",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_USER_LOGIN,
        event_conditions_dict={},
    ),
)

MY_AVG_DAILY_POPULATION = DailyAvgSpanCountMetric(
    name="my_avg_daily_population",
    display_name="My Average Population",
    description="My Average daily count of clients in the population",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={},
    ),
)

MY_AVG_DAILY_POPULATION_GENERAL_INCARCERATION = DailyAvgSpanCountMetric(
    name="my_avg_population_general_incarceration",
    display_name="My Average Population: General Incarceration",
    description="My Average daily count of clients in general incarceration",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": ["INCARCERATION"],
            "compartment_level_2": ["GENERAL"],
        },
    ),
)

MY_TASK_COMPLETIONS = EventCountMetric(
    name="my_task_completions",
    display_name="My Task Completions",
    description="My count of task completions",
    event_selector=EventSelector(
        event_type=EventType.TASK_COMPLETED,
        event_conditions_dict={},
    ),
)

MY_AVG_LSIR_SCORE = DailyAvgSpanValueMetric(
    name="my_avg_lsir_score",
    display_name="My Average LSI-R Score",
    description="My Average daily LSI-R score of the population",
    span_selector=SpanSelector(
        span_type=SpanType.ASSESSMENT_SCORE_SESSION,
        span_conditions_dict={"assessment_type": ["LSIR"]},
    ),
    span_value_numeric="assessment_score",
)


MY_ANY_INCARCERATION_365 = AssignmentEventBinaryMetric(
    name="my_any_incarceration_365",
    display_name="My Any Incarceration Start Within 1 Year of Assignment",
    description="My number of client assignments followed by an incarceration start "
    "within 1 year",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_START,
        event_conditions_dict={},
    ),
)

MY_DAYS_TO_FIRST_INCARCERATION_100 = AssignmentDaysToFirstEventMetric(
    name="my_days_to_first_incarceration_100",
    display_name="My Days To First Incarceration Within 1 Year After Assignment",
    description="My sum of the number of days prior to first incarceration within 1 "
    "year following assignment, for all assignments during the analysis period. Only "
    "counts incarcerations following supervision.",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_START,
        event_conditions_dict={
            "inflow_from_level_1": ["SUPERVISION"],
        },
    ),
    window_length_days=100,
)

MY_EMPLOYER_CHANGES_365 = AssignmentEventCountMetric(
    name="my_employer_changes_365",
    display_name="My Employer Changes Within 1 Year Of Assignment",
    description="My number of times client starts employment with a new employer "
    "within 1 year of assignment",
    event_selector=EventSelector(
        event_type=EventType.EMPLOYMENT_PERIOD_START,
        event_conditions_dict={},
    ),
)

MY_DAYS_SUPERVISED_365 = AssignmentSpanDaysMetric(
    name="my_days_supervised_365",
    display_name="My Days Supervised Within 1 Year Of Assignment",
    description="My sum of the number of supervised days within 1 year following "
    "assignment, for all assignments during the analysis period",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={"compartment_level_1": ["SUPERVISION"]},
    ),
    window_length_days=365,
)

MY_DAYS_AT_LIBERTY_365 = AssignmentSpanDaysMetric(
    name="my_days_at_liberty_365",
    display_name="My Days At Liberty Within 1 Year Of Assignment",
    description="My sum of the number of days spent at liberty within 1 year following "
    "assignment, for all assignments during the analysis period",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={"compartment_level_1": ["LIBERTY"]},
    ),
    window_length_days=365,
)

MY_MAX_DAYS_STABLE_EMPLOYMENT_365 = AssignmentSpanMaxDaysMetric(
    name="my_max_days_stable_employment_365",
    display_name="My Maximum Days Stable Employment Within 1 Year of Assignment",
    description="My number of days in the longest stretch of continuous stable "
    "employment (same employer and job) within 1 year of assignment",
    span_selector=SpanSelector(
        span_type=SpanType.EMPLOYMENT_PERIOD,
        span_conditions_dict={},
    ),
    window_length_days=365,
)
