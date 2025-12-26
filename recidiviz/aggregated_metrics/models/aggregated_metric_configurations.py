# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Defines AggregatedMetric objects"""
from typing import Dict, List, Union

from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentCountMetric,
    AssignmentDaysToFirstEventMetric,
    AssignmentEventBinaryMetric,
    AssignmentEventCountMetric,
    AssignmentSpanDaysMetric,
    AssignmentSpanMaxDaysMetric,
    AssignmentSpanValueAtStartMetric,
    DailyAvgSpanCountMetric,
    DailyAvgSpanValueMetric,
    DailyAvgTimeSinceSpanStartMetric,
    EventCountMetric,
    EventDistinctUnitCountMetric,
    EventValueMetric,
    SpanDistinctUnitCountMetric,
    SumSpanDaysMetric,
)
from recidiviz.calculator.query.state.views.analyst_data.insights_caseload_category_sessions import (
    CASELOAD_CATEGORIES_BY_CATEGORY_TYPE,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_person_events import (
    USAGE_EVENTS_DICT,
)
from recidiviz.calculator.query.state.views.sessions.justice_impact_sessions import (
    JusticeImpactType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.str_field_utils import snake_to_title
from recidiviz.observations.event_selector import EventSelector
from recidiviz.observations.event_type import EventType
from recidiviz.observations.span_selector import SpanSelector
from recidiviz.observations.span_type import SpanType
from recidiviz.task_eligibility.task_completion_event_big_query_view_collector import (
    TaskCompletionEventBigQueryViewCollector,
)
from recidiviz.workflows.types import WorkflowsSystemType

_VIOLATION_CATEGORY_TO_TYPES_DICT: Dict[str, List[str]] = {
    "ABSCONSION": [StateSupervisionViolationType.ABSCONDED.name],
    "NEW_CRIME": [
        StateSupervisionViolationType.FELONY.name,
        StateSupervisionViolationType.LAW.name,
        StateSupervisionViolationType.MISDEMEANOR.name,
    ],
    "TECHNICAL": [StateSupervisionViolationType.TECHNICAL.name],
    "UNKNOWN": [
        StateSupervisionViolationType.INTERNAL_UNKNOWN.name,
        StateSupervisionViolationType.EXTERNAL_UNKNOWN.name,
    ],
}

_NON_ABSCONSION_VIOLATION_TYPES = [
    violation_type
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
    if category != "ABSCONSION"
    for violation_type in types
]

_HOUSING_UNIT_TYPES = [
    "TEMPORARY_SOLITARY_CONFINEMENT",
    "DISCIPLINARY_SOLITARY_CONFINEMENT",
    "ADMINISTRATIVE_SOLITARY_CONFINEMENT",
    "OTHER_SOLITARY_CONFINEMENT",
    "MENTAL_HEALTH_SOLITARY_CONFINEMENT",
    "PROTECTIVE_CUSTODY",
]

ABSCONSIONS_BENCH_WARRANTS = EventCountMetric(
    name="absconsions_bench_warrants",
    display_name="Absconsions/Bench Warrants",
    description="Number of absconsions or bench warrants",
    event_selector=EventSelector(
        event_type=EventType.ABSCONSION_BENCH_WARRANT,
        event_conditions_dict={},
    ),
)

ABSCONSIONS_BENCH_WARRANTS_FROM_PAROLE = EventCountMetric(
    name="absconsions_bench_warrants_from_parole",
    display_name="Absconsions/Bench Warrants From Parole",
    description="Number of absconsions or bench warrants from parole",
    event_selector=EventSelector(
        event_type=EventType.ABSCONSION_BENCH_WARRANT,
        event_conditions_dict={
            "inflow_from_level_2": ["PAROLE"],
        },
    ),
)

ABSCONSIONS_BENCH_WARRANTS_FROM_PROBATION = EventCountMetric(
    name="absconsions_bench_warrants_from_probation",
    display_name="Absconsions/Bench Warrants From Probation",
    description="Number of absconsions or bench warrants from probation",
    event_selector=EventSelector(
        event_type=EventType.ABSCONSION_BENCH_WARRANT,
        event_conditions_dict={
            "inflow_from_level_2": ["PROBATION"],
        },
    ),
)

ANY_INCARCERATION_365 = AssignmentEventBinaryMetric(
    name="any_incarceration_365",
    display_name="Any Incarceration Start Within 1 Year of Assignment",
    description="Number of client assignments followed by an incarceration start "
    "within 1 year",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_START,
        event_conditions_dict={},
    ),
)

ASSIGNMENTS = AssignmentCountMetric(
    name="assignments",
    display_name="Assignments",
    description="Number of client assignments",
    span_selector=SpanSelector(
        span_type=SpanType.PERSON_DEMOGRAPHICS,
        span_conditions_dict={},
    ),
)

AVG_AGE = DailyAvgTimeSinceSpanStartMetric(
    name="avg_age",
    display_name="Average Age",
    description="Average daily age of the population",
    span_selector=SpanSelector(
        span_type=SpanType.PERSON_DEMOGRAPHICS,
        span_conditions_dict={},
    ),
    scale_to_year=True,
)

AVG_CRITICAL_CASELOAD_SIZE = DailyAvgSpanValueMetric(
    name="avg_critical_caseload_size",
    display_name="Average Critical Officer Caseload",
    description="Average count of clients in the officer's caseload among days when "
    "officer has critical caseload size",
    span_selector=SpanSelector(
        span_type=SpanType.SUPERVISION_OFFICER_CASELOAD_COUNT_SPAN,
        span_conditions_dict={
            "active_caseload_count_above_critical_threshold": ["true"]
        },
    ),
    span_value_numeric="active_caseload_count",
)

AVG_DAILY_CASELOAD_OFFICER = DailyAvgSpanValueMetric(
    name="avg_daily_caseload_officer",
    display_name="Average Daily Caseload Per Officer",
    description="Average of the daily population size on each officer caseload",
    span_selector=SpanSelector(
        span_type=SpanType.SUPERVISION_OFFICER_CASELOAD_COUNT_SPAN,
        span_conditions_dict={},
    ),
    span_value_numeric="active_caseload_count",
)

AVG_DAILY_POPULATION = DailyAvgSpanCountMetric(
    name="avg_daily_population",
    display_name="Average Population",
    description="Average daily count of clients in the population",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={},
    ),
)

AVG_DAILY_POPULATION_COMMUNITY_CONFINEMENT = DailyAvgSpanCountMetric(
    name="avg_population_community_confinement",
    display_name="Average Population: Community Confinement",
    description="Average daily count of clients in community confinement",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": ["INCARCERATION", "SUPERVISION"],
            "compartment_level_2": ["COMMUNITY_CONFINEMENT"],
        },
    ),
)

AVG_DAILY_POPULATION_DOMESTIC_VIOLENCE_CASE_TYPE = DailyAvgSpanCountMetric(
    name="avg_population_domestic_violence_case_type",
    display_name="Average Population: Domestic Violence Case Type",
    description="Average daily count of clients on supervision with a domestic "
    "violence case type",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": ["SUPERVISION"],
            "case_type_start": ["DOMESTIC_VIOLENCE"],
        },
    ),
)

AVG_DAILY_POPULATION_DRUG_CASE_TYPE = DailyAvgSpanCountMetric(
    name="avg_population_drug_case_type",
    display_name="Average Population: Drug Case Type",
    description="Average daily count of clients on supervision with a drug case type",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": ["SUPERVISION"],
            "case_type_start": ["DRUG_COURT"],
        },
    ),
)

AVG_DAILY_POPULATION_EMPLOYED = DailyAvgSpanCountMetric(
    name="avg_population_employed",
    display_name="Average Population With Employment",
    description="Average daily count of clients with some form of employment or "
    "alternate occupation/status",
    span_selector=SpanSelector(
        span_type=SpanType.EMPLOYMENT_STATUS_SESSION,
        span_conditions_dict={"is_employed": ["true"]},
    ),
)

AVG_DAILY_POPULATION_FEMALE = DailyAvgSpanCountMetric(
    name="avg_population_female",
    display_name="Average Population: Female",
    description="Average daily count of female clients in the population",
    span_selector=SpanSelector(
        span_type=SpanType.PERSON_DEMOGRAPHICS,
        span_conditions_dict={"sex": ["FEMALE"]},
    ),
)

AVG_DAILY_POPULATION_GENERAL_CASE_TYPE = DailyAvgSpanCountMetric(
    name="avg_population_general_case_type",
    display_name="Average Population: General Case Type",
    description="Average daily count of clients on supervision with a general case type",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": ["SUPERVISION"],
            "case_type_start": ["GENERAL"],
        },
    ),
)

AVG_DAILY_POPULATION_GENERAL_INCARCERATION = DailyAvgSpanCountMetric(
    name="avg_population_general_incarceration",
    display_name="Average Population: General Incarceration",
    description="Average daily count of clients in general incarceration",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": ["INCARCERATION"],
            "compartment_level_2": ["GENERAL"],
        },
    ),
)

# TODO(#39399): This metric assumes that someone would only have one 'RISK' assessment
# span at once, but it's possible for someone to have multiple 'RISK' spans in the
# `ASSESSMENT_SCORE_SESSION` span type (if the assessments are of different types). We
# should properly aggregate/deduplicate as needed for this metric.
AVG_DAILY_POPULATION_HIGH_RISK_LEVEL = DailyAvgSpanCountMetric(
    name="avg_population_high_risk_level",
    display_name="Average Population: High Risk Level",
    description="Average daily count of clients with a high assessed risk level",
    span_selector=SpanSelector(
        span_type=SpanType.ASSESSMENT_SCORE_SESSION,
        span_conditions_dict={
            "assessment_class": ["RISK"],
            "assessment_level": ["HIGH", "MEDIUM_HIGH", "MAXIMUM", "VERY_HIGH"],
        },
    ),
)

AVG_DAILY_POPULATION_LIMITED_SUPERVISION_JUSTICE_IMPACT = DailyAvgSpanCountMetric(
    name="avg_population_limited_supervision_justice_impact",
    display_name="Average Population: Limited Supervision (Justice Impact Type)",
    description="Average daily count of clients on limited supervision, "
    "mutually exclusive from other justice impact types",
    span_selector=SpanSelector(
        span_type=SpanType.JUSTICE_IMPACT_SESSION,
        span_conditions_dict={
            "justice_impact_type": [JusticeImpactType.LIMITED_SUPERVISION.value],
        },
    ),
)

# TODO(#39399): This metric assumes that someone would only have one 'RISK' assessment
# span at once, but it's possible for someone to have multiple 'RISK' spans in the
# `ASSESSMENT_SCORE_SESSION` span type (if the assessments are of different types). We
# should properly aggregate/deduplicate as needed for this metric.
AVG_DAILY_POPULATION_LOW_RISK_LEVEL = DailyAvgSpanCountMetric(
    name="avg_population_low_risk_level",
    display_name="Average Population: Low Risk Level",
    description="Average daily count of clients with a low assessed risk level",
    span_selector=SpanSelector(
        span_type=SpanType.ASSESSMENT_SCORE_SESSION,
        span_conditions_dict={
            "assessment_class": ["RISK"],
            "assessment_level": ["LOW", "LOW_MEDIUM", "MINIMUM"],
        },
    ),
)

AVG_DAILY_POPULATION_MENTAL_HEALTH_CASE_TYPE = DailyAvgSpanCountMetric(
    name="avg_population_mental_health_case_type",
    display_name="Average Population: Mental Health Case Type",
    description="Average daily count of clients on supervision with a mental health "
    "case type",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": ["SUPERVISION"],
            "case_type_start": [
                "SERIOUS_MENTAL_ILLNESS_OR_DISABILITY",
                "MENTAL_HEALTH_COURT",
            ],
        },
    ),
)

AVG_DAILY_POPULATION_NONWHITE = DailyAvgSpanCountMetric(
    name="avg_population_nonwhite",
    display_name="Average Population: Non-White",
    description="Average daily count of non-white clients",
    span_selector=SpanSelector(
        span_type=SpanType.PERSON_DEMOGRAPHICS,
        span_conditions_dict={"prioritized_race_or_ethnicity": '!= "WHITE"'},
    ),
)

AVG_DAILY_POPULATION_NONLIMITED_SUPERVISION = DailyAvgSpanCountMetric(
    name="avg_population_nonlimited_supervision",
    display_name="Average Population: Non-limited Supervision",
    description="Average daily population of individuals with non-limited supervision",
    span_selector=SpanSelector(
        span_type=SpanType.SUPERVISION_LEVEL_SESSION,
        span_conditions_dict={"supervision_level": '!= "LIMITED"'},
    ),
)

AVG_DAILY_POPULATION_NONLIMITED_SUPERVISION_JUSTICE_IMPACT = DailyAvgSpanCountMetric(
    name="avg_population_nonlimited_supervision_justice_impact",
    display_name="Average Population: Non-limited Supervision (Justice Impact Type)",
    description="Average daily population of individuals on non-limited supervision, "
    "mutually exclusive from other justice impact types",
    span_selector=SpanSelector(
        span_type=SpanType.JUSTICE_IMPACT_SESSION,
        span_conditions_dict={
            "justice_impact_type": [JusticeImpactType.NONLIMITED_SUPERVISION.value]
        },
    ),
)

AVG_DAILY_POPULATION_OTHER_CASE_TYPE = DailyAvgSpanCountMetric(
    name="avg_population_other_case_type",
    display_name="Average Population: Other Case Type",
    description="Average daily count of clients on supervision with other case type",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": ["SUPERVISION"],
            "case_type_start": """NOT IN (
    "GENERAL", "DOMESTIC_VIOLENCE", "SEX_OFFENSE", "DRUG_COURT",
    "SERIOUS_MENTAL_ILLNESS_OR_DISABILITY", "MENTAL_HEALTH_COURT"
)""",
        },
    ),
)

AVG_DAILY_POPULATION_PAROLE = DailyAvgSpanCountMetric(
    name="avg_population_parole",
    display_name="Average Population: Parole",
    description="Average daily count of clients on parole",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": ["SUPERVISION"],
            "compartment_level_2": ["PAROLE", "DUAL"],
        },
    ),
)

AVG_DAILY_POPULATION_PAROLE_BOARD_HOLD = DailyAvgSpanCountMetric(
    name="avg_population_parole_board_hold",
    display_name="Average Population: Parole Board Hold",
    description="Average daily count of clients in a parole board hold",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": ["INCARCERATION"],
            "compartment_level_2": ["PAROLE_BOARD_HOLD"],
        },
    ),
)

AVG_DAILY_POPULATION_PAST_FULL_TERM_RELEASE_DATE = DailyAvgSpanCountMetric(
    name="avg_population_past_full_term_release_date",
    display_name="Average Population: Past Full Term Release Date",
    description="Average daily count of clients beyond their full term release date",
    span_selector=SpanSelector(
        span_type=SpanType.TASK_CRITERIA_SPAN,
        span_conditions_dict={
            "criteria": [
                "INCARCERATION_PAST_FULL_TERM_RELEASE_DATE",
                "SUPERVISION_PAST_FULL_TERM_RELEASE_DATE",
            ],
            "meets_criteria": ["true"],
        },
    ),
)

AVG_DAILY_POPULATION_PAST_FULL_TERM_RELEASE_DATE_SUPERVISION = DailyAvgSpanCountMetric(
    name="avg_population_past_full_term_release_date_supervision",
    display_name="Average Supervision Population: Past Full Term Release Date",
    description="Average daily count of clients beyond their full term release date",
    span_selector=SpanSelector(
        span_type=SpanType.TASK_CRITERIA_SPAN,
        span_conditions_dict={
            "criteria": [
                "SUPERVISION_PAST_FULL_TERM_RELEASE_DATE",
            ],
            "meets_criteria": ["true"],
        },
    ),
)

AVG_DAILY_POPULATION_PAST_FULL_TERM_RELEASE_DATE_INCARCERATION = DailyAvgSpanCountMetric(
    name="avg_population_past_full_term_release_date_incarceration",
    display_name="Average Incarceration Population: Past Full Term Release Date",
    description="Average daily count of residents beyond their full term release date",
    span_selector=SpanSelector(
        span_type=SpanType.TASK_CRITERIA_SPAN,
        span_conditions_dict={
            "criteria": [
                "INCARCERATION_PAST_FULL_TERM_RELEASE_DATE",
            ],
            "meets_criteria": ["true"],
        },
    ),
)

AVG_DAILY_POPULATION_PAST_PAROLE_ELIGIBILITY_DATE = DailyAvgSpanCountMetric(
    name="avg_population_past_parole_eligibility_date",
    display_name="Average Population: Past Parole Eligibility Date",
    description="Average daily count of clients beyond their parole eligibility date",
    span_selector=SpanSelector(
        span_type=SpanType.TASK_CRITERIA_SPAN,
        span_conditions_dict={
            "criteria": ["INCARCERATION_PAST_PAROLE_ELIGIBILITY_DATE"],
            "meets_criteria": ["true"],
        },
    ),
)

AVG_DAILY_POPULATION_PROBATION = DailyAvgSpanCountMetric(
    name="avg_population_probation",
    display_name="Average Population: Probation",
    description="Average daily count of clients on probation",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": ["SUPERVISION"],
            "compartment_level_2": ["PROBATION"],
        },
    ),
)

AVG_DAILY_POPULATION_SEX_OFFENSE_CASE_TYPE = DailyAvgSpanCountMetric(
    name="avg_population_sex_offense_case_type",
    display_name="Average Population: Sex Offense Case Type",
    description="Average daily count of clients on supervision with a sex offense case "
    "type",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": ["SUPERVISION"],
            "case_type_start": ["SEX_OFFENSE"],
        },
    ),
)

AVG_DAILY_POPULATION_SHOCK_INCARCERATION = DailyAvgSpanCountMetric(
    name="avg_population_shock_incarceration",
    display_name="Average Population: Shock Incarceration",
    description="Average daily count of clients in shock incarceration",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": ["INCARCERATION"],
            "compartment_level_2": ["SHOCK_INCARCERATION"],
        },
    ),
)

AVG_DAILY_POPULATION_SOLITARY_CONFINEMENT = DailyAvgSpanCountMetric(
    name="avg_population_solitary_confinement",
    display_name="Average Population: Solitary Confinement",
    description="Average daily population of individuals in solitary confinement",
    span_selector=SpanSelector(
        span_type=SpanType.HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSION,
        span_conditions_dict={
            "housing_unit_type_collapsed_solitary": ["SOLITARY_CONFINEMENT"],
        },
    ),
)


AVG_DAILY_POPULATION_STATE_PRISON = DailyAvgSpanCountMetric(
    name="avg_population_state_prison",
    display_name="Average Population: State Prison location type",
    description="Average daily count of people in state prison",
    span_selector=SpanSelector(
        span_type=SpanType.LOCATION_TYPE_SESSION,
        span_conditions_dict={"location_type": ["STATE_PRISON"]},
    ),
)

AVG_DAILY_POPULATION_HOUSING_TYPE_METRICS = [
    DailyAvgSpanCountMetric(
        name=f"avg_population_{housing_type.lower()}",
        display_name=f"Average Population: {snake_to_title(housing_type)}",
        description=f"Average daily count of residents in {snake_to_title(housing_type)}",
        span_selector=SpanSelector(
            span_type=SpanType.HOUSING_TYPE_SESSION,
            span_conditions_dict={"housing_unit_type": [housing_type]},
        ),
    )
    for housing_type in _HOUSING_UNIT_TYPES
]

AVG_ACTIVE_LENGTH_OF_STAY_SOLITARY_CONFINEMENT = DailyAvgTimeSinceSpanStartMetric(
    name="avg_active_length_of_stay_solitary_confinement",
    display_name="Average Active Length of Stay: Solitary Confinement",
    description="Average daily active length of stay in solitary confinement",
    span_selector=SpanSelector(
        span_type=SpanType.HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSION,
        span_conditions_dict={
            "housing_unit_type_collapsed_solitary": ["SOLITARY_CONFINEMENT"],
        },
    ),
)

AVG_ACTIVE_LENGTH_OF_STAY_HOUSING_UNIT_TYPE_METRICS = [
    DailyAvgTimeSinceSpanStartMetric(
        name=f"avg_active_length_of_stay_{housing_type.lower()}",
        display_name=f"Average Active Length of Stay: {snake_to_title(housing_type)}",
        description=f"Average daily active length of stay in {snake_to_title(housing_type)}",
        span_selector=SpanSelector(
            span_type=SpanType.HOUSING_TYPE_SESSION,
            span_conditions_dict={"housing_unit_type": [housing_type]},
        ),
    )
    for housing_type in _HOUSING_UNIT_TYPES
]

AVG_DAILY_POPULATION_SOLITARY_CONFINEMENT_JUSTICE_IMPACT = DailyAvgSpanCountMetric(
    name="avg_population_solitary_confinement_justice_impact",
    display_name="Average Population: Solitary Confinement (Justice Impact Type)",
    description="Average daily population of individuals in solitary confinement, "
    "mutually exclusive from other justice impact types",
    span_selector=SpanSelector(
        span_type=SpanType.JUSTICE_IMPACT_SESSION,
        span_conditions_dict={
            "justice_impact_type": [JusticeImpactType.SOLITARY_CONFINEMENT.value]
        },
    ),
)

_SUPERVISION_LEVEL_SPAN_ATTRIBUTE_DICT: Dict[str, Union[str, List[str]]] = {
    "unsupervised": ["UNSUPERVISED"],
    "limited": ["LIMITED"],
    "minimum": ["MINIMUM"],
    "medium": ["MEDIUM"],
    "maximum": ["MAXIMUM"],
    "high": ["HIGH"],
    "other": 'NOT IN ("UNSUPERVISED", "LIMITED", "MINIMUM", "MEDIUM", "MAXIMUM", "HIGH", "EXTERNAL_UNKNOWN", "INTERNAL_UNKNOWN")',
    "unknown": 'IN ("EXTERNAL_UNKNOWN", "INTERNAL_UNKNOWN") OR supervision_level IS NULL',
}
AVG_DAILY_POPULATION_SUPERVISION_LEVEL_METRICS = [
    DailyAvgSpanCountMetric(
        name=f"avg_population_{level}_supervision_level",
        display_name=f"Average Population: {level.capitalize()} Supervision Level",
        description=f"Average daily count of clients with "
        f"{'an' if level[0] in 'aeiou' else 'a'} {level} supervision level",
        span_selector=SpanSelector(
            span_type=SpanType.SUPERVISION_LEVEL_SESSION,
            span_conditions_dict={"supervision_level": conditions},
        ),
    )
    for level, conditions in _SUPERVISION_LEVEL_SPAN_ATTRIBUTE_DICT.items()
]

DEDUPED_TASK_COMPLETION_EVENT_VB = []
preprocessed_task_types = set()
for b in TaskCompletionEventBigQueryViewCollector().collect_view_builders():
    if b.task_type_name not in preprocessed_task_types:
        preprocessed_task_types.add(b.task_type_name)
        DEDUPED_TASK_COMPLETION_EVENT_VB.append(b)

AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_INCARCERATION = [
    DailyAvgSpanCountMetric(
        name=f"avg_population_task_eligible_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Eligible, {b.task_title}",
        description="Average daily count of clients eligible for task of "
        f"type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.TASK_ELIGIBILITY_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION = [
    DailyAvgSpanCountMetric(
        name=f"avg_population_task_eligible_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Eligible, {b.task_title}",
        description="Average daily count of clients eligible for task of "
        f"type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.TASK_ELIGIBILITY_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

AVG_DAILY_POPULATION_TREATMENT_IN_PRISON = DailyAvgSpanCountMetric(
    name="avg_population_treatment_in_prison",
    display_name="Average Population: Treatment In Prison",
    description="Average daily count of clients in treatment-in-prison incarceration",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": ["INCARCERATION"],
            "compartment_level_2": ["TREATMENT_IN_PRISON"],
        },
    ),
)

_CUSTODY_LEVEL_SPAN_ATTRIBUTE_DICT: Dict[str, Union[str, List[str]]] = {
    "min": ["MINIMUM"],
    "medium": ["MEDIUM"],
    "max": ["MAXIMUM"],
    "close": ["CLOSE"],
    "other": 'NOT IN ("MINIMUM", "MEDIUM", "MAXIMUM", "CLOSE", "EXTERNAL_UNKNOWN", "INTERNAL_UNKNOWN")',
    "unknown": 'IN ("EXTERNAL_UNKNOWN", "INTERNAL_UNKNOWN") OR custody_level IS NULL',
}
AVG_DAILY_POPULATION_CUSTODY_LEVEL_METRICS = [
    DailyAvgSpanCountMetric(
        name=f"avg_population_{level}_custody",
        display_name=f"Average Population: {level.capitalize()} Custody Level",
        description=f"Average daily count of residents with "
        f"{'an' if level[0] in 'aeiou' else 'a'} {level} custody level",
        span_selector=SpanSelector(
            span_type=SpanType.CUSTODY_LEVEL_SESSION,
            span_conditions_dict={"custody_level": conditions},
        ),
    )
    for level, conditions in _CUSTODY_LEVEL_SPAN_ATTRIBUTE_DICT.items()
]

AVG_DAILY_POPULATION_UNKNOWN_CASE_TYPE = DailyAvgSpanCountMetric(
    name="avg_population_unknown_case_type",
    display_name="Average Population: Unknown Case Type",
    description="Average daily count of clients on supervision with unknown case type",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": ["SUPERVISION"],
            "case_type_start": "IS NULL",
        },
    ),
)

AVG_DAILY_POPULATION_MAXIMUM_CUSTODY_JUSTICE_IMPACT = DailyAvgSpanCountMetric(
    name="avg_population_max_custody_justice_impact",
    display_name="Average Population: Maximum Custody (Justice Impact Type)",
    description="Average daily population of individuals in maximum custody, mutually "
    "exclusive from other justice impact types",
    span_selector=SpanSelector(
        span_type=SpanType.JUSTICE_IMPACT_SESSION,
        span_conditions_dict={
            "justice_impact_type": [JusticeImpactType.MAXIMUM_CUSTODY.value]
        },
    ),
)

AVG_DAILY_POPULATION_MEDIUM_CUSTODY_JUSTICE_IMPACT = DailyAvgSpanCountMetric(
    name="avg_population_medium_custody_justice_impact",
    display_name="Average Population: Medium Custody (Justice Impact Type)",
    description="Average daily population of individuals in medium custody, mutually "
    "exclusive from other justice impact types",
    span_selector=SpanSelector(
        span_type=SpanType.JUSTICE_IMPACT_SESSION,
        span_conditions_dict={
            "justice_impact_type": [JusticeImpactType.MEDIUM_CUSTODY.value],
        },
    ),
)

AVG_DAILY_POPULATION_MINIMUM_CUSTODY_JUSTICE_IMPACT = DailyAvgSpanCountMetric(
    name="avg_population_min_custody_justice_impact",
    display_name="Average Population: Minimum Custody (Justice Impact Type)",
    description="Average daily population of individuals in minimum custody, mutually "
    "exclusive from other justice impact types",
    span_selector=SpanSelector(
        span_type=SpanType.JUSTICE_IMPACT_SESSION,
        span_conditions_dict={
            "justice_impact_type": [JusticeImpactType.MINIMUM_CUSTODY.value],
        },
    ),
)

AVG_DAILY_POPULATION_ASSESSMENT_REQUIRED = DailyAvgSpanCountMetric(
    name="avg_population_assessment_required",
    display_name="Average Population: Clients Requiring Risk Assessment",
    description="Average daily population of clients requiring a risk assessment based on their "
    "supervision level",
    span_selector=SpanSelector(
        span_type=SpanType.SUPERVISION_ASSESSMENT_COMPLIANCE_SPAN,
        span_conditions_dict={"assessment_required": ["true"]},
    ),
)

AVG_DAILY_POPULATION_ASSESSMENT_OVERDUE = DailyAvgSpanCountMetric(
    name="avg_population_assessment_overdue",
    display_name="Average Population: Clients Requiring Risk Assessment Whose Assessments Are Overdue",
    description="Average daily population of clients requiring a risk assessment based on their "
    "supervision level who are overdue to receive one",
    span_selector=SpanSelector(
        span_type=SpanType.SUPERVISION_ASSESSMENT_COMPLIANCE_SPAN,
        span_conditions_dict={
            "assessment_required": ["true"],
            "assessment_overdue": ["true"],
        },
    ),
)

AVG_DAILY_POPULATION_CONTACT_REQUIRED = DailyAvgSpanCountMetric(
    name="avg_population_contact_required",
    display_name="Average Population: Clients Requiring A Face-To-Face Contact (Legacy Logic)",
    description="Average daily population of clients requiring a face-to-face contact based on "
    "their supervision level, according to legacy case compliance logic",
    span_selector=SpanSelector(
        span_type=SpanType.SUPERVISION_CONTACT_COMPLIANCE_SPAN,
        span_conditions_dict={"contact_required": ["true"]},
    ),
)

AVG_DAILY_POPULATION_CONTACT_OVERDUE = DailyAvgSpanCountMetric(
    name="avg_population_contact_overdue",
    display_name="Average Population: Clients Requiring Face-To-Face-Contact Whose Contacts Are "
    "Overdue (Legacy Logic)",
    description="Average daily population of clients requiring a face-to-face contact based on "
    "their supervision level who are overdue to receive one, according to legacy case compliance logic",
    span_selector=SpanSelector(
        span_type=SpanType.SUPERVISION_CONTACT_COMPLIANCE_SPAN,
        span_conditions_dict={
            "contact_required": ["true"],
            "contact_overdue": ["true"],
        },
    ),
)

AVG_DAILY_POPULATION_CONTACT_TASKS_ELIGIBLE = DailyAvgSpanCountMetric(
    name="avg_population_contact_tasks_eligible",
    display_name="Average Population: Contact Tasks Eligible",
    description="Average daily population of clients eligible for contact tasks",
    span_selector=SpanSelector(
        span_type=SpanType.TASKS_SCHEDULED_CONTACT_SPAN,
        span_conditions_dict={"is_eligible": ["true"]},
    ),
)

AVG_DAILY_POPULATION_CONTACT_TASKS_ELIGIBLE_WITH_SCHEDULED_APPOINTMENT = DailyAvgSpanCountMetric(
    name="avg_population_contact_tasks_eligible_with_scheduled_appointment",
    display_name="Average Population: Contact Tasks Eligible With Scheduled Appointment",
    description="Average daily population of clients eligible for contact tasks who have "
    "a scheduled appointment date for the contact",
    span_selector=SpanSelector(
        span_type=SpanType.TASKS_SCHEDULED_CONTACT_SPAN,
        span_conditions_dict={
            "has_scheduled_contact": ["true"],
            "is_eligible": ["true"],
        },
    ),
)

AVG_LSIR_SCORE = DailyAvgSpanValueMetric(
    name="avg_lsir_score",
    display_name="Average LSI-R Score",
    description="Average daily LSI-R score of the population",
    span_selector=SpanSelector(
        span_type=SpanType.ASSESSMENT_SCORE_SESSION,
        span_conditions_dict={"assessment_type": ["LSIR"]},
    ),
    span_value_numeric="assessment_score",
)

AVG_NUM_SUPERVISION_OFFICERS_INSIGHTS_CASELOAD_CATEGORY_METRICS = [
    DailyAvgSpanCountMetric(
        name=f"avg_num_supervision_officers_insights_{category_type.value.lower()}_category_type_{category.lower()}",
        display_name=f"Average Daily Count of Supervision Officers: {snake_to_title(category_type.value)} Insights category type, {snake_to_title(category)} category",
        description=f"""Average daily count of officers with the {snake_to_title(category)} category
        in the {snake_to_title(category_type.value)} Insights category type. When the unit of
        analysis is OFFICER, this counts the fraction of an officer that spent that time period
        with the given category for that category type, which is equivalent to the proportion of time
        in the analysis period they spent with that type.""",
        span_selector=SpanSelector(
            span_type=SpanType.INSIGHTS_SUPERVISION_OFFICER_CASELOAD_CATEGORY_SESSION,
            span_conditions_dict={
                "category_type": [category_type.value],
                "caseload_category": [category],
            },
        ),
    )
    for [category_type, categories] in CASELOAD_CATEGORIES_BY_CATEGORY_TYPE.items()
    for category in categories
]

COMMUNITY_CONFINEMENT_SUPERVISION_STARTS = EventCountMetric(
    name="community_confinement_supervision_starts",
    display_name="Community Confinement Supervision Starts",
    description="Number of transitions to community confinement (supervision) from "
    "general incarceration",
    event_selector=EventSelector(
        event_type=EventType.COMPARTMENT_LEVEL_2_START,
        event_conditions_dict={
            "compartment_level_1": ["SUPERVISION"],
            "compartment_level_2": ["COMMUNITY_CONFINEMENT"],
            # filters below prevent counting CC (re)starts from temporary incarceration
            "inflow_from_level_1": ["INCARCERATION"],
            "inflow_from_level_2": ["GENERAL"],
        },
    ),
)

CONTACTS_ATTEMPTED = EventCountMetric(
    name="contacts_attempted",
    display_name="Contacts: Attempted",
    description="Number of attempted contacts",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_CONTACT,
        event_conditions_dict={"status": ["ATTEMPTED"]},
    ),
)

CONTACTS_COMPLETED = EventCountMetric(
    name="contacts_completed",
    display_name="Contacts: Completed",
    description="Number of completed contacts",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_CONTACT,
        event_conditions_dict={"status": ["COMPLETED"]},
    ),
)

CONTACTS_FACE_TO_FACE = EventCountMetric(
    name="contacts_face_to_face",
    display_name="Contacts: Face-To-Face",
    description="Number of completed face-to-face contacts",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_CONTACT,
        event_conditions_dict={
            "status": ["COMPLETED"],
            "contact_type": ["DIRECT", "BOTH_COLLATERAL_AND_DIRECT"],
        },
    ),
)

CONTACTS_HOME_VISIT = EventCountMetric(
    name="contacts_home_visit",
    display_name="Contacts: Home Visit",
    description="Number of completed home visit contacts",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_CONTACT,
        event_conditions_dict={
            "status": ["COMPLETED"],
            "location": ["RESIDENCE"],
            "contact_type": ["DIRECT", "BOTH_COLLATERAL_AND_DIRECT"],
        },
    ),
)

CONTACT_DUE_DATES = EventCountMetric(
    name="contact_due_dates",
    display_name="Contact Due Dates",
    description="Number of contact due dates, counting distinct by date and type",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_CONTACT_DUE,
        event_conditions_dict={},
    ),
    event_segmentation_columns=["tasks_contact_type"],
)

CONTACT_DUE_DATES_MET = EventCountMetric(
    name="contact_due_dates_met",
    display_name="Contact Due Dates Met",
    description="Number of contact due dates for which all requirements were completed prior to due date, counting distinct by date and type",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_CONTACT_DUE,
        event_conditions_dict={"contact_missed": ["false"]},
    ),
    event_segmentation_columns=["tasks_contact_type"],
)

CUSTODY_LEVEL_DOWNGRADES = EventCountMetric(
    name="custody_level_downgrades",
    display_name="Custody Level Downgrades",
    description="Number of changes to a lower custody level",
    event_selector=EventSelector(
        event_type=EventType.CUSTODY_LEVEL_CHANGE,
        event_conditions_dict={"change_type": ["DOWNGRADE"]},
    ),
)

CUSTODY_LEVEL_DOWNGRADES_TO_MINIMUM = EventCountMetric(
    name="custody_level_downgrades_to_minimum",
    display_name="Custody Level Downgrades to Minimum Custody",
    description="Number of changes to minimum custody level",
    event_selector=EventSelector(
        event_type=EventType.CUSTODY_LEVEL_CHANGE,
        event_conditions_dict={
            "change_type": ["DOWNGRADE"],
            "new_custody_level": ["MINIMUM"],
        },
    ),
)

CUSTODY_LEVEL_UPGRADES = EventCountMetric(
    name="custody_level_upgrades",
    display_name="Custody Level Upgrades",
    description="Number of changes to a higher custody level",
    event_selector=EventSelector(
        event_type=EventType.CUSTODY_LEVEL_CHANGE,
        event_conditions_dict={"change_type": ["UPGRADE"]},
    ),
)

DAYS_ABSCONDED_365 = AssignmentSpanDaysMetric(
    name="days_absconded_365",
    display_name="Days Absconded Within 1 Year Of Assignment",
    description="Sum of the number of days with absconsion or bench warrant status "
    "within 1 year following assignment, for all assignments during the analysis "
    "period",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={"compartment_level_2": ["ABSCONSION", "BENCH_WARRANT"]},
    ),
    window_length_days=365,
)

DAYS_AT_LIBERTY_365 = AssignmentSpanDaysMetric(
    name="days_at_liberty_365",
    display_name="Days At Liberty Within 1 Year Of Assignment",
    description="Sum of the number of days spent at liberty within 1 year following "
    "assignment, for all assignments during the analysis period",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={"compartment_level_1": ["LIBERTY"]},
    ),
    window_length_days=365,
)

DAYS_EMPLOYED_365 = AssignmentSpanDaysMetric(
    name="days_employed_365",
    display_name="Days Employed Within 1 Year Of Assignment",
    description="Sum of the number of days clients had valid employment status within "
    "1 year following assignment, for all assignments during the analysis period",
    span_selector=SpanSelector(
        span_type=SpanType.EMPLOYMENT_STATUS_SESSION,
        span_conditions_dict={"is_employed": ["true"]},
    ),
    window_length_days=365,
)

DAYS_IN_COMMUNITY_365 = AssignmentSpanDaysMetric(
    name="days_in_community_365",
    display_name="Days In Community Within 1 Year Of Assignment",
    description="Sum of the number of days spent in community (supervision or at "
    "liberty) within 1 year following assignment, for all assignments during the "
    "analysis period",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={"compartment_level_1": ["SUPERVISION", "LIBERTY"]},
    ),
    window_length_days=365,
)

DAYS_INCARCERATED_365 = AssignmentSpanDaysMetric(
    name="days_incarcerated_365",
    display_name="Days Incarcerated Within 1 Year Of Assignment",
    description="Sum of the number of incarcerated days within 1 year following "
    "assignment, for all assignments during the analysis period",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={"compartment_level_1": ["INCARCERATION"]},
    ),
    window_length_days=365,
)

DAYS_OUT_OF_STATE_365 = AssignmentSpanDaysMetric(
    name="days_out_of_state_365",
    display_name="Days Out of State Within 1 Year Of Assignment",
    description="Sum of the number of days incarcerated or supervised out of state "
    "within 1 year following assignment, for all assignments during the analysis "
    "period",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={
            "compartment_level_1": [
                "INCARCERATION_OUT_OF_STATE",
                "SUPERVISION_OUT_OF_STATE",
            ],
        },
    ),
    window_length_days=365,
)

DAYS_PENDING_CUSTODY_365 = AssignmentSpanDaysMetric(
    name="days_pending_custody_365",
    display_name="Days Pending Custody Within 1 Year Of Assignment",
    description="Sum of the number of days pending custody within 1 year following "
    "assignment, for all assignments during the analysis period",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={"compartment_level_1": ["PENDING_CUSTODY"]},
    ),
    window_length_days=365,
)

DAYS_SINCE_MOST_RECENT_LSIR = DailyAvgTimeSinceSpanStartMetric(
    name="avg_days_since_most_recent_lsir",
    display_name="Days Since Most Recent LSI-R",
    description="Average number of days since a client's most recent LSI-R assessment, "
    "across all days on which client is in population",
    span_selector=SpanSelector(
        span_type=SpanType.ASSESSMENT_SCORE_SESSION,
        span_conditions_dict={"assessment_type": ["LSIR"]},
    ),
)

DAYS_SUPERVISED_365 = AssignmentSpanDaysMetric(
    name="days_supervised_365",
    display_name="Days Supervised Within 1 Year Of Assignment",
    description="Sum of the number of supervised days within 1 year following "
    "assignment, for all assignments during the analysis period",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={"compartment_level_1": ["SUPERVISION"]},
    ),
    window_length_days=365,
)

DAYS_TO_FIRST_ABSCONSION_BENCH_WARRANT_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_absconsion_bench_warrant_365",
    display_name="Days To First Absconsion/Bench Warrant (Legal Status) Within 1 Year "
    "After Assignment",
    description="Sum of the number of days prior to first absconsion/bench warrant "
    "legal status within 1 year following assignment, for all assignments during the "
    "analysis period",
    event_selector=EventSelector(
        event_type=EventType.ABSCONSION_BENCH_WARRANT,
        event_conditions_dict={},
    ),
    window_length_days=365,
)

DAYS_TO_FIRST_INCARCERATION_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_incarceration_365",
    display_name="Days To First Incarceration Within 1 Year After Assignment",
    description="Sum of the number of days prior to first incarceration within 1 year "
    "following assignment, for all assignments during the analysis period",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_START,
        event_conditions_dict={},
    ),
    window_length_days=365,
)

DAYS_TO_FIRST_LIBERTY_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_liberty_365",
    display_name="Days To First Liberty Within 1 Year After Assignment",
    description="Sum of the number of days prior to first liberty transition within 1 "
    "year following assignment, for all assignments during the analysis period",
    event_selector=EventSelector(
        event_type=EventType.TRANSITIONS_TO_LIBERTY_ALL,
        event_conditions_dict={},
    ),
    window_length_days=365,
)

DAYS_TO_FIRST_SUPERVISION_START_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_supervision_start_365",
    display_name="Days To First Supervision Start Within 1 Year After Assignment",
    description="Sum of the number of days prior to first supervision start within 1 "
    "year following assignment, for all assignments during the analysis period",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_START,
        event_conditions_dict={},
    ),
    window_length_days=365,
)

DAYS_TO_FIRST_VIOLATION_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_violation_365",
    display_name="Days To First Violation Within 1 Year After Assignment",
    description="Sum of the number of days prior to first violation within 1 year "
    "following assignment, for all assignments during the analysis period",
    event_selector=EventSelector(
        event_type=EventType.VIOLATION,
        event_conditions_dict={},
    ),
    window_length_days=365,
)

DAYS_TO_FIRST_VIOLATION_365_BY_TYPE_METRICS = [
    AssignmentDaysToFirstEventMetric(
        name=f"days_to_first_violation_{category.lower()}_365",
        display_name=f"Days To First {category.replace('_', ' ').title()} Violation "
        "Within 1 Year After Assignment",
        description="Sum of the number of days prior to first "
        f"{category.replace('_', ' ').lower()} violation within 1 year following "
        "assignment, for all assignments during the analysis period",
        event_selector=EventSelector(
            event_type=EventType.VIOLATION,
            event_conditions_dict={"violation_type": types},
        ),
        window_length_days=365,
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
]

DAYS_TO_FIRST_VIOLATION_RESPONSE_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_violation_response_365",
    display_name="Days To First Violation Response Within 1 Year After Assignment",
    description="Sum of the number of days prior to first violation response within 1 "
    "year following assignment, for all assignments during the analysis period",
    event_selector=EventSelector(
        event_type=EventType.VIOLATION_RESPONSE,
        event_conditions_dict={},
    ),
    window_length_days=365,
)

DAYS_TO_FIRST_VIOLATION_RESPONSE_365_BY_TYPE_METRICS = [
    AssignmentDaysToFirstEventMetric(
        name=f"days_to_first_violation_response_{category.lower()}_365",
        display_name=f"Days To First {category.replace('_', ' ').title()} Violation "
        "Response Within 1 Year After Assignment",
        description="Sum of the number of days prior to first "
        f"{category.replace('_', ' ').lower()} violation response within 1 year "
        "following assignment, for all assignments during the analysis period",
        event_selector=EventSelector(
            event_type=EventType.VIOLATION_RESPONSE,
            event_conditions_dict={"most_serious_violation_type": types},
        ),
        window_length_days=365,
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
]

DRUG_SCREENS = EventCountMetric(
    name="drug_screens",
    display_name="Drug Screens",
    description="Number of drug screens",
    event_selector=EventSelector(
        event_type=EventType.DRUG_SCREEN,
        event_conditions_dict={},
    ),
)

DRUG_SCREENS_POSITIVE = EventCountMetric(
    name="drug_screens_positive",
    display_name="Drug Screens: Positive Result",
    description="Number of drug screens with a positive result",
    event_selector=EventSelector(
        event_type=EventType.DRUG_SCREEN,
        event_conditions_dict={"is_positive_result": ["true"]},
    ),
)

EARLY_DISCHARGE_REQUESTS = EventCountMetric(
    name="early_discharge_requests",
    display_name="Early Discharge Requests",
    description="Number of early discharge requests",
    event_selector=EventSelector(
        event_type=EventType.EARLY_DISCHARGE_REQUEST,
        event_conditions_dict={},
    ),
)

EMPLOYED_STATUS_ENDS = EventCountMetric(
    name="employed_status_ends",
    display_name="Employment Lost",
    description="Number of transitions to unemployment",
    event_selector=EventSelector(
        event_type=EventType.EMPLOYMENT_STATUS_CHANGE,
        event_conditions_dict={"is_employed": ["false"]},
    ),
)

EMPLOYED_STATUS_STARTS = EventCountMetric(
    name="employed_status_starts",
    display_name="Employment Gained",
    description="Number of new employment starts following unemployment",
    event_selector=EventSelector(
        event_type=EventType.EMPLOYMENT_STATUS_CHANGE,
        event_conditions_dict={"is_employed": ["true"]},
    ),
)

EMPLOYER_CHANGES_365 = AssignmentEventCountMetric(
    name="employer_changes_365",
    display_name="Employer Changes Within 1 Year Of Assignment",
    description="Number of times client starts employment with a new employer within 1 "
    "year of assignment",
    event_selector=EventSelector(
        event_type=EventType.EMPLOYMENT_PERIOD_START,
        event_conditions_dict={},
    ),
)

HOUSING_UNIT_TYPE_ENDS = [
    EventCountMetric(
        name=f"housing_unit_type_end_{housing_unit_type.lower()}",
        display_name=f"Housing Unit Type Ends: {snake_to_title(housing_unit_type)}",
        description=f"Number of transfers to {snake_to_title(housing_unit_type)}",
        event_selector=EventSelector(
            event_type=EventType.HOUSING_UNIT_TYPE_END,
            event_conditions_dict={"housing_unit_type": [housing_unit_type]},
        ),
    )
    for housing_unit_type in _HOUSING_UNIT_TYPES
]

HOUSING_UNIT_TYPE_STARTS = [
    EventCountMetric(
        name=f"housing_unit_type_start_{housing_unit_type.lower()}",
        display_name=f"Housing Unit Type Starts: {snake_to_title(housing_unit_type)}",
        description=f"Number of transfers to {snake_to_title(housing_unit_type)}",
        event_selector=EventSelector(
            event_type=EventType.HOUSING_UNIT_TYPE_START,
            event_conditions_dict={"housing_unit_type": [housing_unit_type]},
        ),
    )
    for housing_unit_type in _HOUSING_UNIT_TYPES
]

HOUSING_UNIT_TYPE_LENGTH_OF_STAY_BY_END = [
    EventValueMetric(
        name=f"housing_unit_type_length_of_stay_end_{housing_unit_type.lower()}",
        display_name=f"Housing Unit Type Length of Stay by end date: {snake_to_title(housing_unit_type)}",
        description=f"Length of stay in {snake_to_title(housing_unit_type)} in days, by end date",
        event_selector=EventSelector(
            event_type=EventType.HOUSING_UNIT_TYPE_END,
            event_conditions_dict={"housing_unit_type": [housing_unit_type]},
        ),
        event_value_numeric="length_of_stay",
        event_count_metric=next(
            metric
            for metric in HOUSING_UNIT_TYPE_ENDS
            if metric.name.endswith(housing_unit_type.lower())
        ),
    )
    for housing_unit_type in _HOUSING_UNIT_TYPES
]

HOUSING_UNIT_TYPE_LENGTH_OF_STAY_BY_START = [
    EventValueMetric(
        name=f"housing_unit_type_length_of_stay_start_{housing_unit_type.lower()}",
        display_name=f"Housing Unit Type Length of Stay, by start date: {snake_to_title(housing_unit_type)}",
        description=f"Length of stay in {snake_to_title(housing_unit_type)} in days, aggregated by start date, for periods that have ended",
        event_selector=EventSelector(
            event_type=EventType.HOUSING_UNIT_TYPE_START,
            event_conditions_dict={
                "housing_unit_type": [housing_unit_type],
                "is_active": ["false"],
            },
        ),
        event_value_numeric="length_of_stay",
        event_count_metric=next(
            metric
            for metric in HOUSING_UNIT_TYPE_STARTS
            if metric.name.endswith(housing_unit_type.lower())
        ),
    )
    for housing_unit_type in _HOUSING_UNIT_TYPES
]

INCARCERATIONS_INFERRED = EventCountMetric(
    name="incarcerations_inferred",
    display_name="Inferred Incarcerations",
    description="Number of inferred incarceration events that do not align with an "
    "observed discretionary incarceration session start",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON,
        event_conditions_dict={},
    ),
)

INCARCERATIONS_INFERRED_WITH_VIOLATION_TYPE_METRICS = [
    EventCountMetric(
        name=f"incarcerations_inferred_{category.lower()}_violation",
        display_name=f"Inferred Incarcerations, {category.replace('_', ' ').title()} "
        "Violation",
        description="Number of inferred incarceration events that do not align with an "
        "observed discretionary incarceration session start, for which the most severe violation "
        f"type was {category.replace('_', ' ').lower()}",
        event_selector=EventSelector(
            event_type=EventType.SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON,
            event_conditions_dict={"most_severe_violation_type": types},
        ),
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
]


INCARCERATION_INCIDENTS = EventCountMetric(
    name="incarceration_incidents",
    display_name="Incarceration Incidents",
    description="Number of incarceration incidents",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_INCIDENT,
        event_conditions_dict={},
    ),
)
INCARCERATION_STARTS = EventCountMetric(
    name="incarceration_starts",
    display_name="Incarceration Starts",
    description="Number of observed discretionary incarceration starts",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_START,
        event_conditions_dict={"is_discretionary": ["true"]},
    ),
)

INCARCERATION_STARTS_MOST_SEVERE_VIOLATION_TYPE_NOT_ABSCONSION = EventCountMetric(
    name="incarceration_starts_most_severe_violation_type_not_absconsion",
    display_name="Incarceration Starts, Most Severe Violation Type Is Not Absconsion",
    description="Number of observed discretionary incarceration starts where the most severe violation type"
    "is not absconsion",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_START,
        event_conditions_dict={
            "is_discretionary": ["true"],
            "most_severe_violation_type": _NON_ABSCONSION_VIOLATION_TYPES,
        },
    ),
)

INCARCERATION_STARTS_WITH_VIOLATION_TYPE_METRICS = [
    EventCountMetric(
        name=f"incarceration_starts_{category.lower()}_violation",
        display_name=f"Incarceration Starts, {category.replace('_', ' ').title()} "
        "Violation",
        description="Number of observed discretionary incarceration starts for which the most severe "
        f"violation type was {category.replace('_', ' ').lower()}",
        event_selector=EventSelector(
            event_type=EventType.INCARCERATION_START,
            event_conditions_dict={
                "most_severe_violation_type": types,
                "is_discretionary": ["true"],
            },
        ),
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
]

# TODO(#24974): Deprecate inferred only violation type metric
INCARCERATION_STARTS_WITH_INFERRED_VIOLATION_TYPE_METRICS = [
    EventCountMetric(
        name=f"incarceration_starts_{category.lower()}_violation_inferred",
        display_name=f"Incarceration Starts, Inferred {category.replace('_', ' ').title()} "
        "Violation",
        description="Number of observed discretionary incarceration starts for which the most severe "
        f"violation type was {category.replace('_', ' ').lower()}, based on an inferred violation type",
        event_selector=EventSelector(
            event_type=EventType.INCARCERATION_START,
            event_conditions_dict={
                "most_severe_violation_type": types,
                "violation_is_inferred": ["true"],
                "is_discretionary": ["true"],
            },
        ),
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
    if category in ["TECHNICAL", "UNKNOWN"]
]

INCARCERATION_STARTS_TECHNICAL_VIOLATION_NO_PRIOR_TREATMENT_REFERRAL = EventCountMetric(
    name="incarceration_starts_technical_violation_no_prior_treatment_referral",
    display_name="Incarceration Starts, Technical Violation, No Prior Treatment "
    "Referral",
    description="Number of observed discretionary incarceration starts for which the most severe "
    "violation type was technical, and where there were no preceding treatment"
    "referrals during the past 1 year",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_START,
        event_conditions_dict={
            "most_severe_violation_type": ["TECHNICAL"],
            "prior_treatment_referrals_1y": ["0"],
            "is_discretionary": ["true"],
        },
    ),
)

INCARCERATION_STARTS_AND_INFERRED = EventCountMetric(
    name="incarceration_starts_and_inferred",
    display_name="Incarceration Starts And Inferred Incarcerations",
    description="Number of total observed discretionary incarceration starts or inferred "
    "incarcerations",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_START_AND_INFERRED_START,
        event_conditions_dict={"is_discretionary": ["true"]},
    ),
)

INCARCERATION_STARTS_AND_INFERRED_FROM_PAROLE = EventCountMetric(
    name="incarceration_starts_and_inferred_from_parole",
    display_name="Incarceration Starts And Inferred Incarcerations From Parole",
    description="Number of total observed discretionary incarceration starts or inferred "
    "incarcerations from parole",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_START_AND_INFERRED_START,
        event_conditions_dict={
            "is_discretionary": ["true"],
            "latest_active_supervision_type": ["PAROLE"],
        },
    ),
)

INCARCERATION_STARTS_AND_INFERRED_FROM_PROBATION = EventCountMetric(
    name="incarceration_starts_and_inferred_from_probation",
    display_name="Incarceration Starts And Inferred Incarcerations From Probation",
    description="Number of total observed discretionary incarceration starts or inferred "
    "incarcerations from probation",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_START_AND_INFERRED_START,
        event_conditions_dict={
            "is_discretionary": ["true"],
            "latest_active_supervision_type": ["PROBATION"],
        },
    ),
)

INCARCERATION_STARTS_AND_INFERRED_WITH_VIOLATION_TYPE_METRICS = [
    EventCountMetric(
        name=f"incarceration_starts_and_inferred_{category.lower()}_violation",
        display_name="Incarceration Starts And Inferred Incarcerations, "
        f"{category.replace('_', ' ').title()} Violation",
        description="Number of total observed discretionary incarceration starts or inferred "
        f"incarcerations for which the most severe violation type was "
        f"{category.replace('_', ' ').lower()}",
        event_selector=EventSelector(
            event_type=EventType.INCARCERATION_START_AND_INFERRED_START,
            event_conditions_dict={
                "most_severe_violation_type": types,
                "is_discretionary": ["true"],
            },
        ),
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
]

PAROLE_INCARCERATION_STARTS_AND_INFERRED_WITH_VIOLATION_TYPE_METRICS = [
    EventCountMetric(
        name=f"parole_incarceration_starts_and_inferred_{category.lower()}_violation",
        display_name="Incarceration Starts And Inferred Incarcerations from Parole, "
        f"{category.replace('_', ' ').title()} Violation",
        description="Number of total observed discretionary incarceration starts or inferred "
        f"incarcerations from parole for which the most severe violation type was "
        f"{category.replace('_', ' ').lower()}",
        event_selector=EventSelector(
            event_type=EventType.INCARCERATION_START_AND_INFERRED_START,
            event_conditions_dict={
                "most_severe_violation_type": types,
                "is_discretionary": ["true"],
                "latest_active_supervision_type": ["PAROLE"],
            },
        ),
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
]

# TODO(#24974): Deprecate inferred only violation type metric
INCARCERATION_STARTS_AND_INFERRED_WITH_INFERRED_VIOLATION_TYPE_METRICS = [
    EventCountMetric(
        name=f"incarceration_starts_and_inferred_{category.lower()}_violation_inferred",
        display_name="Incarceration Starts And Inferred Incarcerations, "
        f"Inferred {category.replace('_', ' ').title()} Violation",
        description="Number of total observed discretionary incarceration starts or inferred "
        f"incarcerations for which the most severe violation type was "
        f"{category.replace('_', ' ').lower()}, based on an inferred violation type",
        event_selector=EventSelector(
            event_type=EventType.INCARCERATION_START_AND_INFERRED_START,
            event_conditions_dict={
                "most_severe_violation_type": types,
                "violation_is_inferred": ["true"],
                "is_discretionary": ["true"],
            },
        ),
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
    if category in ["TECHNICAL", "UNKNOWN"]
]

INCARCERATION_STARTS_AND_INFERRED_TECHNICAL_VIOLATION_NO_PRIOR_TREATMENT_REFERRAL = EventCountMetric(
    name="incarceration_starts_and_inferred_technical_violation_no_prior_treatment_referral",
    display_name="Incarceration Starts And Inferred Incarcerations, Technical "
    "Violation, No Prior Treatment Referral",
    description="Number of observed discretionary incarceration starts or inferred incarcerations "
    "for which the most severe violation type was technical, and where there were no "
    "preceding treatment referrals during the past 1 year",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_START_AND_INFERRED_START,
        event_conditions_dict={
            "most_severe_violation_type": ["TECHNICAL"],
            "prior_treatment_referrals_1y": ["0"],
            "is_discretionary": ["true"],
        },
    ),
)

INCARCERATIONS_TEMPORARY = EventCountMetric(
    name="incarceration_starts_temporary",
    display_name="Incarceration Starts, Temporary",
    description="Number of observed temporary incarceration starts",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_START_TEMPORARY,
        event_conditions_dict={},
    ),
)

LATE_OPPORTUNITY_METRICS_INCARCERATION = [
    EventCountMetric(
        name=f"late_opportunity_{b.task_type_name.lower()}_{num_days}_days",
        display_name=f"{num_days} Days Late: {b.task_title}",
        description=f"Number of times clients surpass {num_days} days of being "
        f"eligible for opportunities of type: {b.task_title.lower()}",
        event_selector=EventSelector(
            event_type=EventType[f"TASK_ELIGIBLE_{num_days}_DAYS"],
            event_conditions_dict={"task_type": [b.task_type_name]},
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
    for num_days in [7, 30]
]

LATE_OPPORTUNITY_METRICS_SUPERVISION = [
    EventCountMetric(
        name=f"late_opportunity_{b.task_type_name.lower()}_{num_days}_days",
        display_name=f"{num_days} Days Late: {b.task_title}",
        description=f"Number of times clients surpass {num_days} days of being "
        f"eligible for opportunities of type: {b.task_title.lower()}",
        event_selector=EventSelector(
            event_type=EventType[f"TASK_ELIGIBLE_{num_days}_DAYS"],
            event_conditions_dict={"task_type": [b.task_type_name]},
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
    for num_days in [7, 30]
]

LIBERTY_STARTS = EventCountMetric(
    name="transitions_to_liberty",
    display_name="Transitions To Liberty",
    description="Number of transitions to liberty",
    event_selector=EventSelector(
        event_type=EventType.TRANSITIONS_TO_LIBERTY_ALL,
        event_conditions_dict={},
    ),
)

LSIR_ASSESSMENTS = EventCountMetric(
    name="lsir_assessments",
    display_name="LSI-R Assessments",
    description="Number of LSI-R assessments administered",
    event_selector=EventSelector(
        event_type=EventType.RISK_SCORE_ASSESSMENT,
        event_conditions_dict={"assessment_type": ["LSIR"]},
    ),
)

LSIR_ASSESSMENTS_365 = AssignmentEventCountMetric(
    name="lsir_assessments_365",
    display_name="LSI-R Assessments Within 1 Year Of Assignment",
    description="Number of LSI-R assessments administered within 1 year of assignment",
    event_selector=EventSelector(
        event_type=EventType.RISK_SCORE_ASSESSMENT,
        event_conditions_dict={"assessment_type": ["LSIR"]},
    ),
)

LSIR_ASSESSMENTS_AVG_SCORE = EventValueMetric(
    name="lsir_assessments_avg_score",
    display_name="Average LSI-R Score Of Assessments",
    description="Average LSI-R score across all completed assessments",
    event_selector=EventSelector(
        event_type=EventType.RISK_SCORE_ASSESSMENT,
        event_conditions_dict={"assessment_type": ["LSIR"]},
    ),
    event_value_numeric="assessment_score",
    event_count_metric=LSIR_ASSESSMENTS,
)

LSIR_ASSESSMENTS_AVG_SCORE_CHANGE = EventValueMetric(
    name="lsir_assessments_avg_score_change",
    display_name="Average LSI-R Score Change Of Assessments",
    description="Average change in LSI-R score across all completed assessments",
    event_selector=EventSelector(
        event_type=EventType.RISK_SCORE_ASSESSMENT,
        event_conditions_dict={"assessment_type": ["LSIR"]},
    ),
    event_value_numeric="assessment_score_change",
    event_count_metric=LSIR_ASSESSMENTS,
)

LSIR_ASSESSMENTS_RISK_DECREASE = EventCountMetric(
    name="lsir_assessments_risk_decrease",
    display_name="LSI-R Assessments Yielding Lower Risk Score",
    description="Number of LSI-R assessments resulting in a decrease in risk score",
    event_selector=EventSelector(
        event_type=EventType.RISK_SCORE_ASSESSMENT,
        event_conditions_dict={
            "assessment_type": ["LSIR"],
            "assessment_score_decrease": ["true"],
        },
    ),
)

LSIR_ASSESSMENTS_RISK_INCREASE = EventCountMetric(
    name="lsir_assessments_risk_increase",
    display_name="LSI-R Assessments Yielding Higher Risk Score",
    description="Number of LSI-R assessments resulting in an increase in risk score",
    event_selector=EventSelector(
        event_type=EventType.RISK_SCORE_ASSESSMENT,
        event_conditions_dict={
            "assessment_type": ["LSIR"],
            "assessment_score_increase": ["true"],
        },
    ),
)

LSIR_SCORE_PRESENT_AT_ASSIGNMENT = AssignmentSpanDaysMetric(
    name="lsir_score_present_at_assignment",
    display_name="Assignments with an active LSI-R score",
    description="Number of assignments during which client has an LSI-R score",
    span_selector=SpanSelector(
        span_type=SpanType.ASSESSMENT_SCORE_SESSION,
        span_conditions_dict={"assessment_type": ["LSIR"]},
    ),
    window_length_days=1,
)

AVG_LSIR_SCORE_AT_ASSIGNMENT = AssignmentSpanValueAtStartMetric(
    name="avg_lsir_score_at_assignment",
    display_name="Average LSI-R Score At Assignment",
    description="Average LSI-R score of clients on date of assignment",
    span_selector=SpanSelector(
        span_type=SpanType.ASSESSMENT_SCORE_SESSION,
        span_conditions_dict={"assessment_type": ["LSIR"]},
    ),
    span_value_numeric="assessment_score",
    span_count_metric=LSIR_SCORE_PRESENT_AT_ASSIGNMENT,
    window_length_days=1,
)

MAX_DAYS_STABLE_EMPLOYMENT_365 = AssignmentSpanMaxDaysMetric(
    name="max_days_stable_employment_365",
    display_name="Maximum Days Stable Employment Within 1 Year of Assignment",
    description="Number of days in the longest stretch of continuous stable employment "
    "(same employer and job) within 1 year of assignment",
    span_selector=SpanSelector(
        span_type=SpanType.EMPLOYMENT_PERIOD,
        span_conditions_dict={"is_unemployed": ["false"]},
    ),
)

NUMBER_MONTHS_BETWEEN_DOWNGRADE_AND_ASSESSMENT_DUE = EventValueMetric(
    name="number_months_between_downgrade_and_assessment_due",
    display_name="Number of months between custody level downgraded and assessment due date",
    description="Average number of months ahead of a scheduled custody classification assessment that someone is downgraded."
    "A negative number means someone was downgraded sooner than expected. A positive number means they were"
    "downgraded after the assessment due date",
    event_selector=EventSelector(
        event_type=EventType.CUSTODY_LEVEL_CHANGE,
        event_conditions_dict={"change_type": ["DOWNGRADE"]},
    ),
    event_value_numeric="months_between_assessment_due_and_downgrade",
    event_count_metric=CUSTODY_LEVEL_DOWNGRADES,
)

NUMBER_MONTHS_BETWEEN_DOWNGRADE_TO_MINIMUM_AND_ASSESSMENT_DUE = EventValueMetric(
    name="number_months_between_downgrade_to_minimum_and_assessment_due",
    display_name="Number of months between custody level downgraded to minimum and assessment due date",
    description="Average number of months ahead of a scheduled custody classification assessment that someone is downgraded "
    "to minimum custody level. A negative number means someone was downgraded sooner than expected. "
    "A positive number means they were downgraded after the assessment due date",
    event_selector=EventSelector(
        event_type=EventType.CUSTODY_LEVEL_CHANGE,
        event_conditions_dict={
            "change_type": ["DOWNGRADE"],
            "new_custody_level": ["MINIMUM"],
        },
    ),
    event_value_numeric="months_between_assessment_due_and_downgrade",
    event_count_metric=CUSTODY_LEVEL_DOWNGRADES_TO_MINIMUM,
)

PAROLE_BOARD_HEARINGS = EventCountMetric(
    name="parole_board_hearings",
    display_name="Parole Board Hearings",
    description="Count of parole board hearings",
    event_selector=EventSelector(
        event_type=EventType.PAROLE_HEARING,
        event_conditions_dict={},
    ),
)

PAROLE_BOARD_HEARINGS_APPROVED = EventCountMetric(
    name="parole_board_hearings_approved",
    display_name="Parole Board Hearings: Approved",
    description="Count of approved parole board hearings",
    event_selector=EventSelector(
        event_type=EventType.PAROLE_HEARING,
        event_conditions_dict={"decision": ["APPROVED"]},
    ),
)

PAROLE_BOARD_HEARINGS_AVG_DAYS_SINCE_INCARCERATION = EventValueMetric(
    name="parole_board_hearing_avg_days_since_incarceration",
    display_name="Parole Board Hearings: Avg. Days Since Incarceration Start",
    description="Average number of days between the start of incarceration and all "
    "parole board hearings occurring during the period",
    event_selector=EventSelector(
        event_type=EventType.PAROLE_HEARING,
        event_conditions_dict={},
    ),
    event_value_numeric="days_since_incarceration_start",
    event_count_metric=PAROLE_BOARD_HEARINGS,
)

PAROLE_BOARD_HEARINGS_CONTINUED = EventCountMetric(
    name="parole_board_hearings_continued",
    display_name="Parole Board Hearings: Continued",
    description="Count of continued parole board hearings",
    event_selector=EventSelector(
        event_type=EventType.PAROLE_HEARING,
        event_conditions_dict={"decision": ["CONTINUED"]},
    ),
)

PAROLE_BOARD_HEARINGS_DENIED = EventCountMetric(
    name="parole_board_hearings_denied",
    display_name="Parole Board Hearings: Denied",
    description="Count of denied parole board hearings",
    event_selector=EventSelector(
        event_type=EventType.PAROLE_HEARING,
        event_conditions_dict={"decision": ["DENIED"]},
    ),
)

PENDING_CUSTODY_STARTS = EventCountMetric(
    name="pending_custody_starts",
    display_name="Pending Custody Starts",
    description="Number of transitions to pending custody status",
    event_selector=EventSelector(
        event_type=EventType.PENDING_CUSTODY_START,
        event_conditions_dict={},
    ),
)

PERSON_DAYS_WEIGHTED_JUSTICE_IMPACT = SumSpanDaysMetric(
    name="person_days_weighted_justice_impact",
    display_name="Person-Days: Weighted Justice Impact",
    description="Total number of person-days impacted by the justice system, weighted "
    "by compartment type",
    span_selector=SpanSelector(
        span_type=SpanType.JUSTICE_IMPACT_SESSION,
        span_conditions_dict={},
    ),
    weight_col="justice_impact_weight",
)

# get days in eligibility span metrics for all task types
PERSON_DAYS_TASK_ELIGIBLE_METRICS_INCARCERATION = [
    SumSpanDaysMetric(
        name=f"person_days_task_eligible_{b.task_type_name.lower()}",
        display_name=f"Person-Days Eligible: {b.task_title}",
        description="Total number of person-days spent eligible for opportunities of "
        f"type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.TASK_ELIGIBILITY_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

PERSON_DAYS_TASK_ELIGIBLE_METRICS_SUPERVISION = [
    SumSpanDaysMetric(
        name=f"person_days_task_eligible_{b.task_type_name.lower()}",
        display_name=f"Person-Days Eligible: {b.task_title}",
        description="Total number of person-days spent eligible for opportunities of "
        f"type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.TASK_ELIGIBILITY_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

PROP_PERIOD_WITH_CRITICAL_CASELOAD = DailyAvgSpanCountMetric(
    name="prop_period_with_critical_caseload",
    display_name="Proportion Of Analysis Period With Critical Caseload",
    description="Proportion of the analysis period for which an officer has a critical "
    "caseload size",
    span_selector=SpanSelector(
        span_type=SpanType.SUPERVISION_OFFICER_CASELOAD_COUNT_SPAN,
        span_conditions_dict={
            "active_caseload_count_above_critical_threshold": ["true"]
        },
    ),
)

# This exists to support PROP_SENTENCE_ metrics
SUPERVISION_STARTS_FROM_INCARCERATION = EventCountMetric(
    name="supervision_starts_from_incarceration",
    display_name="Supervision Starts From Incarceration",
    description="Number of transitions to supervision from incarceration",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_START,
        event_conditions_dict={"inflow_from_level_1": ["INCARCERATION"]},
    ),
)

PROP_SENTENCE_SERVED_AT_SUPERVISION_INFLOW_FROM_INCARCERATION = EventValueMetric(
    name="prop_sentence_at_supervision_inflow_from_incarceration",
    display_name="Proportion Sentence Served At Incarceration To Supervision "
    "Transition",
    description="Average proportion of sentence served at the transition from "
    "incarceration to supervision",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_START,
        event_conditions_dict={"inflow_from_level_1": ["INCARCERATION"]},
    ),
    event_value_numeric="prop_sentence_served",
    event_count_metric=SUPERVISION_STARTS_FROM_INCARCERATION,
)

SOLITARY_CONFINEMENT_ENDS = EventCountMetric(
    name="solitary_confinement_ends",
    display_name="Solitary Confinement Ends",
    description="Number of solitary confinement ends",
    event_selector=EventSelector(
        event_type=EventType.SOLITARY_CONFINEMENT_END,
        event_conditions_dict={},
    ),
)

SOLITARY_CONFINEMENT_STARTS = EventCountMetric(
    name="solitary_confinement_starts",
    display_name="Solitary Confinement Starts",
    description="Number of solitary confinement starts",
    event_selector=EventSelector(
        event_type=EventType.SOLITARY_CONFINEMENT_START,
        event_conditions_dict={},
    ),
)

SOLITARY_CONFINEMENT_LENGTH_OF_STAY_BY_END = EventValueMetric(
    name="solitary_confinement_length_of_stay_by_end",
    display_name="Solitary Confinement Length of Stay, by End Date",
    description="Length of stay in solitary confinement in days, by end date",
    event_selector=EventSelector(
        event_type=EventType.SOLITARY_CONFINEMENT_END,
        event_conditions_dict={},
    ),
    event_value_numeric="length_of_stay",
    event_count_metric=SOLITARY_CONFINEMENT_ENDS,
)

SOLITARY_CONFINEMENT_LENGTH_OF_STAY_BY_START = EventValueMetric(
    name="solitary_confinement_length_of_stay_by_start",
    display_name="Solitary Confinement Length of Stay, by Start Date",
    description="Length of stay in solitary confinement in days, by start date",
    event_selector=EventSelector(
        event_type=EventType.SOLITARY_CONFINEMENT_START,
        event_conditions_dict={"is_active": ["false"]},
    ),
    event_value_numeric="length_of_stay",
    event_count_metric=SOLITARY_CONFINEMENT_STARTS,
)

SOLITARY_CONFINEMENT_STARTS_LAST_YEAR = EventValueMetric(
    name="solitary_confinement_starts_last_year",
    display_name="Solitary Confinement Starts in Last Year",
    description="Number of solitary confinement starts in the past year at date of assignment to solitary",
    event_selector=EventSelector(
        event_type=EventType.SOLITARY_CONFINEMENT_START,
        event_conditions_dict={},
    ),
    event_value_numeric="solitary_starts_in_last_year",
    event_count_metric=SOLITARY_CONFINEMENT_STARTS,
)

SUPERVISION_LEVEL_DOWNGRADES = EventCountMetric(
    name="supervision_level_downgrades",
    display_name="Supervision Level Downgrades",
    description="Number of supervision level changes to a lower level",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_LEVEL_CHANGE,
        event_conditions_dict={"change_type": ["DOWNGRADE"]},
    ),
)

SUPERVISION_LEVEL_DOWNGRADES_TO_LIMITED = EventCountMetric(
    name="supervision_level_downgrades_to_limited",
    display_name="Supervision Level Downgrades to Limited Supervision",
    description="Number of supervision level changes to limited supervision",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_LEVEL_CHANGE,
        event_conditions_dict={"new_supervision_level": ["LIMITED"]},
    ),
)

SUPERVISION_LEVEL_UPGRADES = EventCountMetric(
    name="supervision_level_upgrades",
    display_name="Supervision Level Upgrades",
    description="Number of supervision level changes to a higher level",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_LEVEL_CHANGE,
        event_conditions_dict={"change_type": ["UPGRADE"]},
    ),
)

SUPERVISION_STARTS = EventCountMetric(
    name="supervision_starts",
    display_name="Supervision Starts",
    description="Number of transitions to supervision",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_START,
        event_conditions_dict={},
    ),
)

# get unique completion task types
TASK_COMPLETED_METRICS_INCARCERATION = [
    EventCountMetric(
        name=f"task_completions_{b.task_type_name.lower()}",
        display_name=f"Task Completions: {b.task_title}",
        description=f"Number of task completions of type: {b.task_title.lower()}",
        event_selector=EventSelector(
            event_type=EventType.TASK_COMPLETED,
            event_conditions_dict={"task_type": [b.task_type_name]},
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

TASK_COMPLETED_METRICS_SUPERVISION = [
    EventCountMetric(
        name=f"task_completions_{b.task_type_name.lower()}",
        display_name=f"Task Completions: {b.task_title}",
        description=f"Number of task completions of type: {b.task_title.lower()}",
        event_selector=EventSelector(
            event_type=EventType.TASK_COMPLETED,
            event_conditions_dict={"task_type": [b.task_type_name]},
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

TASK_COMPLETED_WHILE_ELIGIBLE_METRICS_INCARCERATION = [
    EventCountMetric(
        name=f"task_completions_while_eligible_{b.task_type_name.lower()}",
        display_name=f"Task Completions While Eligible: {b.task_title}",
        description=f"Number of task completions of type: {b.task_title.lower()} "
        "occurring while eligible for opportunity",
        event_selector=EventSelector(
            event_type=EventType.TASK_COMPLETED,
            event_conditions_dict={
                "task_type": [b.task_type_name],
                "is_eligible": ["true"],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

TASK_COMPLETED_AFTER_ELIGIBLE_7_DAYS_METRICS_SUPERVISION = [
    EventCountMetric(
        name=f"task_completions_after_eligible_7_days_{b.task_type_name.lower()}",
        display_name=f"Task Completions After Eligible 7 Days: {b.task_title}",
        description=f"Number of task completions of type: {b.task_title.lower()}"
        " occurring when the person has been eligible for >7 days",
        event_selector=EventSelector(
            event_type=EventType.TASK_COMPLETED,
            event_conditions_dict={
                "task_type": [b.task_type_name],
                "eligible_for_over_7_days": ["true"],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

TASK_COMPLETED_AFTER_ELIGIBLE_7_DAYS_METRICS_INCARCERATION = [
    EventCountMetric(
        name=f"task_completions_after_eligible_7_days_{b.task_type_name.lower()}",
        display_name=f"Task Completions After Eligible 7 Days: {b.task_title}",
        description=f"Number of task completions of type: {b.task_title.lower()}"
        " occurring when the person has been eligible for >7 days",
        event_selector=EventSelector(
            event_type=EventType.TASK_COMPLETED,
            event_conditions_dict={
                "task_type": [b.task_type_name],
                "eligible_for_over_7_days": ["true"],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

TASK_COMPLETED_WHILE_ELIGIBLE_METRICS_SUPERVISION = [
    EventCountMetric(
        name=f"task_completions_while_eligible_{b.task_type_name.lower()}",
        display_name=f"Task Completions While Eligible: {b.task_title}",
        description=f"Number of task completions of type: {b.task_title.lower()}"
        "occurring while eligible for opportunity",
        event_selector=EventSelector(
            event_type=EventType.TASK_COMPLETED,
            event_conditions_dict={
                "task_type": [b.task_type_name],
                "is_eligible": ["true"],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

DAYS_ELIGIBLE_AT_TASK_COMPLETION_METRICS_INCARCERATION = [
    EventValueMetric(
        name=f"days_eligible_at_task_completion_{b.task_type_name.lower()}",
        display_name=f"Days Eligible At Task Completion: {b.task_title}",
        description=f"Number of days spent eligible for {b.task_title.lower()} opportunity at task completion",
        event_selector=EventSelector(
            event_type=EventType.TASK_COMPLETED,
            event_conditions_dict={"task_type": [b.task_type_name]},
        ),
        event_value_numeric="days_eligible",
        event_count_metric=next(
            metric
            for metric in TASK_COMPLETED_WHILE_ELIGIBLE_METRICS_INCARCERATION
            if metric.event_selector.event_conditions_dict["task_type"]
            == [b.task_type_name]
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

DAYS_ELIGIBLE_AT_TASK_COMPLETION_METRICS_SUPERVISION = [
    EventValueMetric(
        name=f"days_eligible_at_task_completion_{b.task_type_name.lower()}",
        display_name=f"Days Eligible At Task Completion: {b.task_title}",
        description=f"Number of days spent eligible for {b.task_title.lower()} opportunity at task completion",
        event_selector=EventSelector(
            event_type=EventType.TASK_COMPLETED,
            event_conditions_dict={"task_type": [b.task_type_name]},
        ),
        event_value_numeric="days_eligible",
        event_count_metric=next(
            metric
            for metric in TASK_COMPLETED_WHILE_ELIGIBLE_METRICS_SUPERVISION
            if metric.event_selector.event_conditions_dict["task_type"]
            == [b.task_type_name]
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

TREATMENT_REFERRALS = EventCountMetric(
    name="treatment_referrals",
    display_name="Treatment Referrals",
    description="Number of treatment referrals",
    event_selector=EventSelector(
        event_type=EventType.TREATMENT_REFERRAL,
        event_conditions_dict={},
    ),
)

TREATMENT_STARTS = EventCountMetric(
    name="treatment_starts",
    display_name="Treatment Starts",
    description="Number of treatment starts (counts unique program_id's)",
    event_selector=EventSelector(
        event_type=EventType.TREATMENT_START,
        event_conditions_dict={},
    ),
    event_segmentation_columns=["program_id"],
)

VIOLATIONS = EventCountMetric(
    name="violations",
    display_name="Violations: All",
    description="Number of violations",
    event_selector=EventSelector(
        event_type=EventType.VIOLATION,
        event_conditions_dict={},
    ),
)

VIOLATIONS_BY_TYPE_METRICS = [
    EventCountMetric(
        name=f"violations_{category.lower()}",
        display_name=f"Violations: {category.replace('_', ' ').title()}",
        description=f"Number of {category.replace('_', ' ').lower()} violations",
        event_selector=EventSelector(
            event_type=EventType.VIOLATION,
            event_conditions_dict={"violation_type": types},
        ),
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
]

VIOLATIONS_ABSCONSION = next(
    metric
    for metric in VIOLATIONS_BY_TYPE_METRICS
    if metric.name == "violations_absconsion"
)

VIOLATION_RESPONSES = EventCountMetric(
    name="violation_responses",
    display_name="Violation Responses: All",
    description="Number of violation responses",
    event_selector=EventSelector(
        event_type=EventType.VIOLATION_RESPONSE,
        event_conditions_dict={},
    ),
)

VIOLATION_RESPONSES_BY_TYPE_METRICS = [
    EventCountMetric(
        name=f"violation_responses_{category.lower()}",
        display_name=f"Violation Responses: {category.replace('_', ' ').title()}",
        description=f"Number of {category.replace('_', ' ').lower()} violation "
        "responses",
        event_selector=EventSelector(
            event_type=EventType.VIOLATION_RESPONSE,
            event_conditions_dict={"most_serious_violation_type": types},
        ),
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
]

DISTINCT_POPULATION_WORKFLOWS_ELIGIBLE_AND_VISIBLE_IN_TOOL = (
    SpanDistinctUnitCountMetric(
        name="workflows_distinct_people_eligible_and_visible_in_tool",
        display_name="Distinct Population: Eligible And Visible In Tool",
        description="Total distinct count of clients eligible and visible-in-tool",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
                "is_surfaceable": ["true"],
            },
        ),
    )
)

DISTINCT_POPULATION_WORKFLOWS_ELIGIBLE_AND_ACTIONABLE = SpanDistinctUnitCountMetric(
    name="workflows_distinct_people_eligible_and_actionable",
    display_name="Distinct Population: Eligible And Actionable",
    description="Total distinct count of clients eligible and actionable (visible, not marked ineligible, not marked in progress)",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_eligible": ["true"],
            "is_surfaceable": ["true"],
            "in_progress": ["false"],
            "marked_ineligible": ["false"],
        },
    ),
)

DISTINCT_POPULATION_WORKFLOWS_ALMOST_ELIGIBLE_AND_VISIBLE_IN_TOOL = SpanDistinctUnitCountMetric(
    name="workflows_distinct_people_almost_eligible_and_visible_in_tool",
    display_name="Distinct Population: Almost Eligible And Visible In Tool",
    description="Total distinct count of clients almost eligible and visible-in-tool",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_almost_eligible": ["true"],
            "is_surfaceable": ["true"],
        },
    ),
)

DISTINCT_POPULATION_WORKFLOWS_ELIGIBLE_ACTIONABLE_AND_VIEWED = SpanDistinctUnitCountMetric(
    name="workflows_distinct_people_eligible_actionable_and_viewed",
    display_name="Distinct Population: Eligible, Actionable, And Viewed",
    description="Total distinct count of clients eligible, actionable (visible, not marked ineligible, not marked in progress), and viewed (clicked-on)",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_eligible": ["true"],
            "is_surfaceable": ["true"],
            "in_progress": ["false"],
            "marked_ineligible": ["false"],
            "viewed": ["true"],
        },
    ),
)

DISTINCT_POPULATION_WORKFLOWS_ELIGIBLE_AND_ACTIONABLE_METRICS_SUPERVISION = [
    SpanDistinctUnitCountMetric(
        name=f"workflows_distinct_people_eligible_and_actionable_{b.task_type_name.lower()}",
        display_name=f"Distinct Population: Eligible And Actionable, {b.task_title}",
        description=f"Total distinct count of clients eligible and actionable (visible, not marked ineligible, not marked in progress) for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
                "is_surfaceable": ["true"],
                "in_progress": ["false"],
                "marked_ineligible": ["false"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

DISTINCT_POPULATION_WORKFLOWS_ALMOST_ELIGIBLE_AND_ACTIONABLE = SpanDistinctUnitCountMetric(
    name="workflows_distinct_people_almost_eligible_and_actionable",
    display_name="Distinct Population: Almost Eligible And Actionable",
    description="Total distinct count of clients almost eligible and actionable (visible, not marked ineligible, not marked in progress) ",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_almost_eligible": ["true"],
            "is_surfaceable": ["true"],
            "in_progress": ["false"],
            "marked_ineligible": ["false"],
        },
    ),
)

DISTINCT_POPULATION_WORKFLOWS_ALMOST_ELIGIBLE_ACTIONABLE_AND_VIEWED = SpanDistinctUnitCountMetric(
    name="workflows_distinct_people_almost_eligible_actionable_and_viewed",
    display_name="Distinct Population: Almost Eligible, Actionable, And Viewed",
    description="Total distinct count of clients almost eligible, actionable (visible, not marked ineligible, not marked in progress), and viewed (clicked-on)",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_almost_eligible": ["true"],
            "is_surfaceable": ["true"],
            "in_progress": ["false"],
            "marked_ineligible": ["false"],
            "viewed": ["true"],
        },
    ),
)

DISTINCT_POPULATION_WORKFLOWS_ALMOST_ELIGIBLE_AND_ACTIONABLE_METRICS_SUPERVISION = [
    SpanDistinctUnitCountMetric(
        name=f"workflows_distinct_people_almost_eligible_and_actionable_{b.task_type_name.lower()}",
        display_name=f"Distinct Population: Almost Eligible And Actionable, {b.task_title}",
        description=f"Total distinct count of clients almost eligible and actionable (visible, not marked ineligible, not marked in progress) for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_almost_eligible": ["true"],
                "is_surfaceable": ["true"],
                "in_progress": ["false"],
                "marked_ineligible": ["false"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

WORKFLOWS_PRIMARY_USER_ACTIVE_USAGE_EVENTS = EventCountMetric(
    name="workflows_primary_user_active_usage_events",
    display_name="Total Active Usage Events, Primary Workflows Users",
    description="Total number of actions taken by primary Workflows users",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={},
    ),
    event_segmentation_columns=["event_type", "task_type", "person_id"],
)

WORKFLOWS_PRIMARY_USER_LOGINS = EventCountMetric(
    name="workflows_primary_user_logins",
    display_name="Logins, Primary Workflows Users",
    description="Number of logins performed by primary Workflows users",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_USER_LOGIN,
        event_conditions_dict={},
    ),
)

DISTINCT_PROVISIONED_WORKFLOWS_USERS = SpanDistinctUnitCountMetric(
    name="distinct_provisioned_workflows_users",
    display_name="Distinct Provisioned Workflows Users",
    description="Number of distinct Workflows users who are provisioned to have tool access (regardless of role type)",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PROVISIONED_USER_SESSION,
        span_conditions_dict={},
    ),
)
DISTINCT_REGISTERED_PROVISIONED_WORKFLOWS_USERS = SpanDistinctUnitCountMetric(
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
DISTINCT_PROVISIONED_PRIMARY_WORKFLOWS_USERS = SpanDistinctUnitCountMetric(
    name="distinct_provisioned_primary_workflows_users",
    display_name="Distinct Provisioned Primary Workflows Users",
    description="Number of distinct primary Workflows users who are provisioned to have tool access",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PROVISIONED_USER_SESSION,
        span_conditions_dict={"is_primary_user": ["true"]},
    ),
)
DISTINCT_REGISTERED_PRIMARY_WORKFLOWS_USERS = SpanDistinctUnitCountMetric(
    name="distinct_registered_primary_workflows_users",
    display_name="Distinct Registered Primary Workflows Users",
    description="Number of distinct primary (line staff) Workflows users who have signed up/logged into Workflows at least once",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION,
        span_conditions_dict={},
    ),
)
DISTINCT_LOGGED_IN_PRIMARY_WORKFLOWS_USERS = EventDistinctUnitCountMetric(
    name="distinct_logged_in_primary_workflows_users",
    display_name="Distinct Logged In Primary Workflows Users",
    description="Number of distinct primary (line staff) Workflows users who logged into Workflows",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_USER_LOGIN,
        event_conditions_dict={},
    ),
)
DISTINCT_ACTIVE_PRIMARY_WORKFLOWS_USERS = EventDistinctUnitCountMetric(
    name="distinct_active_primary_workflows_users",
    display_name="Distinct Active Primary Workflows Users",
    description="Number of distinct primary (line staff) Workflows users having at least one usage event for the "
    "task type during the time period",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={},
    ),
)
DISTINCT_REGISTERED_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR = SpanDistinctUnitCountMetric(
    name="distinct_registered_primary_workflows_users_with_eligible_caseload_in_past_year",
    display_name="Distinct Registered Primary Workflows Users With Eligible Caseload In Past Year",
    description="Number of distinct primary (line staff) Workflows users who have signed up/logged into Workflows at least once and have had an eligible caseload in the past year",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION,
        span_conditions_dict={
            "user_has_eligible_caseload_in_past_year": ["true"],
        },
    ),
)
DISTINCT_LOGGED_IN_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR = EventDistinctUnitCountMetric(
    name="distinct_logged_in_primary_workflows_users_with_eligible_caseload_in_past_year",
    display_name="Distinct Logged In Primary Workflows Users With Eligible Caseload In Past Year",
    description="Number of distinct primary (line staff) Workflows users who logged into Workflows and have had an eligible caseload in the past year",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_USER_LOGIN,
        event_conditions_dict={
            "user_has_eligible_caseload_in_past_year": ["true"],
        },
    ),
)
DISTINCT_ACTIVE_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR = EventDistinctUnitCountMetric(
    name="distinct_active_primary_workflows_users_with_eligible_caseload_in_past_year",
    display_name="Distinct Active Primary Workflows Users With Eligible Caseload In Past Year",
    description="Number of distinct primary (line staff) Workflows users having at least one usage event for the task type during the time period who had an eligible caseload in the past year",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={
            "user_has_eligible_caseload_in_past_year": ["true"],
        },
    ),
)
LOGINS_BY_PRIMARY_WORKFLOWS_USER = EventCountMetric(
    name="logins_primary_workflows_user",
    display_name="Logins, Primary Workflows Users",
    description="Number of logins performed by primary Workflows users",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_USER_LOGIN,
        event_conditions_dict={},
    ),
)
FIRST_TOOL_ACTIONS = EventCountMetric(
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

DISTINCT_GLOBAL_PROVISIONED_USERS = SpanDistinctUnitCountMetric(
    name="distinct_global_provisioned_users",
    display_name="Distinct Global Provisioned Users",
    description="Number of distinct users who are provisioned to have tool access (regardless of role type) across all tools",
    span_selector=SpanSelector(
        span_type=SpanType.GLOBAL_PROVISIONED_USER_SESSION,
        span_conditions_dict={},
    ),
)
DISTINCT_REGISTERED_GLOBAL_PROVISIONED_USERS = SpanDistinctUnitCountMetric(
    name="distinct_registered_global_provisioned_users",
    display_name="Distinct Registered Global Provisioned Users",
    description=(
        "Number of distinct users who are provisioned to have tool access (regardless of role type) across all tools "
        "who have signed up/logged into any tool at least once"
    ),
    span_selector=SpanSelector(
        span_type=SpanType.GLOBAL_PROVISIONED_USER_SESSION,
        span_conditions_dict={"is_registered": ["true"]},
    ),
)
DISTINCT_LOGGED_IN_GLOBAL_USERS = EventDistinctUnitCountMetric(
    name="distinct_logged_in_global_users",
    display_name="Distinct Logged In Global Users",
    description="Number of distinct users who logged into any tool",
    event_selector=EventSelector(
        event_type=EventType.GLOBAL_USER_LOGIN,
        event_conditions_dict={},
    ),
)
LOGINS_GLOBAL_USER = EventCountMetric(
    name="logins_global_user",
    display_name="Logins, Global Users",
    description="Number of logins performed by all users across all tools",
    event_selector=EventSelector(
        event_type=EventType.GLOBAL_USER_LOGIN,
        event_conditions_dict={},
    ),
)
DISTINCT_ACTIVE_GLOBAL_USER_ALL_TOOLS = EventDistinctUnitCountMetric(
    name="distinct_active_global_users_all_tools",
    display_name="Distinct Active Global Users: All Tools",
    description="Number of distinct users having at least one usage event in any tool",
    event_selector=EventSelector(
        event_type=EventType.GLOBAL_USER_ACTIVE_USAGE_EVENT,
        event_conditions_dict={},
    ),
)

ACTIVE_USAGE_EVENTS_GLOBAL_USER = EventCountMetric(
    name="global_user_active_usage_events",
    display_name="Total Active Usage Events, All Users",
    description="Total number of actions by all users",
    event_selector=EventSelector(
        event_type=EventType.GLOBAL_USER_ACTIVE_USAGE_EVENT,
        event_conditions_dict={},
    ),
    event_segmentation_columns=[
        "event",
        "product_type",
        "context_page_path",
        "person_id",
    ],
)

DISTINCT_USERS_WITH_CSAT_RESPONSES = EventDistinctUnitCountMetric(
    name="distinct_users_with_csat_responses",
    display_name="Distinct Users With Intercom CSAT Responses",
    description="Number of distinct users who submitted at least one CSAT survey response",
    event_selector=EventSelector(
        event_type=EventType.INTERCOM_CSAT_RESPONSE,
        event_conditions_dict={},
    ),
)

CSAT_RESPONSES = EventCountMetric(
    name="csat_responses",
    display_name="CSAT Total Responses",
    description="Total number of Intercom CSAT responses submitted",
    event_selector=EventSelector(
        event_type=EventType.INTERCOM_CSAT_RESPONSE,
        event_conditions_dict={},
    ),
)

CSAT_POSITIVE_RESPONSES = EventCountMetric(
    name="csat_positive_responses",
    display_name="CSAT Positive Responses",
    description="Total customer satisfaction survey responses with a 4-5 score",
    event_selector=EventSelector(
        event_type=EventType.INTERCOM_CSAT_RESPONSE,
        event_conditions_dict={
            "feedback_type": ["POSITIVE"],
        },
    ),
)

CSAT_NEUTRAL_RESPONSES = EventCountMetric(
    name="csat_neutral_responses",
    display_name="CSAT Neutral Responses",
    description="Total customer satisfaction survey responses with a 3 score",
    event_selector=EventSelector(
        event_type=EventType.INTERCOM_CSAT_RESPONSE,
        event_conditions_dict={
            "feedback_type": ["NEUTRAL"],
        },
    ),
)

CSAT_NEGATIVE_RESPONSES = EventCountMetric(
    name="csat_negative_responses",
    display_name="CSAT Negative Responses",
    description="Total customer satisfaction survey responses with a 1-2 score",
    event_selector=EventSelector(
        event_type=EventType.INTERCOM_CSAT_RESPONSE,
        event_conditions_dict={
            "feedback_type": ["NEGATIVE"],
        },
    ),
)

AVG_CSAT_SCORE = EventValueMetric(
    name="avg_csat_score",
    display_name="Average CSAT Score",
    description="Average customer satisfaction score (1-5) across all Intercom survey responses",
    event_selector=EventSelector(
        event_type=EventType.INTERCOM_CSAT_RESPONSE,
        event_conditions_dict={},
    ),
    event_value_numeric="satisfaction_score",
    event_count_metric=CSAT_RESPONSES,
)

DISTINCT_JII_TABLET_APP_PROVISIONED_USERS = SpanDistinctUnitCountMetric(
    name="distinct_jii_tablet_app_provisioned_users",
    display_name="Distinct JII Tablet App Provisioned Users",
    description="Number of distinct users who are provisioned to have tool access to the JII tablet app",
    span_selector=SpanSelector(
        span_type=SpanType.JII_TABLET_APP_PROVISIONED_USER_SESSION,
        span_conditions_dict={},
    ),
)
DISTINCT_REGISTERED_JII_TABLET_APP_PROVISIONED_USERS = SpanDistinctUnitCountMetric(
    name="distinct_registered_jii_tablet_app_provisioned_users",
    display_name="Distinct Registered JII Tablet App Provisioned Users",
    description=(
        "Number of distinct users who are provisioned to have tool access to the JII tablet app "
        "who have signed up/logged into the tablet app at least once"
    ),
    span_selector=SpanSelector(
        span_type=SpanType.JII_TABLET_APP_PROVISIONED_USER_SESSION,
        span_conditions_dict={"is_registered": ["true"]},
    ),
)


DISTINCT_JII_TABLET_APP_PROVISIONED_USERS = SpanDistinctUnitCountMetric(
    name="distinct_jii_tablet_app_provisioned_users",
    display_name="Distinct JII Tablet App Provisioned Users",
    description="Number of distinct users who are provisioned to have tool access to the JII tablet app",
    span_selector=SpanSelector(
        span_type=SpanType.JII_TABLET_APP_PROVISIONED_USER_SESSION,
        span_conditions_dict={},
    ),
)
DISTINCT_REGISTERED_JII_TABLET_APP_PROVISIONED_USERS = SpanDistinctUnitCountMetric(
    name="distinct_registered_jii_tablet_app_provisioned_users",
    display_name="Distinct Registered JII Tablet App Provisioned Users",
    description=(
        "Number of distinct users who are provisioned to have tool access to the JII tablet app "
        "who have signed up/logged into the tablet app at least once"
    ),
    span_selector=SpanSelector(
        span_type=SpanType.JII_TABLET_APP_PROVISIONED_USER_SESSION,
        span_conditions_dict={"is_registered": ["true"]},
    ),
)
DISTINCT_LOGGED_IN_JII_TABLET_APP_USERS = EventDistinctUnitCountMetric(
    name="distinct_logged_in_jii_tablet_app_users",
    display_name="Distinct Logged In JII Tablet App Users",
    description="Number of distinct users who logged into the JII tablet app",
    event_selector=EventSelector(
        event_type=EventType.JII_TABLET_APP_USER_LOGIN,
        event_conditions_dict={},
    ),
)
LOGINS_JII_TABLET_APP_USER = EventCountMetric(
    name="logins_jii_tablet_app_user",
    display_name="Logins, JII Tablet App Users",
    description="Number of logins performed by users of the JII tablet app",
    event_selector=EventSelector(
        event_type=EventType.JII_TABLET_APP_USER_LOGIN,
        event_conditions_dict={},
    ),
)

# Outcome metrics
AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE = DailyAvgSpanCountMetric(
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

AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_ACTIONABLE_AND_VIEWED = DailyAvgSpanCountMetric(
    name="avg_daily_population_task_almost_eligible_actionable_and_viewed",
    display_name="Average Population: Task Almost Eligible, Actionable, And Viewed",
    description="Average daily count of clients almost eligible, actionable (visible, not marked ineligible, not marked in progress), and viewed (clicked-on)",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_almost_eligible": ["true"],
            "is_surfaceable": ["true"],
            "in_progress": ["false"],
            "marked_ineligible": ["false"],
            "viewed": ["true"],
        },
    ),
)

AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_ACTIONABLE_AND_VIEWED_METRICS_SUPERVISION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_almost_eligible_actionable_and_viewed_{b.task_type_name.lower()}",
        display_name=f"Average Population: Almost Eligible, Actionable, And Viewed, {b.task_title}",
        description=f"Average daily count of clients almost eligible, actionable (visible, not marked ineligible, not marked in progress), and viewed (clicked-on) for task type:  {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_almost_eligible": ["true"],
                "is_surfaceable": ["true"],
                "in_progress": ["false"],
                "marked_ineligible": ["false"],
                "viewed": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_ACTIONABLE_AND_VIEWED_METRICS_INCARCERATION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_almost_eligible_actionable_and_viewed_{b.task_type_name.lower()}",
        display_name=f"Average Population: Almost Eligible, Actionable, And Viewed, {b.task_title}",
        description=f"Average daily count of clients almost eligible, actionable (visible, not marked ineligible, not marked in progress), and viewed (clicked-on) for task type:  {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_almost_eligible": ["true"],
                "is_surfaceable": ["true"],
                "in_progress": ["false"],
                "marked_ineligible": ["false"],
                "viewed": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_FUNNEL_METRICS = [
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
AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_MARKED_PERMANENTLY_INELIGIBLE = DailyAvgSpanCountMetric(
    name="avg_population_task_almost_eligible_marked_permanently_ineligible",
    display_name="Average Population: Task Almost Eligible And Marked Permanently Ineligible",
    description="Average daily count of clients almost eligible for selected task type marked permanently ineligible",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_almost_eligible": ["true"],
            "marked_permanently_ineligible": ["true"],
        },
    ),
)

AVG_DAILY_POPULATION_TASK_ELIGIBLE = DailyAvgSpanCountMetric(
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
AVG_DAILY_POPULATION_TASK_ELIGIBLE_FUNNEL_METRICS = [
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
AVG_DAILY_POPULATION_TASK_ELIGIBLE_MARKED_PERMANENTLY_INELIGIBLE = DailyAvgSpanCountMetric(
    name="avg_population_task_eligible_marked_permanently_ineligible",
    display_name="Average Population: Task Eligible And Marked Permanently Ineligible",
    description="Average daily count of clients eligible for selected task type marked permanently ineligible",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_eligible": ["true"],
            "marked_permanently_ineligible": ["true"],
        },
    ),
)

AVG_DAILY_POPULATION_WORKFLOWS_REVIEW_STATUS = [
    DailyAvgSpanCountMetric(
        name=f"avg_population__clients_{review_approval_status.lower()}",
        display_name=f"Average Population: {snake_to_title(review_approval_status)}",
        description=f"Average daily count of clients in the {snake_to_title(review_approval_status).lower()} status",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_surfaceable": ["true"],
                "review_approval_status": [review_approval_status],
            },
        ),
    )
    for review_approval_status in [
        "SNOOZE_REVIEW",
        "GRANT_REVIEW",
        "REVISIONS_REQUESTED",
    ]
]

DISTINCT_POPULATION_WORKFLOWS_ALMOST_ELIGIBLE = SpanDistinctUnitCountMetric(
    name="distinct_clients_almost_eligible",
    display_name="Distinct Population: Almost Eligible",
    description="Total distinct count of clients almost eligible for selected task type",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_almost_eligible": ["true"],
        },
    ),
)

DISTINCT_POPULATION_WORKFLOWS_ALMOST_ELIGIBLE_FUNNEL_METRICS = [
    SpanDistinctUnitCountMetric(
        name=f"distinct_clients_almost_eligible_{k.lower()}",
        display_name=f"Distinct Population: Almost Eligible And {snake_to_title(k)}",
        description=f"Total distinct count of clients almost eligible for selected task type with funnel status "
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

DISTINCT_POPULATION_WORKFLOWS_ALMOST_ELIGIBLE_MARKED_PERMANENTLY_INELIGIBLE = SpanDistinctUnitCountMetric(
    name="distinct_clients_almost_eligible_marked_permanently_ineligible",
    display_name="Distinct Population: Almost Eligible And Marked Permanently Ineligible",
    description="Total distinct count of clients almost eligible for selected task type marked permanently ineligible",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_almost_eligible": ["true"],
            "marked_permanently_ineligible": ["true"],
        },
    ),
)

DISTINCT_POPULATION_WORKFLOWS_ELIGIBLE = SpanDistinctUnitCountMetric(
    name="distinct_clients_eligible",
    display_name="Distinct Population: Eligible",
    description="Total distinct count of clients eligible for selected task type",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_eligible": ["true"],
        },
    ),
)

DISTINCT_POPULATION_WORKFLOWS_ELIGIBLE_FUNNEL_METRICS = [
    SpanDistinctUnitCountMetric(
        name=f"distinct_clients_eligible_{k.lower()}",
        display_name=f"Distinct Population: Eligible And {snake_to_title(k)}",
        description=f"Total distinct count of clients eligible for selected task type with funnel status "
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

DISTINCT_POPULATION_WORKFLOWS_ELIGIBLE_MARKED_PERMANENTLY_INELIGIBLE = SpanDistinctUnitCountMetric(
    name="distinct_clients_eligible_marked_permanently_ineligible",
    display_name="Distinct Population: Eligible And Marked Permanently Ineligible",
    description="Total distinct count of clients eligible for selected task type marked permanently ineligible",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_eligible": ["true"],
            "marked_permanently_ineligible": ["true"],
        },
    ),
)

DISTINCT_POPULATION_WORKFLOWS_REVIEW_STATUS = [
    SpanDistinctUnitCountMetric(
        name=f"distinct_clients_{review_approval_status.lower()}",
        display_name=f"Distinct Population: {snake_to_title(review_approval_status)}",
        description=f"Total distinct count of clients in the {snake_to_title(review_approval_status).lower()} status",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_surfaceable": ["true"],
                "review_approval_status": [review_approval_status],
            },
        ),
    )
    for review_approval_status in [
        "SNOOZE_REVIEW",
        "GRANT_REVIEW",
        "REVISIONS_REQUESTED",
    ]
]


PERSON_DAYS_TASK_ELIGIBLE = SumSpanDaysMetric(
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
TASK_COMPLETIONS = EventCountMetric(
    name="task_completions",
    display_name="Task Completions",
    description="Number of task completions of selected task type",
    event_selector=EventSelector(
        event_type=EventType.TASK_COMPLETED,
        event_conditions_dict={},
    ),
    event_segmentation_columns=["task_type"],
)

TASK_COMPLETIONS_AFTER_TOOL_ACTION = EventCountMetric(
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

TASK_COMPLETIONS_WHILE_ALMOST_ELIGIBLE = EventCountMetric(
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

TASK_COMPLETIONS_WHILE_ALMOST_ELIGIBLE_AFTER_TOOL_ACTION = EventCountMetric(
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

TASK_COMPLETIONS_WHILE_ELIGIBLE = EventCountMetric(
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

DAYS_ELIGIBLE_AT_FIRST_TOOL_ACTION = EventValueMetric(
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
    event_count_metric=FIRST_TOOL_ACTIONS,
)

DAYS_ELIGIBLE_AT_TASK_COMPLETION = EventValueMetric(
    name="days_eligible_at_task_completion",
    display_name="Days Eligible At Task Completion",
    description="Number of days spent eligible for selected opportunity at task completion",
    event_selector=EventSelector(
        event_type=EventType.TASK_COMPLETED,
        event_conditions_dict={},
    ),
    event_value_numeric="days_eligible",
    event_count_metric=TASK_COMPLETIONS,
)

TASK_ELIGIBILITY_STARTS_WHILE_ALMOST_ELIGIBLE_AFTER_TOOL_ACTION = EventCountMetric(
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

# TODO(#55098): Refactor all product-specific adoption metrics to use global usage metrics
DISTINCT_PROVISIONED_INSIGHTS_USERS = SpanDistinctUnitCountMetric(
    name="distinct_provisioned_insights_users",
    display_name="Distinct Provisioned Supervisor Homepage Users",
    description="Number of distinct Supervisor Homepage users who are provisioned to have tool access (regardless of role type)",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PROVISIONED_USER_SESSION,
        span_conditions_dict={},
    ),
)

DISTINCT_REGISTERED_PROVISIONED_INSIGHTS_USERS = SpanDistinctUnitCountMetric(
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

DISTINCT_PROVISIONED_PRIMARY_INSIGHTS_USERS = SpanDistinctUnitCountMetric(
    name="distinct_provisioned_primary_insights_users",
    display_name="Distinct Provisioned Primary Supervisor Homepage Users",
    description="Number of distinct primary Supervisor Homepage users who are provisioned to have tool access",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PROVISIONED_USER_SESSION,
        span_conditions_dict={"is_primary_user": ["true"]},
    ),
)

DISTINCT_REGISTERED_PRIMARY_INSIGHTS_USERS = SpanDistinctUnitCountMetric(
    name="distinct_registered_primary_insights_users",
    display_name="Distinct Total Registered Primary Supervisor Homepage Users",
    description="Number of distinct primary (supervisor) Supervisor Homepage users who have signed up/logged into Supervisor Homepage at least once",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PRIMARY_USER_REGISTRATION_SESSION,
        span_conditions_dict={},
    ),
)

DISTINCT_LOGGED_IN_PRIMARY_INSIGHTS_USERS = EventDistinctUnitCountMetric(
    name="distinct_logged_in_primary_insights_users",
    display_name="Distinct Logged In Primary Supervisor Homepage Users",
    description="Number of distinct primary (supervisor) Supervisor Homepage users who logged into Supervisor Homepage",
    event_selector=EventSelector(
        event_type=EventType.INSIGHTS_USER_LOGIN,
        event_conditions_dict={},
    ),
)

DISTINCT_ACTIVE_PRIMARY_INSIGHTS_USERS = EventDistinctUnitCountMetric(
    name="distinct_active_primary_insights_users",
    display_name="Distinct Active Primary Supervisor Homepage Users",
    description="Number of distinct primary (supervisor) Supervisor Homepage users having at least one active usage event "
    "during the time period",
    event_selector=EventSelector(
        event_type=EventType.INSIGHTS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={},
    ),
)

DISTINCT_ACTIVE_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL = EventDistinctUnitCountMetric(
    name="distinct_active_primary_insights_users_with_outliers_visible_in_tool",
    display_name="Distinct Active Primary Supervisor Homepage Users with Outliers Visible in Tool",
    description="Number of distinct primary (supervisor) Supervisor Homepage users who had outliers and had at least one active usage event "
    "during the time period",
    event_selector=EventSelector(
        event_type=EventType.INSIGHTS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={"has_outlier_officers": ["true"]},
    ),
)

DISTINCT_ACTIVE_PRIMARY_INSIGHTS_USERS_WITHOUT_OUTLIERS_VISIBLE_IN_TOOL = EventDistinctUnitCountMetric(
    name="distinct_active_primary_insights_users_without_outliers_visible_in_tool",
    display_name="Distinct Active Primary Supervisor Homepage Users without Outliers Visible in Tool",
    description="Number of distinct primary (supervisor) Supervisor Homepage users who did not have outliers and had at least one active usage event "
    "during the time period",
    event_selector=EventSelector(
        event_type=EventType.INSIGHTS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={"has_outlier_officers": ["false"]},
    ),
)

LOGINS_PRIMARY_INSIGHTS_USERS = EventCountMetric(
    name="logins_primary_insights_user",
    display_name="Logins, Primary Supervisor Homepage Users",
    description="Number of logins performed by primary Supervisor Homepage users",
    event_selector=EventSelector(
        event_type=EventType.INSIGHTS_USER_LOGIN,
        event_conditions_dict={},
    ),
)

DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_LOGGED_IN = SpanDistinctUnitCountMetric(
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

DISTINCT_PRIMARY_INSIGHTS_USERS_WITHOUT_OUTLIERS_VISIBLE_IN_TOOL_LOGGED_IN = SpanDistinctUnitCountMetric(
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

DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL = SpanDistinctUnitCountMetric(
    name="distinct_primary_insights_users_with_outliers_visible_in_tool",
    display_name="Distinct Primary Supervisor Homepage Users with Outliers Visible in Tool",
    description="Number of primary supervisor homepage users who had outliers visible in the tool",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PRIMARY_USER_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={"has_outlier_officers": ["true"]},
    ),
)

DISTINCT_PRIMARY_INSIGHTS_USERS_WITHOUT_OUTLIERS_VISIBLE_IN_TOOL = SpanDistinctUnitCountMetric(
    name="distinct_primary_insights_users_without_outliers_visible_in_tool",
    display_name="Distinct Primary Supervisor Homepage Users without Outliers Visible in Tool",
    description="Number of primary supervisor homepage users who did not have outliers visible in the tool",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PRIMARY_USER_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={"has_outlier_officers": ["false"]},
    ),
)

DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_VIEWED_STAFF_MEMBER_PAGE = SpanDistinctUnitCountMetric(
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

DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_VIEWED_STAFF_MEMBER_METRIC_PAGE = SpanDistinctUnitCountMetric(
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

DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_VIEWED_CLIENT_PAGE = SpanDistinctUnitCountMetric(
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

DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_VIEWED_ACTION_STRATEGY_POP_UP = SpanDistinctUnitCountMetric(
    name="distinct_primary_insights_users_with_outliers_visible_in_tool_viewed_action_strategy_pop_up",
    display_name="Distinct Primary Supervisor Homepage Users with Outliers Visible in Tool - Viewed Action Strategy Pop-up",
    description="Number of primary supervisor homepage users who had outliers visible in the tool and viewed the action strategy pop-up",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_PRIMARY_USER_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "has_outlier_officers": ["true"],
            "viewed_action_strategy_popup": ["true"],
        },
    ),
)

DISTINCT_PRIMARY_INSIGHTS_USERS_WITH_OUTLIERS_VISIBLE_IN_TOOL_VIEWED_ANY_PAGE_FOR_30_SECONDS = EventDistinctUnitCountMetric(
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

DISTINCT_PRIMARY_INSIGHTS_USERS_VIEWED_ANY_PAGE_FOR_30_SECONDS = EventDistinctUnitCountMetric(
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

DISTINCT_OUTLIER_OFFICERS = SpanDistinctUnitCountMetric(
    name="distinct_outlier_officers_visible_in_tool",
    display_name="Distinct Number Of Outlier Officers Visible In Tool",
    description="Number of distinct outlier officers visible in the tool",
    span_selector=SpanSelector(
        span_type=SpanType.INSIGHTS_SUPERVISION_OFFICER_OUTLIER_STATUS_SESSION,
        span_conditions_dict={
            "is_surfaceable_outlier": ["true"],
        },
    ),
)

DISTINCT_PROVISIONED_TASKS_USERS = SpanDistinctUnitCountMetric(
    name="distinct_provisioned_tasks_users",
    display_name="Distinct Provisioned Tasks Users",
    description="Number of distinct Tasks users who are provisioned to have tool access (regardless of role type)",
    span_selector=SpanSelector(
        span_type=SpanType.TASKS_PROVISIONED_USER_SESSION,
        span_conditions_dict={},
    ),
)

DISTINCT_REGISTERED_PROVISIONED_TASKS_USERS = SpanDistinctUnitCountMetric(
    name="distinct_registered_provisioned_tasks_users",
    display_name="Distinct Registered Provisioned Tasks Users",
    description=(
        "Number of distinct Tasks users who are provisioned to have tool access (regardless of role type) "
        "who have signed up/logged into the Tasks tool at least once"
    ),
    span_selector=SpanSelector(
        span_type=SpanType.TASKS_PROVISIONED_USER_SESSION,
        span_conditions_dict={"is_registered": ["true"]},
    ),
)

DISTINCT_PROVISIONED_PRIMARY_TASKS_USERS = SpanDistinctUnitCountMetric(
    name="distinct_provisioned_primary_tasks_users",
    display_name="Distinct Provisioned Primary Tasks Users",
    description="Number of distinct primary Tasks users who are provisioned to have tool access",
    span_selector=SpanSelector(
        span_type=SpanType.TASKS_PROVISIONED_USER_SESSION,
        span_conditions_dict={"is_primary_user": ["true"]},
    ),
)

DISTINCT_REGISTERED_PRIMARY_TASKS_USERS = SpanDistinctUnitCountMetric(
    name="distinct_registered_primary_tasks_users",
    display_name="Distinct Total Registered Primary Tasks Users",
    description="Number of distinct primary Tasks users who have signed up/logged into the Tasks tool at least once",
    span_selector=SpanSelector(
        span_type=SpanType.TASKS_PRIMARY_USER_REGISTRATION_SESSION,
        span_conditions_dict={},
    ),
)

DISTINCT_LOGGED_IN_PRIMARY_TASKS_USERS = EventDistinctUnitCountMetric(
    name="distinct_logged_in_primary_tasks_users",
    display_name="Distinct Logged In Primary Tasks Users",
    description="Number of distinct primary Tasks users who logged into Tasks",
    event_selector=EventSelector(
        event_type=EventType.TASKS_USER_LOGIN,
        event_conditions_dict={},
    ),
)


LOGINS_PRIMARY_TASKS_USERS = EventCountMetric(
    name="logins_primary_tasks_user",
    display_name="Logins, Primary Tasks Users",
    description="Number of logins performed by primary Tasks users",
    event_selector=EventSelector(
        event_type=EventType.TASKS_USER_LOGIN,
        event_conditions_dict={},
    ),
)

DISTINCT_ACTIVE_PRIMARY_TASKS_USERS = EventDistinctUnitCountMetric(
    name="distinct_active_primary_tasks_users",
    display_name="Distinct Active Primary Tasks Users",
    description="Number of distinct primary Tasks users having at least one active usage event",
    event_selector=EventSelector(
        event_type=EventType.TASKS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={},
    ),
)

# CPA Usage
DISTINCT_PROVISIONED_CASE_PLANNING_ASSISTANT_USERS = SpanDistinctUnitCountMetric(
    name="distinct_provisioned_case_planning_assistant_users",
    display_name="Distinct Provisioned Case Planning Assistant Users",
    description="Number of distinct Case Planning Assistant users who are provisioned to have tool access",
    span_selector=SpanSelector(
        span_type=SpanType.GLOBAL_PROVISIONED_USER_SESSION,
        span_conditions_dict={"is_provisioned_case_planning_assistant": ["true"]},
    ),
)

DISTINCT_REGISTERED_CASE_PLANNING_ASSISTANT_USERS = SpanDistinctUnitCountMetric(
    name="distinct_registered_case_planning_assistant_users",
    display_name="Distinct Registered Case Planning Assistant Users",
    description="Number of distinct Case Planning Assistant users who have signed up/logged into the Case Planning Assistant tool at least once",
    span_selector=SpanSelector(
        span_type=SpanType.GLOBAL_PROVISIONED_USER_SESSION,
        span_conditions_dict={"is_registered_case_planning_assistant": ["true"]},
    ),
)

DISTINCT_LOGGED_IN_CASE_PLANNING_ASSISTANT_USERS = EventDistinctUnitCountMetric(
    name="distinct_logged_in_case_planning_assistant_users",
    display_name="Distinct Logged In Case Planning Assistant Users",
    description="Number of distinct Case Planning Assistant users who logged into the Case Planning Assistant tool",
    event_selector=EventSelector(
        event_type=EventType.GLOBAL_USER_LOGIN,
        event_conditions_dict={"has_case_planning_assistant_access": ["true"]},
    ),
)

DISTINCT_ACTIVE_CASE_PLANNING_ASSISTANT_USERS = EventDistinctUnitCountMetric(
    name="distinct_active_case_planning_assistant_users",
    display_name="Distinct Active Case Planning Assistant Users",
    description="Number of distinct Case Planning Assistant users having at least one active usage event",
    event_selector=EventSelector(
        event_type=EventType.GLOBAL_USER_ACTIVE_USAGE_EVENT,
        event_conditions_dict={"product_type": ["CASE_PLANNING_ASSISTANT"]},
    ),
)

AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_SUPERVISION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_marked_ineligible_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Marked Ineligible, {b.task_title}",
        description=f"Average daily count of residents marked ineligible for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "marked_ineligible": ["true"],
                "is_eligible": ["true"],
                "task_type": [b.task_type_name],
                "is_surfaceable": ["true"],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_INCARCERATION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_marked_ineligible_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Marked Ineligible, {b.task_title}",
        description=f"Average daily count of residents marked ineligible for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "marked_ineligible": ["true"],
                "is_eligible": ["true"],
                "task_type": [b.task_type_name],
                "is_surfaceable": ["true"],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

AVG_DAILY_POPULATION_TASK_MARKED_SUBMITTED_METRICS_SUPERVISION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_marked_submitted_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Marked Submitted, {b.task_title}",
        description=f"Average daily count of clients marked submitted for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "in_progress": ["true"],
                "is_eligible": ["true"],
                "marked_ineligible": ["false"],
                "is_surfaceable": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

AVG_DAILY_POPULATION_TASK_MARKED_SUBMITTED_METRICS_INCARCERATION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_marked_submitted_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Marked Submitted, {b.task_title}",
        description=f"Average daily count of residents marked submitted for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "in_progress": ["true"],
                "is_eligible": ["true"],
                "marked_ineligible": ["false"],
                "is_surfaceable": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_INCARCERATION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_almost_eligible_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Almost Eligible, {b.task_title}",
        description=f"Average daily count of residents almost eligible for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_almost_eligible": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_INCARCERATION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_eligible_and_viewed_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Eligible And Viewed, {b.task_title}",
        description=f"Average daily count of residents eligible and viewed for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
                "viewed": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_SUPERVISION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_eligible_and_viewed_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Eligible And Viewed, {b.task_title}",
        description=f"Average daily count of residents eligible and viewed for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
                "viewed": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

AVG_DAILY_POPULATION_TASK_ELIGIBLE_ACTIONABLE_AND_VIEWED = DailyAvgSpanCountMetric(
    name="avg_daily_population_task_eligible_actionable_and_viewed",
    display_name="Average Population: Eligible, Actionable, And Viewed",
    description="Average daily count of clients eligible, actionable (visible, not marked ineligible, not marked in progress), and viewed (clicked-on)",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_eligible": ["true"],
            "is_surfaceable": ["true"],
            "in_progress": ["false"],
            "marked_ineligible": ["false"],
            "viewed": ["true"],
        },
    ),
)

AVG_DAILY_POPULATION_TASK_ELIGIBLE_ACTIONABLE_AND_VIEWED_METRICS_SUPERVISION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_eligible_actionable_and_viewed_{b.task_type_name.lower()}",
        display_name=f"Average Population: Eligible, Actionable, And Viewed, {b.task_title}",
        description=f"Average daily count of clients eligible, actionable (visible, not marked ineligible, not marked in progress), and viewed (clicked-on) for task type:  {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
                "is_surfaceable": ["true"],
                "in_progress": ["false"],
                "marked_ineligible": ["false"],
                "viewed": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

AVG_DAILY_POPULATION_TASK_ELIGIBLE_ACTIONABLE_AND_VIEWED_METRICS_INCARCERATION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_eligible_actionable_and_viewed_{b.task_type_name.lower()}",
        display_name=f"Average Population: Eligible, Actionable, And Viewed, {b.task_title}",
        description=f"Average daily count of clients eligible, actionable (visible, not marked ineligible, not marked in progress), and viewed (clicked-on) for task type:  {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
                "is_surfaceable": ["true"],
                "in_progress": ["false"],
                "marked_ineligible": ["false"],
                "viewed": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED = DailyAvgSpanCountMetric(
    name="avg_daily_population_task_eligible_and_unviewed",
    display_name="Average Population: Task Eligible And Unviewed",
    description="Average daily count of residents eligible and unviewed",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_eligible": ["true"],
            "viewed": ["false"],
            "is_surfaceable": ["true"],
        },
    ),
)

AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_SUPERVISION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_eligible_and_not_viewed_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Eligible And Not Viewed, {b.task_title}",
        description=f"Average daily count of residents eligible and not viewed for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
                "viewed": ["false"],
                "is_surfaceable": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_INCARCERATION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_eligible_and_not_viewed_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Eligible And Not Viewed, {b.task_title}",
        description=f"Average daily count of residents eligible and not viewed for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
                "viewed": ["false"],
                "is_surfaceable": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_30_DAYS = DailyAvgSpanCountMetric(
    name="avg_daily_population_task_eligible_and_unviewed_30_days",
    display_name="Average Population: Task Eligible And Unviewed >30 Days",
    description="Average daily count of clients eligible and unviewed for >30 days",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        span_conditions_dict={
            "is_eligible_past_30_days": ["true"],
            "viewed": ["false"],
            "is_surfaceable": ["true"],
            "in_progress": ["false"],
            "marked_ineligible": ["false"],
        },
    ),
)

AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_LESS_THAN_30_DAYS = (
    DailyAvgSpanCountMetric(
        name="avg_daily_population_task_eligible_and_unviewed_less_than_30_days",
        display_name="Average Population: Task Eligible And Unviewed <30 Days",
        description="Average daily count of clients eligible and unviewed for <30 days",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_eligible_past_30_days": ["false"],
                "viewed": ["false"],
                "is_surfaceable": ["true"],
                "in_progress": ["false"],
                "marked_ineligible": ["false"],
                "is_eligible": ["true"],
            },
        ),
    )
)

AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_30_DAYS_METRICS_SUPERVISION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_eligible_and_unviewed_30_days_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Eligible And Unviewed >30 Days, {b.task_title}",
        description=f"Average daily count of clients eligible and unviewed for >30 days for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_eligible_past_30_days": ["true"],
                "viewed": ["false"],
                "is_surfaceable": ["true"],
                "in_progress": ["false"],
                "marked_ineligible": ["false"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_LESS_THAN_30_DAYS_METRICS_SUPERVISION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_eligible_and_unviewed_less_than_30_days_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Eligible And Unviewed <30 Days, {b.task_title}",
        description=f"Average daily count of clients eligible and unviewed for <30 days for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
                "is_eligible_past_30_days": ["false"],
                "viewed": ["false"],
                "is_surfaceable": ["true"],
                "in_progress": ["false"],
                "marked_ineligible": ["false"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_30_DAYS_METRICS_INCARCERATION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_eligible_and_unviewed_30_days_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Eligible And Unviewed >30 Days, {b.task_title}",
        description=f"Average daily count of clients eligible and unviewed for >30 days for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_eligible_past_30_days": ["true"],
                "viewed": ["false"],
                "is_surfaceable": ["true"],
                "in_progress": ["false"],
                "marked_ineligible": ["false"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_LESS_THAN_30_DAYS_METRICS_INCARCERATION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_eligible_and_unviewed_less_than_30_days_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Eligible And Unviewed <30 Days, {b.task_title}",
        description=f"Average daily count of clients eligible and unviewed for <30 days for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_eligible": ["true"],
                "is_eligible_past_30_days": ["false"],
                "viewed": ["false"],
                "is_surfaceable": ["true"],
                "in_progress": ["false"],
                "marked_ineligible": ["false"],
                "task_type": [b.task_type_name],
            },
        ),
    )
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_SUPERVISION = [
    DailyAvgSpanCountMetric(
        name=f"avg_daily_population_task_almost_eligible_{b.task_type_name.lower()}",
        display_name=f"Average Population: Task Almost Eligible, {b.task_title}",
        description=f"Average daily count of residents almost eligible for task of type: {b.task_title.lower()}",
        span_selector=SpanSelector(
            span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
            span_conditions_dict={
                "is_almost_eligible": ["true"],
                "task_type": [b.task_type_name],
            },
        ),
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

supervision_task_types = [
    b.task_type_name
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

DISTINCT_ACTIVE_USERS_ALL_SUPERVISION_TASKS = EventDistinctUnitCountMetric(
    name="distinct_active_users_supervision",
    display_name="Distinct Active Users",
    description="Number of distinct supervision Workflows users having at least one usage event during the time period",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={"task_type": supervision_task_types},
    ),
)

incarceration_task_types = [
    b.task_type_name
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.INCARCERATION
]

DISTINCT_ACTIVE_USERS_ALL_INCARCERATION_TASKS = EventDistinctUnitCountMetric(
    name="distinct_active_users_incarceration",
    display_name="Distinct Active Users",
    description="Number of distinct incarceration Workflows users having at least one usage event during the time period",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_ACTIVE_USAGE_EVENT,
        event_conditions_dict={"task_type": incarceration_task_types},
    ),
)

DISTINCT_REGISTERED_USERS_SUPERVISION = SpanDistinctUnitCountMetric(
    name="distinct_registered_users_supervision",
    display_name="Distinct Total Registered Users",
    description="Number of distinct Workflows users who have signed up/logged into Workflows at least once",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION,
        span_conditions_dict={"system_type": ["SUPERVISION"]},
    ),
)

DISTINCT_REGISTERED_USERS_INCARCERATION = SpanDistinctUnitCountMetric(
    name="distinct_registered_users_incarceration",
    display_name="Distinct Total Registered Users",
    description="Number of distinct Workflows users who have signed up/logged into Workflows at least once",
    span_selector=SpanSelector(
        span_type=SpanType.WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION,
        span_conditions_dict={"system_type": ["INCARCERATION"]},
    ),
)

DISTINCT_ACTIVE_WORKFLOWS_PRIMARY_USERS_ALL_TOOLS = EventDistinctUnitCountMetric(
    name="distinct_active_workflows_primary_users_all_tools",
    display_name="Distinct Active Line Staff (Workflows Primary Users) Across All Tools",
    description="Number of distinct line staff users (equivalent of Workflows primary users) having at least one active usage event across all tools during the time period",
    event_selector=EventSelector(
        event_type=EventType.ALL_TOOLS_LINE_STAFF_ACTIVE_USAGE_EVENT,
        event_conditions_dict={},
    ),
)
