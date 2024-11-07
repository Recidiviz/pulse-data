# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
    EventValueMetric,
    MiscAggregatedMetric,
    SumSpanDaysMetric,
)
from recidiviz.calculator.query.state.views.analyst_data.insights_caseload_category_sessions import (
    CASELOAD_CATEGORIES_BY_CATEGORY_TYPE,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
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

AVG_ASSIGNMENTS_OFFICER = MiscAggregatedMetric(
    name="avg_assignments_officer",
    display_name="Average Asssignments Per Officer",
    description="Average of the number of assignments to an officer's caseload",
    populations=[MetricPopulationType.SUPERVISION],
    unit_of_analysis_types=[
        MetricUnitOfAnalysisType.SUPERVISION_UNIT,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
        MetricUnitOfAnalysisType.STATE_CODE,
    ],
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

AVG_CRITICAL_CASELOAD_SIZE = MiscAggregatedMetric(
    name="avg_critical_caseload_size",
    display_name="Average Critical Officer Caseload",
    description="Average count of clients in the officer's caseload among days when "
    "officer has critical caseload size",
    populations=[MetricPopulationType.SUPERVISION],
    unit_of_analysis_types=[MetricUnitOfAnalysisType.SUPERVISION_OFFICER],
)

AVG_CRITICAL_CASELOAD_SIZE_OFFICER = MiscAggregatedMetric(
    name="avg_critical_caseload_size_officer",
    display_name="Average Critical Caseload Size Per Officer",
    description="Average of the count of clients in each officer's caseload among days "
    "when officer has critical caseload size",
    populations=[MetricPopulationType.SUPERVISION],
    unit_of_analysis_types=[
        MetricUnitOfAnalysisType.SUPERVISION_UNIT,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
        MetricUnitOfAnalysisType.STATE_CODE,
    ],
)

AVG_DAILY_CASELOAD_OFFICER = MiscAggregatedMetric(
    name="avg_daily_caseload_officer",
    display_name="Average Daily Caseload Per Officer",
    description="Average of the daily population size on each officer caseload",
    populations=[MetricPopulationType.SUPERVISION],
    unit_of_analysis_types=[
        MetricUnitOfAnalysisType.SUPERVISION_UNIT,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
        MetricUnitOfAnalysisType.STATE_CODE,
    ],
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

AVG_DAILY_POPULATION_CRIME_AGAINST_PERSON = DailyAvgSpanCountMetric(
    name="avg_population_crime_against_person",
    display_name="Average Population: Crime Against Person",
    description="Average daily count of clients sentenced for a crime against person",
    span_selector=SpanSelector(
        span_type=SpanType.SENTENCE_SPAN,
        span_conditions_dict={"any_is_crime_against_person": ["true"]},
    ),
)

AVG_DAILY_POPULATION_CRIME_AGAINST_PROPERTY = DailyAvgSpanCountMetric(
    name="avg_population_crime_against_property",
    display_name="Average Population: Crime Against Property",
    description="Average daily count of clients sentenced for crime against property",
    span_selector=SpanSelector(
        span_type=SpanType.SENTENCE_SPAN,
        span_conditions_dict={"any_is_crime_against_property": ["true"]},
    ),
)

AVG_DAILY_POPULATION_CRIME_AGAINST_SOCIETY = DailyAvgSpanCountMetric(
    name="avg_population_crime_against_society",
    display_name="Average Population: Crime Against Society",
    description="Average daily count of clients sentenced for crime against society",
    span_selector=SpanSelector(
        span_type=SpanType.SENTENCE_SPAN,
        span_conditions_dict={"any_is_crime_against_society": ["true"]},
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
        span_conditions_dict={"gender": ["FEMALE"]},
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

AVG_DAILY_POPULATION_HIGH_RISK_LEVEL = DailyAvgSpanCountMetric(
    name="avg_population_high_risk_level",
    display_name="Average Population: High Risk Level",
    description="Average daily count of clients with a high assessed risk level",
    span_selector=SpanSelector(
        span_type=SpanType.ASSESSMENT_SCORE_SESSION,
        span_conditions_dict={
            "assessment_level": ["HIGH", "MEDIUM_HIGH", "MAXIMUM", "VERY_HIGH"]
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

AVG_DAILY_POPULATION_LOW_RISK_LEVEL = DailyAvgSpanCountMetric(
    name="avg_population_low_risk_level",
    display_name="Average Population: Low Risk Level",
    description="Average daily count of clients with a low assessed risk level",
    span_selector=SpanSelector(
        span_type=SpanType.ASSESSMENT_SCORE_SESSION,
        span_conditions_dict={"assessment_level": ["LOW", "LOW_MEDIUM", "MINIMUM"]},
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
                "SERIOUS_MENTAL_ILLNESS",
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
    "SERIOUS_MENTAL_ILLNESS", "MENTAL_HEALTH_COURT"
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

AVG_DAILY_POPULATION_DRUG_OFFENSE_SENTENCE = DailyAvgSpanCountMetric(
    name="avg_population_drug_offense_sentence",
    display_name="Average Population: Drug Offense",
    description="Average daily population of clients sentenced for at least one drug "
    "offense",
    span_selector=SpanSelector(
        span_type=SpanType.SENTENCE_SPAN,
        span_conditions_dict={"any_is_drug_uniform": ["true"]},
    ),
)

AVG_DAILY_POPULATION_VIOLENT_OFFENSE_SENTENCE = DailyAvgSpanCountMetric(
    name="avg_population_violent_offense_sentence",
    display_name="Average Population: Violent Offense",
    description="Average daily population of clients sentenced for at least one violent"
    " offense",
    span_selector=SpanSelector(
        span_type=SpanType.SENTENCE_SPAN,
        span_conditions_dict={"any_is_violent_uniform": ["true"]},
    ),
)

_SUPERVISION_LEVEL_SPAN_ATTRIBUTE_DICT: Dict[str, Union[str, List[str]]] = {
    "limited": ["LIMITED"],
    "minimum": ["MINIMUM"],
    "medium": ["MEDIUM"],
    "maximum": ["HIGH", "MAXIMUM"],
    "unsupervised": ["UNSUPERVISED"],
    "diversion": ["DIVERSION"],
    "unknown": 'NOT IN ("LIMITED", "MINIMUM", "MEDIUM", "HIGH", "MAXIMUM", '
    '"EXTERNAL_UNKNOWN", "INTERNAL_UNKNOWN")',
    "other": ["EXTERNAL_UNKNOWN", "INTERNAL_UNKNOWN"],
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

AVG_DAILY_POPULATION_MAXIMUM_CUSTODY = DailyAvgSpanCountMetric(
    name="avg_population_max_custody",
    display_name="Average Population: Maximum Custody",
    description="Average daily population of individuals in maximum custody",
    span_selector=SpanSelector(
        span_type=SpanType.CUSTODY_LEVEL_SESSION,
        span_conditions_dict={"custody_level": ["MAXIMUM"]},
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

AVG_DAILY_POPULATION_MEDIUM_CUSTODY = DailyAvgSpanCountMetric(
    name="avg_population_medium_custody",
    display_name="Average Population: Medium Custody",
    description="Average daily population of individuals in medium custody",
    span_selector=SpanSelector(
        span_type=SpanType.CUSTODY_LEVEL_SESSION,
        span_conditions_dict={"custody_level": ["MEDIUM", "CLOSE"]},
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

AVG_DAILY_POPULATION_MINIMUM_CUSTODY = DailyAvgSpanCountMetric(
    name="avg_population_min_custody",
    display_name="Average Population: Minimum Custody",
    description="Average daily population of individuals in minimum custody",
    span_selector=SpanSelector(
        span_type=SpanType.CUSTODY_LEVEL_SESSION,
        span_conditions_dict={
            "custody_level": [
                "MINIMUM",
                "RESTRICTIVE_MINIMUM",
                "INTERNAL_UNKNOWN",
            ],
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
        span_type=SpanType.SUPERVISION_CASE_COMPLIANCE_SPAN,
        span_conditions_dict={"assessment_required": ["true"]},
    ),
)

AVG_DAILY_POPULATION_ASSESSMENT_OVERDUE = DailyAvgSpanCountMetric(
    name="avg_population_assessment_overdue",
    display_name="Average Population: Clients Requiring Risk Assessment Whose Assessments Are Overdue",
    description="Average daily population of clients requiring a risk assessment based on their "
    "supervision level who are overdue to receive one",
    span_selector=SpanSelector(
        span_type=SpanType.SUPERVISION_CASE_COMPLIANCE_SPAN,
        span_conditions_dict={
            "assessment_required": ["true"],
            "assessment_overdue": ["true"],
        },
    ),
)

AVG_DAILY_POPULATION_CONTACT_REQUIRED = DailyAvgSpanCountMetric(
    name="avg_population_contact_required",
    display_name="Average Population: Clients Requiring A Face-To-Face Contact",
    description="Average daily population of clients requiring a face-to-face contact based on "
    "their supervision level",
    span_selector=SpanSelector(
        span_type=SpanType.SUPERVISION_CASE_COMPLIANCE_SPAN,
        span_conditions_dict={"contact_required": ["true"]},
    ),
)

AVG_DAILY_POPULATION_CONTACT_OVERDUE = DailyAvgSpanCountMetric(
    name="avg_population_contact_overdue",
    display_name="Average Population: Clients Requiring Face-To-Face-Contact Whose Contacts Are "
    "Overdue",
    description="Average daily population of clients requiring a face-to-face contact based on "
    "their supervision level who are overdue to receive one",
    span_selector=SpanSelector(
        span_type=SpanType.SUPERVISION_CASE_COMPLIANCE_SPAN,
        span_conditions_dict={
            "contact_required": ["true"],
            "contact_overdue": ["true"],
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

DAYS_SINCE_MOST_RECENT_COMPLETED_CONTACT = DailyAvgTimeSinceSpanStartMetric(
    name="avg_days_since_most_recent_completed_contact",
    display_name="Days Since Most Recent Completed Contact",
    description="Average number of days since a client's most recent completed "
    "contact, across all days on which client is in population",
    span_selector=SpanSelector(
        span_type=SpanType.COMPLETED_CONTACT_SESSION,
        span_conditions_dict={},
    ),
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
INCARCERATION_RELEASES_1_MONTH_AFTER_PAROLE_ELIGIBILITY_DATE = EventCountMetric(
    name="incarceration_releases_1_month_after_parole_eligibility_date",
    display_name="Incarceration Releases 1 Month After Parole Eligibility Date",
    description="Number of releases occurring at least 1 month after one's parole eligibility date",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_RELEASE,
        event_conditions_dict={"parole_release_1_month_flag": ["true"]},
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

DAYS_SENTENCED_AT_LIBERTY_START = EventValueMetric(
    name="days_sentenced_at_liberty_start",
    display_name="Days Sentenced At Liberty Start",
    description="Days sentenced as of the start of liberty",
    event_selector=EventSelector(
        event_type=EventType.TRANSITIONS_TO_LIBERTY_FROM_IN_STATE,
        event_conditions_dict={},
    ),
    event_value_numeric="days_sentenced",
    event_count_metric=LIBERTY_STARTS,
)

DAYS_SERVED_AT_LIBERTY_START = EventValueMetric(
    name="days_served_at_liberty_start",
    display_name="Days Served At Liberty Start",
    description="Days served as of the start of liberty",
    event_selector=EventSelector(
        event_type=EventType.TRANSITIONS_TO_LIBERTY_FROM_IN_STATE,
        event_conditions_dict={},
    ),
    event_value_numeric="days_served",
    event_count_metric=LIBERTY_STARTS,
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
        span_conditions_dict={},
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

PROP_PERIOD_WITH_CRITICAL_CASELOAD = MiscAggregatedMetric(
    name="prop_period_with_critical_caseload",
    display_name="Proportion Of Analysis Period With Critical Caseload",
    description="Proportion of the analysis period for which an officer has a critical "
    "caseload size",
    populations=[MetricPopulationType.SUPERVISION],
    unit_of_analysis_types=[MetricUnitOfAnalysisType.SUPERVISION_OFFICER],
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

INCARCERATION_RELEASES = EventCountMetric(
    name="incarceration_releases",
    display_name="Incarceration Releases",
    description="Number of transitions from incarceration to liberty or supervision",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_RELEASE,
        event_conditions_dict={"outflow_to_level_1": ["LIBERTY", "SUPERVISION"]},
    ),
)

PROP_SENTENCE_SERVED_AT_INCARCERATION_TO_SUPERVISION_TRANSITION = EventValueMetric(
    name="prop_sentence_at_incarceration_outflow_to_supervision",
    display_name="Proportion Sentence Served At Incarceration To Supervision "
    "Transition",
    description="Average proportion of sentence served at the transition from "
    "incarceration to supervision",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_RELEASE,
        event_conditions_dict={"outflow_to_level_1": ["SUPERVISION"]},
    ),
    event_value_numeric="prop_sentence_served",
    event_count_metric=SUPERVISION_STARTS_FROM_INCARCERATION,
)

PROP_SENTENCE_SERVED_AT_LIBERTY_START = EventValueMetric(
    name="prop_sentence_served_at_liberty_start",
    display_name="Proportion Sentence Served At Liberty Start",
    description="Average proportion of sentence served as of the release event",
    event_selector=EventSelector(
        event_type=EventType.TRANSITIONS_TO_LIBERTY_FROM_IN_STATE,
        event_conditions_dict={},
    ),
    event_value_numeric="prop_sentence_served",
    event_count_metric=LIBERTY_STARTS,
)

PROP_SENTENCE_SERVED_AT_INCARCERATION_RELEASE = EventValueMetric(
    name="prop_sentence_served_at_incarceration_release",
    display_name="Proportion Sentence Served At Incarceration Release",
    description="Average proportion of sentence served as of the release from "
    "incarceration to liberty or supervision",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_RELEASE,
        event_conditions_dict={"outflow_to_level_1": ["LIBERTY", "SUPERVISION"]},
    ),
    event_value_numeric="prop_sentence_served",
    event_count_metric=INCARCERATION_RELEASES,
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

SUPERVISION_STARTS_1_MONTH_AFTER_PAROLE_ELIGIBILITY_DATE = EventCountMetric(
    name="supervision_starts_1_month_after_parole_eligibility_date",
    display_name="Supervision Starts One Month After Parole Eligibility",
    description="Number of people who are delayed for parole by at least 1 month",
    event_selector=EventSelector(
        event_type=EventType.INCARCERATION_RELEASE,
        event_conditions_dict={
            "parole_release_1_month_flag": ["true"],
            "outflow_to_level_1": ["SUPERVISION"],
        },
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

WORKFLOWS_CLIENT_STATUS_UPDATE = EventCountMetric(
    name="workflows_client_status_update",
    display_name="Workflows App Client Status Updates",
    description="Number of updates made on workflows app",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_USER_CLIENT_STATUS_UPDATE,
        event_conditions_dict={},
    ),
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
