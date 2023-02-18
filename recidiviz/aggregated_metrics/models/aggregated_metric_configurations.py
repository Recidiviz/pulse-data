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
from typing import Dict, List

from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentAvgSpanValueMetric,
    AssignmentDaysToFirstEventMetric,
    AssignmentSpanDaysMetric,
    DailyAvgSpanCountMetric,
    DailyAvgSpanValueMetric,
    DailyAvgTimeSinceSpanStartMetric,
    EventCountMetric,
    EventValueMetric,
    SumSpanDaysMetric,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.task_eligibility.task_completion_event_big_query_view_collector import (
    TaskCompletionEventBigQueryViewCollector,
)

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

ABSCONSIONS_BENCH_WARRANTS = EventCountMetric(
    name="absconsions_bench_warrants",
    display_name="Absconsions/Bench Warrants",
    description="Number of absconsions or bench warrants",
    event_types=["ABSCONSION_BENCH_WARRANT"],
    event_attribute_filters={},
)

AVG_AGE = DailyAvgTimeSinceSpanStartMetric(
    name="avg_age",
    display_name="Average Age",
    description="Average daily age of the population",
    span_types=["PERSON_DEMOGRAPHICS"],
    span_attribute_filters={},
    scale_to_year=True,
)

AVG_DAILY_POPULATION = DailyAvgSpanCountMetric(
    name="avg_daily_population",
    display_name="Average Population",
    description="Average daily count of clients in the population",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={},
)

AVG_DAILY_POPULATION_COMMUNITY_CONFINEMENT = DailyAvgSpanCountMetric(
    name="avg_population_community_confinement",
    display_name="Average Population: Community Confinement",
    description="Average daily count of clients in community confinement",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["INCARCERATION", "SUPERVISION"],
        "compartment_level_2": ["COMMUNITY_CONFINEMENT"],
    },
)

AVG_DAILY_POPULATION_DOMESTIC_VIOLENCE_CASE_TYPE = DailyAvgSpanCountMetric(
    name="avg_population_domestic_violence_case_type",
    display_name="Average Population: Domestic Violence Case Type",
    description="Average daily count of clients on supervision with a domestic violence case type",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["SUPERVISION"],
        "case_type_start": ["DOMESTIC_VIOLENCE"],
    },
)

AVG_DAILY_POPULATION_DRUG_CASE_TYPE = DailyAvgSpanCountMetric(
    name="avg_population_drug_case_type",
    display_name="Average Population: Drug Case Type",
    description="Average daily count of clients on supervision with a drug case type",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["SUPERVISION"],
        "case_type_start": ["DRUG_COURT"],
    },
)

AVG_DAILY_POPULATION_EMPLOYED = DailyAvgSpanCountMetric(
    name="avg_population_employed",
    display_name="Average Population With Employment",
    description="Average daily count of clients with some form of employment or alternate occupation/status",
    span_types=["EMPLOYMENT_STATUS_SESSION"],
    span_attribute_filters={"is_employed": ["true"]},
)

AVG_DAILY_POPULATION_FEMALE = DailyAvgSpanCountMetric(
    name="avg_population_female",
    display_name="Average Population: Female",
    description="Average daily count of female clients in the population",
    span_types=["PERSON_DEMOGRAPHICS"],
    span_attribute_filters={
        "gender": ["FEMALE"],
    },
)

AVG_DAILY_POPULATION_GENERAL_CASE_TYPE = DailyAvgSpanCountMetric(
    name="avg_population_general_case_type",
    display_name="Average Population: General Case Type",
    description="Average daily count of clients on supervision with a general case type",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["SUPERVISION"],
        "case_type_start": ["GENERAL"],
    },
)

AVG_DAILY_POPULATION_GENERAL_INCARCERATION = DailyAvgSpanCountMetric(
    name="avg_population_general_incarceration",
    display_name="Average Population: General Incarceration",
    description="Average daily count of clients in general incarceration",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["INCARCERATION"],
        "compartment_level_2": ["GENERAL"],
    },
)

AVG_DAILY_POPULATION_HIGH_RISK_LEVEL = DailyAvgSpanCountMetric(
    name="avg_population_high_risk_level",
    display_name="Average Population: High Risk Level",
    description="Average daily count of clients with a high assessed risk level",
    span_types=["ASSESSMENT_SCORE_SESSION"],
    span_attribute_filters={
        "assessment_level": ["HIGH", "MEDIUM_HIGH", "MAXIMUM", "VERY_HIGH"],
    },
)

AVG_DAILY_POPULATION_LOW_RISK_LEVEL = DailyAvgSpanCountMetric(
    name="avg_population_low_risk_level",
    display_name="Average Population: Low Risk Level",
    description="Average daily count of clients with a low assessed risk level",
    span_types=["ASSESSMENT_SCORE_SESSION"],
    span_attribute_filters={
        "assessment_level": ["LOW", "LOW_MEDIUM", "MINIMUM"],
    },
)

AVG_DAILY_POPULATION_MENTAL_HEALTH_CASE_TYPE = DailyAvgSpanCountMetric(
    name="avg_population_mental_health_case_type",
    display_name="Average Population: Mental Health Case Type",
    description="Average daily count of clients on supervision with a mental health case type",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["SUPERVISION"],
        "case_type_start": ["SERIOUS_MENTAL_ILLNESS", "MENTAL_HEALTH_COURT"],
    },
)

AVG_DAILY_POPULATION_NONWHITE = DailyAvgSpanCountMetric(
    name="avg_population_nonwhite",
    display_name="Average Population: Non-White",
    description="Average daily count of non-white clients",
    span_types=["PERSON_DEMOGRAPHICS"],
    span_attribute_filters={
        "prioritized_race_or_ethnicity": '!= "WHITE"',
    },
)

AVG_DAILY_POPULATION_OTHER_CASE_TYPE = DailyAvgSpanCountMetric(
    name="avg_population_other_case_type",
    display_name="Average Population: Other Case Type",
    description="Average daily count of clients on supervision with other case type",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["SUPERVISION"],
        "case_type_start": """NOT IN (
        "GENERAL", "DOMESTIC_VIOLENCE", "SEX_OFFENSE", "DRUG_COURT", 
        "SERIOUS_MENTAL_ILLNESS", "MENTAL_HEALTH_COURT"
    )""",
    },
)

AVG_DAILY_POPULATION_PAROLE = DailyAvgSpanCountMetric(
    name="avg_population_parole",
    display_name="Average Population: Parole",
    description="Average daily count of clients on parole",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["SUPERVISION"],
        "compartment_level_2": ["PAROLE", "DUAL"],
    },
)

AVG_DAILY_POPULATION_PAROLE_BOARD_HOLD = DailyAvgSpanCountMetric(
    name="avg_population_parole_board_hold",
    display_name="Average Population: Parole Board Hold",
    description="Average daily count of clients in a parole board hold",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["INCARCERATION"],
        "compartment_level_2": ["PAROLE_BOARD_HOLD"],
    },
)

AVG_DAILY_POPULATION_PROBATION = DailyAvgSpanCountMetric(
    name="avg_population_probation",
    display_name="Average Population: Probation",
    description="Average daily count of clients on probation",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["SUPERVISION"],
        "compartment_level_2": ["PROBATION"],
    },
)

AVG_DAILY_POPULATION_SEX_OFFENSE_CASE_TYPE = DailyAvgSpanCountMetric(
    name="avg_population_sex_offense_case_type",
    display_name="Average Population: Sex Offense Case Type",
    description="Average daily count of clients on supervision with a sex offense case type",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["SUPERVISION"],
        "case_type_start": ["SEX_OFFENSE"],
    },
)

AVG_DAILY_POPULATION_SHOCK_INCARCERATION = DailyAvgSpanCountMetric(
    name="avg_population_shock_incarceration",
    display_name="Average Population: Shock Incarceration",
    description="Average daily count of clients in shock incarceration",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["INCARCERATION"],
        "compartment_level_2": ["SHOCK_INCARCERATION"],
    },
)

_SUPERVISION_LEVEL_SPAN_ATTRIBUTE_DICT = {
    "limited": ["LIMITED"],
    "minimum": ["MINIMUM"],
    "medium": ["MEDIUM"],
    "maximum": ["HIGH", "MAXIMUM"],
    "unknown": 'NOT IN ("LIMITED", "MINIMUM", "MEDIUM", "HIGH", "MAXIMUM", '
    '"EXTERNAL_UNKNOWN", "INTERVAL_UNKNOWN")',
    "other": ["EXTERNAL_UNKNOWN", "INTERVAL_UNKNOWN"],
}
AVG_DAILY_POPULATION_SUPERVISION_LEVEL_METRICS = [
    DailyAvgSpanCountMetric(
        name=f"avg_population_{level}_supervision_level",
        display_name=f"Average Population: {level.capitalize()} Supervision Level",
        description=f"Average daily count of clients with "
        f"{'an' if level[0] in 'aeiou' else 'a'} {level} supervision level",
        span_types=["SUPERVISION_LEVEL_SESSION"],
        span_attribute_filters={"supervision_level": conditions},
    )
    for level, conditions in _SUPERVISION_LEVEL_SPAN_ATTRIBUTE_DICT.items()
]

AVG_DAILY_POPULATION_TREATMENT_IN_PRISON = DailyAvgSpanCountMetric(
    name="avg_population_treatment_in_prison",
    display_name="Average Population: Treatment In Prison",
    description="Average daily count of clients in treatment-in-prison incarceration",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["INCARCERATION"],
        "compartment_level_2": ["TREATMENT_IN_PRISON"],
    },
)

AVG_DAILY_POPULATION_UNKNOWN_CASE_TYPE = DailyAvgSpanCountMetric(
    name="avg_population_unknown_case_type",
    display_name="Average Population: Unknown Case Type",
    description="Average daily count of clients on supervision with unknown case type",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["SUPERVISION"],
        "case_type_start": "IS NULL",
    },
)

AVG_LSIR_SCORE = DailyAvgSpanValueMetric(
    name="avg_lsir_score",
    display_name="Average LSI-R Score",
    description="Average daily LSI-R score of the population",
    span_types=["ASSESSMENT_SCORE_SESSION"],
    span_attribute_filters={
        "assessment_type": ["LSIR"],
    },
    span_value_numeric="assessment_score",
)

# TODO(#18420): Replace metric type with AssignmentSpanValueAtStart once new metric class is supported
AVG_LSIR_SCORE_AT_ASSIGNMENT = AssignmentAvgSpanValueMetric(
    name="avg_lsir_score_at_assignment",
    display_name="Average LSI-R Score At Assignment",
    description="Average LSI-R score of clients on date of assignment",
    span_types=["ASSESSMENT_SCORE_SESSION"],
    span_attribute_filters={
        "assessment_type": ["LSIR"],
    },
    span_value_numeric="assessment_score",
    window_length_days=1,
)

CONTACTS_ATTEMPTED = EventCountMetric(
    name="contacts_attempted",
    display_name="Contacts: Attempted",
    description="Number of attempted contacts",
    event_types=["SUPERVISION_CONTACT"],
    event_attribute_filters={"contact_status": ["ATTEMPTED"]},
)

CONTACTS_COMPLETED = EventCountMetric(
    name="contacts_completed",
    display_name="Contacts: Completed",
    description="Number of completed contacts",
    event_types=["SUPERVISION_CONTACT"],
    event_attribute_filters={"contact_status": ["COMPLETED"]},
)

CONTACTS_FACE_TO_FACE = EventCountMetric(
    name="contacts_face_to_face",
    display_name="Contacts: Face-To-Face",
    description="Number of completed face-to-face contacts",
    event_types=["SUPERVISION_CONTACT"],
    event_attribute_filters={
        "contact_status": ["COMPLETED"],
        "contact_type": ["DIRECT", "BOTH_COLLATERAL_AND_DIRECT"],
    },
)

CONTACTS_HOME_VISIT = EventCountMetric(
    name="contacts_home_visit",
    display_name="Contacts: Home Visit",
    description="Number of completed home visit contacts",
    event_types=["SUPERVISION_CONTACT"],
    event_attribute_filters={
        "contact_status": ["COMPLETED"],
        "location": ["RESIDENCE"],
        "contact_type": ["DIRECT", "BOTH_COLLATERAL_AND_DIRECT"],
    },
)

DAYS_ABSCONDED_365 = AssignmentSpanDaysMetric(
    name="days_absconded_365",
    display_name="Days Absconded Within 1 Year Of Assignment",
    description="Sum of the number of days with absconsion or bench warrant status "
    "within 1 year following assignment, for all assignments during the analysis period",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_2": ["ABSCONSION", "BENCH_WARRANT"],
    },
    window_length_days=365,
)

DAYS_AT_LIBERTY_365 = AssignmentSpanDaysMetric(
    name="days_at_liberty_365",
    display_name="Days At Liberty Within 1 Year Of Assignment",
    description="Sum of the number of days spent at liberty within 1 year following "
    "assignment, for all assignments during the analysis period",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["LIBERTY"],
    },
    window_length_days=365,
)

DAYS_EMPLOYED_365 = AssignmentSpanDaysMetric(
    name="days_employed_365",
    display_name="Days Employed Within 1 Year Of Assignment",
    description="Sum of the number of days clients had valid employment status within 1 "
    "year following assignment, for all assignments during the analysis period",
    span_types=["EMPLOYMENT_STATUS_SESSION"],
    span_attribute_filters={
        "is_employed": ["true"],
    },
    window_length_days=365,
)

DAYS_IN_COMMUNITY_365 = AssignmentSpanDaysMetric(
    name="days_in_community_365",
    display_name="Days In Community Within 1 Year Of Assignment",
    description="Sum of the number of days spent in community (supervision or at "
    "liberty) within 1 year following assignment, for all assignments during the "
    "analysis period",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["SUPERVISION", "LIBERTY"],
    },
    window_length_days=365,
)

DAYS_INCARCERATED_365 = AssignmentSpanDaysMetric(
    name="days_incarcerated_365",
    display_name="Days Incarcerated Within 1 Year Of Assignment",
    description="Sum of the number of incarcerated days within 1 year following "
    "assignment, for all assignments during the analysis period",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["INCARCERATION"],
    },
    window_length_days=365,
)

DAYS_OUT_OF_STATE_365 = AssignmentSpanDaysMetric(
    name="days_out_of_state_365",
    display_name="Days Out of State Within 1 Year Of Assignment",
    description="Sum of the number of days incarcerated or supervised out of state "
    "within 1 year following assignment, for all assignments during the analysis period",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": [
            "INCARCERATION_OUT_OF_STATE",
            "SUPERVISION_OUT_OF_STATE",
        ],
    },
    window_length_days=365,
)

DAYS_PENDING_CUSTODY_365 = AssignmentSpanDaysMetric(
    name="days_pending_custody_365",
    display_name="Days Pending Custody Within 1 Year Of Assignment",
    description="Sum of the number of days pending custody within 1 year following "
    "assignment, for all assignments during the analysis period",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["PENDING_CUSTODY"],
    },
    window_length_days=365,
)

DAYS_SINCE_MOST_RECENT_COMPLETED_CONTACT = DailyAvgTimeSinceSpanStartMetric(
    name="avg_days_since_most_recent_completed_contact",
    display_name="Days Since Most Recent Completed Contact",
    description="Average number of days since a client's most recent completed contact, "
    "across all days on which client is in population",
    span_types=["COMPLETED_CONTACT_SESSION"],
    span_attribute_filters={},
)

DAYS_SINCE_MOST_RECENT_LSIR = DailyAvgTimeSinceSpanStartMetric(
    name="avg_days_since_most_recent_lsir",
    display_name="Days Since Most Recent LSI-R",
    description="Average number of days since a client's most recent LSI-R assessment, "
    "across all days on which client is in population",
    span_types=["ASSESSMENT_SCORE_SESSION"],
    span_attribute_filters={
        "assessment_type": ["LSIR"],
    },
)

DAYS_SUPERVISED_365 = AssignmentSpanDaysMetric(
    name="days_supervised_365",
    display_name="Days Supervised Within 1 Year Of Assignment",
    description="Sum of the number of supervised days within 1 year following "
    "assignment, for all assignments during the analysis period",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["SUPERVISION"],
    },
    window_length_days=365,
)

DAYS_TO_FIRST_ABSCONSION_BENCH_WARRANT_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_absconsion_bench_warrant_365",
    display_name="Days To First Absconsion/Bench Warrant (Legal Status) Within 1 Year After Assignment",
    description="Sum of the number of days prior to first absconsion/bench warrant legal "
    "status within 1 year following assignment, for all assignments during the analysis period",
    event_types=["ABSCONSION_BENCH_WARRANT"],
    event_attribute_filters={},
    window_length_days=365,
)

DAYS_TO_FIRST_INCARCERATION_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_incarceration_365",
    display_name="Days To First Incarceration Within 1 Year After Assignment",
    description="Sum of the number of days prior to first incarceration within 1 year "
    "following assignment, for all assignments during the analysis period",
    event_types=["INCARCERATION_START"],
    event_attribute_filters={},
    window_length_days=365,
)

DAYS_TO_FIRST_LIBERTY_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_liberty_365",
    display_name="Days To First Liberty Within 1 Year After Assignment",
    description="Sum of the number of days prior to first liberty transition within 1 "
    "year following assignment, for all assignments during the analysis period",
    event_types=["LIBERTY_START"],
    event_attribute_filters={},
    window_length_days=365,
)

DAYS_TO_FIRST_SUPERVISION_START_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_supervision_start_365",
    display_name="Days To First Supervision Start Within 1 Year After Assignment",
    description="Sum of the number of days prior to first supervision start within 1 "
    "year following assignment, for all assignments during the analysis period",
    event_types=["SUPERVISION_START"],
    event_attribute_filters={},
    window_length_days=365,
)

DAYS_TO_FIRST_VIOLATION_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_violation_365",
    display_name="Days To First Violation Within 1 Year After Assignment",
    description="Sum of the number of days prior to first violation within 1 year "
    "following assignment, for all assignments during the analysis period",
    event_types=["VIOLATION"],
    event_attribute_filters={},
    window_length_days=365,
)

DAYS_TO_FIRST_VIOLATION_365_BY_TYPE_METRICS = [
    AssignmentDaysToFirstEventMetric(
        name=f"days_to_first_violation_{category.lower()}_365",
        display_name=f"Days To First {category.title()} Violation Within 1 Year After Assignment",
        description=f"Sum of the number of days prior to first {category.lower()} "
        "violation within 1 year following assignment, for all assignments during the "
        "analysis period",
        event_types=["VIOLATION"],
        event_attribute_filters={"violation_type": types},
        window_length_days=365,
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
]

DAYS_TO_FIRST_VIOLATION_RESPONSE_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_violation_response_365",
    display_name="Days To First Violation Response Within 1 Year After Assignment",
    description="Sum of the number of days prior to first violation response within 1 "
    "year following assignment, for all assignments during the analysis period",
    event_types=["VIOLATION_RESPONSE"],
    event_attribute_filters={},
    window_length_days=365,
)

DAYS_TO_FIRST_VIOLATION_RESPONSE_365_BY_TYPE_METRICS = [
    AssignmentDaysToFirstEventMetric(
        name=f"days_to_first_violation_response_{category.lower()}_365",
        display_name=f"Days To First {category.title()} Violation Response Within 1 Year After Assignment",
        description=f"Sum of the number of days prior to first {category.lower()} "
        "violation response within 1 year following assignment, for all assignments "
        "during the analysis period",
        event_types=["VIOLATION_RESPONSE"],
        event_attribute_filters={"most_serious_violation_type": types},
        window_length_days=365,
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
]

DRUG_SCREENS = EventCountMetric(
    name="drug_screens",
    display_name="Drug Screens",
    description="Number of drug screens",
    event_types=["DRUG_SCREEN"],
    event_attribute_filters={},
)

DRUG_SCREENS_POSITIVE = EventCountMetric(
    name="drug_screens_positive",
    display_name="Drug Screens: Positive Result",
    description="Number of drug screens with a positive result",
    event_types=["DRUG_SCREEN"],
    event_attribute_filters={"is_positive_result": ["true"]},
)

EARLY_DISCHARGE_REQUESTS = EventCountMetric(
    name="early_discharge_requests",
    display_name="Early Discharge Requests",
    description="Number of early discharge requests",
    event_types=["EARLY_DISCHARGE_REQUEST"],
    event_attribute_filters={},
)

EMPLOYED_STATUS_ENDS = EventCountMetric(
    name="employed_status_ends",
    display_name="Employment Lost",
    description="Number of transitions to unemployment",
    event_types=["EMPLOYMENT_STATUS_CHANGE"],
    event_attribute_filters={
        "is_employed": ["false"],
    },
)

EMPLOYED_STATUS_STARTS = EventCountMetric(
    name="employed_status_starts",
    display_name="Employment Gained",
    description="Number of new employment starts following unemployment",
    event_types=["EMPLOYMENT_STATUS_CHANGE"],
    event_attribute_filters={
        "is_employed": ["true"],
    },
)

INCARCERATIONS_INFERRED = EventCountMetric(
    name="incarcerations_inferred",
    display_name="Inferred Incarcerations",
    description="Number of inferred incarceration events that do not align with an observed "
    "incarceration session start",
    event_types=[
        "SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON",
    ],
    event_attribute_filters={"outflow_to_incarceration": ["false"]},
)

INCARCERATIONS_INFERRED_WITH_VIOLATION_TYPE_METRICS = [
    EventCountMetric(
        name=f"incarcerations_inferred_{category.lower()}_violation",
        display_name=f"Inferred Incarcerations, {category.title()} Violation",
        description="Number of inferred incarceration events that do not align with an "
        "observed incarceration session start, for which the most severe violation "
        f"type was {category.lower()}",
        event_types=["SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON"],
        event_attribute_filters={
            "outflow_to_incarceration": ["false"],
            "most_severe_violation_type": types,
        },
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
]

INCARCERATION_STARTS = EventCountMetric(
    name="incarceration_starts",
    display_name="Incarceration Starts",
    description="Number of observed incarceration starts",
    event_types=["INCARCERATION_START"],
    event_attribute_filters={},
)

INCARCERATION_STARTS_WITH_VIOLATION_TYPE_METRICS = [
    EventCountMetric(
        name=f"incarceration_starts_{category.lower()}_violation",
        display_name=f"Incarceration Starts, {category.title()} Violation",
        description="Number of observed incarceration starts for which the most severe "
        f"violation type was {category.lower()}",
        event_types=["INCARCERATION_START"],
        event_attribute_filters={"most_severe_violation_type": types},
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
]

INCARCERATION_STARTS_AND_INFERRED = EventCountMetric(
    name="incarceration_starts_and_inferred",
    display_name="Incarceration Starts And Inferred Incarcerations",
    description="Number of total observed incarceration starts or inferred incarcerations",
    event_types=[
        "INCARCERATION_START",
        "SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON",
    ],
    event_attribute_filters={},
)

INCARCERATION_STARTS_AND_INFERRED_WITH_VIOLATION_TYPE_METRICS = [
    EventCountMetric(
        name=f"incarceration_starts_and_inferred_{category.lower()}_violation",
        display_name=f"Incarceration Starts And Inferred Incarcerations, {category.title()} Violation",
        description="Number of total observed incarceration starts or inferred "
        f"incarcerations for which the most severe violation type was {category.lower()}",
        event_types=[
            "INCARCERATION_START",
            "SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON",
        ],
        event_attribute_filters={"most_severe_violation_type": types},
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
]


INCARCERATIONS_TEMPORARY = EventCountMetric(
    name="incarceration_starts_temporary",
    display_name="Incarceration Starts, Temporary",
    description="Number of observed temporary incarceration starts",
    event_types=["INCARCERATION_START_TEMPORARY"],
    event_attribute_filters={},
)

LATE_OPPORTUNITY_METRICS = [
    EventCountMetric(
        name=f"late_opportunity_{b.task_type_name.lower()}_{num_days}_days",
        display_name=f"{num_days} Days Late: {b.task_title}",
        description=f"Number of times clients surpass {num_days} days of being eligible "
        f"for opportunities of type: {b.task_title.lower()}",
        event_types=[f"TASK_ELIGIBLE_{num_days}_DAYS"],
        event_attribute_filters={
            "task_type": [b.task_type_name],
        },
    )
    for b in TaskCompletionEventBigQueryViewCollector().collect_view_builders()
    for num_days in [7, 30]
]

LIBERTY_STARTS = EventCountMetric(
    name="transitions_to_liberty",
    display_name="Transitions To Liberty",
    description="Number of transitions to liberty",
    event_types=["LIBERTY_START"],
    event_attribute_filters={},
)

LSIR_ASSESSMENTS = EventCountMetric(
    name="lsir_assessments",
    display_name="LSI-R Assessments",
    description="Number of LSI-R assessments administered",
    event_types=["RISK_SCORE_ASSESSMENT"],
    event_attribute_filters={"assessment_type": ["LSIR"]},
)

LSIR_ASSESSMENTS_AVG_SCORE = EventValueMetric(
    name="lsir_assessments_avg_score",
    display_name="Average LSI-R Score Of Assessments",
    description="Average LSI-R score across all completed assessments",
    event_types=["RISK_SCORE_ASSESSMENT"],
    event_attribute_filters={
        "assessment_type": ["LSIR"],
    },
    event_value_numeric="assessment_score",
    event_count_metric=LSIR_ASSESSMENTS,
)

LSIR_ASSESSMENTS_RISK_DECREASE = EventCountMetric(
    name="lsir_assessments_risk_decrease",
    display_name="LSI-R Assessments Yielding Lower Risk Score",
    description="Number of LSI-R assessments resulting in a decrease in risk score",
    event_types=["RISK_SCORE_ASSESSMENT"],
    event_attribute_filters={
        "assessment_type": ["LSIR"],
        "assessment_score_decrease": ["true"],
    },
)

LSIR_ASSESSMENTS_RISK_INCREASE = EventCountMetric(
    name="lsir_assessments_risk_increase",
    display_name="LSI-R Assessments Yielding Higher Risk Score",
    description="Number of LSI-R assessments resulting in an increase in risk score",
    event_types=["RISK_SCORE_ASSESSMENT"],
    event_attribute_filters={
        "assessment_type": ["LSIR"],
        "assessment_score_increase": ["true"],
    },
)

PENDING_CUSTODY_STARTS = EventCountMetric(
    name="pending_custody_starts",
    display_name="Pending Custody Starts",
    description="Number of transitions to pending custody status",
    event_types=["PENDING_CUSTODY_START"],
    event_attribute_filters={},
)

# get days in eligibility span metrics for all task types
PERSON_DAYS_TASK_ELIGIBLE_METRICS = [
    SumSpanDaysMetric(
        name=f"person_days_task_eligible_{b.task_type_name.lower()}",
        display_name=f"Person-Days Eligible: {b.task_title}",
        description="Total number of person-days spent eligible for opportunities of "
        f"type: {b.task_title.lower()}",
        span_types=["TASK_ELIGIBILITY_SESSION"],
        span_attribute_filters={
            "is_eligible": ["true"],
            "task_type": [b.task_type_name],
        },
    )
    for b in TaskCompletionEventBigQueryViewCollector().collect_view_builders()
]

SUPERVISION_LEVEL_DOWNGRADES = EventCountMetric(
    name="supervision_level_downgrades",
    display_name="Supervision Level Downgrades",
    description="Number of supervision level changes to a lower level",
    event_types=["SUPERVISION_LEVEL_CHANGE"],
    event_attribute_filters={"change_type": ["DOWNGRADE"]},
)

SUPERVISION_LEVEL_DOWNGRADES_TO_LIMITED = EventCountMetric(
    name="supervision_level_downgrades_to_limited",
    display_name="Supervision Level Downgrades to Limited Supervision",
    description="Number of supervision level changes to limited supervision",
    event_types=["SUPERVISION_LEVEL_CHANGE"],
    event_attribute_filters={"new_supervision_level": ["LIMITED"]},
)

SUPERVISION_LEVEL_UPGRADES = EventCountMetric(
    name="supervision_level_upgrades",
    display_name="Supervision Level Upgrades",
    description="Number of supervision level changes to a higher level",
    event_types=["SUPERVISION_LEVEL_CHANGE"],
    event_attribute_filters={"change_type": ["UPGRADE"]},
)

SUPERVISION_STARTS = EventCountMetric(
    name="supervision_starts",
    display_name="Supervision Starts",
    description="Number of transitions to supervision",
    event_types=["SUPERVISION_START"],
    event_attribute_filters={},
)

# get unique completion task types
TASK_COMPLETED_METRICS = [
    EventCountMetric(
        name=f"task_completions_{b.task_type_name.lower()}",
        display_name=f"Task Completions: {b.task_title}",
        description=f"Number of task completions of type: {b.task_title.lower()}",
        event_types=["TASK_COMPLETED"],
        event_attribute_filters={"task_type": [b.task_type_name]},
    )
    for b in TaskCompletionEventBigQueryViewCollector().collect_view_builders()
]

TREATMENT_REFERRALS = EventCountMetric(
    name="treatment_referrals",
    display_name="Treatment Referrals",
    description="Number of treatment referrals",
    event_types=["TREATMENT_REFERRAL"],
    event_attribute_filters={},
)

VIOLATIONS = EventCountMetric(
    name="violations",
    display_name="Violations: All",
    description="Number of violations",
    event_types=["VIOLATION"],
    event_attribute_filters={},
)

VIOLATIONS_BY_TYPE_METRICS = [
    EventCountMetric(
        name=f"violations_{category.lower()}",
        display_name=f"Violations: {category.title()}",
        description=f"Number of {category.lower()} violations",
        event_types=[
            "VIOLATION",
        ],
        event_attribute_filters={"violation_type": types},
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
]

VIOLATION_RESPONSES = EventCountMetric(
    name="violation_responses",
    display_name="Violation Responses: All",
    description="Number of violation responses",
    event_types=["VIOLATION_RESPONSE"],
    event_attribute_filters={},
)

VIOLATION_RESPONSES_BY_TYPE_METRICS = [
    EventCountMetric(
        name=f"violation_responses_{category.lower()}",
        display_name=f"Violation Responses: {category.title()}",
        description=f"Number of {category.lower()} violation responses",
        event_types=[
            "VIOLATION_RESPONSE",
        ],
        event_attribute_filters={"most_serious_violation_type": types},
    )
    for category, types in _VIOLATION_CATEGORY_TO_TYPES_DICT.items()
]
