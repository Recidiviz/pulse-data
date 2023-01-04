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
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentDaysToFirstEventMetric,
    AssignmentSpanDaysMetric,
    DailyAvgSpanCountMetric,
    DailyAvgSpanValueMetric,
    DailyAvgTimeSinceSpanStartMetric,
    EventCountMetric,
)
from recidiviz.task_eligibility.task_completion_event_big_query_view_collector import (
    TaskCompletionEventBigQueryViewCollector,
)

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
    span_types=["EMPLOYMENT_PERIOD"],
    span_attribute_filters={},
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

DAYS_EMPLOYED_365 = AssignmentSpanDaysMetric(
    name="days_employed",
    display_name="Days Employed Within 1 Year Of Assignment",
    description="Sum of the number of days clients had valid employment status within 1 year following assignment, "
    "for all assignments during the analysis period",
    span_types=["EMPLOYMENT_STATUS_SESSION"],
    span_attribute_filters={
        "is_employed": ["true"],
    },
    window_length_days=365,
)

DAYS_IN_COMMUNITY_365 = AssignmentSpanDaysMetric(
    name="days_in_community",
    display_name="Days In Community Within 1 Year Of Assignment",
    description="Sum of the number of days spent in community (supervision or at liberty) within 1 year following "
    "assignment, for all assignments during the analysis period",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["SUPERVISION", "LIBERTY"],
    },
    window_length_days=365,
)

DAYS_INCARCERATED_365 = AssignmentSpanDaysMetric(
    name="days_incarcerated",
    display_name="Days Incarcerated Within 1 Year Of Assignment",
    description="Sum of the number of incarcerated days within 1 year following assignment, for all assignments "
    "during the analysis period",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["INCARCERATION"],
    },
    window_length_days=365,
)

DAYS_SINCE_MOST_RECENT_COMPLETED_CONTACT = DailyAvgTimeSinceSpanStartMetric(
    name="avg_days_since_most_recent_completed_contact",
    display_name="Days Since Most Recent Completed Contact",
    description="Average number of days since a client's most recent completed contact, across all days on which "
    "client is in population",
    span_types=["COMPLETED_CONTACT_SESSION"],
    span_attribute_filters={},
)

DAYS_SINCE_MOST_RECENT_LSIR = DailyAvgTimeSinceSpanStartMetric(
    name="avg_days_since_most_recent_lsir",
    display_name="Days Since Most Recent LSI-R",
    description="Average number of days since a client's most recent LSI-R assessment, across all days on which "
    "client is in population",
    span_types=["ASSESSMENT_SCORE_SESSION"],
    span_attribute_filters={
        "assessment_type": ["LSIR"],
    },
)

DAYS_TO_FIRST_ABSCONSION_BENCH_WARRANT_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_absconsion_bench_warrant",
    display_name="Days To First Absconsion/Bench Warrant (Legal Status) Within 1 Year After Assignment",
    description="Sum of the number of days prior to first absconsion/bench warrant legal status within 1 year "
    "following assignment, for all assignments during the analysis period",
    event_types=["ABSCONSION_BENCH_WARRANT"],
    event_attribute_filters={},
    window_length_days=365,
)

DAYS_TO_FIRST_INCARCERATION_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_incarceration",
    display_name="Days To First Incarceration Within 1 Year After Assignment",
    description="Sum of the number of days prior to first incarceration within 1 year following assignment, "
    "for all assignments during the analysis period",
    event_types=["INCARCERATION_START"],
    event_attribute_filters={},
    window_length_days=365,
)

DAYS_TO_FIRST_SUPERVISION_START_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_supervision_start",
    display_name="Days To First Supervision Start Within 1 Year After Assignment",
    description="Sum of the number of days prior to first supervision start within 1 year following assignment, "
    "for all assignments during the analysis period",
    event_types=["SUPERVISION_START"],
    event_attribute_filters={},
    window_length_days=365,
)

DAYS_TO_FIRST_VIOLATION_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_violation",
    display_name="Days To First Violation Within 1 Year After Assignment",
    description="Sum of the number of days prior to first violation within 1 year following assignment, "
    "for all assignments during the analysis period",
    event_types=["VIOLATION"],
    event_attribute_filters={},
    window_length_days=365,
)

DAYS_TO_FIRST_VIOLATION_ABSCONDED_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_violation_absconded",
    display_name="Days To First Absconsion Violation Within 1 Year After Assignment",
    description="Sum of the number of days prior to first absconsion violation within 1 year following assignment, "
    "for all assignments during the analysis period",
    event_types=["VIOLATION"],
    event_attribute_filters={"violation_type": ["ABSCONDED"]},
    window_length_days=365,
)

DAYS_TO_FIRST_VIOLATION_NEW_CRIME_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_violation_new_crime",
    display_name="Days To First New Crime Violation Within 1 Year After Assignment",
    description="Sum of the number of days prior to first new crime violation within 1 year following assignment, "
    "for all assignments during the analysis period",
    event_types=["VIOLATION"],
    event_attribute_filters={"violation_type": ["NEW_CRIME"]},
    window_length_days=365,
)

DAYS_TO_FIRST_VIOLATION_RESPONSE_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_violation_response",
    display_name="Days To First Violation Response Within 1 Year After Assignment",
    description="Sum of the number of days prior to first violation response within 1 year following assignment, "
    "for all assignments during the analysis period",
    event_types=["VIOLATION_RESPONSE"],
    event_attribute_filters={},
    window_length_days=365,
)

DAYS_TO_FIRST_VIOLATION_RESPONSE_ABSCONDED_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_violation_response_absconded",
    display_name="Days To First Absconsion Violation Response Within 1 Year After Assignment",
    description="Sum of the number of days prior to first absconsion violation response within 1 year following "
    "assignment, for all assignments during the analysis period",
    event_types=["VIOLATION_RESPONSE"],
    event_attribute_filters={"violation_type": ["ABSCONDED"]},
    window_length_days=365,
)

DAYS_TO_FIRST_VIOLATION_RESPONSE_NEW_CRIME_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_violation_response_new_crime",
    display_name="Days To First New Crime Violation Response Within 1 Year After Assignment",
    description="Sum of the number of days prior to first new crime violation response within 1 year following "
    "assignment, for all assignments during the analysis period",
    event_types=["VIOLATION_RESPONSE"],
    event_attribute_filters={"violation_type": ["NEW_CRIME"]},
    window_length_days=365,
)

DAYS_TO_FIRST_VIOLATION_RESPONSE_TECHNICAL_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_violation_response_technical",
    display_name="Days To First Technical Violation Response Within 1 Year After Assignment",
    description="Sum of the number of days prior to first technical violation response within 1 year following "
    "assignment, for all assignments during the analysis period",
    event_types=["VIOLATION_RESPONSE"],
    event_attribute_filters={"violation_type": ["TECHNICAL"]},
    window_length_days=365,
)

DAYS_TO_FIRST_VIOLATION_TECHNICAL_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_violation_technical",
    display_name="Days To First Technical Violation Within 1 Year After Assignment",
    description="Sum of the number of days prior to first technical violation within 1 year following assignment, "
    "for all assignments during the analysis period",
    event_types=["VIOLATION"],
    event_attribute_filters={"violation_type": ["TECHNICAL"]},
    window_length_days=365,
)

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

INCARCERATIONS = EventCountMetric(
    name="incarcerations",
    display_name="Incarcerations, All",
    description="Number of transitions to incarceration",
    event_types=["INCARCERATION_START"],
    event_attribute_filters={},
)

INCARCERATIONS_ABSCONDED_VIOLATION = EventCountMetric(
    name="incarcerations_absconded_violation",
    display_name="Incarcerations, Absconded Violation",
    description="Number of transitions to incarceration for which the most severe violation type was absconsion",
    event_types=["INCARCERATION_START"],
    event_attribute_filters={"most_severe_violation_type": ["ABSCONDED"]},
)

INCARCERATIONS_NEW_CRIME_VIOLATION = EventCountMetric(
    name="incarcerations_new_crime_violation",
    display_name="Incarcerations, New Crime Violation",
    description="Number of transitions to incarceration for which the most severe violation type was a new crime",
    event_types=["INCARCERATION_START"],
    event_attribute_filters={
        "most_severe_violation_type": ["LAW", "FELONY", "MISDEMEANOR"]
    },
)

INCARCERATIONS_TECHNICAL_VIOLATION = EventCountMetric(
    name="incarcerations_technical_violation",
    display_name="Incarcerations, Technical Violation",
    description="Number of transitions to incarceration for which the most severe violation type was a technical",
    event_types=["INCARCERATION_START"],
    event_attribute_filters={"most_severe_violation_type": ["TECHNICAL"]},
)

INCARCERATIONS_TEMPORARY = EventCountMetric(
    name="incarcerations_temporary",
    display_name="Incarcerations, Temporary",
    description="Number of transitions to temporary incarceration",
    event_types=["INCARCERATION_START_TEMPORARY"],
    event_attribute_filters={},
)

INCARCERATIONS_UNKNOWN_VIOLATION = EventCountMetric(
    name="incarcerations_unknown_violation",
    display_name="Incarcerations, Unknown Violation",
    description="Number of transitions to incarceration with an unknown violation type",
    event_types=["INCARCERATION_START"],
    event_attribute_filters={
        "most_severe_violation_type": ["INTERNAL_UNKNOWN", "EXTERNAL_UNKNOWN"]
    },
)

LATE_OPPORTUNITY_EARLY_DISCHARGE_30_DAYS = EventCountMetric(
    name="late_opportunity_early_discharge_30_days",
    display_name="Instances Of Surpassing 30 Days of Eligibility For Early Discharge",
    description="Number of times clients surpass 30 days of being eligible for early discharge",
    event_types=["TASK_ELIGIBLE_30_DAYS"],
    event_attribute_filters={
        "task_name": [
            "COMPLETE_DISCHARGE_EARLY_FROM_PAROLE_DUAL_SUPERVISION_REQUEST",
            "COMPLETE_DISCHARGE_EARLY_FROM_PROBATION_SUPERVISION_REQUEST",
            "COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_FORM",
        ]
    },
)

LATE_OPPORTUNITY_EARLY_DISCHARGE_7_DAYS = EventCountMetric(
    name="late_opportunity_early_discharge_7_days",
    display_name="Instances Of Surpassing 7 Days of Eligibility For Early Discharge",
    description="Number of times clients surpass 7 days of being eligible for early discharge",
    event_types=["TASK_ELIGIBLE_7_DAYS"],
    event_attribute_filters={
        "task_name": [
            "COMPLETE_DISCHARGE_EARLY_FROM_PAROLE_DUAL_SUPERVISION_REQUEST",
            "COMPLETE_DISCHARGE_EARLY_FROM_PROBATION_SUPERVISION_REQUEST",
            "COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_FORM",
        ]
    },
)

LATE_OPPORTUNITY_FULL_TERM_DISCHARGE_30_DAYS = EventCountMetric(
    name="late_opportunity_full_term_discharge_30_days",
    display_name="Instances Of Surpassing 30 Days of Eligibility For Full Term Discharge",
    description="Number of times clients surpass 30 days of being eligible for full-term discharge",
    event_types=["TASK_ELIGIBLE_30_DAYS"],
    event_attribute_filters={
        "task_name": ["COMPLETE_FULL_TERM_DISCHARGE_FROM_SUPERVISION"]
    },
)

LATE_OPPORTUNITY_FULL_TERM_DISCHARGE_7_DAYS = EventCountMetric(
    name="late_opportunity_full_term_discharge_7_days",
    display_name="Instances Of Surpassing 7 Days of Eligibility For Full Term Discharge",
    description="Number of times clients surpass 7 days of being eligible for full-term discharge",
    event_types=["TASK_ELIGIBLE_7_DAYS"],
    event_attribute_filters={
        "task_name": ["COMPLETE_FULL_TERM_DISCHARGE_FROM_SUPERVISION"]
    },
)

LATE_OPPORTUNITY_SUPERVISION_LEVEL_DOWNGRADE_30_DAYS = EventCountMetric(
    name="late_opportunity_supervision_level_downgrade_30_days",
    display_name="Instances Of Surpassing 30 Days of Eligibility For Supervision Level Downgrade",
    description="Number of times clients surpass 30 days of being eligible for a supervision level downgrade",
    event_types=["TASK_ELIGIBLE_30_DAYS"],
    event_attribute_filters={"task_name": ["SUPERVISION_LEVEL_DOWNGRADE"]},
)

LATE_OPPORTUNITY_SUPERVISION_LEVEL_DOWNGRADE_7_DAYS = EventCountMetric(
    name="late_opportunity_supervision_level_downgrade_7_days",
    display_name="Instances Of Surpassing 7 Days of Eligibility For Supervision Level Downgrade",
    description="Number of times clients surpass 7 days of being eligible for a supervision level downgrade",
    event_types=["TASK_ELIGIBLE_7_DAYS"],
    event_attribute_filters={"task_name": ["SUPERVISION_LEVEL_DOWNGRADE"]},
)

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

VIOLATIONS_ABSCONDED = EventCountMetric(
    name="violations_absconded",
    display_name="Violations: Absconded",
    description="Number of absconsion violations",
    event_types=["VIOLATION"],
    event_attribute_filters={"violation_type": ["ABSCONDED"]},
)

VIOLATIONS_NEW_CRIME = EventCountMetric(
    name="violations_new_crime",
    display_name="Violations: New Crime",
    description="Number of new crime violations",
    event_types=["VIOLATION"],
    event_attribute_filters={"violation_type": ["FELONY", "LAW", "MISDEMEANOR"]},
)

VIOLATIONS_TECHNICAL = EventCountMetric(
    name="violations_technical",
    display_name="Violations: Technical",
    description="Number of technical violations",
    event_types=["VIOLATION"],
    event_attribute_filters={"violation_type": ["TECHNICAL"]},
)

VIOLATION_RESPONSES = EventCountMetric(
    name="violation_responses",
    display_name="Violation Responses: All",
    description="Number of violation responses",
    event_types=["VIOLATION_RESPONSE"],
    event_attribute_filters={},
)

VIOLATION_RESPONSES_ABSCONDED = EventCountMetric(
    name="violation_responses_absconded",
    display_name="Violation Responses: Absconded",
    description="Number of absconsion violation responses",
    event_types=["VIOLATION_RESPONSE"],
    event_attribute_filters={"most_serious_violation_type": ["ABSCONDED"]},
)

VIOLATION_RESPONSES_NEW_CRIME = EventCountMetric(
    name="violation_responses_new_crime",
    display_name="Violation Responses: New Crime",
    description="Number of new crime violation responses",
    event_types=["VIOLATION_RESPONSE"],
    event_attribute_filters={
        "most_serious_violation_type": ["FELONY", "LAW", "MISDEMEANOR"]
    },
)

VIOLATION_RESPONSES_TECHNICAL = EventCountMetric(
    name="violation_responses_technical",
    display_name="Violation Responses: Technical",
    description="Number of technical violation responses",
    event_types=["VIOLATION_RESPONSE"],
    event_attribute_filters={"most_serious_violation_type": ["TECHNICAL"]},
)
