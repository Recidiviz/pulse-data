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

AVG_DAILY_POPULATION_FEMALE = DailyAvgSpanCountMetric(
    name="avg_population_female",
    display_name="Average Population: Female",
    description="Average daily count of female clients in the population",
    span_types=["PERSON_DEMOGRAPHICS"],
    span_attribute_filters={
        "gender": ["FEMALE"],
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

AVG_DAILY_POPULATION_NONWHITE = DailyAvgSpanCountMetric(
    name="avg_population_nonwhite",
    display_name="Average Population: Non-White",
    description="Average daily count of non-white clients",
    span_types=["PERSON_DEMOGRAPHICS"],
    span_attribute_filters={
        "prioritized_race_or_ethnicity": '!= "WHITE"',
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

DAYS_INCARCERATED_365 = AssignmentSpanDaysMetric(
    name="days_incarcerated",
    display_name="Days Incarcerated Within 1 Year Of Assignment",
    description="Sum of the number of incarcerated days within 1 year following assignment, for all assignments during the analysis period",
    span_types=["COMPARTMENT_SESSION"],
    span_attribute_filters={
        "compartment_level_1": ["INCARCERATION"],
    },
    window_length_days=365,
)

DAYS_SINCE_MOST_RECENT_LSIR = DailyAvgTimeSinceSpanStartMetric(
    name="avg_days_since_most_recent_lsir",
    display_name="Days Since Most Recent LSI-R",
    description="Average number of days since a client's most recent LSI-R assessment, across all days on which client is in population",
    span_types=["ASSESSMENT_SCORE_SESSION"],
    span_attribute_filters={
        "assessment_type": ["LSIR"],
    },
)

DAYS_TO_FIRST_SUPERVISION_START_365 = AssignmentDaysToFirstEventMetric(
    name="days_to_first_supervision_start",
    display_name="Days To First Supervision Start Within 1 Year After Assignment",
    description="Sum of the number of days prior to first supervision start within 1 year following assignment, for all assignments during the analysis period",
    event_types=["SUPERVISION_START"],
    event_attribute_filters={},
    window_length_days=365,
)

DISCHARGES_TO_LIBERTY = EventCountMetric(
    name="discharges_to_liberty",
    display_name="Discharges To Liberty",
    description="Number of transitions to liberty",
    event_types=["LIBERTY_START"],
    event_attribute_filters={},
)

INCARCERATIONS_ABSCONDED_VIOLATION = EventCountMetric(
    name="incarcerations_absconded_violation",
    display_name="Incarcerations, Absconded Violation",
    description="Number of transitions to incarceration for which the most severe violation type was absconsion",
    event_types=["INCARCERATION_START"],
    event_attribute_filters={"most_severe_violation_type": ["ABSCONDED"]},
)

INCARCERATIONS_ALL = EventCountMetric(
    name="incarcerations_all",
    display_name="Incarcerations, All",
    description="Number of transitions to incarceration",
    event_types=["INCARCERATION_START"],
    event_attribute_filters={},
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

SUPERVISION_STARTS = EventCountMetric(
    name="supervision_starts",
    display_name="Supervision Starts",
    description="Number of transitions to supervision",
    event_types=["SUPERVISION_START"],
    event_attribute_filters={},
)
