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
"""Returns all aggregated metric view builders for specified populations and aggregation levels"""
from typing import Dict, List

from recidiviz.aggregated_metrics.aggregated_metrics import (
    generate_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.assignment_event_aggregated_metrics import (
    generate_assignment_event_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.assignment_span_aggregated_metrics import (
    generate_assignment_span_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.metrics_assignment_sessions import (
    generate_metric_assignment_sessions_view_builder,
)
from recidiviz.aggregated_metrics.misc_aggregated_metrics import (
    generate_misc_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    ABSCONSIONS_BENCH_WARRANTS,
    AVG_AGE,
    AVG_DAILY_POPULATION,
    AVG_DAILY_POPULATION_COMMUNITY_CONFINEMENT,
    AVG_DAILY_POPULATION_DOMESTIC_VIOLENCE_CASE_TYPE,
    AVG_DAILY_POPULATION_DRUG_CASE_TYPE,
    AVG_DAILY_POPULATION_EMPLOYED,
    AVG_DAILY_POPULATION_FEMALE,
    AVG_DAILY_POPULATION_GENERAL_CASE_TYPE,
    AVG_DAILY_POPULATION_GENERAL_INCARCERATION,
    AVG_DAILY_POPULATION_HIGH_RISK_LEVEL,
    AVG_DAILY_POPULATION_LOW_RISK_LEVEL,
    AVG_DAILY_POPULATION_MENTAL_HEALTH_CASE_TYPE,
    AVG_DAILY_POPULATION_NONWHITE,
    AVG_DAILY_POPULATION_OTHER_CASE_TYPE,
    AVG_DAILY_POPULATION_PAROLE,
    AVG_DAILY_POPULATION_PAROLE_BOARD_HOLD,
    AVG_DAILY_POPULATION_PROBATION,
    AVG_DAILY_POPULATION_SEX_OFFENSE_CASE_TYPE,
    AVG_DAILY_POPULATION_SHOCK_INCARCERATION,
    AVG_DAILY_POPULATION_SUPERVISION_LEVEL_METRICS,
    AVG_DAILY_POPULATION_TREATMENT_IN_PRISON,
    AVG_DAILY_POPULATION_UNKNOWN_CASE_TYPE,
    AVG_LSIR_SCORE,
    AVG_LSIR_SCORE_AT_ASSIGNMENT,
    COMMUNITY_CONFINEMENT_SUPERVISION_STARTS,
    CONTACTS_ATTEMPTED,
    CONTACTS_COMPLETED,
    CONTACTS_FACE_TO_FACE,
    CONTACTS_HOME_VISIT,
    DAYS_ABSCONDED_365,
    DAYS_AT_LIBERTY_365,
    DAYS_EMPLOYED_365,
    DAYS_IN_COMMUNITY_365,
    DAYS_INCARCERATED_365,
    DAYS_OUT_OF_STATE_365,
    DAYS_PENDING_CUSTODY_365,
    DAYS_SINCE_MOST_RECENT_COMPLETED_CONTACT,
    DAYS_SINCE_MOST_RECENT_LSIR,
    DAYS_SUPERVISED_365,
    DAYS_TO_FIRST_ABSCONSION_BENCH_WARRANT_365,
    DAYS_TO_FIRST_INCARCERATION_365,
    DAYS_TO_FIRST_LIBERTY_365,
    DAYS_TO_FIRST_SUPERVISION_START_365,
    DAYS_TO_FIRST_VIOLATION_365,
    DAYS_TO_FIRST_VIOLATION_365_BY_TYPE_METRICS,
    DAYS_TO_FIRST_VIOLATION_RESPONSE_365,
    DAYS_TO_FIRST_VIOLATION_RESPONSE_365_BY_TYPE_METRICS,
    DRUG_SCREENS,
    DRUG_SCREENS_POSITIVE,
    EARLY_DISCHARGE_REQUESTS,
    EMPLOYED_STATUS_ENDS,
    EMPLOYED_STATUS_STARTS,
    INCARCERATION_STARTS,
    INCARCERATION_STARTS_AND_INFERRED,
    INCARCERATION_STARTS_AND_INFERRED_TECHNICAL_VIOLATION_NO_PRIOR_TREATMENT_REFERRAL,
    INCARCERATION_STARTS_AND_INFERRED_WITH_VIOLATION_TYPE_METRICS,
    INCARCERATION_STARTS_TECHNICAL_VIOLATION_NO_PRIOR_TREATMENT_REFERRAL,
    INCARCERATION_STARTS_WITH_VIOLATION_TYPE_METRICS,
    INCARCERATIONS_INFERRED,
    INCARCERATIONS_INFERRED_WITH_VIOLATION_TYPE_METRICS,
    INCARCERATIONS_TEMPORARY,
    LATE_OPPORTUNITY_METRICS,
    LIBERTY_STARTS,
    LSIR_ASSESSMENTS,
    LSIR_ASSESSMENTS_AVG_SCORE,
    LSIR_ASSESSMENTS_RISK_DECREASE,
    LSIR_ASSESSMENTS_RISK_INCREASE,
    PENDING_CUSTODY_STARTS,
    PERSON_DAYS_TASK_ELIGIBLE_METRICS,
    SUPERVISION_LEVEL_DOWNGRADES,
    SUPERVISION_LEVEL_DOWNGRADES_TO_LIMITED,
    SUPERVISION_LEVEL_UPGRADES,
    SUPERVISION_STARTS,
    TASK_COMPLETED_METRICS,
    TREATMENT_REFERRALS,
    VIOLATION_RESPONSES,
    VIOLATION_RESPONSES_BY_TYPE_METRICS,
    VIOLATIONS,
    VIOLATIONS_BY_TYPE_METRICS,
)
from recidiviz.aggregated_metrics.models.metric_aggregation_level_type import (
    METRIC_AGGREGATION_LEVELS_BY_TYPE,
    MetricAggregationLevelType,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    METRIC_POPULATIONS_BY_TYPE,
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.period_event_aggregated_metrics import (
    generate_period_event_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.period_span_aggregated_metrics import (
    generate_period_span_aggregated_metrics_view_builder,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder

METRICS_BY_POPULATION_TYPE: Dict[MetricPopulationType, List[AggregatedMetric]] = {
    MetricPopulationType.INCARCERATION: [
        # Average daily population
        AVG_DAILY_POPULATION,
        # Average daily population, person demographics
        AVG_AGE,
        AVG_DAILY_POPULATION_FEMALE,
        AVG_DAILY_POPULATION_NONWHITE,
        # Average daily population, compartment level 2
        AVG_DAILY_POPULATION_COMMUNITY_CONFINEMENT,
        AVG_DAILY_POPULATION_GENERAL_INCARCERATION,
        AVG_DAILY_POPULATION_PAROLE_BOARD_HOLD,
        AVG_DAILY_POPULATION_SHOCK_INCARCERATION,
        AVG_DAILY_POPULATION_TREATMENT_IN_PRISON,
        # Risk score
        AVG_DAILY_POPULATION_HIGH_RISK_LEVEL,
        AVG_DAILY_POPULATION_LOW_RISK_LEVEL,
        AVG_LSIR_SCORE,
        AVG_LSIR_SCORE_AT_ASSIGNMENT,
        DAYS_SINCE_MOST_RECENT_LSIR,
        # Events
        COMMUNITY_CONFINEMENT_SUPERVISION_STARTS,
        SUPERVISION_STARTS,
        # Assignment window metrics
        DAYS_AT_LIBERTY_365,
        DAYS_IN_COMMUNITY_365,
        DAYS_INCARCERATED_365,
        DAYS_OUT_OF_STATE_365,
        DAYS_PENDING_CUSTODY_365,
        DAYS_SUPERVISED_365,
        DAYS_TO_FIRST_LIBERTY_365,
        DAYS_TO_FIRST_SUPERVISION_START_365,
    ],
    MetricPopulationType.SUPERVISION: [
        # Average daily population
        AVG_DAILY_POPULATION,
        # Average daily population, person demographics
        AVG_AGE,
        AVG_DAILY_POPULATION_FEMALE,
        AVG_DAILY_POPULATION_NONWHITE,
        # Average daily population, compartment level 2
        AVG_DAILY_POPULATION_COMMUNITY_CONFINEMENT,
        AVG_DAILY_POPULATION_PAROLE,
        AVG_DAILY_POPULATION_PROBATION,
        # Average daily population, case type
        AVG_DAILY_POPULATION_DOMESTIC_VIOLENCE_CASE_TYPE,
        AVG_DAILY_POPULATION_DRUG_CASE_TYPE,
        AVG_DAILY_POPULATION_GENERAL_CASE_TYPE,
        AVG_DAILY_POPULATION_MENTAL_HEALTH_CASE_TYPE,
        AVG_DAILY_POPULATION_OTHER_CASE_TYPE,
        AVG_DAILY_POPULATION_SEX_OFFENSE_CASE_TYPE,
        AVG_DAILY_POPULATION_UNKNOWN_CASE_TYPE,
        # Average daily population, supervision level
        *AVG_DAILY_POPULATION_SUPERVISION_LEVEL_METRICS,
        # Average daily population, other attributes
        AVG_DAILY_POPULATION_EMPLOYED,
        # Risk score
        AVG_DAILY_POPULATION_HIGH_RISK_LEVEL,
        AVG_DAILY_POPULATION_LOW_RISK_LEVEL,
        AVG_LSIR_SCORE,
        AVG_LSIR_SCORE_AT_ASSIGNMENT,
        DAYS_SINCE_MOST_RECENT_LSIR,
        # Events
        ## Session transitions
        ABSCONSIONS_BENCH_WARRANTS,
        EARLY_DISCHARGE_REQUESTS,
        INCARCERATIONS_INFERRED,
        *INCARCERATIONS_INFERRED_WITH_VIOLATION_TYPE_METRICS,
        INCARCERATION_STARTS,
        *INCARCERATION_STARTS_WITH_VIOLATION_TYPE_METRICS,
        INCARCERATION_STARTS_TECHNICAL_VIOLATION_NO_PRIOR_TREATMENT_REFERRAL,
        INCARCERATION_STARTS_AND_INFERRED,
        *INCARCERATION_STARTS_AND_INFERRED_WITH_VIOLATION_TYPE_METRICS,
        INCARCERATION_STARTS_AND_INFERRED_TECHNICAL_VIOLATION_NO_PRIOR_TREATMENT_REFERRAL,
        INCARCERATIONS_TEMPORARY,
        LIBERTY_STARTS,
        PENDING_CUSTODY_STARTS,
        ## Supervision level changes
        SUPERVISION_LEVEL_DOWNGRADES,
        SUPERVISION_LEVEL_UPGRADES,
        SUPERVISION_LEVEL_DOWNGRADES_TO_LIMITED,
        ## Violations
        VIOLATIONS,
        *VIOLATIONS_BY_TYPE_METRICS,
        ## Violation Responses
        VIOLATION_RESPONSES,
        *VIOLATION_RESPONSES_BY_TYPE_METRICS,
        ## Drug Screens
        DRUG_SCREENS,
        DRUG_SCREENS_POSITIVE,
        ## LSI-R Assessments
        LSIR_ASSESSMENTS,
        LSIR_ASSESSMENTS_AVG_SCORE,
        LSIR_ASSESSMENTS_RISK_DECREASE,
        LSIR_ASSESSMENTS_RISK_INCREASE,
        ## Contacts
        CONTACTS_ATTEMPTED,
        CONTACTS_COMPLETED,
        CONTACTS_FACE_TO_FACE,
        CONTACTS_HOME_VISIT,
        DAYS_SINCE_MOST_RECENT_COMPLETED_CONTACT,
        ## Employment Changes
        EMPLOYED_STATUS_ENDS,
        EMPLOYED_STATUS_STARTS,
        ## Program Referrals
        TREATMENT_REFERRALS,
        # TODO(#18344): Use task population_types to only calculate relevant workflows metrics for a single population
        ## Workflows
        *LATE_OPPORTUNITY_METRICS,
        *PERSON_DAYS_TASK_ELIGIBLE_METRICS,
        *TASK_COMPLETED_METRICS,
        # Assignment window metrics
        DAYS_ABSCONDED_365,
        DAYS_AT_LIBERTY_365,
        DAYS_IN_COMMUNITY_365,
        DAYS_INCARCERATED_365,
        DAYS_OUT_OF_STATE_365,
        DAYS_PENDING_CUSTODY_365,
        DAYS_SUPERVISED_365,
        DAYS_EMPLOYED_365,
        DAYS_TO_FIRST_ABSCONSION_BENCH_WARRANT_365,
        DAYS_TO_FIRST_INCARCERATION_365,
        DAYS_TO_FIRST_LIBERTY_365,
        DAYS_TO_FIRST_VIOLATION_365,
        *DAYS_TO_FIRST_VIOLATION_365_BY_TYPE_METRICS,
        DAYS_TO_FIRST_VIOLATION_RESPONSE_365,
        *DAYS_TO_FIRST_VIOLATION_RESPONSE_365_BY_TYPE_METRICS,
    ],
}

LEVELS_BY_POPULATION_TYPE: Dict[
    MetricPopulationType, List[MetricAggregationLevelType]
] = {
    MetricPopulationType.INCARCERATION: [
        MetricAggregationLevelType.FACILITY,
        MetricAggregationLevelType.STATE_CODE,
    ],
    MetricPopulationType.SUPERVISION: [
        MetricAggregationLevelType.SUPERVISION_OFFICER,
        MetricAggregationLevelType.SUPERVISION_OFFICE,
        MetricAggregationLevelType.SUPERVISION_DISTRICT,
        MetricAggregationLevelType.STATE_CODE,
    ],
}


def collect_aggregated_metrics_view_builders() -> List[SimpleBigQueryViewBuilder]:
    """
    Collects all aggregated metrics view builders at all available aggregation levels and populations
    """
    view_builders = []
    for population_type in MetricPopulationType:
        population = METRIC_POPULATIONS_BY_TYPE[population_type]
        all_metrics = METRICS_BY_POPULATION_TYPE[population_type]
        if not all_metrics:
            continue
        for level_type in LEVELS_BY_POPULATION_TYPE[population_type]:
            level = METRIC_AGGREGATION_LEVELS_BY_TYPE[level_type]
            # Build assignment table
            view_builders.append(
                generate_metric_assignment_sessions_view_builder(
                    aggregation_level=level,
                    population=population,
                )
            )

            # Build metric builder views by type
            view_builders.append(
                generate_period_span_aggregated_metrics_view_builder(
                    aggregation_level=level,
                    population=population,
                    metrics=[
                        m
                        for m in all_metrics
                        if isinstance(m, PeriodSpanAggregatedMetric)
                    ],
                )
            )
            view_builders.append(
                generate_period_event_aggregated_metrics_view_builder(
                    aggregation_level=level,
                    population=population,
                    metrics=[
                        m
                        for m in all_metrics
                        if isinstance(m, PeriodEventAggregatedMetric)
                    ],
                )
            )
            view_builders.append(
                generate_assignment_span_aggregated_metrics_view_builder(
                    aggregation_level=level,
                    population=population,
                    metrics=[
                        m
                        for m in all_metrics
                        if isinstance(m, AssignmentSpanAggregatedMetric)
                    ],
                )
            )
            view_builders.append(
                generate_assignment_event_aggregated_metrics_view_builder(
                    aggregation_level=level,
                    population=population,
                    metrics=[
                        m
                        for m in all_metrics
                        if isinstance(m, AssignmentEventAggregatedMetric)
                    ],
                )
            )
            # Add miscellaneous metric view builders if they exist for a given
            # population and aggregation level
            misc_metrics_view_builder = generate_misc_aggregated_metrics_view_builder(
                aggregation_level=level,
                population=population,
            )
            if misc_metrics_view_builder:
                view_builders.append(misc_metrics_view_builder)

            # Build joined view of all metrics
            view_builders.append(
                generate_aggregated_metrics_view_builder(
                    aggregation_level=level, population=population
                )
            )
    return view_builders
