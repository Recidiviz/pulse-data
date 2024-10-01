# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Returns all aggregated metric view builders for specified populations and units of analysis"""
from typing import Dict, List, Optional

from recidiviz.aggregated_metrics.aggregated_metrics import (
    generate_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.assignment_event_aggregated_metrics import (
    generate_assignment_event_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.assignment_span_aggregated_metrics import (
    generate_assignment_span_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.misc_aggregated_metrics import (
    generate_misc_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    EventValueMetric,
    MiscAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    ABSCONSIONS_BENCH_WARRANTS,
    ANY_INCARCERATION_365,
    ASSIGNMENTS,
    AVG_ACTIVE_LENGTH_OF_STAY_HOUSING_UNIT_TYPE_METRICS,
    AVG_ACTIVE_LENGTH_OF_STAY_SOLITARY_CONFINEMENT,
    AVG_AGE,
    AVG_ASSIGNMENTS_OFFICER,
    AVG_CRITICAL_CASELOAD_SIZE,
    AVG_CRITICAL_CASELOAD_SIZE_OFFICER,
    AVG_DAILY_CASELOAD_OFFICER,
    AVG_DAILY_POPULATION,
    AVG_DAILY_POPULATION_COMMUNITY_CONFINEMENT,
    AVG_DAILY_POPULATION_DOMESTIC_VIOLENCE_CASE_TYPE,
    AVG_DAILY_POPULATION_DRUG_CASE_TYPE,
    AVG_DAILY_POPULATION_DRUG_OFFENSE_SENTENCE,
    AVG_DAILY_POPULATION_EMPLOYED,
    AVG_DAILY_POPULATION_FEMALE,
    AVG_DAILY_POPULATION_GENERAL_CASE_TYPE,
    AVG_DAILY_POPULATION_GENERAL_INCARCERATION,
    AVG_DAILY_POPULATION_HIGH_RISK_LEVEL,
    AVG_DAILY_POPULATION_HOUSING_TYPE_METRICS,
    AVG_DAILY_POPULATION_LIMITED_SUPERVISION_JUSTICE_IMPACT,
    AVG_DAILY_POPULATION_LOW_RISK_LEVEL,
    AVG_DAILY_POPULATION_MAXIMUM_CUSTODY,
    AVG_DAILY_POPULATION_MAXIMUM_CUSTODY_JUSTICE_IMPACT,
    AVG_DAILY_POPULATION_MEDIUM_CUSTODY,
    AVG_DAILY_POPULATION_MEDIUM_CUSTODY_JUSTICE_IMPACT,
    AVG_DAILY_POPULATION_MENTAL_HEALTH_CASE_TYPE,
    AVG_DAILY_POPULATION_MINIMUM_CUSTODY,
    AVG_DAILY_POPULATION_MINIMUM_CUSTODY_JUSTICE_IMPACT,
    AVG_DAILY_POPULATION_NONLIMITED_SUPERVISION,
    AVG_DAILY_POPULATION_NONLIMITED_SUPERVISION_JUSTICE_IMPACT,
    AVG_DAILY_POPULATION_NONWHITE,
    AVG_DAILY_POPULATION_OTHER_CASE_TYPE,
    AVG_DAILY_POPULATION_PAROLE,
    AVG_DAILY_POPULATION_PAROLE_BOARD_HOLD,
    AVG_DAILY_POPULATION_PAST_FULL_TERM_RELEASE_DATE,
    AVG_DAILY_POPULATION_PAST_PAROLE_ELIGIBILITY_DATE,
    AVG_DAILY_POPULATION_PROBATION,
    AVG_DAILY_POPULATION_SEX_OFFENSE_CASE_TYPE,
    AVG_DAILY_POPULATION_SHOCK_INCARCERATION,
    AVG_DAILY_POPULATION_SOLITARY_CONFINEMENT,
    AVG_DAILY_POPULATION_SOLITARY_CONFINEMENT_JUSTICE_IMPACT,
    AVG_DAILY_POPULATION_STATE_PRISON,
    AVG_DAILY_POPULATION_SUPERVISION_LEVEL_METRICS,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION,
    AVG_DAILY_POPULATION_TREATMENT_IN_PRISON,
    AVG_DAILY_POPULATION_UNKNOWN_CASE_TYPE,
    AVG_DAILY_POPULATION_VIOLENT_OFFENSE_SENTENCE,
    AVG_LSIR_SCORE,
    AVG_NUM_SUPERVISION_OFFICERS_INSIGHTS_CASELOAD_CATEGORY_METRICS,
    COMMUNITY_CONFINEMENT_SUPERVISION_STARTS,
    CONTACTS_ATTEMPTED,
    CUSTODY_LEVEL_DOWNGRADES,
    CUSTODY_LEVEL_DOWNGRADES_TO_MINIMUM,
    CUSTODY_LEVEL_UPGRADES,
    DAYS_ABSCONDED_365,
    DAYS_AT_LIBERTY_365,
    DAYS_ELIGIBLE_AT_TASK_COMPLETION_METRICS_INCARCERATION,
    DAYS_ELIGIBLE_AT_TASK_COMPLETION_METRICS_SUPERVISION,
    DAYS_EMPLOYED_365,
    DAYS_IN_COMMUNITY_365,
    DAYS_INCARCERATED_365,
    DAYS_SENTENCED_AT_LIBERTY_START,
    DAYS_SERVED_AT_LIBERTY_START,
    DAYS_TO_FIRST_INCARCERATION_365,
    DAYS_TO_FIRST_LIBERTY_365,
    DRUG_SCREENS,
    DRUG_SCREENS_POSITIVE,
    EARLY_DISCHARGE_REQUESTS,
    EMPLOYED_STATUS_ENDS,
    EMPLOYED_STATUS_STARTS,
    EMPLOYER_CHANGES_365,
    HOUSING_UNIT_TYPE_ENDS,
    HOUSING_UNIT_TYPE_LENGTH_OF_STAY_BY_END,
    HOUSING_UNIT_TYPE_LENGTH_OF_STAY_BY_START,
    HOUSING_UNIT_TYPE_STARTS,
    INCARCERATION_INCIDENTS,
    INCARCERATION_RELEASES,
    INCARCERATION_RELEASES_1_MONTH_AFTER_PAROLE_ELIGIBILITY_DATE,
    INCARCERATION_STARTS,
    INCARCERATION_STARTS_AND_INFERRED,
    INCARCERATION_STARTS_AND_INFERRED_TECHNICAL_VIOLATION_NO_PRIOR_TREATMENT_REFERRAL,
    INCARCERATION_STARTS_AND_INFERRED_WITH_VIOLATION_TYPE_METRICS,
    INCARCERATION_STARTS_MOST_SEVERE_VIOLATION_TYPE_NOT_ABSCONSION,
    INCARCERATION_STARTS_TECHNICAL_VIOLATION_NO_PRIOR_TREATMENT_REFERRAL,
    INCARCERATION_STARTS_WITH_VIOLATION_TYPE_METRICS,
    INCARCERATIONS_INFERRED,
    INCARCERATIONS_INFERRED_WITH_VIOLATION_TYPE_METRICS,
    INCARCERATIONS_TEMPORARY,
    LATE_OPPORTUNITY_METRICS_INCARCERATION,
    LATE_OPPORTUNITY_METRICS_SUPERVISION,
    LIBERTY_STARTS,
    LSIR_ASSESSMENTS,
    MAX_DAYS_STABLE_EMPLOYMENT_365,
    NUMBER_MONTHS_BETWEEN_DOWNGRADE_AND_ASSESSMENT_DUE,
    NUMBER_MONTHS_BETWEEN_DOWNGRADE_TO_MINIMUM_AND_ASSESSMENT_DUE,
    PAROLE_BOARD_HEARINGS,
    PAROLE_BOARD_HEARINGS_APPROVED,
    PAROLE_BOARD_HEARINGS_AVG_DAYS_SINCE_INCARCERATION,
    PAROLE_BOARD_HEARINGS_CONTINUED,
    PAROLE_BOARD_HEARINGS_DENIED,
    PENDING_CUSTODY_STARTS,
    PERSON_DAYS_TASK_ELIGIBLE_METRICS_INCARCERATION,
    PERSON_DAYS_TASK_ELIGIBLE_METRICS_SUPERVISION,
    PERSON_DAYS_WEIGHTED_JUSTICE_IMPACT,
    PROP_PERIOD_WITH_CRITICAL_CASELOAD,
    PROP_SENTENCE_SERVED_AT_INCARCERATION_RELEASE,
    PROP_SENTENCE_SERVED_AT_INCARCERATION_TO_SUPERVISION_TRANSITION,
    PROP_SENTENCE_SERVED_AT_LIBERTY_START,
    SOLITARY_CONFINEMENT_ENDS,
    SOLITARY_CONFINEMENT_LENGTH_OF_STAY_BY_END,
    SOLITARY_CONFINEMENT_LENGTH_OF_STAY_BY_START,
    SOLITARY_CONFINEMENT_STARTS,
    SOLITARY_CONFINEMENT_STARTS_LAST_YEAR,
    SUPERVISION_LEVEL_DOWNGRADES,
    SUPERVISION_LEVEL_DOWNGRADES_TO_LIMITED,
    SUPERVISION_LEVEL_UPGRADES,
    SUPERVISION_STARTS,
    SUPERVISION_STARTS_FROM_INCARCERATION,
    TASK_COMPLETED_METRICS_INCARCERATION,
    TASK_COMPLETED_METRICS_SUPERVISION,
    TASK_COMPLETED_WHILE_ELIGIBLE_METRICS_INCARCERATION,
    TASK_COMPLETED_WHILE_ELIGIBLE_METRICS_SUPERVISION,
    TREATMENT_REFERRALS,
    TREATMENT_STARTS,
    UNSUCCESSFUL_SUPERVISION_TERMINATIONS,
    VIOLATIONS,
    VIOLATIONS_BY_TYPE_METRICS,
)
from recidiviz.aggregated_metrics.period_event_aggregated_metrics import (
    generate_period_event_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.period_span_aggregated_metrics import (
    generate_period_span_aggregated_metrics_view_builder,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    METRIC_UNITS_OF_ANALYSIS_BY_TYPE,
    MetricUnitOfAnalysisType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Metrics should be added only if necessary for products or analyses, since additions will have
# a meaningful impact on view update performance.
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
        # Average daily population, solitary confinement
        AVG_DAILY_POPULATION_SOLITARY_CONFINEMENT,
        # Average daily population, state prison
        AVG_DAILY_POPULATION_STATE_PRISON,
        # Average daily population, solitary confinement housing type
        *AVG_DAILY_POPULATION_HOUSING_TYPE_METRICS,
        # Average daily population, facility custody level
        AVG_DAILY_POPULATION_MAXIMUM_CUSTODY,
        AVG_DAILY_POPULATION_MEDIUM_CUSTODY,
        AVG_DAILY_POPULATION_MINIMUM_CUSTODY,
        # Average daily population, sentence info
        AVG_DAILY_POPULATION_DRUG_OFFENSE_SENTENCE,
        AVG_DAILY_POPULATION_VIOLENT_OFFENSE_SENTENCE,
        AVG_DAILY_POPULATION_PAST_FULL_TERM_RELEASE_DATE,
        AVG_DAILY_POPULATION_PAST_PAROLE_ELIGIBILITY_DATE,
        # Risk score
        AVG_DAILY_POPULATION_HIGH_RISK_LEVEL,
        AVG_DAILY_POPULATION_LOW_RISK_LEVEL,
        AVG_LSIR_SCORE,
        # Events
        COMMUNITY_CONFINEMENT_SUPERVISION_STARTS,
        *HOUSING_UNIT_TYPE_ENDS,
        *HOUSING_UNIT_TYPE_LENGTH_OF_STAY_BY_START,
        *HOUSING_UNIT_TYPE_LENGTH_OF_STAY_BY_END,
        *HOUSING_UNIT_TYPE_STARTS,
        INCARCERATION_RELEASES,
        LIBERTY_STARTS,
        SOLITARY_CONFINEMENT_ENDS,
        SOLITARY_CONFINEMENT_STARTS,
        SOLITARY_CONFINEMENT_STARTS_LAST_YEAR,
        SUPERVISION_STARTS,
        SUPERVISION_STARTS_FROM_INCARCERATION,
        INCARCERATION_INCIDENTS,
        # Solitary confinement length of stay
        *AVG_ACTIVE_LENGTH_OF_STAY_HOUSING_UNIT_TYPE_METRICS,
        AVG_ACTIVE_LENGTH_OF_STAY_SOLITARY_CONFINEMENT,
        SOLITARY_CONFINEMENT_LENGTH_OF_STAY_BY_END,
        SOLITARY_CONFINEMENT_LENGTH_OF_STAY_BY_START,
        # Assignment window metrics
        ASSIGNMENTS,
        DAYS_AT_LIBERTY_365,
        DAYS_IN_COMMUNITY_365,
        DAYS_INCARCERATED_365,
        ## Workflows
        # TODO(#21261): Automatically pull in all incarceration related TES metrics when we have a param in TES to distinguish
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_INCARCERATION,
        *LATE_OPPORTUNITY_METRICS_INCARCERATION,
        *PERSON_DAYS_TASK_ELIGIBLE_METRICS_INCARCERATION,
        *TASK_COMPLETED_METRICS_INCARCERATION,
        *TASK_COMPLETED_WHILE_ELIGIBLE_METRICS_INCARCERATION,
        *DAYS_ELIGIBLE_AT_TASK_COMPLETION_METRICS_INCARCERATION,
        ## Custody level changes,
        CUSTODY_LEVEL_UPGRADES,
        CUSTODY_LEVEL_DOWNGRADES,
        CUSTODY_LEVEL_DOWNGRADES_TO_MINIMUM,
        NUMBER_MONTHS_BETWEEN_DOWNGRADE_AND_ASSESSMENT_DUE,
        NUMBER_MONTHS_BETWEEN_DOWNGRADE_TO_MINIMUM_AND_ASSESSMENT_DUE,
        # Parole bord hearings
        PAROLE_BOARD_HEARINGS,
        PAROLE_BOARD_HEARINGS_APPROVED,
        PAROLE_BOARD_HEARINGS_AVG_DAYS_SINCE_INCARCERATION,
        PAROLE_BOARD_HEARINGS_CONTINUED,
        PAROLE_BOARD_HEARINGS_DENIED,
        # Sentence metrics
        DAYS_SENTENCED_AT_LIBERTY_START,
        DAYS_SERVED_AT_LIBERTY_START,
        INCARCERATION_RELEASES_1_MONTH_AFTER_PAROLE_ELIGIBILITY_DATE,
        PROP_SENTENCE_SERVED_AT_INCARCERATION_RELEASE,
        PROP_SENTENCE_SERVED_AT_INCARCERATION_TO_SUPERVISION_TRANSITION,
        PROP_SENTENCE_SERVED_AT_LIBERTY_START,
    ],
    MetricPopulationType.SUPERVISION: [
        # Population/caseload information
        AVG_DAILY_POPULATION,
        AVG_ASSIGNMENTS_OFFICER,
        AVG_CRITICAL_CASELOAD_SIZE,
        AVG_CRITICAL_CASELOAD_SIZE_OFFICER,
        AVG_DAILY_CASELOAD_OFFICER,
        PROP_PERIOD_WITH_CRITICAL_CASELOAD,
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
        AVG_DAILY_POPULATION_NONLIMITED_SUPERVISION,
        ## Average daily population, sentence info
        AVG_DAILY_POPULATION_DRUG_OFFENSE_SENTENCE,
        AVG_DAILY_POPULATION_VIOLENT_OFFENSE_SENTENCE,
        AVG_DAILY_POPULATION_PAST_FULL_TERM_RELEASE_DATE,
        # Average daily population, other attributes
        AVG_DAILY_POPULATION_EMPLOYED,
        # Risk score
        AVG_DAILY_POPULATION_HIGH_RISK_LEVEL,
        AVG_DAILY_POPULATION_LOW_RISK_LEVEL,
        AVG_LSIR_SCORE,
        # Average daily population, caseload category and type
        *AVG_NUM_SUPERVISION_OFFICERS_INSIGHTS_CASELOAD_CATEGORY_METRICS,
        # Events
        ## Session transitions
        ABSCONSIONS_BENCH_WARRANTS,
        EARLY_DISCHARGE_REQUESTS,
        INCARCERATIONS_INFERRED,
        *INCARCERATIONS_INFERRED_WITH_VIOLATION_TYPE_METRICS,
        INCARCERATION_STARTS,
        INCARCERATION_STARTS_MOST_SEVERE_VIOLATION_TYPE_NOT_ABSCONSION,
        *INCARCERATION_STARTS_WITH_VIOLATION_TYPE_METRICS,
        INCARCERATION_STARTS_TECHNICAL_VIOLATION_NO_PRIOR_TREATMENT_REFERRAL,
        INCARCERATION_STARTS_AND_INFERRED,
        *INCARCERATION_STARTS_AND_INFERRED_WITH_VIOLATION_TYPE_METRICS,
        INCARCERATION_STARTS_AND_INFERRED_TECHNICAL_VIOLATION_NO_PRIOR_TREATMENT_REFERRAL,
        INCARCERATIONS_TEMPORARY,
        LIBERTY_STARTS,
        PENDING_CUSTODY_STARTS,
        SUPERVISION_STARTS,
        SUPERVISION_STARTS_FROM_INCARCERATION,
        UNSUCCESSFUL_SUPERVISION_TERMINATIONS,
        VIOLATIONS,
        *VIOLATIONS_BY_TYPE_METRICS,
        ## Supervision level changes
        SUPERVISION_LEVEL_DOWNGRADES,
        SUPERVISION_LEVEL_UPGRADES,
        SUPERVISION_LEVEL_DOWNGRADES_TO_LIMITED,
        ## Drug Screens
        DRUG_SCREENS,
        DRUG_SCREENS_POSITIVE,
        ## LSI-R Assessments
        LSIR_ASSESSMENTS,
        ## Contacts
        CONTACTS_ATTEMPTED,
        ## Employment Changes
        EMPLOYED_STATUS_ENDS,
        EMPLOYED_STATUS_STARTS,
        EMPLOYER_CHANGES_365,
        MAX_DAYS_STABLE_EMPLOYMENT_365,
        ## Program Referrals
        TREATMENT_REFERRALS,
        ## Program Starts
        TREATMENT_STARTS,
        # TODO(#18344): Use task population_types to only calculate relevant workflows metrics for a single population
        ## Workflows - eligibility metrics
        *AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION,
        *LATE_OPPORTUNITY_METRICS_SUPERVISION,
        *PERSON_DAYS_TASK_ELIGIBLE_METRICS_SUPERVISION,
        *TASK_COMPLETED_METRICS_SUPERVISION,
        *TASK_COMPLETED_WHILE_ELIGIBLE_METRICS_SUPERVISION,
        *DAYS_ELIGIBLE_AT_TASK_COMPLETION_METRICS_SUPERVISION,
        # Assignment window metrics
        ASSIGNMENTS,
        ANY_INCARCERATION_365,
        DAYS_ABSCONDED_365,
        DAYS_AT_LIBERTY_365,
        DAYS_IN_COMMUNITY_365,
        DAYS_INCARCERATED_365,
        DAYS_EMPLOYED_365,
        DAYS_TO_FIRST_INCARCERATION_365,
        DAYS_TO_FIRST_LIBERTY_365,
        ## Sentences
        DAYS_SENTENCED_AT_LIBERTY_START,
        DAYS_SERVED_AT_LIBERTY_START,
        PROP_SENTENCE_SERVED_AT_INCARCERATION_TO_SUPERVISION_TRANSITION,
        PROP_SENTENCE_SERVED_AT_LIBERTY_START,
    ],
    MetricPopulationType.JUSTICE_INVOLVED: [
        # Population metrics
        AVG_DAILY_POPULATION,
        AVG_DAILY_POPULATION_LIMITED_SUPERVISION_JUSTICE_IMPACT,
        AVG_DAILY_POPULATION_MAXIMUM_CUSTODY_JUSTICE_IMPACT,
        AVG_DAILY_POPULATION_MEDIUM_CUSTODY_JUSTICE_IMPACT,
        AVG_DAILY_POPULATION_MINIMUM_CUSTODY_JUSTICE_IMPACT,
        AVG_DAILY_POPULATION_NONLIMITED_SUPERVISION_JUSTICE_IMPACT,
        AVG_DAILY_POPULATION_SOLITARY_CONFINEMENT_JUSTICE_IMPACT,
        # Weighted population metrics
        PERSON_DAYS_WEIGHTED_JUSTICE_IMPACT,
        # Metrics required for sentence metrics
        LIBERTY_STARTS,
        # Sentence metrics
        DAYS_SENTENCED_AT_LIBERTY_START,
        DAYS_SERVED_AT_LIBERTY_START,
        PROP_SENTENCE_SERVED_AT_LIBERTY_START,
    ],
}

UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE: Dict[
    MetricPopulationType, List[MetricUnitOfAnalysisType]
] = {
    MetricPopulationType.INCARCERATION: [
        MetricUnitOfAnalysisType.FACILITY,
        MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
        MetricUnitOfAnalysisType.WORKFLOWS_CASELOAD,
        MetricUnitOfAnalysisType.WORKFLOWS_LOCATION,
        MetricUnitOfAnalysisType.STATE_CODE,
    ],
    MetricPopulationType.SUPERVISION: [
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.SUPERVISION_UNIT,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
        MetricUnitOfAnalysisType.WORKFLOWS_CASELOAD,
        MetricUnitOfAnalysisType.WORKFLOWS_LOCATION,
        MetricUnitOfAnalysisType.STATE_CODE,
    ],
    MetricPopulationType.JUSTICE_INVOLVED: [
        MetricUnitOfAnalysisType.WORKFLOWS_CASELOAD,
        MetricUnitOfAnalysisType.WORKFLOWS_LOCATION,
        MetricUnitOfAnalysisType.STATE_CODE,
        MetricUnitOfAnalysisType.FACILITY,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ],
}

UNIT_OF_ANALYSIS_TYPES_TO_EXCLUDE_FROM_NON_ASSIGNMENT_VIEWS: List[
    MetricUnitOfAnalysisType
] = [
    MetricUnitOfAnalysisType.WORKFLOWS_CASELOAD,
    MetricUnitOfAnalysisType.WORKFLOWS_LOCATION,
]


def collect_aggregated_metrics_view_builders(
    *,
    metrics_by_population_dict: Dict[MetricPopulationType, List[AggregatedMetric]],
    units_of_analysis_by_population_dict: Dict[
        MetricPopulationType, List[MetricUnitOfAnalysisType]
    ],
    units_of_analysis_to_exclude_from_non_assignment_views: Optional[
        List[MetricUnitOfAnalysisType]
    ] = None,
    dataset_id_override: Optional[str] = None,
) -> List[SimpleBigQueryViewBuilder]:
    """
    Collects all aggregated metrics view builders at all available units of analysis and populations
    """
    view_builders = []

    # TODO(#29291): Filter all_metrics list down to only metrics we use downstream in
    #  products / Looker, then make it easier for DAs, etc to query configured metrics
    #  in an ad-hoc way from notebooks, etc.
    for population_type, all_metrics in metrics_by_population_dict.items():
        if not all_metrics:
            continue

        # Check that all EventValueMetrics have the configured EventCountMetric included for the same population
        event_value_metric_list = [
            m for m in all_metrics if isinstance(m, EventValueMetric)
        ]
        for metric in event_value_metric_list:
            if metric.event_count_metric not in all_metrics:
                raise ValueError(
                    f"`{metric.event_count_metric.name}` EventCountMetric "
                    f"not found in configured `metrics_by_population_dict` for "
                    f"{population_type.name} population, although this is a required "
                    f"dependency for `{metric.name}` EventValueMetric."
                )

        for unit_of_analysis_type in units_of_analysis_by_population_dict[
            population_type
        ]:
            # Filter out unit of analysis types for which we don't need materialized metric views
            if units_of_analysis_to_exclude_from_non_assignment_views and (
                unit_of_analysis_type
                in units_of_analysis_to_exclude_from_non_assignment_views
            ):
                continue
            unit_of_analysis = METRIC_UNITS_OF_ANALYSIS_BY_TYPE[unit_of_analysis_type]

            # Build metric builder views by type
            # PeriodSpanAggregatedMetric
            period_span_metric_list = [
                m for m in all_metrics if isinstance(m, PeriodSpanAggregatedMetric)
            ]
            if period_span_metric_list:
                view_builders.append(
                    generate_period_span_aggregated_metrics_view_builder(
                        unit_of_analysis=unit_of_analysis,
                        population_type=population_type,
                        metrics=period_span_metric_list,
                    )
                )

            # PeriodEventAggregatedMetric
            period_event_metric_list = [
                m for m in all_metrics if isinstance(m, PeriodEventAggregatedMetric)
            ]
            if period_event_metric_list:
                view_builders.append(
                    generate_period_event_aggregated_metrics_view_builder(
                        unit_of_analysis=unit_of_analysis,
                        population_type=population_type,
                        metrics=period_event_metric_list,
                    )
                )

            # AssignmentSpanAggregatedMetric
            assignment_span_metric_list = [
                m for m in all_metrics if isinstance(m, AssignmentSpanAggregatedMetric)
            ]
            if assignment_span_metric_list:
                view_builders.append(
                    generate_assignment_span_aggregated_metrics_view_builder(
                        unit_of_analysis=unit_of_analysis,
                        population_type=population_type,
                        metrics=assignment_span_metric_list,
                    )
                )

            # AssignmentEventAggregatedMetric
            assignment_event_metric_list = [
                m for m in all_metrics if isinstance(m, AssignmentEventAggregatedMetric)
            ]
            if assignment_event_metric_list:
                view_builders.append(
                    generate_assignment_event_aggregated_metrics_view_builder(
                        unit_of_analysis=unit_of_analysis,
                        population_type=population_type,
                        metrics=assignment_event_metric_list,
                    )
                )

            # verify that ASSIGNMENTS is present if any assignment span/event metrics
            # are present
            if assignment_span_metric_list or assignment_event_metric_list:
                if not ASSIGNMENTS in all_metrics:
                    raise ValueError(
                        "Assignment span/event metrics are present but ASSIGNMENTS is not"
                    )

            # MiscAggregatedMetric
            misc_metric_list = [
                m
                for m in all_metrics
                if isinstance(m, MiscAggregatedMetric)
                and population_type in m.populations
                and unit_of_analysis_type in m.unit_of_analysis_types
            ]
            if misc_metric_list:
                # Even if there are misc metrics, generate_misc_aggregated_metrics_view_builder
                # may return None if there are no metrics for the given population,
                # so first generate the view builder and then check if it is None
                misc_metric_view_builder = (
                    generate_misc_aggregated_metrics_view_builder(
                        unit_of_analysis=unit_of_analysis,
                        population_type=population_type,
                        metrics=misc_metric_list,
                    )
                )
                if misc_metric_view_builder:
                    view_builders.append(misc_metric_view_builder)

            # Build aggregated metrics table combining all
            view_builders.append(
                generate_aggregated_metrics_view_builder(
                    unit_of_analysis=unit_of_analysis,
                    population_type=population_type,
                    metrics=all_metrics,
                    dataset_id_override=dataset_id_override,
                )
            )

    return view_builders


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for view_builder in collect_aggregated_metrics_view_builders(
            metrics_by_population_dict=METRICS_BY_POPULATION_TYPE,
            units_of_analysis_by_population_dict=UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE,
        ):
            view_builder.build_and_print()
