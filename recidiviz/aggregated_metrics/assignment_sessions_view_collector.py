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
"""Functionality for collecting all metric assignment session view builders."""
from recidiviz.aggregated_metrics.assignment_sessions_view_builder import (
    generate_metric_assignment_sessions_view_builder,
    has_configured_assignment_query,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.span_selector import SpanSelector
from recidiviz.observations.span_type import SpanType


def relevant_units_of_analysis_for_population_type(
    population_type: MetricPopulationType,
) -> list[MetricUnitOfAnalysisType]:
    """For the given population type, returns the units of analysis that can be used
    when building metrics about that population.
    """
    match population_type:
        case MetricPopulationType.CUSTOM:
            raise ValueError(
                "Cannot get standard units of analysis for CUSTOM population type."
            )
        case MetricPopulationType.INCARCERATION:
            return [
                MetricUnitOfAnalysisType.FACILITY,
                MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
                MetricUnitOfAnalysisType.STATE_CODE,
                MetricUnitOfAnalysisType.ALL_STATES,
            ]
        case MetricPopulationType.SUPERVISION:
            return [
                MetricUnitOfAnalysisType.INSIGHTS_CASELOAD_CATEGORY,
                MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
                MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
                MetricUnitOfAnalysisType.SUPERVISION_UNIT,
                MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
                MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
                MetricUnitOfAnalysisType.STATE_CODE,
                MetricUnitOfAnalysisType.ALL_STATES,
                MetricUnitOfAnalysisType.OFFICER_OUTLIER_USAGE_COHORT,
            ]
        case MetricPopulationType.JUSTICE_INVOLVED:
            return [
                MetricUnitOfAnalysisType.WORKFLOWS_CASELOAD,
                MetricUnitOfAnalysisType.LOCATION_DETAIL,
                MetricUnitOfAnalysisType.LOCATION,
                MetricUnitOfAnalysisType.STATE_CODE,
                MetricUnitOfAnalysisType.FACILITY,
                MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
                MetricUnitOfAnalysisType.ALL_STATES,
                MetricUnitOfAnalysisType.EXPERIMENT_VARIANT,
                MetricUnitOfAnalysisType.WORKFLOWS_PROVISIONED_USER,
                MetricUnitOfAnalysisType.INSIGHTS_PROVISIONED_USER,
            ]


def _get_person_population_selector(
    population_type: MetricPopulationType,
) -> SpanSelector | None:
    """Returns the population SpanSelector for the MetricUnitOfObservationType.PERSON_ID
    population of the given population type.
    """
    match population_type:
        case MetricPopulationType.CUSTOM:
            raise ValueError(
                "Cannot get standard population selector for CUSTOM population type."
            )
        case MetricPopulationType.INCARCERATION:
            return SpanSelector(
                span_type=SpanType.COMPARTMENT_SESSION,
                span_conditions_dict={
                    "compartment_level_1": ["INCARCERATION"],
                    "compartment_level_2": [
                        StateSpecializedPurposeForIncarceration.GENERAL.value,
                        StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD.value,
                        StateSpecializedPurposeForIncarceration.SAFEKEEPING.value,
                        StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION.value,
                        StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON.value,
                        StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY.value,
                        StateSpecializedPurposeForIncarceration.WEEKEND_CONFINEMENT.value,
                        StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT.value,
                    ],
                },
            )

        case MetricPopulationType.SUPERVISION:
            return SpanSelector(
                span_type=SpanType.COMPARTMENT_SESSION,
                span_conditions_dict={
                    "compartment_level_1": ["SUPERVISION"],
                    "compartment_level_2": [
                        StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT.value,
                        StateSupervisionPeriodSupervisionType.DUAL.value,
                        StateSupervisionPeriodSupervisionType.PAROLE.value,
                        StateSupervisionPeriodSupervisionType.PROBATION.value,
                        StateSupervisionPeriodSupervisionType.WARRANT_STATUS.value,
                    ],
                },
            )

        case MetricPopulationType.JUSTICE_INVOLVED:
            return SpanSelector(
                span_type=SpanType.COMPARTMENT_SESSION,
                span_conditions_dict={
                    # every compartment in the union of incarceration and supervision
                    # with the addition of INFORMAL_PROBATION
                    "compartment_level_1": ["INCARCERATION", "SUPERVISION"],
                    "compartment_level_2": [
                        StateSpecializedPurposeForIncarceration.GENERAL.value,
                        StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD.value,
                        StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION.value,
                        StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON.value,
                        StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY.value,
                        StateSpecializedPurposeForIncarceration.WEEKEND_CONFINEMENT.value,
                        StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT.value,
                        StateSupervisionPeriodSupervisionType.DUAL.value,
                        StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION.value,
                        StateSupervisionPeriodSupervisionType.PAROLE.value,
                        StateSupervisionPeriodSupervisionType.PROBATION.value,
                        StateSupervisionPeriodSupervisionType.WARRANT_STATUS.value,
                    ],
                },
            )


def _get_workflows_primary_user_population_selector(
    population_type: MetricPopulationType,
) -> SpanSelector | None:
    """Returns the population SpanSelector for the
    MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER population of the given population type.
    """
    match population_type:
        case MetricPopulationType.CUSTOM:
            raise ValueError(
                "Cannot get standard population selector for CUSTOM population type."
            )
        case MetricPopulationType.INCARCERATION:
            return SpanSelector(
                span_type=SpanType.WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION,
                span_conditions_dict={"system_type": ["INCARCERATION"]},
            )
        case MetricPopulationType.SUPERVISION:
            return SpanSelector(
                span_type=SpanType.WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION,
                span_conditions_dict={"system_type": ["SUPERVISION"]},
            )
        case MetricPopulationType.JUSTICE_INVOLVED:
            return SpanSelector(
                span_type=SpanType.WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION,
                span_conditions_dict={},
            )


def _get_supervision_officer_population_selector(
    population_type: MetricPopulationType,
) -> SpanSelector | None:
    """Returns the population SpanSelector for the
    MetricUnitOfObservationType.SUPERVISION_OFFICER population of the given population
    type.
    """
    match population_type:
        case MetricPopulationType.CUSTOM:
            raise ValueError(
                "Cannot get standard population selector for CUSTOM population type."
            )
        case MetricPopulationType.INCARCERATION:
            return None
        case MetricPopulationType.SUPERVISION:
            return SpanSelector(
                span_type=SpanType.SUPERVISION_OFFICER_CASELOAD_COUNT_SPAN,
                span_conditions_dict={},
            )
        case MetricPopulationType.JUSTICE_INVOLVED:
            return SpanSelector(
                span_type=SpanType.SUPERVISION_OFFICER_CASELOAD_COUNT_SPAN,
                span_conditions_dict={},
            )


def _get_workflows_surfaceable_caseload_population_selector(
    population_type: MetricPopulationType,
) -> SpanSelector | None:
    """Returns the population SpanSelector for the
    MetricUnitOfObservationType.WORKFLOWS_SURFACEABLE_CASELOAD population of the given population
    type.
    """
    match population_type:
        case MetricPopulationType.CUSTOM:
            raise ValueError(
                "Cannot get standard population selector for CUSTOM population type."
            )
        case MetricPopulationType.INCARCERATION:
            return SpanSelector(
                span_type=SpanType.WORKFLOWS_SURFACEABLE_CASELOAD_SESSION,
                span_conditions_dict={"system_type": ["INCARCERATION"]},
            )
        case MetricPopulationType.SUPERVISION:
            return SpanSelector(
                span_type=SpanType.WORKFLOWS_SURFACEABLE_CASELOAD_SESSION,
                span_conditions_dict={"system_type": ["SUPERVISION"]},
            )
        case MetricPopulationType.JUSTICE_INVOLVED:
            return SpanSelector(
                span_type=SpanType.WORKFLOWS_SURFACEABLE_CASELOAD_SESSION,
                span_conditions_dict={},
            )


def _get_insights_primary_user_population_selector(
    population_type: MetricPopulationType,
) -> SpanSelector | None:
    """Returns the population SpanSelector for the
    MetricUnitOfObservationType.INSIGHTS_PRIMARY_USER population of the given population
    type.
    """
    match population_type:
        case MetricPopulationType.CUSTOM:
            raise ValueError(
                "Cannot get standard population selector for CUSTOM population type."
            )
        case MetricPopulationType.INCARCERATION:
            return None
        case MetricPopulationType.SUPERVISION:
            return SpanSelector(
                span_type=SpanType.INSIGHTS_PRIMARY_USER_REGISTRATION_SESSION,
                span_conditions_dict={},
            )
        case MetricPopulationType.JUSTICE_INVOLVED:
            return SpanSelector(
                span_type=SpanType.INSIGHTS_PRIMARY_USER_REGISTRATION_SESSION,
                span_conditions_dict={},
            )


def _get_workflows_provisioned_user_population_selector(
    population_type: MetricPopulationType,
) -> SpanSelector | None:
    """Returns the population SpanSelector for the
    MetricUnitOfObservationType.WORKFLOWS_PROVISIONED_USER population of the given population
    type.
    """
    match population_type:
        case MetricPopulationType.CUSTOM:
            raise ValueError(
                "Cannot get standard population selector for CUSTOM population type."
            )
        case MetricPopulationType.INCARCERATION:
            return SpanSelector(
                span_type=SpanType.WORKFLOWS_PROVISIONED_USER_SESSION,
                span_conditions_dict={"system_type": ["INCARCERATION"]},
            )
        case MetricPopulationType.SUPERVISION:
            return SpanSelector(
                span_type=SpanType.WORKFLOWS_PROVISIONED_USER_SESSION,
                span_conditions_dict={"system_type": ["SUPERVISION"]},
            )
        case MetricPopulationType.JUSTICE_INVOLVED:
            return SpanSelector(
                span_type=SpanType.WORKFLOWS_PROVISIONED_USER_SESSION,
                span_conditions_dict={},
            )


def _get_insights_provisioned_user_population_selector(
    population_type: MetricPopulationType,
) -> SpanSelector | None:
    """Returns the population SpanSelector for the
    MetricUnitOfObservationType.INSIGHTS_PROVISIONED_USER population of the given population
    type.
    """
    match population_type:
        case MetricPopulationType.CUSTOM:
            raise ValueError(
                "Cannot get standard population selector for CUSTOM population type."
            )
        case MetricPopulationType.INCARCERATION:
            return None
        case MetricPopulationType.SUPERVISION:
            return SpanSelector(
                span_type=SpanType.INSIGHTS_PROVISIONED_USER_SESSION,
                span_conditions_dict={},
            )
        case MetricPopulationType.JUSTICE_INVOLVED:
            return SpanSelector(
                span_type=SpanType.INSIGHTS_PROVISIONED_USER_SESSION,
                span_conditions_dict={},
            )


def _get_tasks_provisioned_user_population_selector(
    population_type: MetricPopulationType,
) -> SpanSelector | None:
    """Returns the population SpanSelector for the
    MetricUnitOfObservationType.TASKS_PROVISIONED_USER population of the given population
    type.
    """
    match population_type:
        case MetricPopulationType.CUSTOM:
            raise ValueError(
                "Cannot get standard population selector for CUSTOM population type."
            )
        case MetricPopulationType.INCARCERATION:
            return None
        case MetricPopulationType.SUPERVISION:
            return SpanSelector(
                span_type=SpanType.TASKS_PROVISIONED_USER_SESSION,
                span_conditions_dict={},
            )
        case MetricPopulationType.JUSTICE_INVOLVED:
            return SpanSelector(
                span_type=SpanType.TASKS_PROVISIONED_USER_SESSION,
                span_conditions_dict={},
            )


def _get_tasks_primary_user_population_selector(
    population_type: MetricPopulationType,
) -> SpanSelector | None:
    """Returns the population SpanSelector for the
    MetricUnitOfObservationType.TASKS_PRIMARY_USER population of the given population
    type.
    """
    match population_type:
        case MetricPopulationType.CUSTOM:
            raise ValueError(
                "Cannot get standard population selector for CUSTOM population type."
            )
        case MetricPopulationType.INCARCERATION:
            return None
        case MetricPopulationType.SUPERVISION:
            return SpanSelector(
                span_type=SpanType.TASKS_PRIMARY_USER_REGISTRATION_SESSION,
                span_conditions_dict={},
            )
        case MetricPopulationType.JUSTICE_INVOLVED:
            return SpanSelector(
                span_type=SpanType.TASKS_PRIMARY_USER_REGISTRATION_SESSION,
                span_conditions_dict={},
            )


def get_standard_population_selector_for_unit_of_observation(
    population_type: MetricPopulationType,
    unit_of_observation_type: MetricUnitOfObservationType,
) -> SpanSelector | None:
    """Returns a SpanSelector that can be used to build a population query for the given
    population type and unit of observation.
    """
    match unit_of_observation_type:
        case MetricUnitOfObservationType.PERSON_ID:
            return _get_person_population_selector(population_type)
        case MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER:
            return _get_workflows_primary_user_population_selector(population_type)
        case MetricUnitOfObservationType.SUPERVISION_OFFICER:
            return _get_supervision_officer_population_selector(population_type)
        case MetricUnitOfObservationType.WORKFLOWS_SURFACEABLE_CASELOAD:
            return _get_workflows_surfaceable_caseload_population_selector(
                population_type
            )
        case MetricUnitOfObservationType.INSIGHTS_PRIMARY_USER:
            return _get_insights_primary_user_population_selector(population_type)
        case MetricUnitOfObservationType.WORKFLOWS_PROVISIONED_USER:
            return _get_workflows_provisioned_user_population_selector(population_type)
        case MetricUnitOfObservationType.INSIGHTS_PROVISIONED_USER:
            return _get_insights_provisioned_user_population_selector(population_type)
        case MetricUnitOfObservationType.TASKS_PRIMARY_USER:
            return _get_tasks_primary_user_population_selector(population_type)
        case MetricUnitOfObservationType.TASKS_PROVISIONED_USER:
            return _get_tasks_provisioned_user_population_selector(population_type)


def collect_assignment_sessions_view_builders() -> list[SimpleBigQueryViewBuilder]:
    """Returns metric assignment sessions view builders for all valid (population type,
    unit of observation, unit of analysis) combinations.
    """
    view_builders = []

    for population_type in MetricPopulationType:
        if population_type is MetricPopulationType.CUSTOM:
            continue

        for unit_of_observation_type in MetricUnitOfObservationType:
            population_selector = (
                get_standard_population_selector_for_unit_of_observation(
                    population_type=population_type,
                    unit_of_observation_type=unit_of_observation_type,
                )
            )
            if not population_selector:
                # It does not make sense to define a population for this unit of
                # observation / population type combo - continue.
                continue

            units_of_analysis = relevant_units_of_analysis_for_population_type(
                population_type
            )

            for unit_of_analysis_type in units_of_analysis:
                if not has_configured_assignment_query(
                    unit_of_analysis_type,
                    unit_of_observation_type,
                ):
                    continue

                view_builders.append(
                    generate_metric_assignment_sessions_view_builder(
                        unit_of_analysis_type=unit_of_analysis_type,
                        unit_of_observation_type=unit_of_observation_type,
                        population_type=population_type,
                        population_selector=population_selector,
                    )
                )

    return view_builders
