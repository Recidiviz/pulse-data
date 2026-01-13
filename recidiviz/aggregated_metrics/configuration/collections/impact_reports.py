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
"""Aggregated metrics collection definition for aggregated metrics used in the
Leadership Impact Reports.
"""

from recidiviz.aggregated_metrics.aggregated_metric_collection_config import (
    AggregatedMetricsCollection,
    AggregatedMetricsCollectionPopulationConfig,
)
from recidiviz.aggregated_metrics.aggregated_metrics_view_collector import (
    collect_aggregated_metric_view_builders_for_collection,
)
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_DAILY_POPULATION,
    AVG_DAILY_POPULATION_ASSESSMENT_OVERDUE,
    AVG_DAILY_POPULATION_ASSESSMENT_REQUIRED,
    AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_SUPERVISION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_ACTIONABLE_AND_VIEWED,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_SUPERVISION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_30_DAYS,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_30_DAYS_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_30_DAYS_METRICS_SUPERVISION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_LESS_THAN_30_DAYS,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_SUPERVISION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION,
    AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE,
    AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_SUPERVISION,
    AVG_DAILY_POPULATION_TASK_MARKED_SUBMITTED,
    AVG_DAILY_POPULATION_TASK_MARKED_SUBMITTED_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_MARKED_SUBMITTED_METRICS_SUPERVISION,
    CONTACT_DUE_DATES,
    CONTACT_DUE_DATES_MET,
    DISTINCT_ACTIVE_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR,
    DISTINCT_ACTIVE_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR_INCARCERATION,
    DISTINCT_ACTIVE_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR_SUPERVISION,
    DISTINCT_ACTIVE_USERS_ALL_INCARCERATION_TASKS,
    DISTINCT_ACTIVE_USERS_ALL_SUPERVISION_TASKS,
    DISTINCT_ACTIVE_USERS_INCARCERATION,
    DISTINCT_ACTIVE_USERS_SUPERVISION,
    DISTINCT_LOGGED_IN_PRIMARY_USERS_ALL_INCARCERATION_TASKS,
    DISTINCT_LOGGED_IN_PRIMARY_USERS_ALL_SUPERVISION_TASKS,
    DISTINCT_LOGGED_IN_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR,
    DISTINCT_LOGGED_IN_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR_INCARCERATION,
    DISTINCT_LOGGED_IN_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR_SUPERVISION,
    DISTINCT_REGISTERED_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR,
    DISTINCT_REGISTERED_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR_INCARCERATION,
    DISTINCT_REGISTERED_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR_SUPERVISION,
    DISTINCT_REGISTERED_USERS_INCARCERATION,
    DISTINCT_REGISTERED_USERS_SUPERVISION,
    TASK_COMPLETED_AFTER_ELIGIBLE_7_DAYS_METRICS_INCARCERATION,
    TASK_COMPLETED_AFTER_ELIGIBLE_7_DAYS_METRICS_SUPERVISION,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.calculator.query.state.dataset_config import IMPACT_REPORTS_DATASET_ID
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

IMPACT_REPORTS_LOOKBACK_MONTHS = 2

EXCLUSIVE_FUNNEL_METRICS = [
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_30_DAYS,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_LESS_THAN_30_DAYS,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_ACTIONABLE_AND_VIEWED,
    AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE,
    AVG_DAILY_POPULATION_TASK_MARKED_SUBMITTED,
]


def _build_impact_reports_usage_aggregated_metrics_collection() -> (
    AggregatedMetricsCollection
):
    time_periods = [
        # Week-long period covering every week-long period in the last 3 months
        MetricTimePeriodConfig.week_periods_rolling_daily(
            # Make sure we cover the entirely of the previous 2 months
            lookback_days=(IMPACT_REPORTS_LOOKBACK_MONTHS + 1)
            * 31
        ),
        # Month-long period covering every month-long period in the last 18 months
        MetricTimePeriodConfig.month_periods_rolling_daily(lookback_days=18 * 31),
        # Year-long periods ending (exclusive) on the first of the month for the last
        # lookback_months months.
        MetricTimePeriodConfig.year_periods_rolling_monthly(
            lookback_months=12 + IMPACT_REPORTS_LOOKBACK_MONTHS
        ),
    ]
    return AggregatedMetricsCollection(
        output_dataset_id=IMPACT_REPORTS_DATASET_ID,
        collection_tag="usage",
        population_configs={
            MetricPopulationType.JUSTICE_INVOLVED: AggregatedMetricsCollectionPopulationConfig(
                output_dataset_id=IMPACT_REPORTS_DATASET_ID,
                population_type=MetricPopulationType.JUSTICE_INVOLVED,
                units_of_analysis={
                    MetricUnitOfAnalysisType.STATE_CODE,
                    MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
                    MetricUnitOfAnalysisType.FACILITY,
                },
                metrics=[
                    # We don't actually use this, but generate this metric to make sure
                    # that one row is generated for every possible metric period.
                    AVG_DAILY_POPULATION,
                    *DISTINCT_ACTIVE_USERS_SUPERVISION,
                    DISTINCT_ACTIVE_USERS_ALL_SUPERVISION_TASKS,
                    DISTINCT_ACTIVE_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR_SUPERVISION,
                    DISTINCT_LOGGED_IN_PRIMARY_USERS_ALL_SUPERVISION_TASKS,
                    DISTINCT_LOGGED_IN_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR_SUPERVISION,
                    DISTINCT_REGISTERED_USERS_SUPERVISION,
                    DISTINCT_REGISTERED_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR_SUPERVISION,
                    *DISTINCT_ACTIVE_USERS_INCARCERATION,
                    DISTINCT_ACTIVE_USERS_ALL_INCARCERATION_TASKS,
                    DISTINCT_ACTIVE_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR_INCARCERATION,
                    DISTINCT_REGISTERED_USERS_INCARCERATION,
                    DISTINCT_REGISTERED_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR_INCARCERATION,
                    DISTINCT_LOGGED_IN_PRIMARY_USERS_ALL_INCARCERATION_TASKS,
                    DISTINCT_LOGGED_IN_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR_INCARCERATION,
                    DISTINCT_REGISTERED_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR,
                    DISTINCT_LOGGED_IN_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR,
                    DISTINCT_ACTIVE_PRIMARY_WORKFLOWS_USERS_WITH_ELIGIBLE_CASELOAD_IN_PAST_YEAR,
                ],
            ),
        },
        time_periods=time_periods,
        disaggregate_by_observation_attributes=None,
    )


def _build_impact_reports_impact_funnel_aggregated_metrics_collection() -> (
    AggregatedMetricsCollection
):
    time_periods = [
        MetricTimePeriodConfig.day_periods(
            # Look back at LEAST 2 whole months to capture the last day of the last
            # two months.
            lookback_days=(IMPACT_REPORTS_LOOKBACK_MONTHS * 31)
        )
    ]

    return AggregatedMetricsCollection.build(
        output_dataset_id=IMPACT_REPORTS_DATASET_ID,
        collection_tag="impact_funnel",
        time_periods=time_periods,
        unit_of_analysis_types_by_population_type={
            MetricPopulationType.SUPERVISION: [
                MetricUnitOfAnalysisType.STATE_CODE,
                MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
            ],
            MetricPopulationType.INCARCERATION: [
                MetricUnitOfAnalysisType.STATE_CODE,
                MetricUnitOfAnalysisType.FACILITY,
            ],
        },
        metrics_by_population_type={
            MetricPopulationType.SUPERVISION: [
                AVG_DAILY_POPULATION,
                *AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_SUPERVISION,
                *AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION,
                *AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_SUPERVISION,
                *AVG_DAILY_POPULATION_TASK_MARKED_SUBMITTED_METRICS_SUPERVISION,
                *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_SUPERVISION,
                *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_SUPERVISION,
                *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_30_DAYS_METRICS_SUPERVISION,
            ],
            MetricPopulationType.INCARCERATION: [
                AVG_DAILY_POPULATION,
                *AVG_DAILY_POPULATION_TASK_ALMOST_ELIGIBLE_METRICS_INCARCERATION,
                *AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_INCARCERATION,
                *AVG_DAILY_POPULATION_TASK_MARKED_INELIGIBLE_METRICS_INCARCERATION,
                *AVG_DAILY_POPULATION_TASK_MARKED_SUBMITTED_METRICS_INCARCERATION,
                *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_VIEWED_METRICS_INCARCERATION,
                *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_NOT_VIEWED_METRICS_INCARCERATION,
                *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_30_DAYS_METRICS_INCARCERATION,
            ],
        },
        disaggregate_by_observation_attributes=None,
    )


def _build_impact_reports_impact_funnel_exclusive_aggregated_metrics_collection() -> (
    AggregatedMetricsCollection
):
    time_periods = [
        MetricTimePeriodConfig.day_periods(
            # Look back at LEAST 2 whole months to capture the last day of the last
            # two months.
            lookback_days=(IMPACT_REPORTS_LOOKBACK_MONTHS * 31)
        )
    ]

    return AggregatedMetricsCollection.build(
        output_dataset_id=IMPACT_REPORTS_DATASET_ID,
        collection_tag="impact_funnel_exclusive",
        time_periods=time_periods,
        unit_of_analysis_types_by_population_type={
            MetricPopulationType.SUPERVISION: [
                MetricUnitOfAnalysisType.STATE_CODE,
                MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
            ],
            MetricPopulationType.INCARCERATION: [
                MetricUnitOfAnalysisType.STATE_CODE,
                MetricUnitOfAnalysisType.FACILITY,
            ],
        },
        metrics_by_population_type={
            MetricPopulationType.SUPERVISION: [
                AVG_DAILY_POPULATION,
                *EXCLUSIVE_FUNNEL_METRICS,
            ],
            MetricPopulationType.INCARCERATION: [
                AVG_DAILY_POPULATION,
                *EXCLUSIVE_FUNNEL_METRICS,
            ],
        },
        disaggregate_by_observation_attributes=["task_type"],
    )


def _build_impact_reports_report_metrics_aggregated_metrics_collection() -> (
    AggregatedMetricsCollection
):
    time_periods = [
        MetricTimePeriodConfig.month_periods(lookback_months=24),
        MetricTimePeriodConfig.quarter_periods_rolling_monthly(lookback_months=24),
    ]

    return AggregatedMetricsCollection(
        output_dataset_id=IMPACT_REPORTS_DATASET_ID,
        collection_tag="report_metrics",
        population_configs={
            MetricPopulationType.JUSTICE_INVOLVED: AggregatedMetricsCollectionPopulationConfig(
                output_dataset_id=IMPACT_REPORTS_DATASET_ID,
                population_type=MetricPopulationType.JUSTICE_INVOLVED,
                units_of_analysis={
                    MetricUnitOfAnalysisType.STATE_CODE,
                    MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
                    MetricUnitOfAnalysisType.FACILITY,
                },
                metrics=[
                    # We don't actually use this, but generate this metric to make sure
                    # that one row is generated for every possible metric period.
                    AVG_DAILY_POPULATION,
                    *TASK_COMPLETED_AFTER_ELIGIBLE_7_DAYS_METRICS_INCARCERATION,
                    *TASK_COMPLETED_AFTER_ELIGIBLE_7_DAYS_METRICS_SUPERVISION,
                ],
            ),
        },
        time_periods=time_periods,
        disaggregate_by_observation_attributes=None,
    )


def _build_impact_reports_compliance_aggregated_metrics_collection() -> (
    AggregatedMetricsCollection
):
    time_periods = [
        MetricTimePeriodConfig.month_periods(lookback_months=24),
    ]

    return AggregatedMetricsCollection(
        output_dataset_id=IMPACT_REPORTS_DATASET_ID,
        collection_tag="compliance",
        population_configs={
            MetricPopulationType.SUPERVISION: AggregatedMetricsCollectionPopulationConfig(
                output_dataset_id=IMPACT_REPORTS_DATASET_ID,
                population_type=MetricPopulationType.SUPERVISION,
                units_of_analysis={
                    MetricUnitOfAnalysisType.STATE_CODE,
                },
                metrics=[
                    AVG_DAILY_POPULATION,
                    AVG_DAILY_POPULATION_ASSESSMENT_REQUIRED,
                    AVG_DAILY_POPULATION_ASSESSMENT_OVERDUE,
                    CONTACT_DUE_DATES,
                    CONTACT_DUE_DATES_MET,
                ],
            ),
        },
        time_periods=time_periods,
        disaggregate_by_observation_attributes=None,
    )


def get_aggregated_metrics_collections() -> list[AggregatedMetricsCollection]:
    return [
        _build_impact_reports_usage_aggregated_metrics_collection(),
        _build_impact_reports_impact_funnel_aggregated_metrics_collection(),
        _build_impact_reports_impact_funnel_exclusive_aggregated_metrics_collection(),
        _build_impact_reports_report_metrics_aggregated_metrics_collection(),
        _build_impact_reports_compliance_aggregated_metrics_collection(),
    ]


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for collection in get_aggregated_metrics_collections():
            for vb in collect_aggregated_metric_view_builders_for_collection(
                collection
            ):
                vb.build_and_print()
