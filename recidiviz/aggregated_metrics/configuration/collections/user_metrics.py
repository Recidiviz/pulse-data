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
"""Aggregated metrics collection definition for aggregated metrics, separated by 
supervision linestaff, facilities linestaff, and supervision supervisors. These metrics 
are used for user reports and information about users' eligible clients for email 
reminders"""

import datetime

from recidiviz.aggregated_metrics.aggregated_metric_collection_config import (
    AggregatedMetricsCollection,
    AggregatedMetricsCollectionPopulationConfig,
)
from recidiviz.aggregated_metrics.aggregated_metrics_view_collector import (
    collect_aggregated_metric_view_builders_for_collection,
)
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriod,
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_30_DAYS_METRICS_SUPERVISION,
    DISTINCT_OUTLIER_OFFICERS,
    DISTINCT_POPULATION_WORKFLOWS_ALMOST_ELIGIBLE_AND_ACTIONABLE,
    DISTINCT_POPULATION_WORKFLOWS_ALMOST_ELIGIBLE_AND_ACTIONABLE_METRICS_SUPERVISION,
    DISTINCT_POPULATION_WORKFLOWS_ELIGIBLE_AND_ACTIONABLE,
    DISTINCT_POPULATION_WORKFLOWS_ELIGIBLE_AND_ACTIONABLE_METRICS_SUPERVISION,
    DISTINCT_PROVISIONED_INSIGHTS_USERS,
    DISTINCT_REGISTERED_USERS_SUPERVISION,
    WORKFLOWS_PRIMARY_USER_ACTIVE_USAGE_EVENTS,
    WORKFLOWS_PRIMARY_USER_LOGINS,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.calculator.query.state.dataset_config import USER_METRICS_DATASET_ID
from recidiviz.common.date import current_date_us_eastern, first_day_of_month
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def _build_workflows_supervision_user_metrics_aggregated_metrics_collection_config() -> AggregatedMetricsCollection:

    current_date = current_date_us_eastern()

    # Start the first user report on Jan 2024, so set the first monthly end date as Feb 1, 2024
    min_end_date = datetime.date(2024, 2, 1)
    max_end_date = first_day_of_month(current_date)

    return AggregatedMetricsCollection(
        collection_tag="workflows",
        output_dataset_id=USER_METRICS_DATASET_ID,
        population_configs={
            MetricPopulationType.SUPERVISION: AggregatedMetricsCollectionPopulationConfig(
                output_dataset_id=USER_METRICS_DATASET_ID,
                population_type=MetricPopulationType.SUPERVISION,
                units_of_analysis={
                    MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
                },
                metrics=[
                    DISTINCT_REGISTERED_USERS_SUPERVISION,
                    WORKFLOWS_PRIMARY_USER_ACTIVE_USAGE_EVENTS,
                    WORKFLOWS_PRIMARY_USER_LOGINS,
                    DISTINCT_POPULATION_WORKFLOWS_ELIGIBLE_AND_ACTIONABLE,
                    *DISTINCT_POPULATION_WORKFLOWS_ELIGIBLE_AND_ACTIONABLE_METRICS_SUPERVISION,
                    DISTINCT_POPULATION_WORKFLOWS_ALMOST_ELIGIBLE_AND_ACTIONABLE,
                    *DISTINCT_POPULATION_WORKFLOWS_ALMOST_ELIGIBLE_AND_ACTIONABLE_METRICS_SUPERVISION,
                    *AVG_DAILY_POPULATION_TASK_ELIGIBLE_AND_UNVIEWED_30_DAYS_METRICS_SUPERVISION,
                    # Used for supervisors who supervise clients to distinguish
                    # the users who do not have access to Insights yet
                    DISTINCT_PROVISIONED_INSIGHTS_USERS,
                ],
            ),
        },
        time_periods=[
            # Last day of month metrics
            MetricTimePeriodConfig(
                interval_unit=MetricTimePeriod.DAY,
                interval_length=1,
                min_period_end_date=min_end_date,
                max_period_end_date=max_end_date,
                rolling_period_unit=MetricTimePeriod.MONTH,
                rolling_period_length=1,
                description=f"Daily metric periods for the last day of the month starting {min_end_date}",
                config_name="end_of_month_starting_jan_2024",
                period_name="DAY",
            ),
            # Current day metrics
            MetricTimePeriodConfig(
                interval_unit=MetricTimePeriod.DAY,
                interval_length=1,
                # Set the end date to tomorrow since periods are end-date exclusive
                min_period_end_date=current_date + datetime.timedelta(days=1),
                max_period_end_date=current_date + datetime.timedelta(days=1),
                rolling_period_unit=None,
                rolling_period_length=None,
                description="Single day metric periods for the current day",
                config_name="day_current_date",
                period_name="CURRENT_DAY",
            ),
            # Monthly metrics
            MetricTimePeriodConfig(
                interval_unit=MetricTimePeriod.MONTH,
                interval_length=1,
                min_period_end_date=min_end_date,
                max_period_end_date=max_end_date,
                rolling_period_length=None,
                rolling_period_unit=None,
                description=f"Monthly metric periods starting {min_end_date}",
                config_name="monthly_starting_jan_2024",
                period_name="MONTH",
            ),
        ],
        disaggregate_by_observation_attributes=None,
    )


def _build_insights_user_metrics_aggregated_metrics_collection_config() -> (
    AggregatedMetricsCollection
):

    current_date = current_date_us_eastern()

    return AggregatedMetricsCollection(
        collection_tag="insights",
        output_dataset_id=USER_METRICS_DATASET_ID,
        population_configs={
            MetricPopulationType.SUPERVISION: AggregatedMetricsCollectionPopulationConfig(
                output_dataset_id=USER_METRICS_DATASET_ID,
                population_type=MetricPopulationType.SUPERVISION,
                units_of_analysis={
                    MetricUnitOfAnalysisType.SUPERVISION_UNIT,
                },
                metrics=[
                    DISTINCT_OUTLIER_OFFICERS,
                    DISTINCT_POPULATION_WORKFLOWS_ELIGIBLE_AND_ACTIONABLE,
                    DISTINCT_POPULATION_WORKFLOWS_ALMOST_ELIGIBLE_AND_ACTIONABLE,
                ],
            ),
        },
        time_periods=[
            # Current day metrics
            MetricTimePeriodConfig(
                interval_unit=MetricTimePeriod.DAY,
                interval_length=1,
                # Set the end date to tomorrow since periods are end-date exclusive
                min_period_end_date=current_date + datetime.timedelta(days=1),
                max_period_end_date=current_date + datetime.timedelta(days=1),
                rolling_period_unit=None,
                rolling_period_length=None,
                description="Single day metric periods for the current day",
                config_name="day_current_date",
                period_name="CURRENT_DAY",
            ),
        ],
        disaggregate_by_observation_attributes=None,
    )


def _build_workflows_facilities_user_metrics_aggregated_metrics_collection_config() -> AggregatedMetricsCollection:

    current_date = current_date_us_eastern()

    return AggregatedMetricsCollection(
        collection_tag="workflows",
        output_dataset_id=USER_METRICS_DATASET_ID,
        population_configs={
            MetricPopulationType.INCARCERATION: AggregatedMetricsCollectionPopulationConfig(
                output_dataset_id=USER_METRICS_DATASET_ID,
                population_type=MetricPopulationType.INCARCERATION,
                units_of_analysis={
                    MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
                },
                metrics=[
                    DISTINCT_POPULATION_WORKFLOWS_ELIGIBLE_AND_ACTIONABLE,
                    DISTINCT_POPULATION_WORKFLOWS_ALMOST_ELIGIBLE_AND_ACTIONABLE,
                ],
            ),
        },
        time_periods=[
            # Current day metrics
            MetricTimePeriodConfig(
                interval_unit=MetricTimePeriod.DAY,
                interval_length=1,
                # Set the end date to tomorrow since periods are end-date exclusive
                min_period_end_date=current_date + datetime.timedelta(days=1),
                max_period_end_date=current_date + datetime.timedelta(days=1),
                rolling_period_unit=None,
                rolling_period_length=None,
                description="Single day metric periods for the current day",
                config_name="day_current_date",
                period_name="CURRENT_DAY",
            ),
        ],
        disaggregate_by_observation_attributes=None,
    )


def get_aggregated_metrics_collections() -> list[AggregatedMetricsCollection]:
    return [
        _build_workflows_supervision_user_metrics_aggregated_metrics_collection_config(),
        _build_insights_user_metrics_aggregated_metrics_collection_config(),
        _build_workflows_facilities_user_metrics_aggregated_metrics_collection_config(),
    ]


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for collection in get_aggregated_metrics_collections():
            for vb in collect_aggregated_metric_view_builders_for_collection(
                collection
            ):
                vb.build_and_print()
