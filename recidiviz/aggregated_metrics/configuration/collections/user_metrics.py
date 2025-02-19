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
"""Aggregated metrics collection definition for aggregated metrics for user reports"""
import datetime

from recidiviz.aggregated_metrics.aggregated_metric_collection_config import (
    AggregatedMetricsCollection,
    AggregatedMetricsCollectionPopulationConfig,
)
from recidiviz.aggregated_metrics.aggregated_metrics_view_collector import (
    collect_aggregated_metric_view_builders_for_collection,
)
from recidiviz.aggregated_metrics.impact_reports_aggregated_metrics_configurations import (
    DISTINCT_REGISTERED_USERS_SUPERVISION,
)
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriod,
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    WORKFLOWS_DISTINCT_PEOPLE_ELIGIBLE_AND_ACTIONABLE,
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


def build_user_metrics_aggregated_metrics_collection_config() -> AggregatedMetricsCollection:

    # Start the first user report on Jan 2024, so set the first monthly end date as Feb 1, 2024
    min_end_date = datetime.date(2024, 2, 1)
    max_end_date = first_day_of_month(current_date_us_eastern())

    return AggregatedMetricsCollection(
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
                    WORKFLOWS_DISTINCT_PEOPLE_ELIGIBLE_AND_ACTIONABLE,
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
    )


USER_METRICS_AGGREGATED_METRICS_COLLECTION_CONFIG = (
    build_user_metrics_aggregated_metrics_collection_config()
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for vb in collect_aggregated_metric_view_builders_for_collection(
            USER_METRICS_AGGREGATED_METRICS_COLLECTION_CONFIG
        ):
            vb.build_and_print()
