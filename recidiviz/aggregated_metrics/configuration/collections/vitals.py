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
"""Aggregated metrics collection definition for aggregated metrics used in Vitals"""

from dateutil.relativedelta import relativedelta
from more_itertools import one

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
    AVG_DAILY_POPULATION,
    AVG_DAILY_POPULATION_ASSESSMENT_OVERDUE,
    AVG_DAILY_POPULATION_ASSESSMENT_REQUIRED,
    AVG_DAILY_POPULATION_CONTACT_OVERDUE,
    AVG_DAILY_POPULATION_CONTACT_REQUIRED,
    AVG_DAILY_POPULATION_PAST_FULL_TERM_RELEASE_DATE_SUPERVISION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.calculator.query.state.dataset_config import VITALS_REPORT_DATASET
from recidiviz.common.date import current_date_us_eastern
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VITALS_METRICS_LOOKBACK_DAYS = 180


def build_vitals_aggregated_metrics_collection_config() -> AggregatedMetricsCollection:
    current_date = current_date_us_eastern()
    sld_metric = one(
        metric
        for metric in AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION
        if metric.name == "avg_population_task_eligible_supervision_level_downgrade"
    )
    min_period_end_date = current_date - relativedelta(
        days=VITALS_METRICS_LOOKBACK_DAYS
    )
    return AggregatedMetricsCollection(
        output_dataset_id=VITALS_REPORT_DATASET,
        population_configs={
            MetricPopulationType.SUPERVISION: AggregatedMetricsCollectionPopulationConfig(
                output_dataset_id=VITALS_REPORT_DATASET,
                population_type=MetricPopulationType.SUPERVISION,
                units_of_analysis={
                    MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
                    MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
                    MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
                    MetricUnitOfAnalysisType.STATE_CODE,
                },
                metrics=[
                    AVG_DAILY_POPULATION,
                    AVG_DAILY_POPULATION_ASSESSMENT_REQUIRED,
                    AVG_DAILY_POPULATION_ASSESSMENT_OVERDUE,
                    AVG_DAILY_POPULATION_CONTACT_REQUIRED,
                    AVG_DAILY_POPULATION_CONTACT_OVERDUE,
                    AVG_DAILY_POPULATION_PAST_FULL_TERM_RELEASE_DATE_SUPERVISION,
                    sld_metric,
                ],
            )
        },
        time_periods=[
            MetricTimePeriodConfig.day_periods(
                lookback_days=VITALS_METRICS_LOOKBACK_DAYS
            ),
            # Config for 30 day rate periods
            MetricTimePeriodConfig(
                interval_unit=MetricTimePeriod.DAY,
                # How long to calculate the averages over. For example, if
                # time_interval_length_in_days is 30, we calculate a 30 day rate. If
                # time_interval_length_in_days is 1, we have a point-in-time count.
                interval_length=30,
                min_period_end_date=min_period_end_date,
                max_period_end_date=current_date,
                # How often to calculate metrics. Since the rolling period is set to 1 DAY, we
                # calculate values for each day in the time period.
                rolling_period_unit=MetricTimePeriod.DAY,
                rolling_period_length=1,
                period_name=MetricTimePeriod.MONTH.value,
                config_name=f"30_days_rolling_last_{VITALS_METRICS_LOOKBACK_DAYS}_days",
                description=(
                    f"30 day-long metric periods, with one ending on every day for the "
                    f"last {VITALS_METRICS_LOOKBACK_DAYS} days"
                ),
            ),
        ],
        disaggregate_by_observation_attributes=None,
    )


VITALS_AGGREGATED_METRICS_COLLECTION_CONFIG = (
    build_vitals_aggregated_metrics_collection_config()
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for vb in collect_aggregated_metric_view_builders_for_collection(
            VITALS_AGGREGATED_METRICS_COLLECTION_CONFIG
        ):
            vb.build_and_print()
