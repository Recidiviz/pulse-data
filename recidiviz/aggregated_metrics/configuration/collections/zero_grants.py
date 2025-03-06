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
    PROP_PERIOD_WITH_CRITICAL_CASELOAD,
    TASK_COMPLETED_METRICS_SUPERVISION,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states_for_bigquery,
)
from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    WORKFLOWS_OPPORTUNITY_CONFIGS,
    PersonRecordType,
)
from recidiviz.common.date import current_date_us_eastern
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# All supervision opportunity configs in outliers enabled states
ZERO_GRANT_OPPORTUNITY_CONFIGURATIONS = [
    opp_config
    for opp_config in WORKFLOWS_OPPORTUNITY_CONFIGS
    if opp_config.state_code.value in get_outliers_enabled_states_for_bigquery()
    and opp_config.person_record_type == PersonRecordType.CLIENT
]


def build_zero_grants_aggregated_metrics_collection_config() -> AggregatedMetricsCollection:
    current_date = current_date_us_eastern()

    zg_task_completion_metrics = [
        m
        for m in TASK_COMPLETED_METRICS_SUPERVISION
        if m.name
        in [
            f"task_completions_{opp_config.task_completion_event.value.lower()}"
            for opp_config in ZERO_GRANT_OPPORTUNITY_CONFIGURATIONS
        ]
    ]

    return AggregatedMetricsCollection(
        output_dataset_id=OUTLIERS_VIEWS_DATASET,
        population_configs={
            MetricPopulationType.SUPERVISION: AggregatedMetricsCollectionPopulationConfig(
                output_dataset_id=OUTLIERS_VIEWS_DATASET,
                population_type=MetricPopulationType.SUPERVISION,
                units_of_analysis={
                    MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
                },
                metrics=[
                    PROP_PERIOD_WITH_CRITICAL_CASELOAD,
                    *zg_task_completion_metrics,
                ],
            )
        },
        time_periods=[
            # Configuration for daily year-long periods
            MetricTimePeriodConfig(
                interval_unit=MetricTimePeriod.YEAR,
                interval_length=1,
                # We only need the period ending on the current day
                min_period_end_date=current_date,
                max_period_end_date=current_date,
                rolling_period_unit=MetricTimePeriod.DAY,
                rolling_period_length=1,
                period_name=MetricTimePeriod.YEAR.value,
                config_name="years_rolling_last_1_days",
                description=("Year-long metric periods, ending on the current day"),
            ),
        ],
    )


ZERO_GRANTS_AGGREGATED_METRICS_COLLECTION_CONFIG = (
    build_zero_grants_aggregated_metrics_collection_config()
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for vb in collect_aggregated_metric_view_builders_for_collection(
            ZERO_GRANTS_AGGREGATED_METRICS_COLLECTION_CONFIG
        ):
            vb.build_and_print()
