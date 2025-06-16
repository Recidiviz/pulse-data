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
"""Aggregated metrics collection definition for aggregated metrics used in the Insights
Outcomes module.
"""
from typing import Any

from recidiviz.aggregated_metrics.aggregated_metric_collection_config import (
    AggregatedMetricsCollection,
)
from recidiviz.aggregated_metrics.aggregated_metrics_view_collector import (
    collect_aggregated_metric_view_builders_for_collection,
)
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_DAILY_POPULATION,
    AVG_DAILY_POPULATION_PAROLE,
    AVG_DAILY_POPULATION_PROBATION,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.outliers.aggregated_metrics_collector import (
    OutliersAggregatedMetricsCollector,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_METRICS_BY_POPULATION_TYPE: dict[MetricPopulationType, list[AggregatedMetric[Any]]] = {
    MetricPopulationType.SUPERVISION: [
        AVG_DAILY_POPULATION,
        AVG_DAILY_POPULATION_PAROLE,
        AVG_DAILY_POPULATION_PROBATION,
        *OutliersAggregatedMetricsCollector.get_metrics(),
    ]
}

_UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE: dict[
    MetricPopulationType, list[MetricUnitOfAnalysisType]
] = {
    MetricPopulationType.SUPERVISION: [
        MetricUnitOfAnalysisType.INSIGHTS_CASELOAD_CATEGORY
    ]
}

_OUTLIERS_METRICS_YEARS_TRACKED = 7


OUTLIERS_AGGREGATED_METRICS_COLLECTION_CONFIG = AggregatedMetricsCollection.build(
    output_dataset_id=OUTLIERS_VIEWS_DATASET,
    time_periods=[
        MetricTimePeriodConfig.year_periods_rolling_monthly(
            lookback_months=_OUTLIERS_METRICS_YEARS_TRACKED * 12
        ),
        MetricTimePeriodConfig.quarter_periods_rolling_monthly(
            lookback_months=_OUTLIERS_METRICS_YEARS_TRACKED * 12
        ),
        MetricTimePeriodConfig.month_periods(
            lookback_months=_OUTLIERS_METRICS_YEARS_TRACKED * 12
        ),
    ],
    unit_of_analysis_types_by_population_type=_UNIT_OF_ANALYSIS_TYPES_BY_POPULATION_TYPE,
    metrics_by_population_type=_METRICS_BY_POPULATION_TYPE,
    disaggregate_by_observation_attributes=None,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for vb in collect_aggregated_metric_view_builders_for_collection(
            OUTLIERS_AGGREGATED_METRICS_COLLECTION_CONFIG
        ):
            vb.build_and_print()
