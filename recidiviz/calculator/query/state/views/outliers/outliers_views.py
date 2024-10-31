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
"""All Outliers views."""
from typing import List

from recidiviz.aggregated_metrics.aggregated_metric_view_collector import (
    collect_aggregated_metrics_view_builders,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_DAILY_POPULATION,
)
from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.calculator.query.state.views.outliers.metric_benchmarks import (
    METRIC_BENCHMARKS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_client_events import (
    SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_clients import (
    SUPERVISION_CLIENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_district_managers import (
    SUPERVISION_DISTRICT_MANAGERS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_impact_metrics_outlier_officers import (
    SUPERVISION_IMPACT_METRICS_OUTLIER_OFFICERS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_impact_metrics_supervisors import (
    SUPERVISION_IMPACT_METRICS_SUPERVISORS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_metrics import (
    SUPERVISION_OFFICER_METRICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_metrics_archive import (
    SUPERVISION_OFFICER_METRICS_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_outlier_status import (
    SUPERVISION_OFFICER_OUTLIER_STATUS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_outlier_status_archive import (
    SUPERVISION_OFFICER_OUTLIER_STATUS_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_supervisors import (
    SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_supervisors_archive import (
    SUPERVISION_OFFICER_SUPERVISORS_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officers import (
    SUPERVISION_OFFICERS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officers_archive import (
    SUPERVISION_OFFICERS_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_state_metrics import (
    SUPERVISION_STATE_METRICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_usage_metrics import (
    SUPERVISION_USAGE_METRICS_VIEW_BUILDER,
)
from recidiviz.outliers.aggregated_metrics_collector import AggregatedMetricsCollector

OUTLIERS_ARCHIVE_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    SUPERVISION_OFFICER_METRICS_ARCHIVE_VIEW_BUILDER,
    SUPERVISION_OFFICER_OUTLIER_STATUS_ARCHIVE_VIEW_BUILDER,
    SUPERVISION_OFFICER_SUPERVISORS_ARCHIVE_VIEW_BUILDER,
    SUPERVISION_OFFICERS_ARCHIVE_VIEW_BUILDER,
]

INSIGHTS_VIEW_BUILDERS_TO_EXPORT: List[BigQueryViewBuilder] = [
    METRIC_BENCHMARKS_VIEW_BUILDER,
    SUPERVISION_DISTRICT_MANAGERS_VIEW_BUILDER,
    SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER,
    SUPERVISION_CLIENTS_VIEW_BUILDER,
    SUPERVISION_OFFICER_METRICS_VIEW_BUILDER,
    SUPERVISION_OFFICER_OUTLIER_STATUS_VIEW_BUILDER,
    SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER,
    SUPERVISION_OFFICERS_VIEW_BUILDER,
    SUPERVISION_STATE_METRICS_VIEW_BUILDER,
]

OUTLIERS_IMPACT_VIEW_BUILDERS_TO_EXPORT: List[BigQueryViewBuilder] = [
    SUPERVISION_IMPACT_METRICS_OUTLIER_OFFICERS_VIEW_BUILDER,
    SUPERVISION_IMPACT_METRICS_SUPERVISORS_VIEW_BUILDER,
    SUPERVISION_USAGE_METRICS_VIEW_BUILDER,
]

INSIGHTS_AGGREGATED_METRICS_VIEW_BUILDERS: List[
    SimpleBigQueryViewBuilder
] = collect_aggregated_metrics_view_builders(
    metrics_by_population_dict={
        MetricPopulationType.SUPERVISION: [
            AVG_DAILY_POPULATION,
            *AggregatedMetricsCollector.get_metrics(),
        ]
    },
    units_of_analysis_by_population_dict={
        MetricPopulationType.SUPERVISION: [
            MetricUnitOfAnalysisType.INSIGHTS_CASELOAD_CATEGORY
        ]
    },
    dataset_id_override=OUTLIERS_VIEWS_DATASET,
)

OUTLIERS_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    *INSIGHTS_VIEW_BUILDERS_TO_EXPORT,
    *OUTLIERS_ARCHIVE_VIEW_BUILDERS,
    *OUTLIERS_IMPACT_VIEW_BUILDERS_TO_EXPORT,
    *INSIGHTS_AGGREGATED_METRICS_VIEW_BUILDERS,
]
