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
"""Aggregated metrics view configuration."""

from typing import Sequence

from recidiviz.aggregated_metrics.aggregated_metrics_view_collector import (
    collect_aggregated_metric_view_builders_for_collection,
    collect_assignments_by_time_period_builders_for_collections,
)
from recidiviz.aggregated_metrics.assignment_sessions_view_collector import (
    collect_assignment_sessions_view_builders,
)
from recidiviz.aggregated_metrics.legacy.collect_standard_aggregated_metric_views import (
    collect_standard_legacy_aggregated_metric_views,
)
from recidiviz.aggregated_metrics.legacy.metric_time_periods import (
    METRIC_TIME_PERIODS_VIEW_BUILDER,
)
from recidiviz.aggregated_metrics.standard_aggregated_metrics_collection_config import (
    STANDARD_COLLECTION_CONFIG,
)
from recidiviz.aggregated_metrics.supervision_officer_caseload_count_spans import (
    SUPERVISION_OFFICER_CASELOAD_COUNT_SPANS_VIEW_BUILDER,
)
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.outliers.outliers_views import (
    OUTLIERS_AGGREGATED_METRICS_COLLECTION_CONFIG,
)


def get_aggregated_metrics_view_builders() -> Sequence[BigQueryViewBuilder]:
    """
    Returns a list of builders for all views related to aggregated metrics
    """
    return [
        METRIC_TIME_PERIODS_VIEW_BUILDER,
        SUPERVISION_OFFICER_CASELOAD_COUNT_SPANS_VIEW_BUILDER,
        # TODO(#35895), TODO(#35897), TODO(#35898), TODO(#35913): Remove these builders
        #  entirely once metrics are fully covered by the new optimized metrics.
        *collect_standard_legacy_aggregated_metric_views(),
        *collect_assignment_sessions_view_builders(),
        *collect_assignments_by_time_period_builders_for_collections(
            [STANDARD_COLLECTION_CONFIG, OUTLIERS_AGGREGATED_METRICS_COLLECTION_CONFIG]
        ),
        *collect_aggregated_metric_view_builders_for_collection(
            STANDARD_COLLECTION_CONFIG
        ),
    ]
