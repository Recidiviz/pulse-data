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

from recidiviz.aggregated_metrics.assignment_sessions_view_collector import (
    collect_assignment_sessions_view_builders,
)
from recidiviz.aggregated_metrics.legacy.collect_standard_aggregated_metric_views import (
    collect_standard_legacy_aggregated_metric_views,
)
from recidiviz.aggregated_metrics.legacy.metric_time_periods import (
    METRIC_TIME_PERIODS_VIEW_BUILDER,
)
from recidiviz.aggregated_metrics.supervision_officer_caseload_count_spans import (
    SUPERVISION_OFFICER_CASELOAD_COUNT_SPANS_VIEW_BUILDER,
)
from recidiviz.big_query.big_query_view import BigQueryViewBuilder


def get_aggregated_metrics_view_builders() -> Sequence[BigQueryViewBuilder]:
    """
    Returns a list of builders for all views related to aggregated metrics
    """
    return [
        METRIC_TIME_PERIODS_VIEW_BUILDER,
        SUPERVISION_OFFICER_CASELOAD_COUNT_SPANS_VIEW_BUILDER,
        *collect_standard_legacy_aggregated_metric_views(),
        *collect_assignment_sessions_view_builders(),
    ]
