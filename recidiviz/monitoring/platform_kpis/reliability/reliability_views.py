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
"""View related to platform reliability KPIs"""

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.monitoring.platform_kpis.reliability.stale_metric_export_spans_by_state import (
    STALE_METRIC_EXPORT_SPANS_BY_STATE_VIEW_BUILDER,
)
from recidiviz.monitoring.platform_kpis.reliability.stale_metric_exports import (
    STALE_METRIC_EXPORTS_VIEW_BUILDER,
)
from recidiviz.monitoring.platform_kpis.reliability.stale_metric_exports_by_state import (
    STALE_METRIC_EXPORTS_BY_STATE_VIEW_BUILDER,
)
from recidiviz.monitoring.platform_kpis.reliability.validation_distinct_hard_failures import (
    VALIDATION_DISTINCT_HARD_FAILURES_VIEW_BUILDER,
)
from recidiviz.monitoring.platform_kpis.reliability.validation_hard_failure_spans import (
    VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER,
)
from recidiviz.monitoring.platform_kpis.reliability.validation_hard_failure_time_to_resolution import (
    VALIDATION_HARD_FAILURE_TIME_TO_RESOLUTION_VIEW_BUILDER,
)
from recidiviz.monitoring.platform_kpis.reliability.validation_ongoing_hard_failures import (
    VALIDATION_ONGOING_HARD_FAILURES_VIEW_BUILDER,
)


def get_platform_reliability_kpi_views_to_update() -> list[BigQueryViewBuilder]:
    return [
        STALE_METRIC_EXPORT_SPANS_BY_STATE_VIEW_BUILDER,
        STALE_METRIC_EXPORTS_BY_STATE_VIEW_BUILDER,
        STALE_METRIC_EXPORTS_VIEW_BUILDER,
        VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER,
        VALIDATION_HARD_FAILURE_TIME_TO_RESOLUTION_VIEW_BUILDER,
        VALIDATION_DISTINCT_HARD_FAILURES_VIEW_BUILDER,
        VALIDATION_ONGOING_HARD_FAILURES_VIEW_BUILDER,
    ]
