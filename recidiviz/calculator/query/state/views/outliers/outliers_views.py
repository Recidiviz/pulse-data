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

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
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
from recidiviz.calculator.query.state.views.outliers.supervision_officer_metrics import (
    SUPERVISION_OFFICER_METRICS_VIEW_BUILDER,
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

OUTLIERS_ARCHIVE_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    SUPERVISION_OFFICER_OUTLIER_STATUS_ARCHIVE_VIEW_BUILDER,
    SUPERVISION_OFFICER_SUPERVISORS_ARCHIVE_VIEW_BUILDER,
    SUPERVISION_OFFICERS_ARCHIVE_VIEW_BUILDER,
]

OUTLIERS_VIEW_BUILDERS_TO_EXPORT: List[BigQueryViewBuilder] = [
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

OUTLIERS_VIEW_BUILDERS: List[BigQueryViewBuilder] = (
    OUTLIERS_VIEW_BUILDERS_TO_EXPORT + OUTLIERS_ARCHIVE_VIEW_BUILDERS
)
