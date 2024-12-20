# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""
View representing spans of time during which a supervision officer was surfaceable
in the Insights (Supervisor Homepage) tool as being an outlier in a given month
for a given metric.
"""

from recidiviz.calculator.query.state.views.analyst_data.insights_supervision_officer_outlier_status_archive_sessions import (
    INSIGHTS_SUPERVISION_OFFICER_OUTLIER_STATUS_ARCHIVE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = """
View representing spans of time during which a supervision officer was surfaceable
in the Insights (Supervisor Homepage) tool as being an outlier in a given month
for a given metric.
"""

VIEW_BUILDER: SpanObservationBigQueryViewBuilder = SpanObservationBigQueryViewBuilder(
    span_type=SpanType.INSIGHTS_SUPERVISION_OFFICER_OUTLIER_STATUS_SESSION,
    description=_VIEW_DESCRIPTION,
    sql_source=INSIGHTS_SUPERVISION_OFFICER_OUTLIER_STATUS_ARCHIVE_SESSIONS_VIEW_BUILDER.table_for_query,
    attribute_cols=["metric_id", "is_surfaceable_outlier"],
    span_start_date_col="start_date",
    span_end_date_col="end_date_exclusive",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
