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
Spans of time over which a primary insights user has some set of
surfaceable officer outliers and has completed some set of usage events
in that month
"""

from recidiviz.calculator.query.state.views.analyst_data.insights_user_impact_funnel_status_sessions import (
    INSIGHTS_USAGE_EVENTS,
    INSIGHTS_USER_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER,
)
from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = """Spans of time over which a primary insights user has some set of
surfaceable officer outliers and has completed some set of usage events
in that month"""


VIEW_BUILDER: SpanObservationBigQueryViewBuilder = SpanObservationBigQueryViewBuilder(
    span_type=SpanType.INSIGHTS_PRIMARY_USER_IMPACT_FUNNEL_STATUS_SESSION,
    description=_VIEW_DESCRIPTION,
    sql_source=INSIGHTS_USER_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER.table_for_query,
    attribute_cols=[
        "has_outlier_officers",
        "num_outlier_officers",
        *[event_type.lower() for event_type in INSIGHTS_USAGE_EVENTS],
    ],
    span_start_date_col="start_date",
    span_end_date_col="end_date_exclusive",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
