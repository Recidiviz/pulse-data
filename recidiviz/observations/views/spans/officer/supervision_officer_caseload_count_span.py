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
"""View with spans of time over which an officer had a given number of clients on
their caseload
"""

from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Spans of time over which an officer had a given number of clients on their caseload"
_SOURCE_DATA_QUERY_TEMPLATE = """
SELECT 
    state_code,
    officer_id,
    start_date,
    end_date_exclusive,
    active_caseload_count,
    active_caseload_count >= 10 AS active_caseload_count_above_critical_threshold,
    total_caseload_count
FROM `{project_id}.sessions.supervision_officer_caseload_count_spans_materialized`
"""

VIEW_BUILDER: SpanObservationBigQueryViewBuilder = SpanObservationBigQueryViewBuilder(
    span_type=SpanType.SUPERVISION_OFFICER_CASELOAD_COUNT_SPAN,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "active_caseload_count",
        "active_caseload_count_above_critical_threshold",
        "total_caseload_count",
    ],
    span_start_date_col="start_date",
    span_end_date_col="end_date_exclusive",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
