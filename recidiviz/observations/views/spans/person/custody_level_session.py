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
"""View with non-overlapping spans of time over which a person has a certain custody
level
"""

from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = (
    "Non-overlapping spans of time over which a person has a certain custody level"
)

_SOURCE_DATA_QUERY_TEMPLATE = """
SELECT
    person_id,
    state_code,
    custody_level,
    start_date,
    end_date_exclusive
FROM
    `{project_id}.sessions.custody_level_sessions_materialized`
WHERE
    custody_level IS NOT NULL
"""

VIEW_BUILDER: SpanObservationBigQueryViewBuilder = SpanObservationBigQueryViewBuilder(
    span_type=SpanType.CUSTODY_LEVEL_SESSION,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=["custody_level"],
    span_start_date_col="start_date",
    span_end_date_col="end_date_exclusive",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
