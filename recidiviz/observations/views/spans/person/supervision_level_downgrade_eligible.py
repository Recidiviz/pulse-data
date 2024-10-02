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
"""View with open supervision mismatch (downgrades only), ends when mismatch corrected
or supervision period ends.
"""

from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = (
    "Open supervision mismatch (downgrades only), ends when mismatch "
    "corrected or supervision period ends"
)

_SOURCE_DATA_QUERY_TEMPLATE = """
SELECT 
    state_code,
    person_id,
    "SUPERVISION_DOWNGRADE" AS task_name,
    mismatch_corrected,
    recommended_supervision_downgrade_level,
    start_date,
    DATE_ADD(end_date, INTERVAL 1 DAY) AS end_date,
FROM
    `{project_id}.sessions.supervision_downgrade_sessions_materialized`
WHERE
    recommended_supervision_downgrade_level IS NOT NULL
"""

VIEW_BUILDER: SpanObservationBigQueryViewBuilder = SpanObservationBigQueryViewBuilder(
    span_type=SpanType.SUPERVISION_LEVEL_DOWNGRADE_ELIGIBLE,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "task_name",
        "mismatch_corrected",
        "recommended_supervision_downgrade_level",
    ],
    span_start_date_col="start_date",
    span_end_date_col="end_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
