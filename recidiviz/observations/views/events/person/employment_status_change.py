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
"""View with employment status changes"""
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Employment status changes"

_SOURCE_DATA_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    is_employed,
    employment_status_start_date
FROM `{project_id}.sessions.supervision_employment_status_sessions_materialized`
QUALIFY
    # only keep transitions where the person gained or lost employment
    # edge case: treat jobs at supervision start as employment gains
    # but no job at supervision start is not an employment loss
    is_employed != IFNULL(LAG(is_employed) OVER (
        PARTITION BY person_id ORDER BY employment_status_start_date
    ), FALSE)
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.EMPLOYMENT_STATUS_CHANGE,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=["is_employed"],
    event_date_col="employment_status_start_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
