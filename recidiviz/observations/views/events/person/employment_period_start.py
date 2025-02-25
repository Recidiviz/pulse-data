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
"""View with employment period starts, keeping at most one employer per period start"""
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = (
    "Employment period starts, keeping at most one employer per period start"
)

_SOURCE_DATA_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    employer_name,
    start_date
FROM
    `{project_id}.normalized_state.state_employment_period`
WHERE
    start_date IS NOT NULL
    AND employment_status != "UNEMPLOYED"
QUALIFY
    -- max one employer per day
    ROW_NUMBER() OVER (
        PARTITION BY person_id, start_date ORDER BY employer_name ASC
    ) = 1
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.EMPLOYMENT_PERIOD_START,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=["employer_name"],
    event_date_col="start_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
