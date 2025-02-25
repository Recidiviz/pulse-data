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
"""View with transitions to temporary incarceration"""
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Transitions to temporary incarceration"

_SOURCE_DATA_QUERY_TEMPLATE = """
SELECT 
    state_code,
    person_id,
    compartment_level_2,
    inflow_from_level_1,
    inflow_from_level_2,
    start_reason,
    COALESCE(start_sub_reason, "INTERNAL_UNKNOWN") AS most_severe_violation_type,
    start_date
FROM
    `{project_id}.sessions.compartment_sessions_materialized`
WHERE
    compartment_level_1 = "INCARCERATION"
    AND compartment_level_2 IN (
        "PAROLE_BOARD_HOLD", "PENDING_CUSTODY", "TEMPORARY_CUSTODY", "SUSPENSION",
        "SHOCK_INCARCERATION"
    )
    -- Exclude transitions between temporary incarceration periods
    AND (
        inflow_from_level_1 != "INCARCERATION"
        OR inflow_from_level_2 NOT IN (
            "PAROLE_BOARD_HOLD", "PENDING_CUSTODY", "TEMPORARY_CUSTODY", "SUSPENSION",
            "SHOCK_INCARCERATION"
        )
    )
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.INCARCERATION_START_TEMPORARY,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "compartment_level_2",
        "inflow_from_level_1",
        "inflow_from_level_2",
        "start_reason",
        "most_severe_violation_type",
    ],
    event_date_col="start_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
