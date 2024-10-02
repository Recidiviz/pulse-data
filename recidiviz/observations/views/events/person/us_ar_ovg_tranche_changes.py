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
"""View with US_AR OVG Points Tranche Changes"""
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "US_AR OVG Points Tranche Changes"

_SOURCE_DATA_QUERY_TEMPLATE = """
-- TODO(#31020): Revisit moving some of this information into upstream ingest / creating a state agnostic view
SELECT
    state_code,
    person_id,
    start_date,
    points,
    tranche,
    IF(tranche > previous_tranche, "INCREASE", "DECREASE") AS change_type
FROM (
    SELECT s.*, 
            s_lag.tranche AS previous_tranche,
    FROM `{project_id}.analyst_data.us_ar_ovg_timeline_materialized` s
    LEFT JOIN `{project_id}.analyst_data.us_ar_ovg_timeline_materialized` s_lag
        ON s.person_id = s_lag.person_id
        AND s.start_date = s_lag.end_date
)
WHERE tranche != previous_tranche
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.US_AR_OVG_TRANCHE_CHANGES,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=["points", "tranche", "change_type"],
    event_date_col="start_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
