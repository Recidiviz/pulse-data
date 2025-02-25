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
"""View with US_AR OVG Incentives"""
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "US_AR OVG Incentives"

_SOURCE_DATA_QUERY_TEMPLATE = """
-- TODO(#31020): Revisit moving some of this information into upstream ingest / creating a state agnostic view
SELECT 
    DISTINCT
    start_date, 
    person_id, 
    state_code,
    'INCENTIVE' AS reward_type,
FROM `{project_id}.analyst_data.us_ar_ovg_events_preprocessed_materialized`
WHERE event_id = 'INCENTIVE'
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.US_AR_INCENTIVES,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "reward_type",
    ],
    event_date_col="start_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
