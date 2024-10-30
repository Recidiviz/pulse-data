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
"""View with releases from incarceration or supervision to liberty."""
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.views.events.person.incarceration_release import (
    VIEW_BUILDER as INCARCERATION_RELEASE_VIEW_BUILDER,
)
from recidiviz.observations.views.events.person.supervision_release import (
    VIEW_BUILDER as SUPERVISION_RELEASE_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#34511): Figure out how to consolidate TRANSITIONS_TO_LIBERTY_ALL and
#  TRANSITIONS_TO_LIBERTY_FROM_IN_STATE into a single event type.
_VIEW_DESCRIPTION = (
    "View capturing releases from incarceration or supervision to liberty."
)

# TODO(#34498): Remove all JSON_EXTRACT_SCALAR from this query once single-event views
#  do not package attributes into JSON.
_SOURCE_DATA_QUERY_TEMPLATE = f"""
WITH combined_releases AS (
    SELECT
        state_code,
        person_id,
        event_date,
        JSON_EXTRACT_SCALAR(event_attributes, "$.inflow_from_level_1") AS inflow_from_level_1,
        JSON_EXTRACT_SCALAR(event_attributes, "$.days_sentenced") AS days_sentenced,
        JSON_EXTRACT_SCALAR(event_attributes, "$.days_served") AS days_served,
        JSON_EXTRACT_SCALAR(event_attributes, "$.prop_sentence_served") AS prop_sentence_served,
    FROM `{{project_id}}.{INCARCERATION_RELEASE_VIEW_BUILDER.table_for_query.to_str()}`
    WHERE JSON_EXTRACT_SCALAR(event_attributes, "$.outflow_to_level_1") = "LIBERTY"
    
    UNION ALL
    
    SELECT
        state_code,
        person_id,
        event_date,
        JSON_EXTRACT_SCALAR(event_attributes, "$.inflow_from_level_1") AS inflow_from_level_1,
        JSON_EXTRACT_SCALAR(event_attributes, "$.days_sentenced") AS days_sentenced,
        JSON_EXTRACT_SCALAR(event_attributes, "$.days_served") AS days_served,
        JSON_EXTRACT_SCALAR(event_attributes, "$.prop_sentence_served") AS prop_sentence_served,
    FROM `{{project_id}}.{SUPERVISION_RELEASE_VIEW_BUILDER.table_for_query.to_str()}`
    WHERE JSON_EXTRACT_SCALAR(event_attributes, "$.outflow_to_level_1") = "LIBERTY"
)
SELECT
    state_code,
    person_id,
    event_date,
    inflow_from_level_1,
    days_sentenced,
    days_served,
    prop_sentence_served,
FROM combined_releases
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.TRANSITIONS_TO_LIBERTY_FROM_IN_STATE,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "inflow_from_level_1",
        "days_sentenced",
        "days_served",
        "prop_sentence_served",
    ],
    event_date_col="event_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
