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
"""View with transitions to solitary confinement"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.task_eligibility.utils.general_criteria_builders import (
    num_events_within_time_interval_spans,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Transitions to solitary confinement"

_SOURCE_DATA_QUERY_TEMPLATE = f"""
WITH solitary_starts AS (
    SELECT
        state_code,
        person_id,
        start_date,
        DATE_DIFF({nonnull_end_date_exclusive_clause('end_date_exclusive')}, start_date, DAY) AS length_of_stay,
        end_date_exclusive IS NOT NULL AS is_active,
        start_date AS event_date
    FROM `{{project_id}}.sessions.housing_unit_type_collapsed_solitary_sessions_materialized`
    WHERE housing_unit_type_collapsed_solitary = "SOLITARY_CONFINEMENT"
)
,
{num_events_within_time_interval_spans(
    events_cte="solitary_starts",
    date_interval=1,
    date_part="YEAR"
)}
SELECT
    solitary_starts.state_code,
    solitary_starts.person_id,
    solitary_starts.start_date,
    solitary_starts.length_of_stay,
    solitary_starts.is_active,
    event_count_spans.event_count AS solitary_starts_in_last_year
FROM solitary_starts
LEFT JOIN event_count_spans
ON
    solitary_starts.state_code = event_count_spans.state_code
    AND solitary_starts.person_id = event_count_spans.person_id
    AND solitary_starts.start_date = event_count_spans.start_date
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.SOLITARY_CONFINEMENT_START,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "solitary_starts_in_last_year",
        "length_of_stay",
        "is_active",
    ],
    event_date_col="start_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
