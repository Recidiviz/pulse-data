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
"""View with releases from supervision to liberty"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Releases from supervision to liberty"

_SOURCE_DATA_QUERY_TEMPLATE = f"""
WITH supervision_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        compartment_level_2,
        inflow_from_level_1,
        inflow_from_level_2,
        outflow_to_level_1,
        -- getting proportion of sentence days
        DATE_DIFF(end_date_exclusive, start_date, DAY) AS days_served,
    FROM
        `{{project_id}}.sessions.compartment_sessions_materialized`
    WHERE
        compartment_level_1 = "SUPERVISION"
        AND outflow_to_level_1 = "LIBERTY"
)
,
sentence_deadline_spans AS (
    SELECT
        sentence_span_start.state_code,
        sentence_span_start.person_id,
        sentence_span_start.start_date,
        sentence_span_start.end_date_exclusive,
        MAX(task_deadlines.projected_supervision_release_date) AS projected_supervision_release_snapshot_date,
    FROM
        `{{project_id}}.sessions.sentence_spans_materialized` sentence_span_start,
        UNNEST(sentence_deadline_id_array) AS sentence_deadline_id
    LEFT JOIN
        `{{project_id}}.sessions.sentence_deadline_spans_materialized` task_deadlines
    USING
        (person_id, state_code, sentence_deadline_id)
    GROUP BY 1, 2, 3, 4
)

SELECT
    sessions.state_code,
    sessions.person_id,
    sessions.start_date,
    sessions.end_date_exclusive,
    -- Projected release date as of the start of supervision session
    sentence_span_start.projected_supervision_release_snapshot_date AS original_projected_release_date,
    -- Projected release date as of the end of supervision session
    sentence_span_end.projected_supervision_release_snapshot_date AS updated_projected_release_date,
    sessions.compartment_level_2,
    sessions.inflow_from_level_1,
    sessions.inflow_from_level_2,
    sessions.outflow_to_level_1,

    -- calculate proportion of days served relative to sentence snapshot
    sessions.days_served,
    IF(DATE_DIFF(
        sentence_span_start.projected_supervision_release_snapshot_date,
        sessions.start_date, DAY
    ) < 0, NULL, DATE_DIFF(
        sentence_span_start.projected_supervision_release_snapshot_date,
        sessions.start_date, DAY
    )) AS days_sentenced,
    SAFE_DIVIDE(
        sessions.days_served,
        DATE_DIFF(
            sentence_span_start.projected_supervision_release_snapshot_date,
            sessions.start_date, DAY
        )
    ) AS prop_sentence_served,
FROM
    supervision_sessions sessions
LEFT JOIN
    sentence_deadline_spans sentence_span_start
ON
    sessions.person_id = sentence_span_start.person_id
    AND sessions.start_date BETWEEN sentence_span_start.start_date AND {nonnull_end_date_exclusive_clause("sentence_span_start.end_date_exclusive")}
LEFT JOIN
    sentence_deadline_spans sentence_span_end
ON
    sessions.person_id = sentence_span_end.person_id
    AND sessions.end_date_exclusive BETWEEN sentence_span_end.start_date AND {nonnull_end_date_exclusive_clause("sentence_span_end.end_date_exclusive")}
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.SUPERVISION_RELEASE,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "start_date",
        "original_projected_release_date",
        "updated_projected_release_date",
        "compartment_level_2",
        "inflow_from_level_1",
        "inflow_from_level_2",
        "outflow_to_level_1",
        "days_sentenced",
        "days_served",
        "prop_sentence_served",
    ],
    event_date_col="end_date_exclusive",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
