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
"""Sessionized view of each individual. Session defined as continuous time in a given location type"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

LOCATION_TYPE_SESSIONS_VIEW_NAME = "location_type_sessions"

LOCATION_TYPE_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of each individual. Session defined as continuous stay associated with a given location type. Location type sessions may be overlapping."""

LOCATION_TYPE_SESSIONS_QUERY_TEMPLATE = f"""
    WITH location_sessions_cte AS (
        SELECT ls.*, lm.location_type 
        FROM
            `{{project_id}}.sessions.location_sessions_materialized` ls
        LEFT JOIN
            `{{project_id}}.reference_views.location_metadata_materialized` lm
        ON
            location = location_external_id    
    ),
    -- Deal with overlapping location type spans
    {create_sub_sessions_with_attributes('location_sessions_cte',
                                         end_date_field_name="end_date_exclusive")},
    dedup_cte AS (
        -- Note, this ensures that someone doesn't have duplicate spans for the same location type (e.g. where they have
        -- District A and District B but the same location type. This does still allow for people to have different
        -- location types at the same time, and the final output may therefore contain overlapping sessions
        SELECT
            DISTINCT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            location_type
        FROM
            sub_sessions_with_attributes
    )
    SELECT *
    FROM ({aggregate_adjacent_spans(table_name='dedup_cte',
                                    attribute='location_type',
                                    end_date_field_name="end_date_exclusive")})
    """

LOCATION_TYPE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=LOCATION_TYPE_SESSIONS_VIEW_NAME,
    view_query_template=LOCATION_TYPE_SESSIONS_QUERY_TEMPLATE,
    description=LOCATION_TYPE_SESSIONS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LOCATION_TYPE_SESSIONS_VIEW_BUILDER.build_and_print()
