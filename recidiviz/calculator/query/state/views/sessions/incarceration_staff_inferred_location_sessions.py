# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Sessionized view of incarceration staff. Session defined as continuous time
associated with a given facility based on the location of assigned residents."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.staff_inferred_location_sessions import (
    build_staff_inferred_location_sessions_query_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "incarceration_staff_inferred_location_sessions"

_VIEW_DESCRIPTION = """Sessionized view of incarceration staff. Session defined as continuous time
associated with a given facility based on the location of assigned residents."""

_QUERY_TEMPLATE = build_staff_inferred_location_sessions_query_template(
    assignment_sessions_sql_source=f"""
WITH facility_sessions AS (
    SELECT DISTINCT
        state_code,
        person_id,
        facility,
        start_date,
        end_date_exclusive,
    FROM
        `{{project_id}}.sessions.location_sessions_materialized`
    WHERE
        facility IS NOT NULL
)
,
incarceration_staff_assignment_sessions AS (
    SELECT 
        state_code,
        incarceration_staff_assignment_id AS incarceration_staff_id,
        person_id,
        DATE(start_date) AS start_date,
        DATE(end_date_exclusive) AS end_date_exclusive,
    FROM `{{project_id}}.sessions.incarceration_staff_assignment_sessions_preprocessed_materialized`
)
{create_intersection_spans(
    table_1_name="incarceration_staff_assignment_sessions",
    table_2_name="facility_sessions",
    index_columns=["person_id", "state_code"],
    table_1_columns=["incarceration_staff_id"],
    table_2_columns=["facility"],
    use_left_join=True
)}
    """,
    staff_index_cols=["state_code", "incarceration_staff_id"],
    location_cols=["facility"],
    start_date_col_name="start_date",
    end_date_exclusive_col_name="end_date_exclusive",
)

INCARCERATION_STAFF_INFERRED_LOCATION_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "incarceration_staff_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_STAFF_INFERRED_LOCATION_SESSIONS_VIEW_BUILDER.build_and_print()
