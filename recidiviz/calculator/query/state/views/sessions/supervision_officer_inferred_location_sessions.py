# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Sessionized view of supervision officers. Session defined as continuous time
associated with a given primary office with at least a critical caseload count."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.staff_inferred_location_sessions import (
    build_staff_inferred_location_sessions_query_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_officer_inferred_location_sessions"

_VIEW_DESCRIPTION = (
    "Sessionized view of officers' inferred locations. Session defined as continuous stay "
    "associated with a given office + district. This association is determined as the "
    "modal location across each officer's caseload. end_date is exclusive."
)

_QUERY_TEMPLATE = build_staff_inferred_location_sessions_query_template(
    assignment_sessions_sql_source="""
SELECT DISTINCT
    state_code,
    person_id,
    attr.supervising_officer_external_id,
    attr.supervision_district AS district,
    attr.supervision_district_name AS district_name,
    attr.supervision_office AS office,
    attr.supervision_office_name AS office_name,
    start_date,
    end_date_exclusive,
FROM
    `{project_id}.sessions.dataflow_sessions_materialized`,
    UNNEST(session_attributes) AS attr
WHERE
    compartment_level_1 IN ("SUPERVISION", "SUPERVISION_OUT_OF_STATE")
    AND attr.supervising_officer_external_id IS NOT NULL
""",
    staff_index_cols=["state_code", "supervising_officer_external_id"],
    location_cols=[
        "district",
        "district_name",
        "office",
        "office_name",
    ],
    start_date_col_name="start_date",
    end_date_exclusive_col_name="end_date_exclusive",
)

SUPERVISION_OFFICER_INFERRED_LOCATION_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "supervising_officer_external_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_INFERRED_LOCATION_SESSIONS_VIEW_BUILDER.build_and_print()
