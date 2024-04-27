# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Sessionized view of each individual on supervision. Session defined as continuous
time on caseload associated with a given unit supervisor, based on the relationship
between officers and their supervisor.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_intersection_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_UNIT_SESSIONS_VIEW_NAME = "supervision_unit_supervisor_sessions"

SUPERVISION_UNIT_SUPERVISOR_SESSIONS_VIEW_DESCRIPTION = """
Sessionized view of each individual. Session defined as continuous stay on supervision 
associated with a given unit supervisor. Unit sessions may be overlapping.
"""

SUPERVISION_UNIT_SUPERVISOR_SESSIONS_QUERY_TEMPLATE = f"""
WITH officer_sessions AS (
    SELECT *, supervising_officer_external_id AS officer_id, 
    FROM `{{project_id}}.sessions.supervision_officer_sessions_materialized`
)
,
unit_supervisor_attributes AS (
    SELECT
        a.state_code,
        a.officer_id,
        supervisor_staff_id AS unit_supervisor,
        INITCAP(TRIM(CONCAT(COALESCE(
            JSON_EXTRACT_SCALAR(b.full_name, '$.given_names'), ''), 
            ' ', 
            COALESCE(JSON_EXTRACT_SCALAR(b.full_name, '$.surname'), '')
        ))) AS unit_supervisor_name,
        a.start_date,
        a.end_date_exclusive,
    FROM
        `{{project_id}}.sessions.supervision_staff_attribute_sessions_materialized` a,
    UNNEST(supervisor_staff_id_array) AS supervisor_staff_id
    INNER JOIN
        `{{project_id}}.normalized_state.state_staff` b
    ON
        a.state_code = b.state_code
        AND supervisor_staff_id = b.staff_id
    WHERE "SUPERVISION_OFFICER" IN UNNEST(a.role_type_array)
)
,
overlapping_spans AS (
    {create_intersection_spans(
        table_1_name="officer_sessions", 
        table_2_name="unit_supervisor_attributes", 
        index_columns=["state_code", "officer_id"],
        table_1_columns=["person_id"],
        table_2_columns=["unit_supervisor", "unit_supervisor_name"]
    )}
)
,
{create_sub_sessions_with_attributes(table_name="overlapping_spans", end_date_field_name="end_date_exclusive")},
sub_sessions_dedup_cte AS (
    SELECT DISTINCT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        unit_supervisor,
        unit_supervisor_name,
    FROM
        sub_sessions_with_attributes
)
{aggregate_adjacent_spans(
    table_name="sub_sessions_dedup_cte", 
    index_columns=["state_code","person_id"], 
    attribute=["unit_supervisor", "unit_supervisor_name"],
    session_id_output_name="supervision_unit_session_id",
    end_date_field_name="end_date_exclusive",
)}
"""

SUPERVISION_UNIT_SUPERVISOR_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_UNIT_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_UNIT_SUPERVISOR_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_UNIT_SUPERVISOR_SESSIONS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_UNIT_SUPERVISOR_SESSIONS_VIEW_BUILDER.build_and_print()
