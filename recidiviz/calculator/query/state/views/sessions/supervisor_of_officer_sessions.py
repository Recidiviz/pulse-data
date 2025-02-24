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
"""View containing periods of time where a given staff member supervised at least one officer."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_intersection_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISOR_OF_OFFICER_SESSIONS_VIEW_NAME = "supervisor_of_officer_sessions"

SUPERVISOR_OF_OFFICER_SESSIONS_DESCRIPTION = """View containing periods of time where a given staff member supervised at least one officer."""

SUPERVISOR_OF_OFFICER_SESSIONS_QUERY_TEMPLATE = f"""
    WITH officers_with_caseload_spans AS (
        SELECT state_code, officer_id, start_date, end_date
        FROM `{{project_id}}.aggregated_metrics.supervision_officer_caseload_count_spans_materialized`
        WHERE caseload_count > 0
    )
    , supervisor_spans AS (
        SELECT s.state_code, supervisor_staff_external_id, id.external_id AS officer_id, start_date, end_date, TRUE AS is_supervision_officer_supervisor
        FROM `{{project_id}}.normalized_state.state_staff_supervisor_period` s
        INNER JOIN `{{project_id}}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` id
        USING (staff_id)
    )
    , supervisor_of_officer_spans AS (
        {create_intersection_spans(
            table_1_name="officers_with_caseload_spans",
            table_2_name="supervisor_spans",
            index_columns=["state_code", "officer_id"],
            table_2_columns=["supervisor_staff_external_id", "is_supervision_officer_supervisor"],
            table_1_end_date_field_name="end_date",
            table_2_end_date_field_name="end_date"
        )}
    )
    , supervisor_of_officer_primary_staff_spans AS (
        -- The previous table was dealing with supervisor<>officer relationships, but for this view
        -- we only care about whether or not someone was a supervisor in a span of time, so get rid
        -- of the previous officer_id field.
        SELECT state_code, supervisor_staff_external_id AS staff_external_id, start_date, end_date_exclusive, is_supervision_officer_supervisor
        FROM supervisor_of_officer_spans
    )
    -- Use utility functions to re-sessionize intersection spans
    , {create_sub_sessions_with_attributes(
        table_name="supervisor_of_officer_primary_staff_spans",
        index_columns=["state_code", "staff_external_id"],
        end_date_field_name="end_date_exclusive",
    )}
    , sub_sessions_dedup AS (
        SELECT DISTINCT * FROM sub_sessions_with_attributes
    )
    {aggregate_adjacent_spans(
        table_name="sub_sessions_dedup", 
        index_columns=["state_code", "staff_external_id"],
        attribute=["is_supervision_officer_supervisor"],
        end_date_field_name="end_date_exclusive"
    )}
    """


SUPERVISOR_OF_OFFICER_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SESSIONS_DATASET,
    view_id=SUPERVISOR_OF_OFFICER_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISOR_OF_OFFICER_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISOR_OF_OFFICER_SESSIONS_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISOR_OF_OFFICER_SESSIONS_VIEW_BUILDER.build_and_print()
