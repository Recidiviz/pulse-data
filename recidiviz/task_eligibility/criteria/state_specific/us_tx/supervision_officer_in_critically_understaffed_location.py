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
"""
Defines a criteria span view that shows spans of time during which a client
is supervised by an officer who is located in a critically understaffed location
and thus has modified contact and assessment standards.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_intersection_spans,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_SUPERVISION_OFFICER_IN_CRITICALLY_UNDERSTAFFED_LOCATION"

_DESCRIPTION = """
Defines a criteria span view that shows spans of time during which a client
is supervised by an officer who is located in a critically understaffed location
and thus has modified contact and assessment standards.
"""


_REASON_QUERY = f"""
WITH supervision_officer_sessions AS (
    SELECT
        a.state_code,
        a.person_id,
        b.staff_id,
        a.start_date,
        a.end_date_exclusive,
    FROM
        `{{project_id}}.sessions.supervision_officer_sessions_materialized` a
    INNER JOIN
        `{{project_id}}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` b
    ON
        a.supervising_officer_external_id = b.external_id
        AND a.state_code = b.state_code
)
,
critically_understaffed_locations_sessions AS (
    SELECT
        state_code,
        staff_id,
        start_date,
        end_date_exclusive,
        TRUE AS officer_in_critically_understaffed_location,
    FROM
        `{{project_id}}.analyst_data.supervision_staff_in_critically_understaffed_location_sessions_preprocessed_materialized`
)
,
intersection_spans AS (
    {create_intersection_spans(
        table_1_name="supervision_officer_sessions",
        table_2_name="critically_understaffed_locations_sessions",
        index_columns=["state_code", "staff_id"],
        table_1_columns=["person_id"],
        table_2_columns=["officer_in_critically_understaffed_location"]
    )}
)
,
aggregated_spans AS (
    {aggregate_adjacent_spans(
        table_name="intersection_spans",
        index_columns=["state_code", "person_id"],
        attribute=["officer_in_critically_understaffed_location"],
        end_date_field_name="end_date_exclusive",
    )}
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive AS end_date,
    TRUE AS meets_criteria,
    TO_JSON(STRUCT(officer_in_critically_understaffed_location)) AS reason,
    officer_in_critically_understaffed_location,
FROM
    aggregated_spans
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_REASON_QUERY,
    state_code=StateCode.US_TX,
    meets_criteria_default=False,
    reasons_fields=[
        ReasonsField(
            name="officer_in_critically_understaffed_location",
            type=bigquery.enums.StandardSqlTypeNames.BOOL,
            description="Boolean indicating whether client is assigned to an officer in a critically understaffed location",
        )
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
