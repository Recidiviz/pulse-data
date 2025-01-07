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
View representing spans of time over which an Insights user is associated with
a person, based on the set of clients supervised by an officer supervised by the
Insights user.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "insights_user_person_assignment_sessions"

_VIEW_DESCRIPTION = """
View representing spans of time over which an Insights user is associated with
a person, based on the set of clients supervised by an officer supervised by the
Insights user.
"""

_QUERY_TEMPLATE = f"""
WITH insights_users AS (
    SELECT
        a.state_code,
        a.insights_user_email_address AS email_address,
        a.start_date,
        a.end_date_exclusive,
        b.staff_id AS unit_supervisor
    FROM
        `{{project_id}}.analyst_data.insights_provisioned_user_registration_sessions_materialized` a
    INNER JOIN
        `{{project_id}}.normalized_state.state_staff` b
    ON
        LOWER(a.insights_user_email_address) = LOWER(b.email)
        AND a.state_code = b.state_code
)
,
person_assignments AS (
    SELECT * FROM `{{project_id}}.sessions.supervision_unit_supervisor_sessions_materialized`
)
{create_intersection_spans(
    table_1_name="insights_users",
    table_2_name="person_assignments",
    index_columns=["unit_supervisor", "state_code"],
    table_1_columns=["email_address"],
    table_2_columns=["person_id"],
)}

"""

INSIGHTS_USER_PERSON_ASSIGNMENT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    description=_VIEW_DESCRIPTION,
    view_query_template=_QUERY_TEMPLATE,
    clustering_fields=["person_id", "state_code"],
    should_materialize=True,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INSIGHTS_USER_PERSON_ASSIGNMENT_SESSIONS_VIEW_BUILDER.build_and_print()
