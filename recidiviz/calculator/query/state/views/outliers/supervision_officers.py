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
"""A parole and/or probation officer/agent with regular contact with clients"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import (
    get_pseudonymized_id_query_str,
    today_between_start_date_and_nullable_end_date_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states_for_bigquery,
)
from recidiviz.calculator.query.state.views.outliers.utils import (
    most_recent_staff_attrs_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICERS_VIEW_NAME = "supervision_officers"

SUPERVISION_OFFICERS_DESCRIPTION = (
    """A parole and/or probation officer/agent with regular contact with clients"""
)


def query_template() -> str:
    """Creates a query to identify supervision officers in Insights"""
    state_queries = []
    for state in get_outliers_enabled_states_for_bigquery():
        state_queries.append(
            f"""
    SELECT
        attrs.officer_id AS external_id,
        attrs.staff_id,
        attrs.state_code,
        staff.full_name,
        -- This pseudonymized_id will match the one for the user in the auth0 roster. Hashed
        -- attributes must be kept in sync with recidiviz.auth.helpers.generate_pseudonymized_id.
        {get_pseudonymized_id_query_str("IF(attrs.state_code = 'US_IX', 'US_ID', attrs.state_code) || attrs.officer_id")} AS pseudonymized_id,
        staff.email,
        COALESCE(attrs.supervision_district_name,attrs.supervision_district_name_inferred) AS supervision_district,
        -- TODO(#29942): Deprecate once array is deployed and fully in use
        attrs.supervisor_staff_external_id_array[SAFE_OFFSET(0)] AS supervisor_external_id,
        attrs.specialized_caseload_type_primary AS specialized_caseload_type,
        attrs.supervisor_staff_external_id_array AS supervisor_external_ids,
        assignment.earliest_person_assignment_date,
    FROM (
        -- A supervision officer in the Insights product is anyone that has open session 
        -- in supervision_officer_sessions, in which they are someone's supervising officer 
        SELECT DISTINCT
            state_code,
            supervising_officer_external_id AS external_id
        FROM `{{project_id}}.sessions.supervision_officer_sessions_materialized`
        WHERE
            -- Only include officers who have open officer sessions
            {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
    ) supervision_staff
    INNER JOIN attrs
        ON attrs.state_code = supervision_staff.state_code AND attrs.officer_id = supervision_staff.external_id 
    INNER JOIN `{{project_id}}.normalized_state.state_staff` staff 
        ON attrs.staff_id = staff.staff_id AND attrs.state_code = staff.state_code
    LEFT JOIN (
        SELECT state_code, officer_id, MIN(assignment_date) AS earliest_person_assignment_date
        FROM `{{project_id}}.aggregated_metrics.supervision_officer_metrics_person_assignment_sessions_materialized`
        GROUP BY 1,2
    ) assignment
        ON attrs.officer_id = assignment.officer_id AND attrs.state_code = assignment.state_code
    WHERE staff.state_code = '{state}'
"""
        )

    all_state_queries = "\n      UNION ALL\n".join(state_queries)

    return f"""
    WITH 
    attrs AS (
        {most_recent_staff_attrs_cte()}
    )
    {all_state_queries}
"""


SUPERVISION_OFFICERS_QUERY_TEMPLATE = f"""
WITH
supervision_officers AS (
    {query_template()}
)

SELECT {{columns}}
FROM supervision_officers
"""

SUPERVISION_OFFICERS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=SUPERVISION_OFFICERS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICERS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICERS_DESCRIPTION,
    should_materialize=True,
    columns=[
        "state_code",
        "external_id",
        "staff_id",
        "full_name",
        "pseudonymized_id",
        "email",
        "supervisor_external_id",
        "supervisor_external_ids",
        "supervision_district",
        "specialized_caseload_type",
        "earliest_person_assignment_date",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICERS_VIEW_BUILDER.build_and_print()
