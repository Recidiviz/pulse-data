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
"""View that represents attributes about every primary user (line staff) 
of the Workflows tool, with spans reflecting historical product roster
information where available"""


from typing import Dict, List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.types import WorkflowsSystemType

_VIEW_NAME = "workflows_primary_user_registration_sessions"

_VIEW_DESCRIPTION = """View that represents attributes about every primary user
(line staff) of the Workflows tool, with spans reflecting historical product roster
information where available."""

PRIMARY_USER_ROLE_TYPES_BY_SYSTEM_TYPE: Dict[WorkflowsSystemType, List[str]] = {
    WorkflowsSystemType.INCARCERATION: [
        "facilities_staff",
        "facilities_line_staff",
    ],
    WorkflowsSystemType.SUPERVISION: [
        "supervision_officer",
        "supervision_staff",
        "supervision_line_staff",
    ],
}
PRIMARY_USER_ROLE_TYPES = sorted(
    set().union(*PRIMARY_USER_ROLE_TYPES_BY_SYSTEM_TYPE.values())
)

_QUERY_TEMPLATE = f"""
# Get the first product archive export date for each user
WITH earliest_product_roster_archive_session AS (
    SELECT
        *
    FROM
        `{{project_id}}.analyst_data.workflows_user_product_roster_archive_sessions_materialized`
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY state_code, workflows_user_email_address ORDER BY start_date) = 1
)
,
# For sign ups occurring before the first product archive export date, 
# backfill roster information with product roster attributes from the first
# product roster export date to span the time between signup and product roster exports
backfilled_product_roster_sessions AS (
    SELECT
        b.state_code,
        b.workflows_user_email_address,
        # Use initial signup date as the start date of backfill session
        a.workflows_signup_date AS start_date,
        # Use first available product roster date as the end date of backfill session
        b.start_date AS end_date_exclusive,
        b.role,
        b.location_id,
    FROM
        `{{project_id}}.analyst_data.workflows_user_signups_materialized` a
    INNER JOIN
        earliest_product_roster_archive_session b
    ON
        a.state_code = b.state_code
        AND a.workflows_user_email_address = b.workflows_user_email_address
        AND a.workflows_signup_date < b.start_date
)
,
# Union together backfilled information with roster information
unioned_roster_sessions AS (
    SELECT
        * 
    FROM
        backfilled_product_roster_sessions

    UNION ALL

    SELECT
        state_code,
        workflows_user_email_address,
        start_date,
        end_date_exclusive,
        role,
        location_id,
    FROM
        `{{project_id}}.analyst_data.workflows_user_product_roster_archive_sessions_materialized`
)
,
# Filter to line-staff role types and pull out a system_type flag based on role type
primary_user_roster_sessions AS (
    SELECT
        state_code,
        workflows_user_email_address,
        start_date,
        end_date_exclusive,
        CASE
            WHEN role IN ({list_to_query_string(PRIMARY_USER_ROLE_TYPES_BY_SYSTEM_TYPE[WorkflowsSystemType.INCARCERATION], quoted = True)})
            THEN "{WorkflowsSystemType.INCARCERATION.value}"
            WHEN role IN ({list_to_query_string(PRIMARY_USER_ROLE_TYPES_BY_SYSTEM_TYPE[WorkflowsSystemType.SUPERVISION], quoted = True)})
            THEN "{WorkflowsSystemType.SUPERVISION.value}"
        END AS system_type,
        location_id,
    FROM
        unioned_roster_sessions
    # Filter to only primary user types
    WHERE
        role IN ({list_to_query_string(PRIMARY_USER_ROLE_TYPES, quoted = True)})
)
{aggregate_adjacent_spans(
    table_name='primary_user_roster_sessions',
    index_columns=["state_code", "workflows_user_email_address"],
    attribute=['system_type', 'location_id'],
    session_id_output_name='registration_session_id',
    end_date_field_name='end_date_exclusive'
)}
"""

WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    description=_VIEW_DESCRIPTION,
    view_query_template=_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSIONS_VIEW_BUILDER.build_and_print()
