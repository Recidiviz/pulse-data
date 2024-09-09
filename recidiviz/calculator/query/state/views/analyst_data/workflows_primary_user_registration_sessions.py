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
from recidiviz.calculator.query.bq_utils import (
    MAGIC_START_DATE,
    list_to_query_string,
    nonnull_end_date_clause,
)
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
        "facilities_line_staff",
        "facilities_staff",
    ],
    WorkflowsSystemType.SUPERVISION: [
        "supervision_line_staff",
        "supervision_officer",
        "supervision_staff",
    ],
}
PRIMARY_USER_ROLE_TYPES = sorted(
    set().union(*PRIMARY_USER_ROLE_TYPES_BY_SYSTEM_TYPE.values())
)

# Date after which we consider product roster archive to reflect validated
# role and location information. We will backfill information starting at a user's
# first signup date and ending on this date, for users present in the roster on this
# date.
PRODUCT_ROSTER_ARCHIVE_FIRST_VALIDATED_DATE = "2024-09-06"

_QUERY_TEMPLATE = f"""
# Get the first product archive export date for each user after the first validated
# product roster archive export date, and backfill over all time
WITH backfilled_product_roster_sessions AS (
    SELECT
        state_code,
        workflows_user_email_address,
        DATE("{MAGIC_START_DATE}") AS start_date,
        # Use first validated product roster date as the end date of backfill session
        # If product roster archive session only starts after 
        # PRODUCT_ROSTER_ARCHIVE_FIRST_VALIDATED_DATE, use the start date of the archive
        GREATEST(start_date, "{PRODUCT_ROSTER_ARCHIVE_FIRST_VALIDATED_DATE}") AS end_date_exclusive,
        roles_as_string,
        location_id,
    FROM
        `{{project_id}}.analyst_data.workflows_user_product_roster_archive_sessions_materialized`
    WHERE
        "{PRODUCT_ROSTER_ARCHIVE_FIRST_VALIDATED_DATE}" < {nonnull_end_date_clause("end_date_exclusive")}
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY state_code, workflows_user_email_address ORDER BY start_date) = 1
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
        # If start date is before first validated archive date, truncate
        # This ensures no overlap between backfilled sessions and roster sessions
        GREATEST(start_date, "{PRODUCT_ROSTER_ARCHIVE_FIRST_VALIDATED_DATE}") AS start_date,
        end_date_exclusive,
        roles_as_string,
        location_id,
    FROM
        `{{project_id}}.analyst_data.workflows_user_product_roster_archive_sessions_materialized`
    WHERE
        {nonnull_end_date_clause("end_date_exclusive")} > "{PRODUCT_ROSTER_ARCHIVE_FIRST_VALIDATED_DATE}"
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
        unioned_roster_sessions,
    UNNEST(SPLIT(roles_as_string, ",")) AS role
    # Filter to only primary user types
    WHERE
        role IN ({list_to_query_string(PRIMARY_USER_ROLE_TYPES, quoted = True)})
)
,
aggregated_roster_sessions AS (
    {aggregate_adjacent_spans(
        table_name='primary_user_roster_sessions',
        index_columns=["state_code", "workflows_user_email_address"],
        attribute=['system_type', 'location_id'],
        session_id_output_name='registration_session_id',
        end_date_field_name='end_date_exclusive'
    )}
)
-- Truncate all registration sessions to start after the first signup/login event
SELECT
    a.state_code,
    a.workflows_user_email_address,
    GREATEST(a.start_date, b.workflows_signup_date) AS start_date,
    a.end_date_exclusive,
    a.registration_session_id,
    a.system_type,
    a.location_id,
FROM
    aggregated_roster_sessions a
# Only include roster sessions that overlap with the period following first signup
INNER JOIN
    `{{project_id}}.analyst_data.workflows_user_signups_materialized` b
ON
    a.state_code = b.state_code
    AND a.workflows_user_email_address = b.workflows_user_email_address
    AND b.workflows_signup_date < {nonnull_end_date_clause("a.end_date_exclusive")}

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
