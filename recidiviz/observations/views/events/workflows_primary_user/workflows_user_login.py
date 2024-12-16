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
"""View with events where the workflows user logged into the dashboard
"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.views.spans.workflows_primary_user.workflows_primary_user_registration_session import (
    VIEW_BUILDER as WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = (
    "View with events where the workflows user logged into the dashboard"
)

_QUERY_TEMPLATE = f"""
WITH registration_sessions AS (
    SELECT
        *
    FROM
        `{{project_id}}.{WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION_VIEW_BUILDER.table_for_query.to_str()}`
)
-- Join logins to all live opportunities corresponding to the system type(s)
-- for which the user has access according to primary user registration sessions.
SELECT
    logins.state_code,
    logins.email_address,
    logins.login_date,
    registration_sessions.task_type,
    registration_sessions.system_type,
    registration_sessions.decarceral_impact_type,
    registration_sessions.is_jii_decarceral_transition,
    registration_sessions.task_type_is_live,
    registration_sessions.task_type_is_fully_launched,
FROM
    `{{project_id}}.analyst_data.all_auth0_login_events_materialized` logins
INNER JOIN
    registration_sessions
ON
    logins.state_code = registration_sessions.state_code
    AND logins.email_address = registration_sessions.email_address
    AND logins.login_date BETWEEN registration_sessions.start_date AND {nonnull_end_date_clause("registration_sessions.end_date")}
    AND task_type_is_live = "true"
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.WORKFLOWS_USER_LOGIN,
    description=_VIEW_DESCRIPTION,
    sql_source=_QUERY_TEMPLATE,
    attribute_cols=[
        "task_type",
        "system_type",
        "decarceral_impact_type",
        "is_jii_decarceral_transition",
        "task_type_is_live",
        "task_type_is_fully_launched",
    ],
    event_date_col="login_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
