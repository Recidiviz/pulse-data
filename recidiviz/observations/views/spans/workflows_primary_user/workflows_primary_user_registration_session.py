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
"""View with spans of time over which a primary workflows user is a registered user of
the workflows tool.
"""

from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Spans of time over which a primary workflows user is a registered user of the workflows tool"

_SOURCE_DATA_QUERY_TEMPLATE = """
SELECT
    state_code,
    sessions.workflows_user_email_address AS email_address,
    metadata.completion_event_type AS task_type,
    system_type,
    metadata.decarceral_impact_type,
    metadata.is_jii_decarceral_transition,
    metadata.has_mandatory_due_date,
    launches.first_access_date IS NOT NULL AS task_type_is_live,
    IFNULL(launches.is_fully_launched, FALSE) AS task_type_is_fully_launched,
    start_date,
    end_date_exclusive
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized` sessions
# Join with completion event metadata on system_type to get all task types that a user
# could theoretically access based on their system type access (supervision vs. incarceration)
LEFT JOIN
    `{project_id}.reference_views.completion_event_type_metadata_materialized` metadata
USING
    (system_type)
LEFT JOIN
    `{project_id}.analyst_data.workflows_live_completion_event_types_by_state_materialized` launches
USING
    (state_code, completion_event_type)
WHERE
    is_registered
    AND is_primary_user
"""

VIEW_BUILDER: SpanObservationBigQueryViewBuilder = SpanObservationBigQueryViewBuilder(
    span_type=SpanType.WORKFLOWS_PRIMARY_USER_REGISTRATION_SESSION,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "task_type",
        "system_type",
        "decarceral_impact_type",
        "is_jii_decarceral_transition",
        "has_mandatory_due_date",
        "task_type_is_live",
        "task_type_is_fully_launched",
    ],
    span_start_date_col="start_date",
    span_end_date_col="end_date_exclusive",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
