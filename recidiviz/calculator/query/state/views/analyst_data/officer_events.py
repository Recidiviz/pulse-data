# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Creates the view builder and view for officer events concatenated in a common
format."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.experiments.dataset_config import (
    CASE_TRIAGE_SEGMENT_DATASET,
    EXPERIMENTS_DATASET,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    PO_REPORT_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

OFFICER_EVENTS_VIEW_NAME = "officer_events"

OFFICER_EVENTS_VIEW_DESCRIPTION = "View concatenating officer events in a common format"

OFFICER_EVENTS_QUERY_TEMPLATE = """
-- case triage feedback given
SELECT
    state_code,
    officer_external_id,
    "CASE_TRIAGE_FEEDBACK" AS event,
    feedback_date AS event_date,
    feedback_type AS attribute_1,
FROM
    `{project_id}.{experiments_dataset}.case_triage_feedback_actions`

UNION ALL

-- case triage action taken (click, scroll to bottom, etc.)
SELECT
    users.state_code,
    users.officer_external_id,
    "CASE_TRIAGE_ACTION" AS event,
    EXTRACT(DATETIME FROM tracks.timestamp AT TIME ZONE 'US/Pacific') AS event_date,
    event AS attribute_1,  -- kind of action taken
FROM (
    -- dedup tracks
    SELECT
        *
    FROM
        `{project_id}.{case_triage_segment_dataset}.tracks`
    WHERE
        TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1
    ) tracks
INNER JOIN
    `{project_id}.{static_reference_dataset}.case_triage_users` users
ON
    tracks.user_id = users.segment_id
    
UNION ALL
    
-- case triage user sees a page
SELECT
    users.state_code,
    users.officer_external_id,
    "CASE_TRIAGE_PAGE" AS event,
    EXTRACT(DATETIME FROM pages.timestamp AT TIME ZONE 'US/Pacific') AS event_date,
    path as attribute_1,  -- which page the user saw
FROM (
    -- dedup pages
    SELECT
        *
    FROM
        `{project_id}.{case_triage_segment_dataset}.pages`
    WHERE
        TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1
    ) pages
INNER JOIN
    `{project_id}.{static_reference_dataset}.case_triage_users` users
ON
    pages.user_id = users.segment_id

UNION ALL

-- po monthly report action taken (delivered, opened, clicked)
SELECT
    users.state_code,
    users.officer_external_id,
    "PO_MONTHLY_REPORT_ACTION" AS event,
    EXTRACT(DATETIME FROM events.event_datetime AT TIME ZONE 'US/Pacific') AS event_date,
    event AS attribute_1, -- kind of action taken
FROM `{project_id}.{po_report_dataset}.sendgrid_po_report_email_events_materialized` events
INNER JOIN
    `{project_id}.{static_reference_dataset}.po_report_recipients` users
ON events.email = users.email_address
"""

OFFICER_EVENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=OFFICER_EVENTS_VIEW_NAME,
    view_query_template=OFFICER_EVENTS_QUERY_TEMPLATE,
    description=OFFICER_EVENTS_VIEW_DESCRIPTION,
    case_triage_segment_dataset=CASE_TRIAGE_SEGMENT_DATASET,
    experiments_dataset=EXPERIMENTS_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    po_report_dataset=PO_REPORT_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "officer_external_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OFFICER_EVENTS_VIEW_BUILDER.build_and_print()
