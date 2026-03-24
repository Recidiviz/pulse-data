# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""View builder representing all pageview events from the JII tablet app,
recorded via the Segment pages table in jii_frontend_prod_segment_metrics."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_column import (
    COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
    DateTime,
    Integer,
    String,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_ID = "all_jii_segment_pages"

_SCHEMA = [
    String(
        name="id", description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT, mode="NULLABLE"
    ),
    String(
        name="user_id",
        description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
        mode="REQUIRED",
    ),
    String(
        name="state_code",
        description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
        mode="REQUIRED",
    ),
    String(
        name="session_id",
        description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
        mode="NULLABLE",
    ),
    DateTime(
        name="event_ts",
        description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
        mode="NULLABLE",
    ),
    String(
        name="context_page_path",
        description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
        mode="NULLABLE",
    ),
    String(
        name="context_page_url",
        description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
        mode="NULLABLE",
    ),
    Integer(
        name="person_id",
        description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
        mode="NULLABLE",
    ),
]

_QUERY_TEMPLATE = """
WITH deduped_pages AS (
    SELECT
        id,
        user_id,
        state_code,
        session_id,
        DATETIME(timestamp, "US/Eastern") AS event_ts,
        context_page_path,
        context_page_url,
    FROM `{project_id}.jii_frontend_prod_segment_metrics.pages`
    -- Deduplicate events loaded more than once
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1
)
SELECT
    deduped_pages.*,
    p.person_id,
FROM deduped_pages
-- Will drop page views where user_id = anonres001 or user_id = anonres002 or is_recidiviz_user = TRUE
INNER JOIN
    `{project_id}.workflows_views.pseudonymized_id_to_person_id_materialized` p
ON
    deduped_pages.state_code = p.state_code
    AND deduped_pages.user_id = p.pseudonymized_id
    AND p.pseudonymized_id_type = "PERSON_EXTERNAL_ID_WITH_RESIDENT_RECORD_SALT"
"""

# TODO(#43316): Use standardized SegmentEventBigQueryViewBuilder instead once it supports pages and identifies from JII tablet app
ALL_JII_SEGMENT_PAGES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id="segment_events",
    view_id=_VIEW_ID,
    description=__doc__,
    view_query_template=_QUERY_TEMPLATE,
    should_materialize=True,
    schema=_SCHEMA,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ALL_JII_SEGMENT_PAGES_VIEW_BUILDER.build_and_print()
