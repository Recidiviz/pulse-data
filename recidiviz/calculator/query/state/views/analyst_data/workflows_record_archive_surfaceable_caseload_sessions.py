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
"""
View representing spans of time during which a caseload was surfaceable in the
Workflows tool for a particular opportunity type, according to client_record_archive 
or resident_record_archive.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "workflows_record_archive_surfaceable_caseload_sessions"

_VIEW_DESCRIPTION = """
View representing spans of time during which a caseload was surfaceable in the
Workflows tool for a particular opportunity type, according to client_record_archive 
or resident_record_archive.
"""

_QUERY_TEMPLATE = f"""
WITH surfaceable_person_sessions AS (
    SELECT * 
    FROM `{{project_id}}.analyst_data.workflows_record_archive_surfaceable_person_sessions_materialized`
)
,
{create_sub_sessions_with_attributes("surfaceable_person_sessions", index_columns=["state_code", "caseload_id", "opportunity_type"], end_date_field_name="end_date_exclusive")}
,
-- Dedup to single spans of time where at least one client was eligible
sub_sessions_dedup AS (
    SELECT DISTINCT
        state_code,
        caseload_id,
        opportunity_type,
        start_date,
        end_date_exclusive,
    FROM
        sub_sessions_with_attributes
)
,
aggregated_sessions AS (
    -- For every caseload and opportunity, aggregate across contiguous periods of time
    -- where at least one surfaceable client exists on the caseload
    {aggregate_adjacent_spans(
        "sub_sessions_dedup", 
        index_columns=["state_code", "caseload_id", "opportunity_type"], 
        attribute=[],
        end_date_field_name="end_date_exclusive")
    }
)
SELECT
    aggregated_sessions.*,
    completion_event_type,
    CASE person_record_type
        WHEN "CLIENT" THEN "SUPERVISION"
        WHEN "RESIDENT" THEN "INCARCERATION" 
    END AS system_type
FROM
    aggregated_sessions
INNER JOIN
    `{{project_id}}.reference_views.workflows_opportunity_configs_materialized`
USING
    (state_code, opportunity_type)
"""

WORKFLOWS_RECORD_ARCHIVE_SURFACEABLE_CASELOAD_SESSIONS_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=ANALYST_VIEWS_DATASET,
        view_id=_VIEW_NAME,
        description=_VIEW_DESCRIPTION,
        view_query_template=_QUERY_TEMPLATE,
        clustering_fields=["state_code", "opportunity_type"],
        should_materialize=True,
    )
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_RECORD_ARCHIVE_SURFACEABLE_CASELOAD_SESSIONS_VIEW_BUILDER.build_and_print()
