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
View representing spans of time during which a person was surfaceable in the
Workflows tool for a particular opportunity type, caseload, and location,
according to client_record_archive or resident_record_archive.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "workflows_record_archive_surfaceable_person_sessions"

_VIEW_DESCRIPTION = """
View representing spans of time during which a person was surfaceable in the
Workflows tool for a particular opportunity type, caseload, and location,
according to client_record_archive or resident_record_archive.
"""

_QUERY_TEMPLATE = f"""
-- Combines resident & client archive exports and uses export dates to construct
-- spans of time where person was surfaceable for an opportunity, accounting for 
-- export failures.

-- Uses the next available export_date as the session exclusive end date
-- to ensure that surfaceable sessions are closed when a client
-- ceases to become surfaceable, but remain open when the export
-- failed for a given day across the state.
WITH surfaceable_archive_spans AS (
    -- resident record archive
    SELECT
        resident_record.state_code,
        resident_record.person_id,
        opportunity_type,
        resident_record.officer_id AS caseload_id,
        resident_record.facility_id AS location_id,
        resident_record.export_date AS start_date,
        MIN(future_exports.future_export_date) AS end_date_exclusive,
    FROM
        `{{project_id}}.workflows_views.resident_record_archive_materialized` resident_record,
        UNNEST(SPLIT(all_eligible_opportunities)) opportunity_type
    LEFT JOIN (
        SELECT DISTINCT
            state_code,
            export_date AS future_export_date,
        FROM
            `{{project_id}}.workflows_views.resident_record_archive_materialized`
    ) future_exports
    ON
        resident_record.state_code = future_exports.state_code
        AND resident_record.export_date < future_exports.future_export_date
    -- Filter to only rows where person is surfaceable for an opportunity
    WHERE
        NULLIF(all_eligible_opportunities, "") IS NOT NULL
        AND person_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6

    UNION ALL
    -- client record archive
    SELECT
        client_record.state_code,
        client_record.person_id,
        opportunity_type,
        client_record.officer_id AS caseload_id,
        client_record.district AS location_id,
        client_record.export_date AS start_date,
        MIN(future_exports.future_export_date) AS end_date_exclusive,
    FROM
        `{{project_id}}.workflows_views.client_record_archive_materialized` client_record,
        UNNEST(SPLIT(all_eligible_opportunities)) opportunity_type
    LEFT JOIN (
        SELECT DISTINCT
            state_code,
            export_date AS future_export_date,
        FROM
            `{{project_id}}.workflows_views.client_record_archive_materialized`
    ) future_exports
    ON
        client_record.state_code = future_exports.state_code
        AND client_record.export_date < future_exports.future_export_date
    -- Filter to only rows where person is surfaceable for an opportunity
    WHERE
        NULLIF(all_eligible_opportunities, "") IS NOT NULL
        AND person_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6
)
-- For every person and opportunity, aggregate across contiguous periods of assignment
-- to a caseload and location
{aggregate_adjacent_spans(
    "surfaceable_archive_spans", 
    index_columns=["state_code", "person_id", "opportunity_type"], 
    attribute=["caseload_id", "location_id"],
    end_date_field_name="end_date_exclusive")
}
"""

WORKFLOWS_RECORD_ARCHIVE_SURFACEABLE_PERSON_SESSIONS_VIEW_BUILDER = (
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
        WORKFLOWS_RECORD_ARCHIVE_SURFACEABLE_PERSON_SESSIONS_VIEW_BUILDER.build_and_print()
