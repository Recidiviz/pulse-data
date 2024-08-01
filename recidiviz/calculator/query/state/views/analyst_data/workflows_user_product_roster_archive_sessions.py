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
"""A sessionized view of archived product roster information, reflecting
historical changes in information about tool users reflected in admin panel"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "workflows_user_product_roster_archive_sessions"

_VIEW_DESCRIPTION = """A sessionized view of archived product roster information,
reflecting historical changes in information about Workflows users as reflected
in the admin panel.
"""

_QUERY_TEMPLATE = f"""
WITH product_roster_archive AS (
    SELECT
        CASE state_code WHEN "US_ID" THEN "US_IX" ELSE state_code END AS state_code,
        email_address AS workflows_user_email_address,
        export_date AS start_date,
        LEAD(export_date) OVER (
            PARTITION BY state_code, email_address ORDER BY export_date
        ) AS end_date_exclusive,
        role,
        #TODO(#31965) district field is not guaranteed to be an id until roster sync is complete
        district AS location_id,
    FROM
        `{{project_id}}.export_archives.product_roster_archive`
    WHERE
        routes_workflows
        OR routes_workflowsFacilities
        OR routes_workflowsSupervision
)
{aggregate_adjacent_spans(
    table_name='product_roster_archive',
    index_columns=["state_code", "workflows_user_email_address"],
    attribute=['role', 'location_id'],
    session_id_output_name='product_roster_session_id',
    end_date_field_name='end_date_exclusive'
)}
"""

WORKFLOWS_USER_PRODUCT_ROSTER_ARCHIVE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    description=_VIEW_DESCRIPTION,
    view_query_template=_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_USER_PRODUCT_ROSTER_ARCHIVE_SESSIONS_VIEW_BUILDER.build_and_print()
