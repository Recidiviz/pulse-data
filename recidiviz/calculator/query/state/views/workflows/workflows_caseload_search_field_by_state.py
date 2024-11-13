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
View uses caseload search id's emitted via surfaced_in_list segment event
to identify the type of search field configured for each state and system type.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import WORKFLOWS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.types import WorkflowsSystemType

_VIEW_NAME = "workflows_caseload_search_field_by_state"

_VIEW_DESCRIPTION = """
View uses caseload search id's emitted via surfaced_in_list segment event
to identify the type of search field configured for each state and system type.
"""

_QUERY_TEMPLATE = f"""
WITH search_field_counts AS (
    SELECT
        state_code,
        CASE person_record_type
            WHEN "CLIENT" THEN "{WorkflowsSystemType.SUPERVISION.value}"
            WHEN "RESIDENT" THEN "{WorkflowsSystemType.INCARCERATION.value}"
        END system_type,
        search_field,
        COUNT(*) AS n,
    FROM
        `{{project_id}}.workflows_views.clients_surfaced`
    INNER JOIN
        `{{project_id}}.reference_views.workflows_opportunity_configs_materialized`
    USING
        (state_code, opportunity_type)
    WHERE
        search_field IS NOT NULL
    GROUP BY
        1, 2, 3
)
-- For each state and system type, get the search field that most often shows up in search
SELECT
    state_code,
    system_type,
    search_field
FROM
    search_field_counts
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY state_code, system_type
        ORDER BY n DESC
    ) = 1
"""

WORKFLOWS_CASELOAD_SEARCH_FIELD_BY_STATE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    should_materialize=True,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_CASELOAD_SEARCH_FIELD_BY_STATE_VIEW_BUILDER.build_and_print()
