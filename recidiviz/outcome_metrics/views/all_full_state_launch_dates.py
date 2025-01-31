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
"""View of all full-state launches and their launch dates"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import TRANSITIONS_DATASET_ID
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "all_full_state_launch_dates"

_VIEW_DESCRIPTION = "View of all full-state launches and their launch dates"

_QUERY_TEMPLATE = """
#TODO(#37768): Remove special case for Workflows once experiment_assignments is synced and validated
WITH experiment_assignments AS (
    SELECT
        state_code,
        experiment_id,
        variant_date AS launch_date
    FROM
        `{project_id}.experiments_metadata.experiment_assignments_materialized` 
    WHERE
        unit_type = "STATE"
        AND experiment_id NOT LIKE "%WORKFLOWS"
)
,
workflows_assignments AS (
    SELECT
        state_code,
        experiment_id,
        launch_date,
    FROM
        `{project_id}.static_reference_tables.workflows_launch_metadata_materialized` launch
    LEFT JOIN 
        `{project_id}.reference_views.workflows_opportunity_configs_materialized` config
    USING
        (state_code, completion_event_type)
    WHERE
        is_fully_launched
    )

SELECT * FROM experiment_assignments 
UNION ALL 
SELECT * FROM workflows_assignments
"""

ALL_FULL_STATE_LAUNCH_DATES_VIEW_BUILDER: SimpleBigQueryViewBuilder = (
    SimpleBigQueryViewBuilder(
        dataset_id=TRANSITIONS_DATASET_ID,
        view_id=_VIEW_NAME,
        view_query_template=_QUERY_TEMPLATE,
        description=_VIEW_DESCRIPTION,
        clustering_fields=["state_code"],
        should_materialize=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ALL_FULL_STATE_LAUNCH_DATES_VIEW_BUILDER.build_and_print()
