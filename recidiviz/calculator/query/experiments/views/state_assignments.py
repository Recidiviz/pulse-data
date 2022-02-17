# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Creates the view builder and view for state experiment assignments."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.experiments.dataset_config import EXPERIMENTS_DATASET
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STATE_ASSIGNMENTS_VIEW_NAME = "state_assignments"

STATE_ASSIGNMENTS_VIEW_DESCRIPTION = (
    "Tracks assignments of treatment variants to states in an experiment. These are "
    "generally state-wide policy changes."
)

STATE_ASSIGNMENTS_QUERY_TEMPLATE = """
-- last day data observable in sessions
WITH last_day_of_data AS (
    SELECT
        state_code,
        MIN(last_day_of_data) AS last_day_of_data,
    FROM
        `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
    GROUP BY 1
)

-- State-level assignments from static reference table
, state_assignments AS (
    SELECT
        experiment_id,
        state_code,
        variant_id,
        variant_date,
    FROM
        `{project_id}.{static_reference_dataset}.experiment_state_assignments_materialized`
)

-- Add state-level last day data observed
SELECT *
FROM state_assignments
INNER JOIN last_day_of_data USING(state_code)
"""

STATE_ASSIGNMENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=EXPERIMENTS_DATASET,
    view_id=STATE_ASSIGNMENTS_VIEW_NAME,
    view_query_template=STATE_ASSIGNMENTS_QUERY_TEMPLATE,
    description=STATE_ASSIGNMENTS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    should_materialize=True,
    clustering_fields=["experiment_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STATE_ASSIGNMENTS_VIEW_BUILDER.build_and_print()
