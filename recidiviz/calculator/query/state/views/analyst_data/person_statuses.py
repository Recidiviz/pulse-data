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
"""Creates the view builder and view for client (person) statuses concatenated in a
common format."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PERSON_STATUSES_VIEW_NAME = "person_statuses"

PERSON_STATUSES_VIEW_DESCRIPTION = (
    "View concatenating client (person) statuses in a common format"
)

PERSON_STATUSES_QUERY_TEMPLATE = """
# compartment level 1 statuses
# useful for getting populations over time by compartment, e.g. for measuring
# statuses or events as rates.
SELECT
    state_code,
    person_id,
    compartment_level_1 AS status,
    start_date,
    end_date,
    CAST(NULL AS STRING) AS attribute_1,
    CAST(NULL AS STRING) AS attribute_2,
FROM
    `{project_id}.{sessions_dataset}.compartment_level_1_super_sessions_materialized`

UNION ALL

# open supervision mismatch (downgrades only)
# ends when mismatch corrected or supervision period ends
SELECT
    state_code,
    person_id,
    "SUPERVISION_LEVEL_DOWNGRADE_RECOMMENDED" AS status,
    start_date,
    end_date,
    -- flag for whether PO corrected mismatch (vs end for another reason)
    CAST(mismatch_corrected AS STRING) AS attribute_1,
    recommended_supervision_downgrade_level AS attribute_2,
FROM
    `{project_id}.{sessions_dataset}.supervision_downgrade_sessions_materialized`
WHERE
    recommended_supervision_downgrade_level IS NOT NULL
"""

PERSON_STATUSES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=PERSON_STATUSES_VIEW_NAME,
    view_query_template=PERSON_STATUSES_QUERY_TEMPLATE,
    description=PERSON_STATUSES_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_STATUSES_VIEW_BUILDER.build_and_print()
