# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""View that parses and formats state agent names in various ways for use downstream."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STATE_STAFF_WITH_NAMES_VIEW_NAME = "state_staff_with_names"

STATE_STAFF_WITH_NAMES_VIEW_DESCRIPTION = """View that parses and formats state agent names in various ways for use downstream."""

STATE_STAFF_WITH_NAMES_QUERY_TEMPLATE = """
WITH staff_names_base AS (
    SELECT
        state_code,
        staff_id,
        full_name_json,
        given_names,
        surname,
        TRIM(CONCAT(COALESCE(given_names, ''), ' ', COALESCE(surname, ''))) AS full_name,
        -- TODO(#21702): Eliminate all references to legacy_supervising_officer_external_id -
        -- views should be joining to staff_id instead
        external_id AS legacy_supervising_officer_external_id,
        email
    FROM (
        SELECT
          state_code,
          staff_id,
          email,
          full_name AS full_name_json,
          REPLACE(JSON_EXTRACT(full_name, '$.given_names'), '"', '')  AS given_names,
          REPLACE(JSON_EXTRACT(full_name, '$.surname'), '"', '') AS surname
        FROM `{project_id}.normalized_state.state_staff`
    ) s
    LEFT JOIN
    `{project_id}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized`
    USING(staff_id)
)
SELECT 
    *,
    CONCAT(INITCAP(SPLIT(given_names, " ")[OFFSET(0)]), " ", INITCAP(surname)) AS full_name_clean,
    -- TODO(#8674): Remove the agent_external_id_with_full_name once the Lantern FE 
    -- is no longer relying on this value
    CONCAT(legacy_supervising_officer_external_id, IFNULL(CONCAT(': ', NULLIF(TRIM(full_name), '')), '')) as agent_external_id_with_full_name,
FROM staff_names_base
"""

STATE_STAFF_WITH_NAMES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=STATE_STAFF_WITH_NAMES_VIEW_NAME,
    view_query_template=STATE_STAFF_WITH_NAMES_QUERY_TEMPLATE,
    description=STATE_STAFF_WITH_NAMES_VIEW_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STATE_STAFF_WITH_NAMES_VIEW_BUILDER.build_and_print()
