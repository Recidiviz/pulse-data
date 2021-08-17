# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Table for augmenting agent information that has been ingested into the state_agent table with state-specific info
in static tables.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    agent_state_specific_full_name,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

AUGMENTED_AGENT_INFO_VIEW_NAME = "augmented_agent_info"

AUGMENTED_AGENT_INFO_DESCRIPTION = """Agent information table that adds agent info from one-off reference tables to info from the state_agent table
    for use in the pipelines.
    """

# TODO(#4159) Remove the US_PA state-specific logic once we have given and surnames set in ingest
AUGMENTED_AGENT_INFO_QUERY_TEMPLATE = f"""
    /*{{description}}*/
    WITH
    agents_base AS (
      SELECT
        state_code,
        agent_type,
        external_id,
        REPLACE(JSON_EXTRACT(full_name, '$.full_name'), '"', '')  AS full_name,
        REPLACE(JSON_EXTRACT(full_name, '$.given_names'), '"', '')  AS given_names,
        REPLACE(JSON_EXTRACT(full_name, '$.surname'), '"', '') AS surname,
        -- TODO(#5445): We are currently shoving in a hack to pick one possible name for the agent.
        -- We should come up with a smarter way to have a single name for the agent.
        ROW_NUMBER() OVER (PARTITION BY state_code, agent_type, external_id ORDER BY CHAR_LENGTH(full_name) DESC NULLS LAST) AS rn
      FROM `{{project_id}}.{{base_dataset}}.state_agent` agent
    ),
    agent_names AS (
        SELECT
          state_code,
          agent_type,
          external_id,
          CASE
            WHEN given_names IS NOT NULL THEN given_names
            WHEN full_name IS NOT NULL THEN TRIM(SPLIT(full_name, ',')[SAFE_OFFSET(1)])
            ELSE NULL
          END AS given_names,
          CASE
            WHEN surname IS NOT NULL THEN surname
            WHEN full_name IS NOT NULL THEN TRIM(SPLIT(full_name, ',')[SAFE_OFFSET(0)])
            ELSE NULL
          END AS surname,
          {agent_state_specific_full_name('state_code')}
        FROM agents_base
        WHERE rn = 1
    ),
    agents AS (
      SELECT
        agent_id,
        state_code,
        agent_type,
        external_id,
        given_names,
        surname,
        agent_names.full_name
      FROM `{{project_id}}.{{base_dataset}}.state_agent`
      INNER JOIN agent_names
      USING (state_code, agent_type, external_id)
    )
    SELECT
      *,
      -- TODO(#8835): Deprecate this field for external_id.
      agents.external_id AS agent_external_id
    FROM agents

"""

AUGMENTED_AGENT_INFO_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=AUGMENTED_AGENT_INFO_VIEW_NAME,
    view_query_template=AUGMENTED_AGENT_INFO_QUERY_TEMPLATE,
    description=AUGMENTED_AGENT_INFO_DESCRIPTION,
    base_dataset=dataset_config.STATE_BASE_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        AUGMENTED_AGENT_INFO_VIEW_BUILDER.build_and_print()
