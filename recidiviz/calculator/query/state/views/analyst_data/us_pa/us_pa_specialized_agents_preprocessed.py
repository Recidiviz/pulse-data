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
"""State-specific preprocessing for PA raw data on specialized agents"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    US_PA_RAW_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_PA_SPECIALIZED_AGENTS_PREPROCESSED_VIEW_NAME = (
    "us_pa_specialized_agents_preprocessed"
)

US_PA_SPECIALIZED_AGENTS_PREPROCESSED_VIEW_DESCRIPTION = """State-specific preprocessing for PA raw data on specialized
agents. Uses an adhoc spreadsheet created by PA with agent names and a flag for specialized agents
"""

US_PA_SPECIALIZED_AGENTS_PREPROCESSED_QUERY_TEMPLATE = """
WITH dedup_specialty_agents AS (
    SELECT AgentType,
           AgentName,
           external_id,
    FROM 
        `{project_id}.{raw_dataset}.specialty_agents_latest`
    -- Deals with small number of duplicate names by first making sure FAST agents are associated with FAST district, 
    -- then deterministically de-duplicating on DistrictOffice name
    QUALIFY 
        ROW_NUMBER() OVER(PARTITION BY external_id ORDER BY IF(AgentType = "FAST", 0, 1), DistrictOffice ) = 1
)
SELECT 'US_PA' AS state_code, 
        external_id AS officer_id, 
        AgentType AS specialized_agent_type,
        AgentType != "GENERAL" AS specialized_agent_flag 
FROM 
    dedup_specialty_agents 
WHERE 
    external_id IS NOT NULL 
"""

US_PA_SPECIALIZED_AGENTS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_PA_SPECIALIZED_AGENTS_PREPROCESSED_VIEW_NAME,
    view_query_template=US_PA_SPECIALIZED_AGENTS_PREPROCESSED_QUERY_TEMPLATE,
    description=US_PA_SPECIALIZED_AGENTS_PREPROCESSED_VIEW_DESCRIPTION,
    raw_dataset=US_PA_RAW_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_PA_SPECIALIZED_AGENTS_PREPROCESSED_VIEW_BUILDER.build_and_print()
