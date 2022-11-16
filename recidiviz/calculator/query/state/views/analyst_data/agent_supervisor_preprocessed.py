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
"""Preprocessing file that unions state-specific pre-processing files for agents mapped to latest supervisor"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

AGENT_SUPERVISOR_PREPROCESSED_VIEW_NAME = "agent_supervisor_preprocessed"

AGENT_SUPERVISOR_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessing file that unions state-specific pre-processing files
for agents mapped to latest supervisor"""

AGENT_SUPERVISOR_PREPROCESSED_QUERY_TEMPLATE = """
SELECT *
FROM 
    `{project_id}.{analyst_dataset}.us_pa_agent_supervisor_preprocessed`
"""

AGENT_SUPERVISOR_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=AGENT_SUPERVISOR_PREPROCESSED_VIEW_NAME,
    view_query_template=AGENT_SUPERVISOR_PREPROCESSED_QUERY_TEMPLATE,
    description=AGENT_SUPERVISOR_PREPROCESSED_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        AGENT_SUPERVISOR_PREPROCESSED_VIEW_BUILDER.build_and_print()
