# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Links SupervisionPeriods and their associated supervising agent."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME = (
    "supervision_period_to_agent_association"
)

SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_DESCRIPTION = (
    """Links SupervisionPeriods and their associated agent."""
)

SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
      sup.state_code,
      sup.person_id,
      sup.supervision_period_id,
      agents.agent_id,
      CAST(agents.external_id AS STRING) as agent_external_id,
    FROM
      `{project_id}.{base_dataset}.state_supervision_period` sup
    LEFT JOIN
      `{project_id}.{reference_views_dataset}.augmented_agent_info` agents
    ON agents.state_code = sup.state_code AND agents.agent_id = sup.supervising_officer_id
    WHERE agents.external_id IS NOT NULL
"""

SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
    view_query_template=SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_QUERY_TEMPLATE,
    description=SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_DESCRIPTION,
    base_dataset=dataset_config.STATE_BASE_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_BUILDER.build_and_print()
