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
"""Links NormalizedStateSupervisionPeriod entities to their associated supervising
agents."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

NORMALIZED_SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME = (
    "normalized_supervision_period_to_agent_association"
)

NORMALIZED_SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_DESCRIPTION = """Links NormalizedStateSupervisionPeriod entities to their associated supervising agents."""

NORMALIZED_SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_QUERY_TEMPLATE = """
    /*{description}*/
WITH normalized_sps_with_matched_officer_ids AS (
    SELECT
        norm_sup.person_id,
        norm_sup.state_code,
        norm_sup.supervision_period_id,
        -- If the normalized SP matches an SP from the state table with a set officer, then use that.
        -- If not, use agent from the closest overlapping agent span.
        COALESCE(sup.supervising_officer_id, agents.supervising_officer_id) as agent_id
    FROM
        `{project_id}.{normalized_state_base_dataset}.state_supervision_period` norm_sup
    LEFT JOIN
        `{project_id}.{state_base_dataset}.state_supervision_period` sup
    USING (supervision_period_id)
    LEFT JOIN
        -- Take the start/end dates of agent assignments from the state table
        `{project_id}.{state_base_dataset}.state_supervision_period` agents
    ON norm_sup.person_id = agents.person_id AND 
        -- Join normalized SP with agent spans that overlap
        GREATEST(norm_sup.start_date, agents.start_date) < LEAST(COALESCE(norm_sup.termination_date, CURRENT_DATE('US/Eastern')), COALESCE(agents.termination_date, CURRENT_DATE('US/Eastern')))
        AND agents.supervising_officer_id IS NOT NULL
    -- If a normalized SP overlaps with multiple agent spans, order by whichever has a more recent start date
    QUALIFY ROW_NUMBER() OVER (PARTITION BY norm_sup.supervision_period_id ORDER BY agents.start_date DESC) = 1
)

SELECT
    sup.state_code,
    sup.person_id,
    sup.supervision_period_id,
    agents.agent_id,
    agents.external_id as agent_external_id
FROM
    normalized_sps_with_matched_officer_ids sup
LEFT JOIN
    `{project_id}.{reference_views_dataset}.augmented_agent_info` agents
USING(state_code, agent_id)
"""

NORMALIZED_SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=NORMALIZED_SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
    view_query_template=NORMALIZED_SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_QUERY_TEMPLATE,
    description=NORMALIZED_SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_DESCRIPTION,
    state_base_dataset=dataset_config.STATE_BASE_DATASET,
    normalized_state_base_dataset=dataset_config.NORMALIZED_STATE_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        NORMALIZED_SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_BUILDER.build_and_print()
