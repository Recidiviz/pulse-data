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
"""Reference views for Workflows Opportunity configurations"""

import attr

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import REFERENCE_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

WORKFLOWS_OPPORTUNITY_CONFIGS_VIEW_NAME = "workflows_opportunity_configs"

WORKFLOWS_OPPORTUNITY_CONFIGS_DESCRIPTION = (
    "Configurations for all Workflows opportunities"
)

# TODO(#19054): Unify the EligibilityQueryConfig and OpportunityExportConfig in this config class
@attr.s
class WorkflowsOpportunityConfig:
    # The state code the Workflow applies to
    state_code: StateCode = attr.ib()

    # The string used to represent the opportunity in the front-end
    opportunity_type: str = attr.ib()

    # The experiment_id corresponding to this opportunity, which is used in state_assignments
    experiment_id: str = attr.ib()


WORKFLOWS_OPPORTUNITY_CONFIGS = [
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="compliantReporting",
        experiment_id="US_TN_COMPLIANT_REPORTING_WORKFLOWS",
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="supervisionLevelDowngrade",
        experiment_id="US_TN_SUPERVISION_LEVEL_DOWNGRADE_WORKFLOWS",
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ID,
        opportunity_type="pastFTRD",
        experiment_id="US_ID_LSU_ED_DISCHARGE_WORKFLOWS",
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ID,
        opportunity_type="LSU",
        experiment_id="US_ID_LSU_ED_DISCHARGE_WORKFLOWS",
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ID,
        opportunity_type="usIdSupervisionLevelDowngrade",
        experiment_id="US_ID_SUPERVISION_LEVEL_DOWNGRADE_WORKFLOWS",
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ID,
        opportunity_type="earnedDischarge",
        experiment_id="US_ID_LSU_ED_DISCHARGE_WORKFLOWS",
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="pastFTRD",
        experiment_id="US_IX_LSU_ED_DISCHARGE_WORKFLOWS",
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="LSU",
        experiment_id="US_IX_LSU_ED_DISCHARGE_WORKFLOWS",
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="usIdSupervisionLevelDowngrade",
        experiment_id="US_IX_SUPERVISION_LEVEL_DOWNGRADE_WORKFLOWS",
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_IX,
        opportunity_type="earnedDischarge",
        experiment_id="US_IX_LSU_ED_DISCHARGE_WORKFLOWS",
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_TN,
        opportunity_type="usTnExpiration",
        experiment_id="",
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ME,
        opportunity_type="usMeSCCP",
        experiment_id="US_ME_SCCP_WORKFLOWS",
    ),
    WorkflowsOpportunityConfig(
        state_code=StateCode.US_ND,
        opportunity_type="earlyTermination",
        experiment_id="US_ND_EARLY_TERMINATION_WORKFLOWS",
    ),
]

WORKFLOWS_OPPORTUNITY_CONFIGS_QUERY_TEMPLATE = "UNION ALL".join(
    [
        f"""
SELECT
    "{config.state_code.value}" AS state_code,
    "{config.opportunity_type}" AS opportunity_type,
    "{config.experiment_id}" AS experiment_id,
"""
        for config in WORKFLOWS_OPPORTUNITY_CONFIGS
    ]
)

WORKFLOWS_OPPORTUNITY_CONFIGS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=REFERENCE_VIEWS_DATASET,
    view_id=WORKFLOWS_OPPORTUNITY_CONFIGS_VIEW_NAME,
    view_query_template=WORKFLOWS_OPPORTUNITY_CONFIGS_QUERY_TEMPLATE,
    description=WORKFLOWS_OPPORTUNITY_CONFIGS_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_OPPORTUNITY_CONFIGS_VIEW_BUILDER.build_and_print()
