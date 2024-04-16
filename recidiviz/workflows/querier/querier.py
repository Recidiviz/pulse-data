#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  ============================================================================
"""Querier class to encapsulate requests to the Workflows postgres DBs."""
import logging
from functools import cached_property
from typing import Dict, List, Set, Union

import attr
from sqlalchemy.orm import sessionmaker

from recidiviz.calculator.query.state.views.outliers.workflows_enabled_states import (
    get_workflows_enabled_states,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema.workflows.schema import (
    Opportunity,
    OpportunityConfiguration,
    OpportunityStatus,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.workflows.types import (
    FullOpportunityConfig,
    FullOpportunityInfo,
    OpportunityConfig,
    OpportunityInfo,
    WorkflowsSystemType,
)
from recidiviz.workflows.utils.utils import (
    get_config_for_opportunity,
    get_system_for_opportunity,
)


@attr.s(auto_attribs=True)
class WorkflowsQuerier:
    """Implements Querier abstractions for Workflows data sources"""

    state_code: StateCode = attr.ib()
    database_manager: StateSegmentedDatabaseManager = attr.ib(
        factory=lambda: StateSegmentedDatabaseManager(  # type: ignore
            get_workflows_enabled_states(), SchemaType.WORKFLOWS
        )
    )

    @cached_property
    def database_session(self) -> sessionmaker:
        return self.database_manager.get_session(self.state_code)

    def get_opportunities(self) -> List[FullOpportunityInfo]:
        """Returns all opportunities configured in a state
        irrespective of feature-variant gating or system."""
        with self.database_session() as session:
            opportunities = session.query(Opportunity).with_entities(
                Opportunity.state_code,
                Opportunity.opportunity_type,
                Opportunity.gating_feature_variant,
                Opportunity.updated_at,
                Opportunity.updated_by,
            )

            infos: List[FullOpportunityInfo] = []

            for opportunity in opportunities:
                config = get_config_for_opportunity(opportunity.opportunity_type)

                infos.append(
                    FullOpportunityInfo(
                        state_code=opportunity.state_code,
                        opportunity_type=opportunity.opportunity_type,
                        gating_feature_variant=opportunity.gating_feature_variant,
                        url_section=config.opportunity_type_path_str,
                        firestore_collection=config.export_collection_name,
                        system_type=get_system_for_opportunity(
                            opportunity.opportunity_type
                        ),
                        completion_event=str(config.task_completion_event),
                        experiment_id=config.experiment_id,
                        last_updated_at=opportunity.updated_at,
                        last_updated_by=opportunity.updated_by,
                    )
                )

            return infos

    def get_enabled_opportunities(
        self,
        allowed_systems: List[WorkflowsSystemType],
        active_feature_variants: List[str],
    ) -> List[OpportunityInfo]:
        """Returns opportunities enabled for the state given the allowed systems
        and active feature variants"""

        opportunities = self.get_opportunities()

        fv_set: Set[Union[str, None]] = set(active_feature_variants)
        fv_set.add(None)

        return [
            opp
            for opp in opportunities
            if opp.gating_feature_variant in fv_set
            and opp.system_type in allowed_systems
        ]

    def get_active_configs_for_opportunity_types(
        self, opportunity_types: List[str]
    ) -> List[FullOpportunityConfig]:
        """Returns all active configs for the given opportunity types. Does not
        filter based on active feature variants."""
        with self.database_session() as session:
            configs = session.query(OpportunityConfiguration).filter(
                OpportunityConfiguration.opportunity_type.in_(opportunity_types),
                OpportunityConfiguration.status == OpportunityStatus.ACTIVE,
            )

            return [
                FullOpportunityConfig(
                    id=config.id,
                    state_code=config.state_code,
                    opportunity_type=config.opportunity_type,
                    created_by=config.created_by,
                    created_at=config.created_at,
                    description=config.description,
                    status=config.status,
                    display_name=config.display_name,
                    methodology_url=config.methodology_url,
                    initial_header=config.initial_header,
                    denial_reasons=config.denial_reasons,
                    eligible_criteria_copy=config.eligible_criteria_copy,
                    ineligible_criteria_copy=config.ineligible_criteria_copy,
                    dynamic_eligibility_text=config.dynamic_eligibility_text,
                    call_to_action=config.call_to_action,
                    snooze=config.snooze,
                    feature_variant=config.feature_variant,
                    is_alert=config.is_alert,
                    denial_text=config.denial_text,
                    sidebar_components=config.sidebar_components,
                )
                for config in configs
            ]

    def get_top_config_for_opportunity_types(
        self, opportunity_types: List[str], active_feature_variants: List[str]
    ) -> Dict[str, OpportunityConfig]:
        """Returns one OpportunityConfig for each provided opportunity type.
        If multiple configs are active for a provided opportunity type, we return
        a config whose gating feature variant is among the provided active
        feature variants. If none match, we return the config with no gating
        feature variant set.
        """
        active_configs = self.get_active_configs_for_opportunity_types(
            opportunity_types
        )

        config_map: Dict[str, OpportunityConfig] = {}

        for opportunity_type in opportunity_types:
            configs = [
                c for c in active_configs if c.opportunity_type == opportunity_type
            ]

            gated_configs = [
                c for c in configs if c.feature_variant in active_feature_variants
            ]

            if len(gated_configs) > 0:
                # there should only be one gated config for a user
                # in the case of two, use a deterministic sort
                if len(gated_configs) > 1:
                    relevant_fvs = ",".join(c.feature_variant for c in gated_configs)
                    logging.warning(
                        "Multiple gated configs returned for %s. Relevant FVs: %s",
                        opportunity_type,
                        relevant_fvs,
                    )
                    gated_configs.sort(key=lambda c: c.created_at)

                config_map[opportunity_type] = OpportunityConfig.from_full_config(
                    gated_configs[0]
                )
            else:
                default_configs = [c for c in configs if c.feature_variant is None]
                if len(default_configs) == 0:
                    logging.error("No default config set for  %s", opportunity_type)
                elif len(default_configs) > 1:
                    logging.error(
                        "Multiple (%d) default configs found for %s. Using most recent.",
                        len(default_configs),
                        opportunity_type,
                    )
                    default_configs.sort(key=lambda c: c.created_at)
                    config_map[opportunity_type] = OpportunityConfig.from_full_config(
                        default_configs[-1]
                    )
                else:
                    config_map[opportunity_type] = OpportunityConfig.from_full_config(
                        default_configs[0]
                    )

        return config_map
