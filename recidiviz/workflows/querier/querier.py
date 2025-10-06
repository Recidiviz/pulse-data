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
import datetime
import logging
from functools import cached_property
from typing import Any, Dict, List, Optional, Set, Union

import attr
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert
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
    WorkflowsSystemType,
)
from recidiviz.workflows.utils.utils import get_configs, get_system_for_config


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
            db_opportunities = {
                opp.opportunity_type: opp
                for opp in session.query(Opportunity).with_entities(
                    Opportunity.state_code,
                    Opportunity.opportunity_type,
                    Opportunity.gating_feature_variant,
                    Opportunity.homepage_position,
                    Opportunity.updated_at,
                    Opportunity.updated_by,
                )
            }

            infos: List[FullOpportunityInfo] = []

            for config in get_configs():
                normalized_state_code = config.state_code
                if normalized_state_code == StateCode.US_IX:
                    normalized_state_code = StateCode.US_ID

                if normalized_state_code.value != self.state_code.value:
                    continue

                if config.opportunity_type in db_opportunities:
                    opportunity = db_opportunities[config.opportunity_type]
                    infos.append(
                        FullOpportunityInfo(
                            state_code=normalized_state_code.value,
                            opportunity_type=config.opportunity_type,
                            gating_feature_variant=opportunity.gating_feature_variant,
                            url_section=config.opportunity_type_path_str,
                            firestore_collection=config.export_collection_name,
                            system_type=get_system_for_config(config),
                            homepage_position=opportunity.homepage_position,
                            completion_event=str(config.task_completion_event),
                            experiment_id=config.experiment_id,
                            last_updated_at=opportunity.updated_at,
                            last_updated_by=opportunity.updated_by,
                        )
                    )
                else:
                    infos.append(
                        FullOpportunityInfo(
                            state_code=normalized_state_code.value,
                            opportunity_type=config.opportunity_type,
                            gating_feature_variant=None,
                            url_section=config.opportunity_type_path_str,
                            firestore_collection=config.export_collection_name,
                            system_type=get_system_for_config(config),
                            homepage_position=None,
                            completion_event=str(config.task_completion_event),
                            experiment_id=config.experiment_id,
                            last_updated_at=None,
                            last_updated_by=None,
                        )
                    )

            return infos

    def get_enabled_opportunities(
        self,
        allowed_systems: List[WorkflowsSystemType],
        active_feature_variants: List[str],
    ) -> List[FullOpportunityInfo]:
        """Returns opportunities enabled for the state given the allowed systems
        and active feature variants"""

        opportunities = self.get_opportunities()

        fv_set: Set[Union[str, None]] = set(active_feature_variants)
        fv_set.add(None)

        return [
            opp
            for opp in opportunities
            if opp.last_updated_at is not None
            and opp.gating_feature_variant in fv_set
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

            return [FullOpportunityConfig.from_db_entry(config) for config in configs]

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
                    logging.warning("No default config set for  %s", opportunity_type)
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

    def get_configs_for_type(
        self,
        opportunity_type: str,
        offset: int = 0,
        limit: int = 10,
        status: Optional[OpportunityStatus] = None,
    ) -> List[FullOpportunityConfig]:
        """
        Given an opportunity type, returns all stored configs with the most-recent first.
        """
        with self.database_session() as session:
            configs = session.query(OpportunityConfiguration).filter(
                OpportunityConfiguration.opportunity_type == opportunity_type,
            )

            if status is not None:
                configs = configs.filter(OpportunityConfiguration.status == status)

            configs = (
                configs.order_by(OpportunityConfiguration.created_at.desc())
                .offset(offset)
                .limit(limit)
            )

            return [FullOpportunityConfig.from_db_entry(config) for config in configs]

    def get_config_for_id(
        self, opportunity_type: str, config_id: int
    ) -> Optional[FullOpportunityConfig]:
        """
        Given an id and an opportunity type, returns the config with that id, if any
        """
        with self.database_session() as session:
            # The id column is an autoincrementing primary key. It is guaranteed to be
            # unique by the DB. This query will always find 0 or 1 documents.
            config = (
                session.query(OpportunityConfiguration)
                .filter(
                    OpportunityConfiguration.opportunity_type == opportunity_type,
                    OpportunityConfiguration.id == config_id,
                )
                .first()
            )

            if config is None:
                return None

            return FullOpportunityConfig.from_db_entry(config)

    def update_opportunity(
        self,
        opportunity_type: str,
        updated_by: str,
        updated_at: datetime.datetime,
        gating_feature_variant: Optional[str],
        homepage_position: int,
    ) -> None:
        """
        Updates the opportunity record itself, creating it if it doesn't yet exist.
        """
        with self.database_session() as session:
            known_types = [o.opportunity_type for o in get_configs()]
            if opportunity_type not in known_types:
                raise ValueError(
                    "Opportunity type does not exist in WORKFLOWS_OPPORTUNITY_CONFIGS"
                )

            session.execute(
                insert(Opportunity)
                .values(
                    state_code=self.state_code.value,
                    opportunity_type=opportunity_type,
                    updated_by=updated_by,
                    updated_at=updated_at,
                    gating_feature_variant=gating_feature_variant,
                    homepage_position=homepage_position,
                )
                .on_conflict_do_update(
                    index_elements=["state_code", "opportunity_type"],
                    set_={
                        "updated_by": updated_by,
                        "updated_at": updated_at,
                        "gating_feature_variant": gating_feature_variant,
                        "homepage_position": homepage_position,
                    },
                )
            )
            session.commit()

    def add_config(
        self,
        opportunity_type: str,
        *,
        created_by: str,
        created_at: datetime.datetime,
        variant_description: str,
        revision_description: str,
        feature_variant: Optional[str],
        display_name: str,
        priority: str,
        methodology_url: str,
        is_alert: bool,
        initial_header: Optional[str],
        denial_reasons: list[dict[str, str]],
        eligible_criteria_copy: list[dict[str, str]],
        ineligible_criteria_copy: list[dict[str, str]],
        dynamic_eligibility_text: str,
        eligibility_date_text: Optional[str],
        hide_denial_revert: bool,
        tooltip_eligibility_text: Optional[str],
        call_to_action: Optional[str],
        subheading: Optional[str],
        denial_text: Optional[str],
        snooze: Optional[dict[str, Any]],
        sidebar_components: list[str],
        tab_groups: Optional[list[dict[str, Any]]],
        compare_by: Optional[list[Any]],
        notifications: list[Any],
        staging_id: Optional[str] = None,
        zero_grants_tooltip: Optional[str],
        denied_tab_title: Optional[str],
        denial_adjective: Optional[str],
        denial_noun: Optional[str],
        supports_submitted: bool,
        supports_ineligible: bool,
        submitted_tab_title: Optional[str],
        empty_tab_copy: list[dict[str, str]],
        tab_preface_copy: list[dict[str, str]],
        subcategory_headings: list[dict[str, str]],
        subcategory_orderings: list[dict[str, list[str]]],
        mark_submitted_options_by_tab: list[dict[str, list[str]]],
        oms_criteria_header: Optional[str],
        non_oms_criteria_header: Optional[str],
        non_oms_criteria: list[dict[str, str]],
        highlight_cases_on_homepage: bool,
        highlighted_case_cta_copy: Optional[str],
        overdue_opportunity_callout_copy: Optional[str],
        snooze_companion_opportunity_types: Optional[list[str]],
        case_notes_title: Optional[str],
    ) -> int:
        """
        Given an opportunity type and a config, adds that config to the database,
        deactivating any existing configs with the same gating for the given opportunity.

        Takes as kwargs every attribute of FullOpportunityConfig except for
        state_code, status, opportunity_type, and id. staging_id is optional.
        """
        with self.database_session() as session:
            # This level of transaction isolation is required to ensure that another
            # configuration isn't created during our transaction and therefore missed
            # by our update statement.
            # There is no retry logic, so a serialization failure will cause the whole request to fail
            session.connection(execution_options={"isolation_level": "SERIALIZABLE"})

            insert_statement = (
                insert(OpportunityConfiguration)
                .values(
                    state_code=self.state_code.value,
                    opportunity_type=opportunity_type,
                    status=OpportunityStatus.ACTIVE,
                    created_by=created_by,
                    priority=priority,
                    created_at=created_at,
                    variant_description=variant_description,
                    revision_description=revision_description,
                    feature_variant=feature_variant,
                    display_name=display_name,
                    methodology_url=methodology_url,
                    is_alert=is_alert,
                    initial_header=initial_header,
                    denial_reasons=denial_reasons,
                    eligible_criteria_copy=eligible_criteria_copy,
                    ineligible_criteria_copy=ineligible_criteria_copy,
                    dynamic_eligibility_text=dynamic_eligibility_text,
                    eligibility_date_text=eligibility_date_text,
                    hide_denial_revert=hide_denial_revert,
                    tooltip_eligibility_text=tooltip_eligibility_text,
                    call_to_action=call_to_action,
                    subheading=subheading,
                    denial_text=denial_text,
                    snooze=snooze,
                    sidebar_components=sidebar_components,
                    tab_groups=tab_groups,
                    compare_by=compare_by,
                    notifications=notifications,
                    staging_id=staging_id,
                    zero_grants_tooltip=zero_grants_tooltip,
                    denied_tab_title=denied_tab_title,
                    denial_adjective=denial_adjective,
                    denial_noun=denial_noun,
                    supports_submitted=supports_submitted,
                    supports_ineligible=supports_ineligible,
                    submitted_tab_title=submitted_tab_title,
                    empty_tab_copy=empty_tab_copy,
                    tab_preface_copy=tab_preface_copy,
                    subcategory_headings=subcategory_headings,
                    subcategory_orderings=subcategory_orderings,
                    mark_submitted_options_by_tab=mark_submitted_options_by_tab,
                    oms_criteria_header=oms_criteria_header,
                    non_oms_criteria_header=non_oms_criteria_header,
                    non_oms_criteria=non_oms_criteria,
                    highlight_cases_on_homepage=highlight_cases_on_homepage,
                    highlighted_case_cta_copy=highlighted_case_cta_copy,
                    overdue_opportunity_callout_copy=overdue_opportunity_callout_copy,
                    snooze_companion_opportunity_types=snooze_companion_opportunity_types,
                    case_notes_title=case_notes_title,
                )
                .returning(OpportunityConfiguration.id)
            )

            config_id = session.execute(insert_statement).scalar()

            # Deactivate all configs with matching gating
            update_statement = (
                update(OpportunityConfiguration)
                .filter(
                    OpportunityConfiguration.opportunity_type == opportunity_type,
                    OpportunityConfiguration.feature_variant == feature_variant,
                    OpportunityConfiguration.id != config_id,
                )
                .values(status=OpportunityStatus.INACTIVE)
            )

            session.execute(update_statement)
            session.commit()

            return config_id

    def activate_config(self, opportunity_type: str, config_id: int) -> int:
        """
        Given an opportunity type and a config id, if a matching deactivated config exists,
        create a new copy of that config with its status set to active and deactivate any
        existing configs for this opportunity with the same feature variant gating. Raises
        an exception if the config does not exist or the config is already active.
        """
        config = self.get_config_for_id(opportunity_type, config_id)

        if config is None:
            raise ValueError("Config does not exist")
        if config.status == OpportunityStatus.ACTIVE:
            raise ValueError("Config is already active")

        add_config_args = config.__dict__.copy()
        for field in ("state_code", "id", "status"):
            add_config_args.pop(field)

        # add_config will create a new activated config and deactivate existing configs with the same feature gating
        return self.add_config(**add_config_args)

    def deactivate_config(self, opportunity_type: str, config_id: int) -> None:
        """
        Given an opportunity type and a config id, deactivates that config in the database.
        Raises an exception if the config does not exist, the config is already inactive,
        or the config is the default config for the opportunity (feature_variant=None).
        """
        with self.database_session() as session:
            config = self.get_config_for_id(opportunity_type, config_id)

            if config is None:
                raise ValueError("Config does not exist")
            if config.status == OpportunityStatus.INACTIVE:
                raise ValueError("Config is already inactive")
            if config.feature_variant is None:
                raise ValueError("Cannot deactivate default config")

            update_statement = (
                update(OpportunityConfiguration)
                .filter(
                    OpportunityConfiguration.opportunity_type == opportunity_type,
                    OpportunityConfiguration.id == config_id,
                )
                .values(status=OpportunityStatus.INACTIVE)
            )

            session.execute(update_statement)
            session.commit()
