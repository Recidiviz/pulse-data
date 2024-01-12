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
"""Tests for exemptions.py."""
import unittest
from collections import defaultdict
from typing import Dict, List, Set

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_controller_factory import (
    DirectIngestControllerFactory,
)
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IngestViewManifestCompilerDelegateImpl,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities as entities_schema
from recidiviz.pipelines.ingest.state.exemptions import (
    ENTITY_UNIQUE_CONSTRAINT_ERROR_EXEMPTIONS,
    GLOBAL_UNIQUE_CONSTRAINT_ERROR_EXEMPTIONS,
    INGEST_VIEW_ORDER_EXEMPTIONS,
    INGEST_VIEW_TREE_MERGER_ERROR_EXEMPTIONS,
)


class TestExemptions(unittest.TestCase):
    """Unit tests for exemptions.py"""

    def test_ingest_view_tree_merger_names_match_launched_ingest_views(self) -> None:
        state_codes = get_existing_direct_ingest_states()
        state_code_to_launchable_views: Dict[StateCode, Set[str]] = defaultdict(set)
        for state_code in state_codes:
            region = get_direct_ingest_region(region_code=state_code.value)
            ingest_manifest_collector = IngestViewManifestCollector(
                region=region,
                delegate=IngestViewManifestCompilerDelegateImpl(
                    region=region, schema_type=SchemaType.STATE
                ),
            )
            all_launchable_views = ingest_manifest_collector.launchable_ingest_views(
                ingest_instance=DirectIngestInstance.PRIMARY, is_dataflow_pipeline=True
            )
            state_code_to_launchable_views[state_code] = set(all_launchable_views)

        for (
            state_code,
            exempted_views,
        ) in INGEST_VIEW_TREE_MERGER_ERROR_EXEMPTIONS.items():
            self.assertTrue(state_code in state_code_to_launchable_views)
            exempted_difference = exempted_views.difference(
                state_code_to_launchable_views[state_code]
            )
            self.assertEqual(0, len(exempted_difference))

    def test_global_unique_constraint_exemption_names_match_defined_constraints(
        self,
    ) -> None:
        global_constraints_to_exempt = {
            constraint_name
            for _, constraint_names in GLOBAL_UNIQUE_CONSTRAINT_ERROR_EXEMPTIONS.items()
            for constraint_name in constraint_names
        }

        global_constraints: Set[str] = set()
        all_entities = get_all_entity_classes_in_module(entities_schema)
        for entity in all_entities:
            for constraint in entity.global_unique_constraints():
                global_constraints.add(constraint.name)

        exempt_difference = global_constraints_to_exempt.difference(global_constraints)
        self.assertEqual(0, len(exempt_difference))

    def test_entity_unique_constraint_exemption_names_match_defined_constraints(
        self,
    ) -> None:
        entity_constraints_to_exempt = {
            constraint_name
            for _, constraint_names in ENTITY_UNIQUE_CONSTRAINT_ERROR_EXEMPTIONS.items()
            for constraint_name in constraint_names
        }

        entity_constraints: Set[str] = set()
        all_entities = get_all_entity_classes_in_module(entities_schema)
        for entity in all_entities:
            for constraint in entity.entity_tree_unique_constraints():
                entity_constraints.add(constraint.name)

        exempt_difference = entity_constraints_to_exempt.difference(entity_constraints)
        self.assertEqual(0, len(exempt_difference))

    def test_ingest_view_order_exemption_lists_match_launched_ingest_views(
        self,
    ) -> None:
        state_codes = get_existing_direct_ingest_states()
        state_code_to_launchable_views: Dict[StateCode, Set[str]] = defaultdict(set)
        for state_code in state_codes:
            region = get_direct_ingest_region(region_code=state_code.value)
            ingest_manifest_collector = IngestViewManifestCollector(
                region=region,
                delegate=IngestViewManifestCompilerDelegateImpl(
                    region=region, schema_type=SchemaType.STATE
                ),
            )
            all_launchable_views = ingest_manifest_collector.launchable_ingest_views(
                ingest_instance=DirectIngestInstance.PRIMARY, is_dataflow_pipeline=True
            )
            state_code_to_launchable_views[state_code] = set(all_launchable_views)

        for (
            state_code,
            ordered_views,
        ) in INGEST_VIEW_ORDER_EXEMPTIONS.items():
            self.assertTrue(state_code in state_code_to_launchable_views)
            ordered_difference = set(ordered_views).difference(
                state_code_to_launchable_views[state_code]
            )
            self.assertEqual(0, len(ordered_difference))

    def test_ingest_view_order_list_matches_controller_rank_lists(self) -> None:
        state_codes = get_existing_direct_ingest_states()
        state_code_to_ingest_view_rank_list: Dict[StateCode, List[str]] = defaultdict(
            list
        )
        for state_code in state_codes:
            region = get_direct_ingest_region(region_code=state_code.value)
            controller_cls = DirectIngestControllerFactory.get_controller_class(region)
            state_code_to_ingest_view_rank_list[
                state_code
            ] = controller_cls._get_ingest_view_rank_list(  # pylint: disable=protected-access
                DirectIngestInstance.PRIMARY
            )

        for (
            state_code,
            ordered_views,
        ) in INGEST_VIEW_ORDER_EXEMPTIONS.items():
            self.assertTrue(state_code in state_code_to_ingest_view_rank_list)
            if state_code in [StateCode.US_ND, StateCode.US_ME]:
                # US_ND & US_ME have ingest views that are deployed to ingest in Dataflow only
                continue
            self.assertListEqual(
                ordered_views, state_code_to_ingest_view_rank_list[state_code]
            )
