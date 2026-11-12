# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests that the shared external-id selection registry stays in lockstep with the
id types that can need a display/stable selection: the activity pipeline's
allowed-multiples allowlist, plus every id type an identity mapping emits (external
ids in the Identity Service are additive, so id churn can leave an identity with two
active ids of a type that never allows multiples within a single cluster)."""
import unittest

from recidiviz.common.constants.state.external_id_types import (
    US_CA_CDCNO,
    US_CA_DOC,
    US_CO_PARTYID,
    US_CO_STAFF_SERIAL_NUMBER,
    US_NE_ID_NBR,
    US_NYC_BOOKCASENUMBER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.external_id_selection import (
    shared_selection_tenant_and_id_type_keys,
)
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.identity_ingest_view_manifest_compiler_delegate import (
    IdentityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.pipelines.ingest.activity.multiple_external_id_helpers import (
    person_external_id_types_with_allowed_multiples_per_person,
    staff_external_id_types_with_allowed_multiples_per_person,
)

# The (state, id_type) pairs that allow multiple ids per entity but whose display/stable
# selection the shared module does not own, because it depends on id-active dates or
# ingest-set flags the Identity Service does not have. The activity pipeline selects these
# in its own normalization delegates instead.
_PIPELINE_ONLY_SELECTION_KEYS: set[tuple[StateCode, str]] = {
    # US_CA person ids: selected by recency.
    # TODO(OBT-53893): Remove when US_CA's activity pipeline moves to the Identity Service.
    (StateCode.US_CA, US_CA_DOC),
    (StateCode.US_CA, US_CA_CDCNO),
    # US_CO staff ids: chosen by flags set at ingest.
    # TODO(OBT-55091): Remove when US_CO's activity pipeline moves to the Identity Service.
    (StateCode.US_CO, US_CO_PARTYID),
    (StateCode.US_CO, US_CO_STAFF_SERIAL_NUMBER),
    # US_NE person ids: narrowed to the ingest-set flag, then selected by recency.
    # TODO(OBT-53894): Remove when US_NE's activity pipeline moves to the Identity Service.
    (StateCode.US_NE, US_NE_ID_NBR),
    # US_NYC person ids: chosen by a flag set at ingest.
    # TODO(OBT-53970): Remove when US_NYC's activity pipeline moves to the Identity Service.
    (StateCode.US_NYC, US_NYC_BOOKCASENUMBER),
}


def _shared_selection_keys_by_state_code() -> set[tuple[StateCode, str]]:
    """Returns the shared selection registry's keys, with each tenant mapped to its
    StateCode so they can be compared against the state-keyed allowed-multiples allowlist.
    """
    return {
        (tenant.to_state_code(), id_type)
        for tenant, id_type in shared_selection_tenant_and_id_type_keys()
    }


def _identity_emitted_selection_keys() -> set[tuple[StateCode, str]]:
    """Returns each (state, id_type) pair some identity ingest mapping emits. Every
    such pair can need a display/stable selection even when the id type never allows
    multiples within a single cluster, because external ids in the Identity Service
    are additive and id churn can leave an identity with two active ids of the type.
    """
    emitted_keys: set[tuple[StateCode, str]] = set()
    for state_code in get_existing_direct_ingest_states():
        region = direct_ingest_regions.get_direct_ingest_region(
            state_code.value.lower()
        )
        collector = IngestViewManifestCollector(
            region=region,
            delegate=IdentityIngestViewManifestCompilerDelegate(region=region),
            ingest_pipeline_type=IngestPipelineType.IDENTITY,
        )
        for manifest in collector.ingest_view_to_manifest.values():
            emitted_keys |= {
                (state_code, id_type)
                for id_type in manifest.root_entity_external_id_types
            }
    return emitted_keys


class TestSharedSelectionCoversAllowedMultiples(unittest.TestCase):
    """Tests the shared selection registry against the allowed-multiples allowlist."""

    def test_registry_and_pipeline_only_keys_partition_allowed_multiples(self) -> None:
        allowed_multiples_keys = {
            (state_code, id_type)
            for state_code in StateCode
            for id_type in (
                person_external_id_types_with_allowed_multiples_per_person(state_code)
                | staff_external_id_types_with_allowed_multiples_per_person(state_code)
            )
        }
        covered_keys = (
            _shared_selection_keys_by_state_code() | _PIPELINE_ONLY_SELECTION_KEYS
        )

        missing_selection = sorted(
            allowed_multiples_keys - covered_keys, key=lambda k: (k[0].value, k[1])
        )
        self.assertEqual(
            [],
            missing_selection,
            f"These (state, id_type) pairs allow multiple ids per entity but have no "
            f"display/stable selection: {missing_selection}. Add each to "
            f"_SELECTION_STRATEGY_BY_TENANT_AND_ID_TYPE in "
            f"recidiviz.common.external_id_selection if its selection is a pure function "
            f"of the id strings, or to _PIPELINE_ONLY_SELECTION_KEYS in this test if it "
            f"needs id-active dates or ingest-set flags.",
        )

        orphaned_selection = sorted(
            covered_keys - allowed_multiples_keys - _identity_emitted_selection_keys(),
            key=lambda k: (k[0].value, k[1]),
        )
        self.assertEqual(
            [],
            orphaned_selection,
            f"These (state, id_type) pairs have a display/stable selection but neither "
            f"allow multiple ids per entity nor are emitted by any identity mapping: "
            f"{orphaned_selection}. Remove each from "
            f"_SELECTION_STRATEGY_BY_TENANT_AND_ID_TYPE or _PIPELINE_ONLY_SELECTION_KEYS.",
        )

    def test_registry_and_pipeline_only_keys_do_not_overlap(self) -> None:
        self.assertEqual(
            set(),
            _shared_selection_keys_by_state_code() & _PIPELINE_ONLY_SELECTION_KEYS,
        )

    def test_no_id_type_allows_multiples_for_both_person_and_staff(self) -> None:
        for state_code in StateCode:
            self.assertEqual(
                set(),
                person_external_id_types_with_allowed_multiples_per_person(state_code)
                & staff_external_id_types_with_allowed_multiples_per_person(state_code),
                f"id_type allows multiples for both person and staff in "
                f"[{state_code.value}], so a (state, id_type) selection key would be "
                f"ambiguous.",
            )
