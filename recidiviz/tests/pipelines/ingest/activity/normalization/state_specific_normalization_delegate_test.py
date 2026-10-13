# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for StateSpecificNormalizationDelegate"""
import unittest

from recidiviz.common.constants.state.external_id_types import US_ND_ELITE
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.ingest_mappings.activity_ingest_view_manifest_compiler_delegate import (
    ActivityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.persistence.entity.activity.entities import StatePersonExternalId
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.pipelines.ingest.activity.expected_output_helpers import (
    get_expected_output_normalized_entity_classes,
    get_expected_output_pre_normalization_entity_classes,
)
from recidiviz.pipelines.ingest.activity.normalization.state_specific_normalization_delegate import (
    StateSpecificNormalizationDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_normalization_delegate,
)
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS
from recidiviz.utils.types import assert_subclass_list


class TestStateSpecificNormalizationDelegate(unittest.TestCase):
    """Tests for StateSpecificNormalizationDelegate"""

    def test_select_display_id_for_person_raises_if_fewer_than_2_ids(self) -> None:
        delegate = StateSpecificNormalizationDelegate()
        with self.assertRaisesRegex(ValueError, "fewer than 2 ids"):
            delegate.select_display_id_for_person_external_ids_of_type(
                state_code=StateCode.US_XX,
                id_type="US_XX_SOME_ID",
                person_external_ids_of_type=[],
            )

    def test_select_stable_id_for_person_raises_if_fewer_than_2_ids(self) -> None:
        delegate = StateSpecificNormalizationDelegate()
        with self.assertRaisesRegex(ValueError, "fewer than 2 ids"):
            delegate.select_stable_id_for_person_external_ids_of_type(
                state_code=StateCode.US_XX,
                id_type="US_XX_SOME_ID",
                person_external_ids_of_type=[],
            )

    def test_select_display_id_for_staff_raises_if_fewer_than_2_ids(self) -> None:
        delegate = StateSpecificNormalizationDelegate()
        with self.assertRaisesRegex(ValueError, "fewer than 2 ids"):
            delegate.select_display_id_for_staff_external_ids_of_type(
                state_code=StateCode.US_XX,
                id_type="US_XX_SOME_ID",
                staff_external_ids_of_type=[],
            )

    def test_select_stable_id_for_staff_raises_if_fewer_than_2_ids(self) -> None:
        delegate = StateSpecificNormalizationDelegate()
        with self.assertRaisesRegex(ValueError, "fewer than 2 ids"):
            delegate.select_stable_id_for_staff_external_ids_of_type(
                state_code=StateCode.US_XX,
                id_type="US_XX_SOME_ID",
                staff_external_ids_of_type=[],
            )

    @staticmethod
    def _make_person_external_id(
        *,
        external_id: str,
        state_code: StateCode,
        id_type: str,
        is_current_display_id_for_type: bool | None = None,
        is_stable_id_for_type: bool | None = None,
    ) -> StatePersonExternalId:
        return StatePersonExternalId(
            person_external_id_id=1,
            state_code=state_code.value,
            external_id=external_id,
            id_type=id_type,
            is_current_display_id_for_type=is_current_display_id_for_type,
            is_stable_id_for_type=is_stable_id_for_type,
            id_active_from_datetime=None,
            id_active_to_datetime=None,
        )

    def test_selects_display_and_stable_via_shared_module_for_registered_state(
        self,
    ) -> None:
        """A state with no ingest-set flags defers to the shared selection module,
        which picks the alphabetically highest id as the display id and the lowest as
        the stable id for US_ND_ELITE.
        """
        delegate = StateSpecificNormalizationDelegate()
        ids = [
            self._make_person_external_id(
                external_id="B", state_code=StateCode.US_ND, id_type=US_ND_ELITE
            ),
            self._make_person_external_id(
                external_id="A", state_code=StateCode.US_ND, id_type=US_ND_ELITE
            ),
            self._make_person_external_id(
                external_id="C", state_code=StateCode.US_ND, id_type=US_ND_ELITE
            ),
        ]
        self.assertEqual(
            "C",
            delegate.select_display_id_for_person_external_ids_of_type(
                state_code=StateCode.US_ND,
                id_type=US_ND_ELITE,
                person_external_ids_of_type=ids,
            ).external_id,
        )
        self.assertEqual(
            "A",
            delegate.select_stable_id_for_person_external_ids_of_type(
                state_code=StateCode.US_ND,
                id_type=US_ND_ELITE,
                person_external_ids_of_type=ids,
            ).external_id,
        )

    def test_raises_when_registered_id_type_also_hydrates_display_flag(self) -> None:
        """A registry entry and ingest-set flags are mutually exclusive. If a registered
        (tenant, id_type) also hydrates the flag, the base delegate raises rather than
        silently honoring the flag while the Identity Service picks via the shared module.
        """
        delegate = StateSpecificNormalizationDelegate()
        ids = [
            self._make_person_external_id(
                external_id="A",
                state_code=StateCode.US_ND,
                id_type=US_ND_ELITE,
                is_current_display_id_for_type=True,
            ),
            self._make_person_external_id(
                external_id="C",
                state_code=StateCode.US_ND,
                id_type=US_ND_ELITE,
                is_current_display_id_for_type=False,
            ),
        ]
        with self.assertRaisesRegex(
            ValueError,
            r"has both an ingest-set display/stable flag and a shared selection strategy",
        ):
            delegate.select_display_id_for_person_external_ids_of_type(
                state_code=StateCode.US_ND,
                id_type=US_ND_ELITE,
                person_external_ids_of_type=ids,
            )

    def test_uses_pre_hydrated_display_flag_for_unregistered_id_type(self) -> None:
        """For an id_type with no shared selection strategy, the base delegate honors the
        id ingest marked as the display id.
        """
        delegate = StateSpecificNormalizationDelegate()
        ids = [
            self._make_person_external_id(
                external_id="A",
                state_code=StateCode.US_XX,
                id_type="US_XX_SOME_ID",
                is_current_display_id_for_type=True,
            ),
            self._make_person_external_id(
                external_id="Z",
                state_code=StateCode.US_XX,
                id_type="US_XX_SOME_ID",
                is_current_display_id_for_type=False,
            ),
        ]
        self.assertEqual(
            "A",
            delegate.select_display_id_for_person_external_ids_of_type(
                state_code=StateCode.US_XX,
                id_type="US_XX_SOME_ID",
                person_external_ids_of_type=ids,
            ).external_id,
        )

    def test_extra_entities_generated_via_normalization(self) -> None:
        """Checks that the extra_entities_generated_via_normalization() function does
        not crash for each state and also does not return any extra entities we would
        have already inferred from the input entities.
        """
        for state_code in get_existing_direct_ingest_states():
            state_specific_delegate = get_state_specific_normalization_delegate(
                state_code.value
            )

            region = get_direct_ingest_region(region_code=state_code.value)
            ingest_manifest_collector = IngestViewManifestCollector(
                region=region,
                delegate=ActivityIngestViewManifestCompilerDelegate(region=region),
                ingest_pipeline_type=IngestPipelineType.ACTIVITY,
            )
            for project_id in DATA_PLATFORM_GCP_PROJECTS:
                ingest_view_context = IngestViewContentsContext.build_for_project(
                    project_id=project_id,
                    is_sandbox=False,
                    state_code=state_code,
                )
                # These are the entity types that are output from ingest mappings
                normalization_input_types = get_expected_output_pre_normalization_entity_classes(
                    ingest_manifest_collector,
                    ingest_views_to_run=ingest_manifest_collector.launchable_ingest_views(
                        ingest_view_context
                    ),
                )

                # These are the types that would be output from ingest, given no custom
                # logic in extra_entities_generated_via_normalization().
                generic_normalization_output_types = (
                    get_expected_output_normalized_entity_classes(
                        normalization_input_types,
                        delegate=StateSpecificNormalizationDelegate(),
                    )
                )

                # These are the extra entities that this state identifies as ones that
                # are output just by normalization.
                extra_normalized_types = set(
                    assert_subclass_list(
                        state_specific_delegate.extra_entities_generated_via_normalization(
                            normalization_input_types
                        ),
                        Entity,
                    )
                )

                if extraneous_types := extra_normalized_types.intersection(
                    generic_normalization_output_types
                ):
                    raise ValueError(
                        f"Found types returned by "
                        f"extra_entities_generated_via_normalization() for "
                        f"{state_code.value} in {project_id} which are already "
                        f"expected to be generated by normalization logic: "
                        f"{sorted(t.__name__ for t in extraneous_types)} This function "
                        f"should only return types that are uniquely generated by "
                        f"normalization logic for this state given normalization input "
                        f"types: {sorted(t.__name__ for t in normalization_input_types)}"
                    )
