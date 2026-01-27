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
"""Tests for expected_output_helpers.py"""
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state import entities, normalized_entities
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.pipelines.ingest.state.expected_output_helpers import (
    get_expected_output_normalized_entity_classes,
    get_expected_output_pre_normalization_entity_classes,
    get_ingest_pipeline_output_tables_for_schema,
)
from recidiviz.pipelines.ingest.state.normalization.state_specific_normalization_delegate import (
    StateSpecificNormalizationDelegate,
)
from recidiviz.tests.ingest.direct import fake_regions


class TestExpectedOutputHelpers(unittest.TestCase):
    """Tests for expected_output_helpers.py"""

    def test_get_ingest_pipeline_output_tables_for_schema_state(self) -> None:
        expected_table_ids = {
            "state_assessment",
            "state_charge",
            "state_charge_incarceration_sentence_association",
            "state_charge_supervision_sentence_association",
            "state_charge_v2",
            "state_charge_v2_state_sentence_association",
            "state_drug_screen",
            "state_early_discharge",
            "state_employment_period",
            "state_incarceration_incident",
            "state_incarceration_incident_outcome",
            "state_incarceration_period",
            "state_incarceration_sentence",
            "state_person",
            "state_person_address_period",
            "state_person_alias",
            "state_person_external_id",
            "state_person_housing_status_period",
            "state_person_race",
            "state_person_staff_relationship_period",
            "state_program_assignment",
            "state_sentence",
            "state_sentence_group",
            "state_sentence_group_length",
            "state_sentence_length",
            "state_sentence_status_snapshot",
            "state_staff",
            "state_staff_caseload_type_period",
            "state_staff_external_id",
            "state_staff_location_period",
            "state_staff_role_period",
            "state_staff_supervisor_period",
            "state_supervision_case_type_entry",
            "state_scheduled_supervision_contact",
            "state_supervision_contact",
            "state_supervision_period",
            "state_supervision_sentence",
            "state_supervision_violated_condition_entry",
            "state_supervision_violation",
            "state_supervision_violation_response",
            "state_supervision_violation_response_decision_entry",
            "state_supervision_violation_type_entry",
            "state_task_deadline",
        }
        self.assertEqual(
            expected_table_ids,
            get_ingest_pipeline_output_tables_for_schema(entities),
        )

    def test_get_ingest_pipeline_output_tables_for_schema_normalized_state(
        self,
    ) -> None:
        expected_table_ids = {
            "state_assessment",
            "state_charge",
            "state_charge_incarceration_sentence_association",
            "state_charge_supervision_sentence_association",
            "state_charge_v2",
            "state_charge_v2_state_sentence_association",
            "state_drug_screen",
            "state_early_discharge",
            "state_employment_period",
            "state_incarceration_incident",
            "state_incarceration_incident_outcome",
            "state_incarceration_period",
            "state_incarceration_sentence",
            "state_person",
            "state_person_address_period",
            "state_person_alias",
            "state_person_external_id",
            "state_person_housing_status_period",
            "state_person_race",
            "state_person_staff_relationship_period",
            "state_program_assignment",
            "state_sentence",
            "state_sentence_group",
            "state_sentence_group_length",
            "state_sentence_imposed_group",
            "state_sentence_inferred_group",
            "state_sentence_length",
            "state_sentence_status_snapshot",
            "state_staff",
            "state_staff_caseload_type_period",
            "state_staff_external_id",
            "state_staff_location_period",
            "state_staff_role_period",
            "state_staff_supervisor_period",
            "state_supervision_case_type_entry",
            "state_scheduled_supervision_contact",
            "state_supervision_contact",
            "state_supervision_period",
            "state_supervision_sentence",
            "state_supervision_violated_condition_entry",
            "state_supervision_violation",
            "state_supervision_violation_response",
            "state_supervision_violation_response_decision_entry",
            "state_supervision_violation_type_entry",
            "state_task_deadline",
        }
        self.assertEqual(
            expected_table_ids,
            get_ingest_pipeline_output_tables_for_schema(normalized_entities),
        )

    def test_get_expected_output_pre_normalization_entity_classes(self) -> None:
        region = direct_ingest_regions.get_direct_ingest_region(
            region_code=StateCode.US_DD.value, region_module_override=fake_regions
        )
        ingest_view_manifest_collector = IngestViewManifestCollector(
            region=region,
            delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
        )
        all_ingest_views = ingest_view_manifest_collector.ingest_view_to_manifest.keys()
        pre_normalization_output_classes = (
            get_expected_output_pre_normalization_entity_classes(
                ingest_manifest_collector=ingest_view_manifest_collector,
                ingest_views_to_run=list(all_ingest_views),
            )
        )

        self.assertEqual(
            {
                entities.StatePerson,
                entities.StatePersonExternalId,
                entities.StateIncarcerationSentence,
                entities.StateSupervisionSentence,
                entities.StateCharge,
                entities.StateSentence,
                entities.StateChargeV2,
                entities.StateIncarcerationPeriod,
            },
            pre_normalization_output_classes,
        )

    def test_get_expected_output_pre_normalization_entity_classes_filtered_views(
        self,
    ) -> None:
        region = direct_ingest_regions.get_direct_ingest_region(
            region_code=StateCode.US_DD.value, region_module_override=fake_regions
        )
        ingest_view_manifest_collector = IngestViewManifestCollector(
            region=region,
            delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
        )
        pre_normalization_output_classes = (
            get_expected_output_pre_normalization_entity_classes(
                ingest_manifest_collector=ingest_view_manifest_collector,
                ingest_views_to_run=["ingest12"],
            )
        )

        self.assertEqual(
            {
                entities.StatePerson,
                entities.StatePersonExternalId,
            },
            pre_normalization_output_classes,
        )

    def test_get_expected_output_normalized_entity_classes(self) -> None:
        normalization_output_classes = get_expected_output_normalized_entity_classes(
            expected_output_pre_normalization_entity_classes={
                entities.StatePerson,
                entities.StatePersonExternalId,
            },
            delegate=StateSpecificNormalizationDelegate(),
        )

        self.assertEqual(
            {
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
            },
            normalization_output_classes,
        )

    def test_get_expected_output_inferred_groups(self) -> None:
        normalization_output_classes = get_expected_output_normalized_entity_classes(
            expected_output_pre_normalization_entity_classes={
                entities.StatePerson,
                entities.StatePersonExternalId,
                entities.StateSentence,
                entities.StateSentenceStatusSnapshot,
            },
            delegate=StateSpecificNormalizationDelegate(),
        )

        self.assertEqual(
            {
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
                normalized_entities.NormalizedStateSentence,
                normalized_entities.NormalizedStateSentenceStatusSnapshot,
                normalized_entities.NormalizedStateSentenceInferredGroup,
                normalized_entities.NormalizedStateSentenceImposedGroup,
            },
            normalization_output_classes,
        )

    def test_get_expected_output_normalized_entity_classes_state_specific_logic(
        self,
    ) -> None:
        class _CustomNormalizationDelegate(StateSpecificNormalizationDelegate):
            def extra_entities_generated_via_normalization(
                self, normalization_input_types: set[type[Entity]]
            ) -> set[type[NormalizedStateEntity]]:
                return {
                    normalized_entities.NormalizedStatePersonAlias,
                    normalized_entities.NormalizedStatePersonHousingStatusPeriod,
                }

        normalization_output_classes = get_expected_output_normalized_entity_classes(
            expected_output_pre_normalization_entity_classes={
                entities.StatePerson,
                entities.StatePersonExternalId,
            },
            delegate=_CustomNormalizationDelegate(),
        )

        self.assertEqual(
            {
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
                normalized_entities.NormalizedStatePersonAlias,
                normalized_entities.NormalizedStatePersonHousingStatusPeriod,
            },
            normalization_output_classes,
        )
