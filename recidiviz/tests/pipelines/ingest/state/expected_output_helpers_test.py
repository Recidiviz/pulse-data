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
from recidiviz.persistence.entity.state import entities, normalized_entities
from recidiviz.pipelines.ingest.state.expected_output_helpers import (
    get_expected_output_normalized_entity_classes,
    get_expected_output_pre_normalization_entity_classes,
    get_pipeline_output_tables,
)
from recidiviz.tests.ingest.direct import fake_regions


class TestExpectedOutputHelpers(unittest.TestCase):
    """Tests for expected_output_helpers.py"""

    def test_expected_pipeline_output(self) -> None:
        expected_output_entities = {
            entities.StatePerson,
            entities.StatePersonExternalId,
            entities.StateIncarcerationSentence,
            entities.StateCharge,
            entities.StateSentence,
            entities.StateChargeV2,
        }

        expected_table_ids = {
            "state_person",
            "state_person_external_id",
            "state_incarceration_sentence",
            "state_charge",
            "state_charge_incarceration_sentence_association",
            "state_sentence",
            "state_charge_v2",
            "state_charge_v2_state_sentence_association",
        }
        self.assertEqual(
            expected_table_ids,
            get_pipeline_output_tables(expected_output_entities),
        )

    def test_expected_pipeline_output_child_in_association_not_present(self) -> None:
        expected_output_entities = {
            entities.StatePerson,
            entities.StatePersonExternalId,
            entities.StateSentence,
        }

        expected_table_ids = {
            "state_person",
            "state_person_external_id",
            "state_sentence",
        }
        self.assertEqual(
            expected_table_ids,
            get_pipeline_output_tables(expected_output_entities),
        )

    def test_expected_pipeline_output_normalized_entities(self) -> None:
        expected_output_entities = {
            normalized_entities.NormalizedStatePerson,
            normalized_entities.NormalizedStatePersonExternalId,
            normalized_entities.NormalizedStateIncarcerationSentence,
            normalized_entities.NormalizedStateCharge,
            normalized_entities.NormalizedStateSentence,
            normalized_entities.NormalizedStateChargeV2,
        }

        expected_table_ids = {
            "state_person",
            "state_person_external_id",
            "state_incarceration_sentence",
            "state_charge",
            "state_charge_incarceration_sentence_association",
            "state_sentence",
            "state_charge_v2",
            "state_charge_v2_state_sentence_association",
        }
        self.assertEqual(
            expected_table_ids,
            get_pipeline_output_tables(expected_output_entities),
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
            }
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
            }
        )

        self.assertEqual(
            {
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
                normalized_entities.NormalizedStateSentence,
                # TODO(#32306): This test should produce
                #  NormalizedStateSentenceGroupInferred once normalization starts
                #  producing those objects.
            },
            normalization_output_classes,
        )
