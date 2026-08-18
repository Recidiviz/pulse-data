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
"""Tests for Dataflow BigQuery source tables"""
import unittest
from unittest.mock import patch

from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.pipelines.pipeline_names import (
    IDENTITY_INGEST_PIPELINE_NAME,
    METRICS_PIPELINE_NAME,
    SUPPLEMENTAL_PIPELINE_NAME,
)
from recidiviz.source_tables.dataflow_output_table_collector import (
    get_dataflow_output_source_table_collections,
)
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineOutputSourceTableLabel,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)
from recidiviz.source_tables.source_table_repository import SourceTableRepository


class TestDataflowOutputTableCollector(unittest.TestCase):
    """Test for collecting dataflow source tables"""

    def setUp(self) -> None:
        super().setUp()

        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "recidiviz-456"

        self.source_table_repository = SourceTableRepository(
            source_table_collections=get_dataflow_output_source_table_collections()
        )

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_supplemental(self) -> None:
        """Tests the expected output schema of supplemental pipelines."""
        supplemental_collection = (
            self.source_table_repository.get_collection_with_labels(
                labels=[
                    DataflowPipelineOutputSourceTableLabel(
                        pipeline_name=SUPPLEMENTAL_PIPELINE_NAME
                    )
                ]
            )
        )

        self.assert_source_tables_match(
            supplemental_collection,
            expected_addresses=[
                "supplemental_data.us_ix_case_note_matched_entities",
                "supplemental_data.us_me_snoozed_opportunities",
            ],
        )

    def test_metrics(self) -> None:
        """Tests the expected output schema of metrics pipelines."""
        metric_collection = self.source_table_repository.get_collection_with_labels(
            labels=[
                DataflowPipelineOutputSourceTableLabel(
                    pipeline_name=METRICS_PIPELINE_NAME
                )
            ]
        )

        self.assert_source_tables_match(
            metric_collection,
            expected_addresses=[
                "dataflow_metrics.incarceration_admission_metrics",
                "dataflow_metrics.incarceration_commitment_from_supervision_metrics",
                "dataflow_metrics.incarceration_population_span_metrics",
                "dataflow_metrics.incarceration_release_metrics",
                "dataflow_metrics.program_participation_metrics",
                "dataflow_metrics.recidivism_rate_metrics",
                "dataflow_metrics.supervision_case_compliance_metrics",
                "dataflow_metrics.supervision_out_of_state_population_metrics",
                "dataflow_metrics.supervision_population_metrics",
                "dataflow_metrics.supervision_population_span_metrics",
                "dataflow_metrics.supervision_start_metrics",
                "dataflow_metrics.supervision_termination_metrics",
                "dataflow_metrics.violation_with_response_metrics",
            ],
        )

    def _identity_collections_with_dataset_suffix(
        self, suffix: str
    ) -> list[SourceTableCollection]:
        """Returns the identity ingest pipeline collections whose dataset id ends
        with |suffix|. The cluster and fragment collections share the identity
        pipeline label, so the dataset suffix distinguishes them."""
        return [
            collection
            for collection in self.source_table_repository.get_collections_with_labels(
                labels=[
                    DataflowPipelineOutputSourceTableLabel(
                        pipeline_name=IDENTITY_INGEST_PIPELINE_NAME
                    )
                ]
            )
            if collection.dataset_id.endswith(suffix)
        ]

    def test_identity_fragment(self) -> None:
        """Tests the expected output schema of the identity ingest pipeline's
        debug pre-clustering fragment output, one collection per tenant."""
        identity_collections = self._identity_collections_with_dataset_suffix(
            "_identity_fragment"
        )

        state_codes = get_direct_ingest_states_existing_in_env()
        self.assertEqual(len(identity_collections), len(state_codes))
        self.assertEqual(
            {c.dataset_id for c in identity_collections},
            {
                f"{state_code.value.lower()}_identity_fragment"
                for state_code in state_codes
            },
        )

        for collection in identity_collections:
            self.assert_source_tables_match(
                collection,
                expected_addresses=[
                    f"{collection.dataset_id}.identity_alias",
                    f"{collection.dataset_id}.identity_attributes",
                    f"{collection.dataset_id}.identity_email",
                    f"{collection.dataset_id}.identity_ethnicity",
                    f"{collection.dataset_id}.identity_external_id",
                    f"{collection.dataset_id}.identity_fragment",
                    f"{collection.dataset_id}.identity_gender",
                    f"{collection.dataset_id}.identity_name",
                    f"{collection.dataset_id}.identity_phone_number",
                    f"{collection.dataset_id}.identity_race",
                    f"{collection.dataset_id}.identity_sex",
                ],
            )

    def test_identity_cluster(self) -> None:
        """Tests the expected output schema of the identity ingest pipeline's
        cluster output, one regenerable collection per tenant. This is the
        public output the Identity Service consumes."""
        identity_collections = self._identity_collections_with_dataset_suffix(
            "_identity_cluster"
        )

        state_codes = get_direct_ingest_states_existing_in_env()
        self.assertEqual(len(identity_collections), len(state_codes))
        self.assertEqual(
            {c.dataset_id for c in identity_collections},
            {
                f"{state_code.value.lower()}_identity_cluster"
                for state_code in state_codes
            },
        )

        for collection in identity_collections:
            self.assertEqual(
                collection.update_config,
                SourceTableCollectionUpdateConfig.regenerable(),
            )
            self.assert_source_tables_match(
                collection,
                expected_addresses=[
                    f"{collection.dataset_id}.identity_cluster",
                    f"{collection.dataset_id}.identity_cluster_alias",
                    f"{collection.dataset_id}.identity_cluster_email",
                    f"{collection.dataset_id}.identity_cluster_ethnicity",
                    f"{collection.dataset_id}.identity_cluster_external_id",
                    f"{collection.dataset_id}.identity_cluster_gender",
                    f"{collection.dataset_id}.identity_cluster_name",
                    f"{collection.dataset_id}.identity_cluster_phone_number",
                    f"{collection.dataset_id}.identity_cluster_race",
                    f"{collection.dataset_id}.identity_cluster_sex",
                ],
            )

    def test_identity_rejections(self) -> None:
        """Tests the expected output schema of the identity ingest pipeline's
        rejections output, one regenerable collection per tenant holding the
        rejected_identity_cluster table."""
        identity_collections = self._identity_collections_with_dataset_suffix(
            "_identity_rejections"
        )

        state_codes = get_direct_ingest_states_existing_in_env()
        self.assertEqual(len(identity_collections), len(state_codes))
        self.assertEqual(
            {c.dataset_id for c in identity_collections},
            {
                f"{state_code.value.lower()}_identity_rejections"
                for state_code in state_codes
            },
        )

        for collection in identity_collections:
            self.assertEqual(
                collection.update_config,
                SourceTableCollectionUpdateConfig.regenerable(),
            )
            self.assert_source_tables_match(
                collection,
                expected_addresses=[
                    f"{collection.dataset_id}.rejected_identity_cluster"
                ],
            )

    def test_identity_ingest_view_results(self) -> None:
        """Tests the expected output schema of the identity ingest pipeline's
        ingest-view-results debug output, one collection per tenant."""
        identity_collections = self.source_table_repository.get_collections_with_labels(
            labels=[
                DataflowPipelineOutputSourceTableLabel(
                    pipeline_name=IDENTITY_INGEST_PIPELINE_NAME
                )
            ]
        )
        results_collections = [
            c
            for c in identity_collections
            if c.dataset_id.endswith("_identity_ingest_view_results")
        ]

        state_codes = get_direct_ingest_states_existing_in_env()
        self.assertEqual(len(results_collections), len(state_codes))
        self.assertEqual(
            {c.dataset_id for c in results_collections},
            {
                f"{state_code.value.lower()}_identity_ingest_view_results"
                for state_code in state_codes
            },
        )

    def assert_source_tables_match(
        self,
        normalized_collection: SourceTableCollection,
        expected_addresses: list[str],
    ) -> None:
        found_bq_tables = sorted(
            [
                source_table.address.to_str()
                for source_table in normalized_collection.source_tables
            ]
        )

        self.assertEqual(
            len(expected_addresses),
            len(set(expected_addresses)),
            f"Found duplicate values in expected_addresses: {expected_addresses}",
        )

        self.assertEqual(
            set(found_bq_tables),
            set(expected_addresses),
            f"The list of expected output tables for all of our Dataflow pipelines "
            f"has changed. If this test fails due to a newly added / deleted table "
            f"(e.g. a newly added ingest view), update the the "
            f"expected_addresses list accordingly: {expected_addresses}",
        )

        self.assertEqual(
            expected_addresses,
            sorted(expected_addresses),
            "The expected_addresses should be alphabetically sorted",
        )
