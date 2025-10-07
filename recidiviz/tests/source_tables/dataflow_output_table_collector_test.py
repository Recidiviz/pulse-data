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

from recidiviz.pipelines.pipeline_names import (
    METRICS_PIPELINE_NAME,
    SUPPLEMENTAL_PIPELINE_NAME,
)
from recidiviz.source_tables.dataflow_output_table_collector import (
    get_dataflow_output_source_table_collections,
)
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineSourceTableLabel,
    SourceTableCollection,
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
                    DataflowPipelineSourceTableLabel(
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
                DataflowPipelineSourceTableLabel(pipeline_name=METRICS_PIPELINE_NAME)
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
