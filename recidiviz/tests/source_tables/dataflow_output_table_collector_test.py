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

import mock

from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.pipeline_names import (
    METRICS_PIPELINE_NAME,
    NORMALIZATION_PIPELINE_NAME,
    SUPPLEMENTAL_PIPELINE_NAME,
)
from recidiviz.source_tables.dataflow_output_table_collector import (
    get_dataflow_output_source_table_collections,
)
from recidiviz.source_tables.source_table_config import (
    DataflowPipelineSourceTableLabel,
    SourceTableCollection,
    StateSpecificSourceTableLabel,
)
from recidiviz.source_tables.source_table_repository import SourceTableRepository


class TestDataflowOutputTableCollector(unittest.TestCase):
    """Test for collecting dataflow source tables"""

    def setUp(self) -> None:
        super().setUp()
        self.direct_ingest_patcher = mock.patch(
            "recidiviz.source_tables.normalization_pipeline_output_table_collector."
            "get_direct_ingest_states_existing_in_env",
            return_value=[StateCode.US_XX, StateCode.US_YY],
        )
        self.direct_ingest_patcher.start()

        self.source_table_repository = SourceTableRepository(
            source_table_collections=get_dataflow_output_source_table_collections()
        )

    def tearDown(self) -> None:
        self.direct_ingest_patcher.stop()

    def test_normalization_tables(self) -> None:
        """Tests the expected output schema of normalization pipelines."""
        pipeline_output_label = DataflowPipelineSourceTableLabel(
            NORMALIZATION_PIPELINE_NAME
        )
        us_xx_label = StateSpecificSourceTableLabel(state_code=StateCode.US_XX)
        us_yy_label = StateSpecificSourceTableLabel(state_code=StateCode.US_YY)

        normalization_collections = self.source_table_repository.get_collections(
            labels=[pipeline_output_label]
        )
        self.assertEqual(len(normalization_collections), 2)

        collected_labels = [
            source_table_collection.labels
            for source_table_collection in normalization_collections
        ]

        # Only the state-specific datasets are actual dataflow pipeline outputs
        expected_labels = [
            [pipeline_output_label, us_xx_label],
            [pipeline_output_label, us_yy_label],
        ]
        self.assertListEqual(collected_labels, expected_labels)

        us_xx_normalized_collection = self.source_table_repository.get_collection(
            labels=[pipeline_output_label, us_xx_label]
        )
        us_yy_normalized_collection = self.source_table_repository.get_collection(
            labels=[pipeline_output_label, us_yy_label]
        )

        self.assert_source_tables_match(
            us_xx_normalized_collection,
            expected_addresses=[
                "us_xx_normalized_state.state_assessment",
                "us_xx_normalized_state.state_charge",
                "us_xx_normalized_state.state_charge_incarceration_sentence_association",
                "us_xx_normalized_state.state_charge_supervision_sentence_association",
                "us_xx_normalized_state.state_early_discharge",
                "us_xx_normalized_state.state_incarceration_period",
                "us_xx_normalized_state.state_incarceration_sentence",
                "us_xx_normalized_state.state_program_assignment",
                "us_xx_normalized_state.state_staff_role_period",
                "us_xx_normalized_state.state_supervision_case_type_entry",
                "us_xx_normalized_state.state_supervision_contact",
                "us_xx_normalized_state.state_supervision_period",
                "us_xx_normalized_state.state_supervision_sentence",
                "us_xx_normalized_state.state_supervision_violated_condition_entry",
                "us_xx_normalized_state.state_supervision_violation",
                "us_xx_normalized_state.state_supervision_violation_response",
                "us_xx_normalized_state.state_supervision_violation_response_decision_entry",
                "us_xx_normalized_state.state_supervision_violation_type_entry",
            ],
        )
        self.assert_source_tables_match(
            us_yy_normalized_collection,
            expected_addresses=[
                "us_yy_normalized_state.state_assessment",
                "us_yy_normalized_state.state_charge",
                "us_yy_normalized_state.state_charge_incarceration_sentence_association",
                "us_yy_normalized_state.state_charge_supervision_sentence_association",
                "us_yy_normalized_state.state_early_discharge",
                "us_yy_normalized_state.state_incarceration_period",
                "us_yy_normalized_state.state_incarceration_sentence",
                "us_yy_normalized_state.state_program_assignment",
                "us_yy_normalized_state.state_staff_role_period",
                "us_yy_normalized_state.state_supervision_case_type_entry",
                "us_yy_normalized_state.state_supervision_contact",
                "us_yy_normalized_state.state_supervision_period",
                "us_yy_normalized_state.state_supervision_sentence",
                "us_yy_normalized_state.state_supervision_violated_condition_entry",
                "us_yy_normalized_state.state_supervision_violation",
                "us_yy_normalized_state.state_supervision_violation_response",
                "us_yy_normalized_state.state_supervision_violation_response_decision_entry",
                "us_yy_normalized_state.state_supervision_violation_type_entry",
            ],
        )

    def test_supplemental(self) -> None:
        """Tests the expected output schema of supplemental pipelines."""
        supplemental_collection = self.source_table_repository.get_collection(
            labels=[
                DataflowPipelineSourceTableLabel(
                    pipeline_name=SUPPLEMENTAL_PIPELINE_NAME
                )
            ]
        )

        self.assert_source_tables_match(
            supplemental_collection,
            expected_addresses=[
                "supplemental_data.us_ix_case_note_matched_entities",
            ],
        )

    def test_metrics(self) -> None:
        """Tests the expected output schema of metrics pipelines."""
        metric_collection = self.source_table_repository.get_collection(
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
                "dataflow_metrics.supervision_success_metrics",
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
