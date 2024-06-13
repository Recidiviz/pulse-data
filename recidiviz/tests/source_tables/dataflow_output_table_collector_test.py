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
import json
import unittest

import mock
import pytest

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
    NormalizedStateSpecificEntitySourceTableLabel,
    SourceTableCollection,
)
from recidiviz.source_tables.source_table_repository import SourceTableRepository


@pytest.mark.usefixtures("snapshottest_snapshot")
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
        """Tests the expected output of normalization pipelines. If the test fails, update snapshots by running:
        pytest recidiviz/tests/source_tables/dataflow_output_table_collector_test.py --snapshot-update"""
        pipeline_output_label = DataflowPipelineSourceTableLabel(
            NORMALIZATION_PIPELINE_NAME
        )
        us_xx_label = NormalizedStateSpecificEntitySourceTableLabel(
            state_code=StateCode.US_XX
        )
        us_yy_label = NormalizedStateSpecificEntitySourceTableLabel(
            state_code=StateCode.US_YY
        )

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
            snapshot_name="us_xx_normalized_collection.json",
        )
        self.assert_source_tables_match(
            us_yy_normalized_collection,
            snapshot_name="us_yy_normalized_collection.json",
        )

    def test_supplemental(self) -> None:
        """Tests the expected output of supplemental pipelines. If the test fails, update snapshots by running:
        pytest recidiviz/tests/source_tables/dataflow_output_table_collector_test.py --snapshot-update"""
        supplemental_collection = self.source_table_repository.get_collection(
            labels=[
                DataflowPipelineSourceTableLabel(
                    pipeline_name=SUPPLEMENTAL_PIPELINE_NAME
                )
            ]
        )

        self.assert_source_tables_match(
            supplemental_collection, snapshot_name="supplemental.json"
        )

    def test_metrics(self) -> None:
        """Tests the expected output of metrics pipelines. If the test fails, update snapshots by running:
        pytest recidiviz/tests/source_tables/dataflow_output_table_collector_test.py --snapshot-update"""
        metric_collection = self.source_table_repository.get_collection(
            labels=[
                DataflowPipelineSourceTableLabel(pipeline_name=METRICS_PIPELINE_NAME)
            ]
        )

        self.assert_source_tables_match(metric_collection, snapshot_name="metrics.json")

    def assert_source_tables_match(
        self, normalized_collection: SourceTableCollection, snapshot_name: str
    ) -> None:
        self.snapshot.assert_match(  # type: ignore[attr-defined]
            json.dumps(
                sorted(
                    [
                        source_table.address.to_str()
                        for source_table in normalized_collection.source_tables
                    ]
                ),
                indent=4,
            ),
            name=snapshot_name,
        )
