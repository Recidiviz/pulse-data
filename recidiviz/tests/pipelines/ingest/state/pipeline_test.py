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
"""Tests the state ingest pipeline."""
from typing import Any, Dict, Iterable

from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BQ_EMULATOR_PROJECT_ID,
)
from recidiviz.tests.pipelines.fake_bigquery import (
    FakeWriteOutputToBigQueryWithValidator,
    FakeWriteToBigQueryFactory,
)
from recidiviz.tests.pipelines.ingest.state.test_case import StateIngestPipelineTestCase
from recidiviz.tests.pipelines.utils.run_pipeline_test_utils import run_test_pipeline


class TestStateIngestPipeline(StateIngestPipelineTestCase):
    """Tests the state ingest pipeline."""

    def setUp(self) -> None:
        super().setUp()
        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(
            FakeWriteOutputToBigQueryWithValidator
        )

        self.pipeline_class = pipeline.StateIngestPipeline

    def run_test_pipeline(
        self,
        ingest_view_results: Dict[str, Iterable[Dict[str, Any]]],
        expected_entity_types: Iterable[str],
        ingest_view_results_only: bool = False,
    ) -> None:
        """Runs a test version of the state ingest pipeline."""
        project = BQ_EMULATOR_PROJECT_ID
        dataset = "test_dataset"

        read_from_bq_constructor = self.create_fake_bq_read_source_constructor
        read_all_from_bq_constructor = self.create_fake_bq_read_all_source_constructor
        # TODO(#24067) Rather than combine the constructors, we should have separate ones.
        write_to_bq_constructor = (
            self.fake_bq_sink_factory.create_fake_bq_sink_constructor(
                expected_dataset=dataset,
                expected_output_tags=list(expected_entity_types),
                expected_output=ingest_view_results,
                validator_fn_generator=self.validate_ingest_pipeline_results,
            )
        )

        run_test_pipeline(
            pipeline_cls=self.pipeline_class,
            state_code=self.region_code.value,
            project_id=project,
            dataset_id=dataset,
            read_from_bq_constructor=read_from_bq_constructor,
            write_to_bq_constructor=write_to_bq_constructor,
            read_all_from_bq_constructor=read_all_from_bq_constructor,
            ingest_view_results_only=ingest_view_results_only,
        )

    def test_state_ingest_pipeline(self) -> None:
        self.setup_region_raw_data_bq_tables(test_name="ingest_integration")
        expected_ingest_view_output = {
            ingest_view: self.get_expected_ingest_view_results(
                ingest_view_name=ingest_view, test_name="ingest_integration"
            )
            for ingest_view in self.ingest_view_manifest_collector.launchable_ingest_views()
        }
        expected_entity_types = {
            entity_type
            for ingest_view in self.ingest_view_manifest_collector.launchable_ingest_views()
            for entity_type in self.get_expected_output_entity_types(
                ingest_view_name=ingest_view, test_name="ingest_integration"
            )
        }

        self.run_test_pipeline(expected_ingest_view_output, list(expected_entity_types))

    def test_state_ingest_pipeline_ingest_view_results_only(self) -> None:
        self.setup_region_raw_data_bq_tables(test_name="ingest_integration")
        expected_ingest_view_output = {
            ingest_view: self.get_expected_ingest_view_results(
                ingest_view_name=ingest_view, test_name="ingest_integration"
            )
            for ingest_view in self.ingest_view_manifest_collector.launchable_ingest_views()
        }
        self.run_test_pipeline(
            expected_ingest_view_output, [], ingest_view_results_only=True
        )
