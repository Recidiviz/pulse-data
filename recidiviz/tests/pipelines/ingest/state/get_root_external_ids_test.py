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
"""Testing the GetRootExternalIdClusterEdges PTransform."""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pipeline_test import TestPipeline, assert_that, equal_to

from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.tests.pipelines.ingest.state.test_case import StateIngestPipelineTestCase


class TestGetRootExternalIdClusterEdges(StateIngestPipelineTestCase):
    """Tests the GetRootExternalIds PTransform."""

    def setUp(self) -> None:
        super().setUp()
        apache_beam_pipeline_options = PipelineOptions()
        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = False
        self.test_pipeline = TestPipeline(options=apache_beam_pipeline_options)

    def test_get_root_external_ids(self) -> None:
        expected_output = [
            (("ID1", "US_DD_ID_TYPE"), None),
            (("ID2", "US_DD_ID_TYPE"), None),
        ]
        output = (
            self.test_pipeline
            | beam.Create(
                self.get_expected_root_entities_from_fixture(
                    ingest_view_name="ingest12", test_name="ingest12"
                )
            )
            | beam.ParDo(pipeline.GetRootExternalIdClusterEdges())
        )
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()

    def test_get_root_external_ids_multiple_external_ids(self) -> None:
        expected_output = [
            (("ID1", "US_DD_ID_TYPE"), ("VALUE3", "US_DD_ID_ANOTHER_TYPE")),
            (
                ("VALUE3", "US_DD_ID_ANOTHER_TYPE"),
                ("ID1", "US_DD_ID_TYPE"),
            ),
            (("ID3", "US_DD_ID_TYPE"), ("VALUE4", "US_DD_ID_ANOTHER_TYPE")),
            (
                ("VALUE4", "US_DD_ID_ANOTHER_TYPE"),
                ("ID3", "US_DD_ID_TYPE"),
            ),
        ]
        output = (
            self.test_pipeline
            | beam.Create(
                self.get_expected_root_entities_from_fixture(
                    ingest_view_name="ingestMultipleRootExternalIds",
                    test_name="ingestMultipleRootExternalIds",
                )
            )
            | beam.ParDo(pipeline.GetRootExternalIdClusterEdges())
        )
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()
