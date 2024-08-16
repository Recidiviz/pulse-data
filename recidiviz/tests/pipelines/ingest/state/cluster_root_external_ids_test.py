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
"""Testing the ClusterRootExternalIds PTransform."""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pipeline_test import TestPipeline, assert_that, equal_to

from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


class TestClusterExternalIds(BigQueryEmulatorTestCase):
    """Tests the ClusterRootExternalIds PTransform."""

    def setUp(self) -> None:
        super().setUp()
        apache_beam_pipeline_options = PipelineOptions()
        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = False
        self.test_pipeline = TestPipeline(options=apache_beam_pipeline_options)

        self.external_id_1 = ("ID1", "TYPE_1")
        self.external_id_2 = ("ID2", "TYPE_2")
        self.external_id_3 = ("ID3", "TYPE_3")
        self.external_id_4 = ("ID4", "TYPE_1")
        self.external_id_5 = ("ID5", "TYPE_2")
        self.external_id_6 = ("ID6", "TYPE_3")
        self.external_id_7 = ("ID7", "TYPE_4")
        self.external_id_8 = ("ID8", "TYPE_2")
        self.external_id_9 = ("ID9", "TYPE_4")

    def test_cluster_external_ids(self) -> None:
        expected_output = [
            (
                self.external_id_1,
                {
                    self.external_id_1,
                    self.external_id_2,
                    self.external_id_3,
                    self.external_id_4,
                },
            ),
            (
                self.external_id_2,
                {
                    self.external_id_1,
                    self.external_id_2,
                    self.external_id_3,
                    self.external_id_4,
                },
            ),
            (
                self.external_id_3,
                {
                    self.external_id_1,
                    self.external_id_2,
                    self.external_id_3,
                    self.external_id_4,
                },
            ),
            (
                self.external_id_4,
                {
                    self.external_id_1,
                    self.external_id_2,
                    self.external_id_3,
                    self.external_id_4,
                },
            ),
            (self.external_id_5, {self.external_id_5, self.external_id_8}),
            (self.external_id_6, {self.external_id_6, self.external_id_7}),
            (self.external_id_7, {self.external_id_6, self.external_id_7}),
            (self.external_id_8, {self.external_id_5, self.external_id_8}),
            (self.external_id_9, {self.external_id_9}),
        ]
        output = (
            self.test_pipeline
            | beam.Create(
                [
                    (self.external_id_1, self.external_id_2),
                    (self.external_id_2, self.external_id_3),
                    (self.external_id_2, self.external_id_4),
                    (self.external_id_1, None),
                    (self.external_id_1, None),
                    (self.external_id_2, None),
                    (self.external_id_2, self.external_id_1),
                    (self.external_id_3, self.external_id_2),
                    (self.external_id_4, self.external_id_2),
                    (self.external_id_5, None),
                    (self.external_id_5, None),
                    (self.external_id_6, None),
                    (self.external_id_6, None),
                    (self.external_id_6, None),
                    (self.external_id_7, None),
                    (self.external_id_6, self.external_id_7),
                    (self.external_id_7, self.external_id_6),
                    (self.external_id_8, self.external_id_5),
                    (self.external_id_5, self.external_id_8),
                    (self.external_id_9, None),
                ]
            )
            | pipeline.ClusterRootExternalIds()
        )
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()
