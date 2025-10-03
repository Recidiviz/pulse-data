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
from itertools import permutations

import apache_beam as beam
from apache_beam.pipeline_test import assert_that, equal_to

from recidiviz.pipelines.ingest.state import pipeline
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline


class TestClusterExternalIds(BigQueryEmulatorTestCase):
    """Tests the ClusterRootExternalIds PTransform."""

    def setUp(self) -> None:
        super().setUp()
        self.test_pipeline = create_test_pipeline()

        self.external_id_1 = ("ID1", "TYPE_1")
        self.external_id_2 = ("ID2", "TYPE_2")
        self.external_id_3 = ("ID3", "TYPE_3")
        self.external_id_4 = ("ID4", "TYPE_1")
        self.external_id_5 = ("ID5", "TYPE_2")
        self.external_id_6 = ("ID6", "TYPE_3")
        self.external_id_7 = ("ID7", "TYPE_4")
        self.external_id_8 = ("ID8", "TYPE_2")
        self.external_id_9 = ("ID9", "TYPE_4")
        self.external_id_10 = ("ID10", "TYPE_4")
        self.external_id_11 = ("ID11", "TYPE_4")

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

    def test_chain_of_ids(self) -> None:
        """Tests that, given edges 1 -> 2 -> 3, we properly merge no matter what order
        they are presented in.
        """
        input_ids = [
            (self.external_id_1, None),
            (self.external_id_1, self.external_id_2),
            (self.external_id_2, self.external_id_3),
        ]

        all_ids_set = {
            self.external_id_1,
            self.external_id_2,
            self.external_id_3,
        }

        # Every id should be associated with every other id
        expected_output = [
            (self.external_id_1, all_ids_set),
            (self.external_id_2, all_ids_set),
            (self.external_id_3, all_ids_set),
        ]

        for i, ordered_input_ids in enumerate(permutations(input_ids)):
            output = (
                self.test_pipeline
                | f"Create with permutation {i}" >> beam.Create(ordered_input_ids)
                | f"Cluster with permutation {i}" >> pipeline.ClusterRootExternalIds()
            )

            assert_that(
                output, equal_to(expected_output), label=f"Assert for permutation {i}"
            )
            self.test_pipeline.run()

    def test_long_chain_of_ids(self) -> None:
        """Tests that, given a long chain of edges, we merge properly, even if the order
        is scrambled
        """

        # This represents a chain of ids 1 -> 2 -> 3 -> ... -> 11
        input_ids = [
            (self.external_id_7, self.external_id_8),
            (self.external_id_8, self.external_id_9),
            (self.external_id_3, self.external_id_4),
            (self.external_id_4, self.external_id_5),
            (self.external_id_5, self.external_id_6),
            (self.external_id_1, None),
            (self.external_id_1, self.external_id_2),
            (self.external_id_2, self.external_id_3),
            (self.external_id_6, self.external_id_7),
            (self.external_id_9, self.external_id_10),
            (self.external_id_10, self.external_id_11),
        ]

        all_ids_set = {
            self.external_id_1,
            self.external_id_2,
            self.external_id_3,
            self.external_id_4,
            self.external_id_5,
            self.external_id_6,
            self.external_id_7,
            self.external_id_8,
            self.external_id_9,
            self.external_id_10,
            self.external_id_11,
        }

        # Every id should be associated with every other id
        expected_output = [
            (self.external_id_1, all_ids_set),
            (self.external_id_2, all_ids_set),
            (self.external_id_3, all_ids_set),
            (self.external_id_4, all_ids_set),
            (self.external_id_5, all_ids_set),
            (self.external_id_6, all_ids_set),
            (self.external_id_7, all_ids_set),
            (self.external_id_8, all_ids_set),
            (self.external_id_9, all_ids_set),
            (self.external_id_10, all_ids_set),
            (self.external_id_11, all_ids_set),
        ]

        output = (
            self.test_pipeline
            | beam.Create(input_ids)
            | pipeline.ClusterRootExternalIds()
        )

        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()
