# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for GetRootExternalIdClusterEdges with IdentityFragment inputs."""
import unittest

import apache_beam as beam
from apache_beam.pipeline_test import assert_that, equal_to

from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityExternalId,
    IdentityFragment,
)
from recidiviz.pipelines.ingest.transforms.get_root_external_ids import (
    GetRootExternalIdClusterEdges,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline

_TENANT = "US_XX"


def _make_fragment(*id_pairs: tuple[str, str]) -> IdentityFragment:
    """Creates an external-id-only `IdentityFragment` with the given
    (`external_id`, `id_type`) pairs."""
    return IdentityFragment(
        tenant=_TENANT,
        external_ids=[
            IdentityExternalId(
                tenant=_TENANT,
                external_id=ext_id,
                id_type=id_type,
            )
            for ext_id, id_type in id_pairs
        ],
    )


class TestGetIdentityFragmentClusterEdges(unittest.TestCase):
    """Tests GetRootExternalIdClusterEdges with IdentityFragment inputs."""

    def setUp(self) -> None:
        self.test_pipeline = create_test_pipeline()

    def test_single_external_id(self) -> None:
        fragment = _make_fragment(("ID1", "TYPE_A"))
        output = (
            self.test_pipeline
            | beam.Create([fragment])
            | beam.ParDo(GetRootExternalIdClusterEdges())
        )
        assert_that(output, equal_to([(("ID1", "TYPE_A"), None)]))
        self.test_pipeline.run()

    def test_two_external_ids(self) -> None:
        fragment = _make_fragment(("ID1", "TYPE_A"), ("ID2", "TYPE_B"))
        output = (
            self.test_pipeline
            | beam.Create([fragment])
            | beam.ParDo(GetRootExternalIdClusterEdges())
        )
        assert_that(
            output,
            equal_to(
                [
                    (("ID1", "TYPE_A"), ("ID2", "TYPE_B")),
                    (("ID2", "TYPE_B"), ("ID1", "TYPE_A")),
                ]
            ),
        )
        self.test_pipeline.run()

    def test_three_external_ids(self) -> None:
        fragment = _make_fragment(
            ("ID1", "TYPE_A"), ("ID2", "TYPE_B"), ("ID3", "TYPE_C")
        )
        output = (
            self.test_pipeline
            | beam.Create([fragment])
            | beam.ParDo(GetRootExternalIdClusterEdges())
        )
        expected = [
            (("ID1", "TYPE_A"), ("ID2", "TYPE_B")),
            (("ID1", "TYPE_A"), ("ID3", "TYPE_C")),
            (("ID2", "TYPE_B"), ("ID1", "TYPE_A")),
            (("ID2", "TYPE_B"), ("ID3", "TYPE_C")),
            (("ID3", "TYPE_C"), ("ID1", "TYPE_A")),
            (("ID3", "TYPE_C"), ("ID2", "TYPE_B")),
        ]
        assert_that(output, equal_to(expected))
        self.test_pipeline.run()

    def test_multiple_fragments(self) -> None:
        fragments = [
            _make_fragment(("ID1", "TYPE_A"), ("ID2", "TYPE_B")),
            _make_fragment(("ID3", "TYPE_A")),
        ]
        output = (
            self.test_pipeline
            | beam.Create(fragments)
            | beam.ParDo(GetRootExternalIdClusterEdges())
        )
        expected = [
            (("ID1", "TYPE_A"), ("ID2", "TYPE_B")),
            (("ID2", "TYPE_B"), ("ID1", "TYPE_A")),
            (("ID3", "TYPE_A"), None),
        ]
        assert_that(output, equal_to(expected))
        self.test_pipeline.run()
