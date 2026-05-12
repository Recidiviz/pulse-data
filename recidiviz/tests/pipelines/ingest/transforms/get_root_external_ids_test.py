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
"""Tests for GetRootExternalIdClusterEdges."""
import unittest

import apache_beam as beam
import attr
from apache_beam.testing.util import assert_that, equal_to

from recidiviz.persistence.entity.base_entity import (
    ExternalIdEntity,
    HasMultipleExternalIdsEntity,
    RootEntity,
)
from recidiviz.pipelines.ingest.transforms.get_root_external_ids import (
    GetRootExternalIdClusterEdges,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline


@attr.s(eq=False, kw_only=True)
class _FakeExternalId(ExternalIdEntity):
    pass


@attr.s(eq=False, kw_only=True)
class _FakeEntity(HasMultipleExternalIdsEntity[_FakeExternalId], RootEntity):
    external_ids: list[_FakeExternalId] = attr.ib(factory=list)

    def get_external_ids(self) -> list[_FakeExternalId]:
        return self.external_ids

    @classmethod
    def back_edge_field_name(cls) -> str:
        return "fake_entity"


def _make_entity(*id_pairs: tuple[str, str]) -> _FakeEntity:
    return _FakeEntity(
        external_ids=[
            _FakeExternalId(external_id=ext_id, id_type=id_type)
            for ext_id, id_type in id_pairs
        ],
    )


class TestGetRootExternalIdClusterEdges(unittest.TestCase):
    """Tests GetRootExternalIdClusterEdges with simple fake entities."""

    def setUp(self) -> None:
        self.test_pipeline = create_test_pipeline()

    def test_single_external_id(self) -> None:
        output = (
            self.test_pipeline
            | beam.Create([_make_entity(("ID1", "TYPE_A"))])
            | beam.ParDo(GetRootExternalIdClusterEdges())
        )
        assert_that(output, equal_to([(("ID1", "TYPE_A"), None)]))
        self.test_pipeline.run()

    def test_two_external_ids(self) -> None:
        output = (
            self.test_pipeline
            | beam.Create([_make_entity(("ID1", "TYPE_A"), ("ID2", "TYPE_B"))])
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
        output = (
            self.test_pipeline
            | beam.Create(
                [_make_entity(("ID1", "TYPE_A"), ("ID2", "TYPE_B"), ("ID3", "TYPE_C"))]
            )
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

    def test_multiple_entities(self) -> None:
        entities = [
            _make_entity(("ID1", "TYPE_A"), ("ID2", "TYPE_B")),
            _make_entity(("ID3", "TYPE_A")),
        ]
        output = (
            self.test_pipeline
            | beam.Create(entities)
            | beam.ParDo(GetRootExternalIdClusterEdges())
        )
        expected = [
            (("ID1", "TYPE_A"), ("ID2", "TYPE_B")),
            (("ID2", "TYPE_B"), ("ID1", "TYPE_A")),
            (("ID3", "TYPE_A"), None),
        ]
        assert_that(output, equal_to(expected))
        self.test_pipeline.run()

    def test_non_has_multiple_external_ids_raises(self) -> None:
        """Passing an entity that doesn't implement HasMultipleExternalIdsEntity raises."""

        @attr.s(eq=False)
        class _BadEntity(RootEntity):
            @classmethod
            def back_edge_field_name(cls) -> str:
                return "bad"

        with self.assertRaises(Exception):
            _ = (
                self.test_pipeline
                | beam.Create([_BadEntity()])
                | beam.ParDo(GetRootExternalIdClusterEdges())
            )
            self.test_pipeline.run()
