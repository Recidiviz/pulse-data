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
"""Tests for walk_entity_dag.py."""

import unittest
from typing import List, Tuple
from unittest.mock import patch

from recidiviz.persistence.entity import entity_utils
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.walk_entity_dag import EntityDagEdge, walk_entity_dag
from recidiviz.tests.persistence.database.schema_entity_converter.base_schema_entity_converter_test import (
    SimpsonsFamily,
)
from recidiviz.tests.persistence.database.schema_entity_converter.fake_entities import (
    Child,
    Parent,
    Root,
)
from recidiviz.tests.persistence.database.schema_entity_converter.fake_schema_direction_checker import (
    FAKE_SCHEMA_DIRECTION_CHECKER,
)
from recidiviz.tests.persistence.entity import fake_entities


def return_entity_fn(entity: Entity, _ancestors: List[EntityDagEdge]) -> Entity:
    return entity


def get_entity_name_and_edges(
    entity: Entity, ancestors: List[EntityDagEdge]
) -> Tuple[str, List[Tuple[str, str]]]:
    """Returns a tuple with the entity name and a list of edges representing the path
    to that entity.
    """

    def get_name(e: Entity) -> str:
        if isinstance(e, Root):
            return "Root"
        if isinstance(e, (Child, Parent)):
            return e.full_name
        raise ValueError("Unexpected entity type {}")

    return (
        get_name(entity),
        [(get_name(i.parent), get_name(i.child)) for i in ancestors],
    )


class WalkDagTreeTest(unittest.TestCase):
    """Tests for walk_entity_dag.py."""

    def setUp(self) -> None:
        self.module_for_entity_patcher = patch(
            f"{entity_utils.__name__}.get_module_for_entity_class",
            return_value=fake_entities,
        )
        self.module_for_entity_patcher.start()

        self.direction_checker_for_module_patcher = patch(
            f"{entity_utils.__name__}.direction_checker_for_module",
            return_value=FAKE_SCHEMA_DIRECTION_CHECKER,
        )
        self.direction_checker_for_module_patcher.start()

    def tearDown(self) -> None:
        self.module_for_entity_patcher.stop()
        self.direction_checker_for_module_patcher.stop()

    def test_walk_entity_dag_no_children(self) -> None:
        family = SimpsonsFamily()
        entities = walk_entity_dag(family.root, return_entity_fn)

        self.assertEqual([family.root], entities)

        ancestors_map = walk_entity_dag(family.root, get_entity_name_and_edges)
        self.assertEqual([("Root", [])], ancestors_map)

    def test_walk_entity_dag_simple(self) -> None:
        family = SimpsonsFamily()
        family.root.parents = family.parent_entities
        entities = walk_entity_dag(family.root, return_entity_fn)

        self.assertEqual(
            [family.root, family.homer, family.marge],
            entities,
        )

        ancestors_map = walk_entity_dag(family.root, get_entity_name_and_edges)

        self.assertEqual(
            [
                ("Root", []),
                ("Homer Simpson", [("Root", "Homer Simpson")]),
                ("Marge Simpson", [("Root", "Marge Simpson")]),
            ],
            ancestors_map,
        )

    def test_walk_entity_dag_fully_connected(self) -> None:
        family = SimpsonsFamily()
        family.root.parents = family.parent_entities
        family.homer.children = family.child_entities
        family.marge.children = family.child_entities
        family.bart.parents = family.parent_entities
        family.lisa.parents = family.parent_entities
        family.maggie.parents = family.parent_entities

        entities = walk_entity_dag(family.root, return_entity_fn)

        self.assertEqual(
            [
                family.root,
                family.homer,
                family.bart,
                family.lisa,
                family.maggie,
                family.marge,
            ],
            entities,
        )

        ancestors_map = walk_entity_dag(family.root, get_entity_name_and_edges)

        self.assertEqual(
            [
                ("Root", []),
                ("Homer Simpson", [("Root", "Homer Simpson")]),
                (
                    "Bart Simpson",
                    [("Root", "Homer Simpson"), ("Homer Simpson", "Bart Simpson")],
                ),
                (
                    "Lisa Simpson",
                    [("Root", "Homer Simpson"), ("Homer Simpson", "Lisa Simpson")],
                ),
                (
                    "Maggie Simpson",
                    [("Root", "Homer Simpson"), ("Homer Simpson", "Maggie Simpson")],
                ),
                ("Marge Simpson", [("Root", "Marge Simpson")]),
            ],
            ancestors_map,
        )

    def test_walk_entity_dag_explore_all_paths(self) -> None:
        family = SimpsonsFamily()
        family.root.parents = family.parent_entities
        family.homer.children = family.child_entities
        family.marge.children = family.child_entities
        family.bart.parents = family.parent_entities
        family.lisa.parents = family.parent_entities
        family.maggie.parents = family.parent_entities

        entities = walk_entity_dag(
            family.root, return_entity_fn, explore_all_paths=True
        )

        self.assertEqual(
            [
                family.root,
                family.homer,
                family.bart,
                family.lisa,
                family.maggie,
                family.marge,
                # There are two different paths that can be taken to reach the children
                # so they are represented twice in the list
                family.bart,
                family.lisa,
                family.maggie,
            ],
            entities,
        )

        ancestors_map = walk_entity_dag(
            family.root,
            get_entity_name_and_edges,
            explore_all_paths=True,
        )

        self.assertEqual(
            [
                ("Root", []),
                ("Homer Simpson", [("Root", "Homer Simpson")]),
                (
                    "Bart Simpson",
                    [("Root", "Homer Simpson"), ("Homer Simpson", "Bart Simpson")],
                ),
                (
                    "Lisa Simpson",
                    [("Root", "Homer Simpson"), ("Homer Simpson", "Lisa Simpson")],
                ),
                (
                    "Maggie Simpson",
                    [("Root", "Homer Simpson"), ("Homer Simpson", "Maggie Simpson")],
                ),
                ("Marge Simpson", [("Root", "Marge Simpson")]),
                # The second time we visit the children, the ancestor path is different
                (
                    "Bart Simpson",
                    [("Root", "Marge Simpson"), ("Marge Simpson", "Bart Simpson")],
                ),
                (
                    "Lisa Simpson",
                    [("Root", "Marge Simpson"), ("Marge Simpson", "Lisa Simpson")],
                ),
                (
                    "Maggie Simpson",
                    [("Root", "Marge Simpson"), ("Marge Simpson", "Maggie Simpson")],
                ),
            ],
            ancestors_map,
        )

    def test_walk_entity_dag_start_from_inner_node(self) -> None:
        family = SimpsonsFamily()
        family.root.parents = family.parent_entities
        family.homer.children = family.child_entities
        family.marge.children = family.child_entities
        family.bart.parents = family.parent_entities
        family.lisa.parents = family.parent_entities
        family.maggie.parents = family.parent_entities

        entities = walk_entity_dag(family.homer, return_entity_fn)

        self.assertEqual(
            [
                family.homer,
                family.bart,
                family.lisa,
                family.maggie,
            ],
            entities,
        )

        ancestors_map = walk_entity_dag(family.homer, get_entity_name_and_edges)

        self.assertEqual(
            [
                ("Homer Simpson", []),
                ("Bart Simpson", [("Homer Simpson", "Bart Simpson")]),
                ("Lisa Simpson", [("Homer Simpson", "Lisa Simpson")]),
                ("Maggie Simpson", [("Homer Simpson", "Maggie Simpson")]),
            ],
            ancestors_map,
        )
