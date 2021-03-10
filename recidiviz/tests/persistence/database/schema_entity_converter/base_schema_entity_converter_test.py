# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for BaseSchemaEntityConverter"""
from types import ModuleType
from typing import Type
from unittest import TestCase
from unittest.mock import create_autospec

from more_itertools import one

from recidiviz.persistence.database.schema_entity_converter.base_schema_entity_converter import (
    BaseSchemaEntityConverter,
    FieldNameType,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.entity_utils import SchemaEdgeDirectionChecker
from recidiviz.tests.persistence.database.schema_entity_converter import (
    test_entities as entities,
)
from recidiviz.tests.persistence.database.schema_entity_converter import (
    test_schema as schema,
)
from recidiviz.tests.persistence.database.schema_entity_converter.test_base_schema import (
    TestBase,
)
from recidiviz.tests.persistence.database.schema_entity_converter.test_entities import (
    RootType,
)
from recidiviz.tests.utils import fakes


class _TestSchemaEntityConverter(BaseSchemaEntityConverter):
    """
    Test implementation of BaseSchemaEntityConverter which references a simple
    database schema
    """

    CLASS_HIERARCHY = [
        entities.Root.__name__,
        entities.Parent.__name__,
        entities.Child.__name__,
        entities.Toy.__name__,
    ]

    def __init__(self):
        direction_checker = SchemaEdgeDirectionChecker(self.CLASS_HIERARCHY, entities)
        super().__init__(direction_checker)

    def _get_schema_module(self) -> ModuleType:
        return schema

    def _get_entities_module(self) -> ModuleType:
        return entities

    def _should_skip_field(self, entity_cls: Type, field: FieldNameType) -> bool:
        return False

    def _populate_indirect_back_edges(self, _):
        return


class TestBaseSchemaEntityConverter(TestCase):
    """Tests for BaseSchemaEntityConverter"""

    def setUp(self) -> None:
        self.database_key = create_autospec(SQLAlchemyDatabaseKey)
        self.database_key.declarative_meta = TestBase
        self.database_key.isolation_level = "SERIALIZABLE"
        self.database_key.poolclass = None
        fakes.use_in_memory_sqlite_database(self.database_key)

        session = SessionFactory.for_database(self.database_key)
        self.assertEqual(len(session.query(schema.Root).all()), 0)
        self.assertEqual(len(session.query(schema.Parent).all()), 0)
        self.assertEqual(len(session.query(schema.Child).all()), 0)

    def tearDown(self) -> None:
        fakes.teardown_in_memory_sqlite_databases()

    def test_add_behavior(self):
        session = SessionFactory.for_database(self.database_key)
        self.assertEqual(len(session.query(schema.Root).all()), 0)
        self.assertEqual(len(session.query(schema.Parent).all()), 0)
        self.assertEqual(len(session.query(schema.Child).all()), 0)

        parent = entities.Parent.new_with_defaults(
            full_name="Krusty the Clown",
        )
        converter = _TestSchemaEntityConverter()
        schema_parent = converter.convert(parent)

        session = SessionFactory.for_database(self.database_key)
        session.add(schema_parent)
        session.commit()

        parents = session.query(schema.Parent).all()
        self.assertEqual(len(parents), 1)

        child = entities.Child.new_with_defaults(full_name="Child name")

        schema_child = converter.convert(child)
        parents[0].children.append(schema_child)
        session.commit()

        children = session.query(schema.Child).all()
        self.assertEqual(len(children), 1)
        self.assertEqual(len(children[0].parents), 1)

        parent2 = entities.Parent.new_with_defaults(
            full_name="Krusty the Clown 2",
        )
        children[0].parents = [converter.convert(parent2)]

        session.commit()

        parents = session.query(schema.Parent).all()
        self.assertEqual(len(parents), 2)

        children = session.query(schema.Child).all()
        self.assertEqual(len(children), 1)
        self.assertEqual(len(children[0].parents), 1)

    def test_add_behavior_2(self):
        parent = entities.Parent.new_with_defaults(
            full_name="Krusty the Clown",
        )
        converter = _TestSchemaEntityConverter()
        schema_parent = converter.convert(parent)

        session = SessionFactory.for_database(self.database_key)
        session.add(schema_parent)
        session.commit()

        parents = session.query(schema.Parent).all()
        self.assertEqual(len(parents), 1)

        root = entities.Root.new_with_defaults(
            type=RootType.SIMPSONS,
        )

        db_root = converter.convert(root)
        db_root.parents.append(parents[0])
        session.add(db_root)
        session.commit()

        roots = session.query(schema.Root).all()
        self.assertEqual(len(roots), 1)

        parents = session.query(schema.Parent).all()
        self.assertEqual(len(parents), 1)

    def test_convert_single_node(self):
        parent = entities.Parent.new_with_defaults(
            parent_id=1234, full_name="Krusty the Clown", children=[]
        )
        converter = _TestSchemaEntityConverter()
        schema_parent = converter.convert(parent)

        session = SessionFactory.for_database(self.database_key)
        session.add(schema_parent)
        session.commit()

        parents = session.query(schema.Parent).all()
        self.assertEqual(len(parents), 1)

        children = session.query(schema.Child).all()
        self.assertEqual(len(children), 0)

        converted_parent = _TestSchemaEntityConverter().convert(one(parents))

        self.assertEqual(parent, converted_parent)

    def test_convert_single_node_no_primary_key(self):
        parent = entities.Parent.new_with_defaults(
            full_name="Krusty the Clown", children=[]
        )
        converter = _TestSchemaEntityConverter()
        schema_parent = converter.convert(parent)

        # Converting entity to schema won't add a primary key if there isn't
        # one already.
        self.assertIsNone(schema_parent.parent_id)

        session = SessionFactory.for_database(self.database_key)
        session.add(schema_parent)
        session.commit()

        parents = session.query(schema.Parent).all()
        self.assertEqual(len(parents), 1)

        children = session.query(schema.Child).all()
        self.assertEqual(len(children), 0)

        converted_parent = _TestSchemaEntityConverter().convert(one(parents))

        # ...but there will be a primary key after adding to the DB
        self.assertIsNotNone(converted_parent.parent_id)

        self.assertEqual(parent.full_name, converted_parent.full_name)
        self.assertEqual(parent.children, converted_parent.children)

    def _check_children(self, parent, expected_children):
        self.assertEqual(len(expected_children), len(parent.children))

        child_names = [child.full_name for child in parent.children]
        expected_child_names = [child.full_name for child in expected_children]

        self.assertEqual(set(expected_child_names), set(child_names))

    def _check_parents(self, child, expected_parents):
        self.assertEqual(len(expected_parents), len(child.parents))

        parent_names = [child.full_name for child in child.parents]
        expected_parent_names = [child.full_name for child in expected_parents]

        self.assertEqual(set(expected_parent_names), set(parent_names))

    def _run_nuclear_family_test(self, parent_entities, child_entities):
        schema_parents = _TestSchemaEntityConverter().convert_all(parent_entities)

        session = SessionFactory.for_database(self.database_key)
        for parent in schema_parents:
            session.add(parent)
        session.commit()

        db_parents = session.query(schema.Parent).all()
        self.assertEqual(len(db_parents), len(parent_entities))

        db_children = session.query(schema.Child).all()
        self.assertEqual(len(db_children), len(child_entities))

        converted_parents = _TestSchemaEntityConverter().convert_all(db_parents)

        for converted_parent in converted_parents:
            self._check_children(converted_parent, child_entities)
            for converted_child in converted_parent.children:
                self._check_parents(converted_child, parent_entities)

    class SimpsonsFamily:
        """
        Convenience class for instantiating a bunch of Entity objects for use
        in tests. The relationships between these objects are intentionally not
        set.
        """

        def __init__(self):
            self.root = entities.Root.new_with_defaults(
                root_id=314, type=RootType.SIMPSONS, parents=[]
            )

            self.homer = entities.Parent.new_with_defaults(
                parent_id=1011, full_name="Homer Simpson", children=[]
            )

            self.marge = entities.Parent.new_with_defaults(
                parent_id=1213, full_name="Marge Simpson", children=[]
            )

            self.bart = entities.Child.new_with_defaults(
                child_id=123, full_name="Bart Simpson", parents=[]
            )

            self.lisa = entities.Child.new_with_defaults(
                child_id=456, full_name="Lisa Simpson", parents=[]
            )

            self.maggie = entities.Child.new_with_defaults(
                child_id=789, full_name="Maggie Simpson", parents=[]
            )

            self.parent_entities = [self.homer, self.marge]
            self.child_entities = [self.bart, self.lisa, self.maggie]

    def test_convert_many_to_many_full_graph(self):
        """
        Tests conversion on a many-to-many schema where all edges are
        explicitly represented on entities.
        """

        family = self.SimpsonsFamily()

        family.homer.children = family.child_entities
        family.marge.children = family.child_entities
        family.bart.parents = family.parent_entities
        family.lisa.parents = family.parent_entities
        family.maggie.parents = family.parent_entities

        self._run_nuclear_family_test(family.parent_entities, family.child_entities)

    def test_convert_many_to_many_partial_graph_1(self):
        """
        Tests conversion on a many-to-many schema where all edges can be
        inferred but are not explicitly set.
        """

        family = self.SimpsonsFamily()

        family.homer.children = family.child_entities
        family.marge.children = family.child_entities

        self._run_nuclear_family_test(family.parent_entities, family.child_entities)

    def test_convert_many_to_many_partial_graph_2(self):
        """
        Tests conversion on a many-to-many schema where all edges can be
        inferred but are not explicitly set.
        """

        family = self.SimpsonsFamily()

        family.marge.children = family.child_entities
        family.bart.parents = [family.homer]
        family.lisa.parents = [family.homer]
        family.maggie.parents = [family.homer]

        self._run_nuclear_family_test(family.parent_entities, family.child_entities)

    def test_convert_many_to_many_partial_graph_3(self):
        """
        Tests conversion on a many-to-many schema where all edges can be
        inferred but are not explicitly set.
        """

        family = self.SimpsonsFamily()

        family.homer.children = [family.bart]
        family.marge.children = [family.lisa, family.maggie]
        family.bart.parents = family.parent_entities
        family.lisa.parents = family.parent_entities
        family.maggie.parents = family.parent_entities

        self._run_nuclear_family_test(family.parent_entities, family.child_entities)

    def test_simple_tree(self):
        """
        Tests converting a simple graph with one root node and one child
        """
        family = self.SimpsonsFamily()
        self.assertEqual(len(family.homer.children), 0)
        family.root.parents = [family.homer]

        schema_root = _TestSchemaEntityConverter().convert(family.root)

        session = SessionFactory.for_database(self.database_key)
        session.add(schema_root)
        session.commit()

        db_roots = session.query(schema.Root).all()
        self.assertEqual(len(db_roots), 1)
        db_parents = session.query(schema.Parent).all()
        self.assertEqual(len(db_parents), 1)

        converted_root = _TestSchemaEntityConverter().convert(one(db_roots))
        self.assertEqual(len(converted_root.parents), 1)
        self.assertEqual(converted_root.parents[0], family.homer)
        self.assertEqual(converted_root.parents[0].children, [])

    def test_convert_rooted_graph(self):
        """
        Tests converting a graph that has a single root node that is connected
        either directly or indirectly to all entities.
        """
        family = self.SimpsonsFamily()

        family.root.parents = family.parent_entities
        family.homer.children = family.child_entities
        family.marge.children = family.child_entities

        schema_root = _TestSchemaEntityConverter().convert(family.root)

        session = SessionFactory.for_database(self.database_key)
        session.add(schema_root)
        session.commit()

        db_root = session.query(schema.Root).all()
        self.assertEqual(len(db_root), 1)

        db_parents = session.query(schema.Parent).all()
        self.assertEqual(len(db_parents), len(family.parent_entities))

        db_children = session.query(schema.Child).all()
        self.assertEqual(len(db_children), len(family.child_entities))

        converted_root = _TestSchemaEntityConverter().convert(one(db_root))

        for converted_parent in converted_root.parents:
            self._check_children(converted_parent, family.child_entities)
            for converted_child in converted_parent.children:
                self._check_parents(converted_child, family.parent_entities)

    def test_many_to_one_no_backref(self):
        family = self.SimpsonsFamily()
        self.assertEqual(len(family.homer.children), 0)
        family.root.parents = [family.homer]
        family.homer.children = [family.bart, family.maggie]
        toy = entities.Toy.new_with_defaults(toy_id=456789, name="Skateboard")
        family.bart.favorite_toy = toy
        family.maggie.favorite_toy = toy

        schema_root = _TestSchemaEntityConverter().convert(family.root)

        session = SessionFactory.for_database(self.database_key)
        session.add(schema_root)
        session.commit()

        db_roots = session.query(schema.Root).all()
        self.assertEqual(len(db_roots), 1)
        db_parents = session.query(schema.Parent).all()
        self.assertEqual(len(db_parents), 1)
        db_children = session.query(schema.Child).all()
        self.assertEqual(len(db_children), 2)
        db_toys = session.query(schema.Toy).all()
        self.assertEqual(len(db_toys), 1)

        converted_root = _TestSchemaEntityConverter().convert(one(db_roots))
        self.assertEqual(len(converted_root.parents), 1)
        self.assertEqual(len(converted_root.parents[0].children), 2)
        self.assertEqual(converted_root.parents[0].children[0].favorite_toy, toy)
        self.assertEqual(converted_root.parents[0].children[1].favorite_toy, toy)

    # TODO(#1894): Write more unit tests for bugfixes in #1816
