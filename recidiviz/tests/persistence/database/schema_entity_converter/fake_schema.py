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
"""Fake database schema definitions for schema defined in test_entities.py """
from sqlalchemy import Column, Enum, ForeignKey, Integer, String, Table
from sqlalchemy.orm import relationship

from recidiviz.tests.persistence.database.schema_entity_converter.fake_base_schema import (
    FakeBase,
)
from recidiviz.tests.persistence.database.schema_entity_converter.fake_entities import (
    RootType,
)

root_type = Enum(RootType.SIMPSONS.value, RootType.FRIENDS.value, name="root_type")


class Root(FakeBase):
    """Represents a Root object in the test schema"""

    __tablename__ = "root"

    root_id = Column(Integer, primary_key=True)

    root_type = Column(root_type)

    parents = relationship("Parent", lazy="joined")


association_table = Table(
    "state_parent_child_association",
    FakeBase.metadata,
    Column("parent_id", Integer, ForeignKey("parent.parent_id")),
    Column("child_id", Integer, ForeignKey("child.child_id")),
)


class Parent(FakeBase):
    """Represents a Parent object in the test schema"""

    __tablename__ = "parent"

    parent_id = Column(Integer, primary_key=True)

    full_name = Column(String(255))

    root_id = Column(Integer, ForeignKey("root.root_id"))

    children = relationship(
        "Child", secondary=association_table, back_populates="parents"
    )


class Child(FakeBase):
    """Represents a Child object in the test schema"""

    __tablename__ = "child"

    child_id = Column(Integer, primary_key=True)

    full_name = Column(String(255))

    parents = relationship(
        "Parent", secondary=association_table, back_populates="children"
    )

    favorite_toy_id = Column(Integer, ForeignKey("toy.toy_id"))
    favorite_toy = relationship("Toy", uselist=False)


class Toy(FakeBase):
    """Represents a Toy object in the test schema"""

    __tablename__ = "toy"

    toy_id = Column(Integer, primary_key=True)

    name = Column(String(255))
