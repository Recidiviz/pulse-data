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

"""Mixin class for database entities"""
from functools import lru_cache
from typing import Dict, List, Optional, Set, Type, TypeVar

from sqlalchemy.inspection import inspect
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql.sqltypes import Boolean

from recidiviz.persistence.entity.core_entity import CoreEntity


class DatabaseEntity(CoreEntity):
    """Mixin class to provide helper methods to expose database entity
    properties
    """

    Property = TypeVar("Property", RelationshipProperty, ColumnProperty)

    @classmethod
    @lru_cache(maxsize=None)
    def get_primary_key_column_name(cls) -> str:
        """Returns string name of primary key column of the table

        NOTE: This name is the *column* name on the table, which is not
        guaranteed to be the same as the *attribute* name on the ORM object.
        """
        # primary_key returns a tuple containing a single column
        return inspect(cls).primary_key[0].name

    @classmethod
    @lru_cache(maxsize=None)
    def get_column_property_names(cls) -> Set[str]:
        """Returns set of string names of all properties of the entity that
        correspond to columns in the table.

        NOTE: These names are the *attribute* names on the ORM object, which are
        not guaranteed to be the same as the *column* names in the table. This
        distinction is important in cases where a different attribute name is
        used because the column name is a Python reserved keyword like "class".
        """
        return cls._get_entity_property_names_by_type(ColumnProperty)

    @classmethod
    @lru_cache(maxsize=None)
    def get_foreign_key_names(cls) -> List[str]:
        """Returns set of string names of all properties of the entity that
        correspond to foreign keys of other database entities.
        """
        return [col.name for col in inspect(cls).columns if col.foreign_keys]

    @classmethod
    @lru_cache(maxsize=None)
    def get_relationship_property_names(cls) -> Set[str]:
        """Returns set of string names of all properties of the entity that
        correspond to relationships to other database entities.
        """
        return inspect(cls).relationships.keys()

    @classmethod
    @lru_cache(maxsize=None)
    def is_relationship_property(cls, property_name: str) -> Boolean:
        return property_name in cls.get_relationship_property_names()

    @classmethod
    @lru_cache(maxsize=None)
    def get_relationship_property_class_name(cls, property_name: str) -> Optional[str]:
        if not cls.is_relationship_property(property_name):
            return None
        prop = inspect(cls).relationships[property_name]
        return prop.entity.class_.__name__

    @classmethod
    @lru_cache(maxsize=None)
    def get_relationship_property_names_and_properties(
        cls,
    ) -> Dict[str, RelationshipProperty]:
        """Returns a dictionary where the keys are the string names of all
        properties of |cls| that correspond to relationships to other database
        entities, and the values are the corresponding properties.
        """
        return cls._get_entity_names_and_properties_by_type(RelationshipProperty)

    @classmethod
    @lru_cache(maxsize=None)
    def get_property_name_by_column_name(cls, column_name: str) -> str:
        """Returns string name of ORM object attribute corresponding to
        |column_name| on table
        """
        return next(
            name for name in cls.get_column_property_names() if column_name == name
        )

    def get_primary_key(self) -> Optional[int]:
        """Returns primary key value for entity"""
        return getattr(self, type(self)._get_primary_key_property_name(), None)

    @classmethod
    @lru_cache(maxsize=None)
    def _get_primary_key_property_name(cls) -> str:
        """Returns string name of primary key column property of the entity

        NOTE: This name is the *attribute* name on the ORM object, which is not
        guaranteed to be the same as the *column* name in the table.
        """
        return cls.get_property_name_by_column_name(cls.get_primary_key_column_name())

    @classmethod
    @lru_cache(maxsize=None)
    def _get_entity_property_names_by_type(
        cls, property_type: Type[Property]
    ) -> Set[str]:
        """Returns set of string names of all properties of |cls| that match the
        type of |property_type|.
        """
        return set(cls._get_entity_names_and_properties_by_type(property_type).keys())

    @classmethod
    def _get_entity_names_and_properties_by_type(
        cls, property_type: Type[Property]
    ) -> Dict[str, Property]:
        """Returns a dictionary where the keys are the string names of all
        properties of |cls| that are of type |property_type|, and the
        values are the corresponding properties.
        """

        names_to_properties = {}

        # pylint: disable=protected-access
        for name, property_object in inspect(cls)._props.items():
            if isinstance(property_object, property_type):
                names_to_properties[name] = property_object

        return names_to_properties

    def __repr__(self) -> str:
        return self.limited_pii_repr()
