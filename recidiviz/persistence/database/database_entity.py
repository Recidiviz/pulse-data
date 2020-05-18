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
from typing import Optional

from sqlalchemy.inspection import inspect

from recidiviz.persistence.entity.core_entity import CoreEntity


class DatabaseEntity(CoreEntity):
    """Mixin class to provide helper methods to expose database entity
    properties
    """

    _COLUMN_PROPERTY_TYPE_NAME = 'ColumnProperty'
    _RELATIONSHIP_PROPERTY_TYPE_NAME = 'RelationshipProperty'

    @classmethod
    def get_primary_key_column_name(cls):
        """Returns string name of primary key column of the table

        NOTE: This name is the *column* name on the table, which is not
        guaranteed to be the same as the *attribute* name on the ORM object.
        """
        # primary_key returns a tuple containing a single column
        return inspect(cls).primary_key[0].name

    @classmethod
    def get_column_property_names(cls):
        """Returns set of string names of all properties of the entity that
        correspond to columns in the table.

        NOTE: These names are the *attribute* names on the ORM object, which are
        not guaranteed to be the same as the *column* names in the table. This
        distinction is important in cases where a different attribute name is
        used because the column name is a Python reserved keyword like "class".
        """
        return cls._get_entity_property_names_by_type(
            cls._COLUMN_PROPERTY_TYPE_NAME)

    @classmethod
    def get_foreign_key_names(cls):
        """Returns set of string names of all properties of the entity that
        correspond to foreign keys of other database entities.
        """
        return [col.name for col in inspect(cls).columns if col.foreign_keys]

    @classmethod
    def get_relationship_property_names(cls):
        """Returns set of string names of all properties of the entity that
        correspond to relationships to other database entities.
        """
        return inspect(cls).relationships.keys()

    @classmethod
    def is_relationship_property(cls, property_name):
        return property_name in cls.get_relationship_property_names()

    @classmethod
    def get_relationship_property_class_name(
            cls, property_name) -> Optional[str]:
        if not cls.is_relationship_property(property_name):
            return None
        prop = inspect(cls).relationships[property_name]
        return prop.entity.class_.__name__

    @classmethod
    def get_relationship_property_names_and_properties(cls):
        """Returns a dictionary where the keys are the string names of all
        properties of |cls| that correspond to relationships to other database
        entities, and the values are the corresponding properties.
        """
        return cls._get_entity_names_and_properties_by_type(
            cls._RELATIONSHIP_PROPERTY_TYPE_NAME)

    @classmethod
    def get_property_name_by_column_name(cls, column_name):
        """Returns string name of ORM object attribute corresponding to
        |column_name| on table
        """
        return next(name
                    for name in cls.get_column_property_names()
                    if column_name == name)

    def get_primary_key(self):
        """Returns primary key value for entity"""
        return getattr(self, type(self)._get_primary_key_property_name(), None)

    @classmethod
    def _get_primary_key_property_name(cls):
        """Returns string name of primary key column property of the entity

        NOTE: This name is the *attribute* name on the ORM object, which is not
        guaranteed to be the same as the *column* name in the table.
        """
        return cls.get_property_name_by_column_name(
            cls.get_primary_key_column_name())

    @classmethod
    def _get_entity_property_names_by_type(cls, type_name):
        """Returns set of string names of all properties of |cls| that match the
        type of |type_name|.
        """
        # pylint: disable=protected-access
        return {name for name, property in inspect(cls)._props.items()
                if type(property).__name__ == type_name}

    @classmethod
    def _get_entity_names_and_properties_by_type(cls, type_name):
        """Returns a dictionary where the keys are the string names of all
        properties of |cls| that match the type of |type_name|, and the
        values are the corresponding properties.
        """

        names_to_properties = {}

        # pylint: disable=protected-access
        for name, property_object in inspect(cls)._props.items():
            if type(property_object).__name__ == type_name and \
                    hasattr(property_object, 'argument') and \
                    hasattr(property_object.argument, 'arg'):
                names_to_properties[name] = property_object

        return names_to_properties

    def __repr__(self):
        """String representation of a DatabaseEntity object that prints DB IDs and external ids for better debugging."""
        property_strs = sorted([f'{key}={value}'
                                for key, value in self.__dict__.items()
                                if key in {self.get_primary_key_column_name(), 'external_id'}])
        properties_str = ', '.join(property_strs)
        return f'{self.__class__.__name__}({properties_str})'
