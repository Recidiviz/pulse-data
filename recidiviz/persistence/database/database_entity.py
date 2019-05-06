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

from sqlalchemy.inspection import inspect


class DatabaseEntity:
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
    def get_relationship_property_names(cls):
        """Returns set of string names of all properties of the entity that
        correspond to relationships to other database entities.
        """
        return cls._get_entity_property_names_by_type(
            cls._RELATIONSHIP_PROPERTY_TYPE_NAME)

    @classmethod
    def get_property_name_by_column_name(cls, column_name):
        """Returns string name of ORM object attribute corresponding to
        |column_name| on table
        """
        return next(name for name, property
                    # pylint: disable=protected-access
                    in inspect(cls)._props.items()
                    if type(property).__name__ == cls._COLUMN_PROPERTY_TYPE_NAME
                    # columns returns a list containing a single column
                    and property.columns[0].name == column_name)

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
