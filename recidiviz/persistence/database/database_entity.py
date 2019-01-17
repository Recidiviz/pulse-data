"""Mixin class for database entities"""

from sqlalchemy.inspection import inspect


class DatabaseEntity:
    """Mixin class to provide helper methods to expose database entity
    properties
    """

    @classmethod
    def get_column_property_names(cls):
        """Returns set of string names of all properties of the entity that
        correspond to columns in the table.

        NOTE: These names are the *attribute* names on the ORM object, which are
        not guaranteed to be the same as the *column* names in the table. This
        distinction is important in cases where a different attribute name is
        used because the column name is a Python reserved keyword like "class".
        """
        return cls._get_entity_property_names_by_type('ColumnProperty')

    @classmethod
    def get_relationship_property_names(cls):
        """Returns set of string names of all properties of the entity that
        correspond to relationships to other database entities.
        """
        return cls._get_entity_property_names_by_type('RelationshipProperty')

    @classmethod
    def _get_entity_property_names_by_type(cls, type_name):
        """Returns set of string names of all properties of |cls| that match the
        type of |type_name|.
        """
        # pylint: disable=protected-access
        entity_properties = inspect(cls)._props
        return {property_name for property_name in entity_properties
                if type(entity_properties[property_name]).__name__ == type_name}
