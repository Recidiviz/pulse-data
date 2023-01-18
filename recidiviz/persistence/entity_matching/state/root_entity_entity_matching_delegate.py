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
"""Defines a delegate interface for a class that contains logic specific to the type of
root entity that is being matched in entity matching.
"""

import abc
from typing import Generic, Type

from recidiviz.persistence.persistence_utils import RootEntityT, SchemaRootEntityT


# TODO(#17854): Do away with this delegate entirely by a) passing the
#  schema_root_entity_cls directly to entity matcher class and b) making a RootEntity
#  interface which exposes the backedge field name.
class RootEntityEntityMatchingDelegate(Generic[RootEntityT, SchemaRootEntityT]):
    """A delegate interface for a class that contains logic specific to the type of
    root entity that is being matched in entity matching.
    """

    # TODO(#17854): Delete this method and get backedge name from RootEntity class instead.
    @abc.abstractmethod
    def get_root_entity_backedge_field_name(self) -> str:
        """For non-root entity objects that hang off of the root entity associated with
        this delegate, returns the name of the relationship field that points back to
        the root entity (e.g the "person" field on StateSupervisionPeriod).
        """

    @abc.abstractmethod
    def get_schema_root_entity_cls(self) -> Type[SchemaRootEntityT]:
        """Returns the class for the SQLAlchemy root entity type associated with this
        delegate.
        """
