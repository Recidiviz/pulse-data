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
from typing import Generic, List, Set, Type

from recidiviz.persistence.database.session import Session
from recidiviz.persistence.persistence_utils import RootEntityT, SchemaRootEntityT


class RootEntityEntityMatchingDelegate(Generic[RootEntityT, SchemaRootEntityT]):
    """A delegate interface for a class that contains logic specific to the type of
    root entity that is being matched in entity matching.
    """

    @abc.abstractmethod
    def read_root_entities_with_external_ids(
        self, session: Session, region_code: str, external_ids: Set[str]
    ) -> List[SchemaRootEntityT]:
        """Reads and returns all matching root entities of the appropriate type from the
        DB that have one of the provided |external_ids|.
        """

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
