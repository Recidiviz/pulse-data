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
"""Contains the base class to handle state specific matching."""
from typing import List, Optional, Type, Callable

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity_matching.entity_matching_types import EntityTree
from recidiviz.persistence.entity_matching.state.state_matching_utils import (
    default_merge_flat_fields,
    read_persons_by_root_entity_cls,
)


class BaseStateMatchingDelegate:
    """Base class to handle state specific matching logic."""

    def __init__(
        self,
        region_code: str,
        allowed_root_entity_classes_override: Optional[
            List[Type[DatabaseEntity]]
        ] = None,
    ) -> None:
        self.region_code = region_code.upper()
        self.allowed_root_entity_classes: List[Type[DatabaseEntity]] = (
            [schema.StatePerson]
            if not allowed_root_entity_classes_override
            else allowed_root_entity_classes_override
        )

    def get_region_code(self) -> str:
        """Returns the region code for this object."""
        return self.region_code

    def read_potential_match_db_persons(
        self, session: Session, ingested_persons: List[schema.StatePerson]
    ) -> List[schema.StatePerson]:
        """Reads and returns all persons from the DB that are needed for entity matching in this state, given the
        |ingested_persons|.
        """
        db_persons = read_persons_by_root_entity_cls(
            session,
            self.region_code,
            ingested_persons,
            self.allowed_root_entity_classes,
        )
        return db_persons

    def merge_flat_fields(
        self, from_entity: DatabaseEntity, to_entity: DatabaseEntity
    ) -> DatabaseEntity:
        """Merges appropriate non-relationship fields on the |new_entity| onto the |old_entity|. Returns the newly
        merged entity.
        """

        merge_for_type = (
            self.get_merge_flat_fields_override_for_type(from_entity.__class__)
            or default_merge_flat_fields
        )
        return merge_for_type(new_entity=from_entity, old_entity=to_entity)

    def get_merge_flat_fields_override_for_type(
        self, _cls: Type[DatabaseEntity]
    ) -> Optional[Callable[..., DatabaseEntity]]:
        """This can be overridden by child classes to specify an state-specific merge method for the provided |_cls|.
        If a callable is returned, it must have the keyword inputs of `new_entity` and `old_entity`.

        If nothing is returned, entities of type |_cls| will be merged with `default_merge_flat_fields`.
        """

    def perform_match_preprocessing(
        self, ingested_persons: List[schema.StatePerson]
    ) -> None:
        """This can be overridden by child classes to perform state-specific preprocessing on the |ingested_persons|
        that will occur immediately before the |ingested_persons| are matched with their database counterparts.
        """

    def perform_match_postprocessing(
        self, matched_persons: List[schema.StatePerson]
    ) -> None:
        """This can be overridden by child classes to perform state-specific postprocessing on the |matched_persons|
        that will occur immediately after the |matched_persons| are matched with their database counterparts.
        """

    def get_non_external_id_match(
        self, ingested_entity_tree: EntityTree, db_entity_trees: List[EntityTree]
    ) -> Optional[EntityTree]:
        """This method can be overridden by child classes to allow for state specific matching logic that does not rely
        solely on matching by external_id.

        If a match is found for the provided |ingested_entity_tree| within the |db_entity_trees| in this manner, it
        should be returned.
        """
