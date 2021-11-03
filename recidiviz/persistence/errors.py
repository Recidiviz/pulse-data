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
"""Contains errors for the persistence directory."""
from typing import Sequence

from recidiviz.persistence.entity.core_entity import CoreEntity


class PersistenceError(Exception):
    """Raised when an error with the persistence layer is encountered."""


class EntityMatchingError(Exception):
    """Raised when an error with entity matching is encountered."""

    def __init__(self, msg: str, entity_name: str):
        self.entity_name = entity_name
        super().__init__(msg)


class MatchedMultipleDatabaseEntitiesError(EntityMatchingError):
    """Raised when an ingested entity is matched to multiple database entities."""

    def __init__(
        self, ingested_entity: CoreEntity, database_entities: Sequence[CoreEntity]
    ):

        database_ids = "\n".join(str(e.get_id()) for e in database_entities)
        msg = (
            "Matched one ingested entity to multiple database entities."
            f"\nIngested entity: {ingested_entity}"
            f"\nDatabase entity db ids: {database_ids}"
        )
        super().__init__(msg, ingested_entity.get_entity_name())


class MatchedMultipleIngestedEntitiesError(EntityMatchingError):
    """Raised when a database entity is matched to multiple ingested entities."""

    def __init__(
        self, database_entity: CoreEntity, ingested_entities: Sequence[CoreEntity]
    ):
        entities = "\n".join(str(e) for e in ingested_entities)
        msg = (
            "Matched one database entity to multiple ingested entities."
            f"\nDatabase entity db id: {database_entity.get_id()}"
            f"\nIngested entities: {entities}"
        )
        super().__init__(msg, database_entity.get_entity_name())
