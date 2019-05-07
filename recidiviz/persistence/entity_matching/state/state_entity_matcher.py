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
# ============================================================================

"""Contains logic to match state-level database entities with state-level
ingested entities."""

from typing import List

from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.database.schema.state import dao
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity_matching.base_entity_matcher import \
    BaseEntityMatcher
from recidiviz.persistence.entity_matching.entity_matching_utils import \
    is_birthdate_match


class StateEntityMatcher(BaseEntityMatcher[entities.StatePerson]):
    """Base class for all entity matchers."""

    def get_people_by_external_ids(self, session, region, with_external_ids) \
            -> List[entities.StatePerson]:
        return dao.read_people_by_external_ids(
            session, region, with_external_ids)

    def get_people_without_external_ids(self, session, region,
                                        without_external_ids) \
            -> List[entities.StatePerson]:
        # TODO(1625): Fill this in with an appropriate query
        return dao.read_people(session)

    def is_person_match(self, *,
                        db_entity: entities.StatePerson,
                        ingested_entity: entities.StatePerson) -> bool:
        # TODO(1625): Update to match against external_ids list
        if db_entity.external_id or ingested_entity.external_id:
            return db_entity.external_id == ingested_entity.external_id

        if not all([db_entity.full_name, ingested_entity.full_name]):
            return False

        return (db_entity.full_name == ingested_entity.full_name
                and is_birthdate_match(db_entity, ingested_entity))

    def match_child_entities(self, *, db_person: entities.StatePerson,
                             ingested_person: entities.StatePerson,
                             orphaned_entities: List[Entity]):
        # TODO(1625): Fill this in with appropriate logic
        pass
