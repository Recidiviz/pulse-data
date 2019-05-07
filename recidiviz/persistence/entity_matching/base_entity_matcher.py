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

"""Contains logic to match database entities with ingested entities."""

import logging
from abc import abstractmethod
from typing import List, Generic, Dict, cast

import attr
from opencensus.stats import measure, view, aggregation

from recidiviz import Session
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.entity.entities import PersonType
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity_matching import entity_matching_utils as utils
from recidiviz.persistence.errors import EntityMatchingError, \
    MatchedMultipleIngestedEntitiesError
from recidiviz.utils import monitoring


m_matching_errors = measure.MeasureInt(
    'persistence/entity_matching/error_count',
    'Number of EntityMatchingErrors thrown for a specific entity type', '1')

matching_errors_by_entity_view = view.View(
    'recidiviz/persistence/entity_matching/error_count',
    'Sum of the errors in the entit matching layer, by entity',
    [monitoring.TagKey.REGION, monitoring.TagKey.ENTITY_TYPE],
    m_matching_errors,
    aggregation.SumAggregation())

monitoring.register_views([matching_errors_by_entity_view])


@attr.s(frozen=True, kw_only=True)
class MatchedEntities(Generic[PersonType]):
    """
    Object that contains output for entity matching
    - people: List of all successfully matched and unmatched people.
        This list does NOT include any people for which Entity Matching raised
        an Exception.
    - orphaned entities: All entities that were orphaned during matching.
        These will need to be added to the session separately from the
        returned people.
    - error count: The number of errors raised during the matching process.
    """
    people: List[PersonType] = attr.ib(factory=list)
    orphaned_entities: List[Entity] = attr.ib(factory=list)
    error_count: int = attr.ib(default=0)

    def __add__(self, other):
        return MatchedEntities(
            people=self.people + other.people,
            orphaned_entities=self.orphaned_entities + other.orphaned_entities,
            error_count=self.error_count + other.error_count)


class BaseEntityMatcher(Generic[PersonType]):
    """Base class for all entity matchers."""

    def run_match(self, session: Session, region: str,
                  ingested_people: List[PersonType]) \
            -> MatchedEntities:
        """
        Attempts to match all people from |ingested_people| with corresponding
        people in our database for the given |region|. Returns an
        MatchedEntities object that contains the results of matching.
        """
        with_external_ids = []
        without_external_ids = []
        for ingested_person in ingested_people:
            if ingested_person.external_id:
                with_external_ids.append(ingested_person)
            else:
                without_external_ids.append(ingested_person)

        db_people_with_external_ids = self.get_people_by_external_ids(
            session, region, with_external_ids)
        matches_with_external_id = self._match_people_and_return_error_count(
            db_people=db_people_with_external_ids,
            ingested_people=with_external_ids)

        db_people_without_external_ids = \
            self.get_people_without_external_ids(
                session, region, without_external_ids)
        matches_without_external_ids =\
            self._match_people_and_return_error_count(
                db_people=db_people_without_external_ids,
                ingested_people=without_external_ids)

        return matches_with_external_id + matches_without_external_ids

    def _match_people_and_return_error_count(
            self, *,
            db_people: List[PersonType],
            ingested_people: List[PersonType]) -> MatchedEntities:
        """
        Attempts to match all people from |ingested_people| with people from the
        |db_people|. Returns an MatchedEntities object that contains the results
        of matching.
        """
        people = []
        orphaned_entities = []
        error_count = 0
        matched_people_by_db_id: Dict[int, PersonType] = {}

        for ingested_person in ingested_people:
            try:
                ingested_person_orphans: List[Entity] = []
                self._match_person(
                    ingested_person=ingested_person,
                    db_people=db_people,
                    orphaned_entities=ingested_person_orphans,
                    matched_people_by_db_id=matched_people_by_db_id)

                people.append(ingested_person)
                orphaned_entities.extend(ingested_person_orphans)
            except EntityMatchingError as e:
                logging.exception(
                    'Found %s while matching ingested person. \nPerson: %s',
                    e.__class__.__name__,
                    ingested_person)
                _increment_error(e.entity_name)
                error_count += 1
        return MatchedEntities(people=people,
                               orphaned_entities=orphaned_entities,
                               error_count=error_count)

    def _match_person(
            self, *,
            ingested_person: PersonType,
            db_people: List[PersonType],
            orphaned_entities: List[Entity],
            matched_people_by_db_id: Dict[int, PersonType]) -> None:
        """
        Finds the best match for the provided |ingested_person| from the
        provided |db_people|. If a match exists, the primary key is added onto
        the |ingested_person| and then we attempt to match all children
        entities.
        """
        db_person = cast(PersonType,
                         utils.get_best_match(ingested_person,
                                              db_people,
                                              self.is_person_match,
                                              matched_people_by_db_id.keys()))

        if db_person:
            person_id = _get_person_id(db_person)
            logging.debug('Successfully matched to person with ID %s',
                          person_id)
            # If the match was previously matched to a different database
            # person, raise an error.
            if person_id in matched_people_by_db_id:
                matches = [ingested_person,
                           matched_people_by_db_id[person_id]]
                raise MatchedMultipleIngestedEntitiesError(db_person, matches)

            _set_person_id(ingested_person, person_id)
            matched_people_by_db_id[cast(int, person_id)] = ingested_person

            self.match_child_entities(db_person=db_person,
                                      ingested_person=ingested_person,
                                      orphaned_entities=orphaned_entities)

    @abstractmethod
    def get_people_by_external_ids(self, session, region, with_external_ids) \
            -> List[PersonType]:
        """Returns all people for the given region that have external_ids that
        match the external_ids from the ingested people."""

    @abstractmethod
    def get_people_without_external_ids(
            self, session, region, without_external_ids) \
            -> List[PersonType]:
        """Returns all people for the given region that have no external ids
        and must be matched to the ingest people in some other way."""

    @abstractmethod
    def is_person_match(self, *,
                        db_entity: PersonType,
                        ingested_entity: PersonType) -> bool:
        """Given a database person and an ingested person, determine if they
        should be considered the same person."""

    @abstractmethod
    def match_child_entities(
            self, *,
            db_person: PersonType,
            ingested_person: PersonType,
            orphaned_entities: List[Entity]):
        """
        Attempts to match all child entities on the |ingested_person| with child
        entities on the |db_person|. For any |ingested_person| child entity, if
        a matching entity exists as a child on |db_person|, the primary key is
        updated on the ingested booking and we attempt to match their children
        entities.
        """


def _increment_error(entity_name: str) -> None:
    mtags = {monitoring.TagKey.ENTITY_TYPE: entity_name}
    with monitoring.measurements(mtags) as measurements:
        measurements.measure_int_put(m_matching_errors, 1)


def _get_person_id(db_person: PersonType):
    if not db_person:
        return None

    if isinstance(db_person, county_entities.Person):
        return db_person.person_id

    if isinstance(db_person, state_entities.StatePerson):
        return db_person.state_person_id

    raise ValueError("Invalid person type of [{}]"
                     .format(db_person.__class__.__name__))


def _set_person_id(ingested_person: PersonType, person_id):
    if isinstance(ingested_person, county_entities.Person):
        ingested_person.person_id = person_id
    elif isinstance(ingested_person, state_entities.StatePerson):
        ingested_person.state_person_id = person_id
    else:
        raise ValueError("Invalid person type of [{}]"
                         .format(ingested_person.__class__.__name__))
