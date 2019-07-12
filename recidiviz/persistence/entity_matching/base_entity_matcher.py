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

from abc import abstractmethod
from typing import List, Generic

import attr
from opencensus.stats import measure, view, aggregation

from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities import EntityPersonType
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


# TODO(1907): Rename people -> persons
@attr.s(frozen=True, kw_only=True)
class MatchedEntities(Generic[EntityPersonType]):
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
    people: List[EntityPersonType] = attr.ib(factory=list)
    orphaned_entities: List[Entity] = attr.ib(factory=list)
    error_count: int = attr.ib(default=0)

    def __add__(self, other):
        return MatchedEntities(
            people=self.people + other.people,
            orphaned_entities=self.orphaned_entities + other.orphaned_entities,
            error_count=self.error_count + other.error_count)


class BaseEntityMatcher(Generic[EntityPersonType]):
    """Base class for all entity matchers."""

    @abstractmethod
    def run_match(self, session: Session, region: str,
                  ingested_people: List[EntityPersonType]) \
            -> MatchedEntities:
        """
        Attempts to match all people from |ingested_people| with corresponding
        people in our database for the given |region|. Returns an
        MatchedEntities object that contains the results of matching.
        """


def increment_error(entity_name: str) -> None:
    mtags = {monitoring.TagKey.ENTITY_TYPE: entity_name}
    with monitoring.measurements(mtags) as measurements:
        measurements.measure_int_put(m_matching_errors, 1)
