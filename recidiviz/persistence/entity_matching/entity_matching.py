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
"""Contains logic to match database entities with ingested entities."""

from typing import List, Optional

from opencensus.stats import measure, view, aggregation

from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.entity.entities import EntityPersonType
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity_matching.base_entity_matcher import \
    BaseEntityMatcher, MatchedEntities
from recidiviz.persistence.entity_matching.county.county_entity_matcher import \
    CountyEntityMatcher
from recidiviz.persistence.entity_matching.state.state_entity_matcher import \
    StateEntityMatcher
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

_EMPTY_MATCH_OUTPUT = MatchedEntities(people=[],
                                      orphaned_entities=[],
                                      error_count=0,
                                      total_root_entities=0)


def match(session: Session,
          region: str,
          ingested_people: List[EntityPersonType]) -> MatchedEntities:
    matcher = _get_matcher(ingested_people)
    if not matcher:
        return _EMPTY_MATCH_OUTPUT

    return matcher.run_match(session, region, ingested_people)


def _get_matcher(ingested_people: List[EntityPersonType]) \
        -> Optional[BaseEntityMatcher]:
    sample = next(iter(ingested_people), None)
    if not sample:
        return None

    if isinstance(sample, county_entities.Person):
        return CountyEntityMatcher()

    if isinstance(sample, state_entities.StatePerson):
        return StateEntityMatcher()

    raise ValueError('Invalid person type of [{}]'
                     .format(sample.__class__.__name__))
