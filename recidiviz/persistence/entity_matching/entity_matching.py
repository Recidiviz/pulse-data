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

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.entity.entities import EntityPersonType
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity_matching.base_entity_matcher import BaseEntityMatcher
from recidiviz.persistence.entity_matching.county.county_entity_matcher import (
    CountyEntityMatcher,
)
from recidiviz.persistence.entity_matching.entity_matching_types import MatchedEntities
from recidiviz.persistence.entity_matching.state.state_entity_matcher import (
    StateEntityMatcher,
)
from recidiviz.persistence.entity_matching.state.state_matching_delegate_factory import (
    StateMatchingDelegateFactory,
)
from recidiviz.utils import trace

_EMPTY_MATCH_OUTPUT = MatchedEntities(
    people=[], orphaned_entities=[], error_count=0, total_root_entities=0
)


@trace.span
def match(
    session: Session,
    region: str,
    ingested_people: List[EntityPersonType],
    ingest_metadata: IngestMetadata,
) -> MatchedEntities:
    matcher = _get_matcher(ingested_people, region, ingest_metadata)
    if not matcher:
        return _EMPTY_MATCH_OUTPUT

    return matcher.run_match(session, region, ingested_people)


def _get_matcher(
    ingested_people: List[EntityPersonType],
    region_code: str,
    ingest_metadata: IngestMetadata,
) -> Optional[BaseEntityMatcher]:
    sample = next(iter(ingested_people), None)
    if not sample:
        return None

    if isinstance(sample, county_entities.Person):
        return CountyEntityMatcher()

    if isinstance(sample, state_entities.StatePerson):
        state_matching_delegate = StateMatchingDelegateFactory.build(
            region_code=region_code,
            ingest_metadata=ingest_metadata,
        )
        return StateEntityMatcher(state_matching_delegate)

    raise ValueError(f"Invalid person type of [{sample.__class__.__name__}]")
