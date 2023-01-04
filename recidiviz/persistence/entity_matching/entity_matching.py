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

from typing import List

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity_matching.entity_matching_types import MatchedEntities
from recidiviz.persistence.entity_matching.state.state_entity_matcher import (
    StateEntityMatcher,
)
from recidiviz.persistence.entity_matching.state.state_matching_delegate_factory import (
    StateMatchingDelegateFactory,
)
from recidiviz.persistence.persistence_utils import RootEntityT
from recidiviz.utils import trace


@trace.span
def match(
    session: Session,
    region: str,
    ingested_root_entities: List[RootEntityT],
    ingest_metadata: IngestMetadata,
) -> MatchedEntities:
    matcher = _get_matcher(region, ingest_metadata)
    return matcher.run_match(session, region, ingested_root_entities)


def _get_matcher(
    region_code: str,
    ingest_metadata: IngestMetadata,
) -> StateEntityMatcher:
    state_matching_delegate = StateMatchingDelegateFactory.build(
        region_code=region_code,
        ingest_metadata=ingest_metadata,
    )
    return StateEntityMatcher(state_matching_delegate)
