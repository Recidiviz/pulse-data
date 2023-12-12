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

from typing import List, Type

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.monitoring import trace
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity_matching.legacy.entity_matching_types import (
    MatchedEntities,
)
from recidiviz.persistence.entity_matching.legacy.state.state_entity_matcher import (
    StateEntityMatcher,
)
from recidiviz.persistence.entity_matching.legacy.state.state_specific_entity_matching_delegate_factory import (
    StateSpecificEntityMatchingDelegateFactory,
)
from recidiviz.persistence.persistence_utils import RootEntityT, SchemaRootEntityT


@trace.span
def match(
    *,
    session: Session,
    region: str,
    root_entity_cls: Type[RootEntityT],
    schema_root_entity_cls: Type[SchemaRootEntityT],
    ingested_root_entities: List[RootEntityT],
    ingest_metadata: IngestMetadata,
) -> MatchedEntities[SchemaRootEntityT]:
    matcher = _get_matcher(
        region,
        ingest_metadata,
        root_entity_cls,
        schema_root_entity_cls,
    )
    return matcher.run_match(session, ingested_root_entities)


def _get_matcher(
    region_code: str,
    ingest_metadata: IngestMetadata,
    root_entity_cls: Type[RootEntityT],
    schema_root_entity_cls: Type[SchemaRootEntityT],
) -> StateEntityMatcher[RootEntityT, SchemaRootEntityT]:
    state_matching_delegate = StateSpecificEntityMatchingDelegateFactory.build(
        region_code=region_code,
        ingest_metadata=ingest_metadata,
    )
    return StateEntityMatcher(
        root_entity_cls=root_entity_cls,
        schema_root_entity_cls=schema_root_entity_cls,
        state_specific_logic_delegate=state_matching_delegate,
    )
