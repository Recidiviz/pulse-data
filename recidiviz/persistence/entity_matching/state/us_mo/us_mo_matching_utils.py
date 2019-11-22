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
"""Contains util methods for UsMoMatchingDelegate."""
from typing import cast, List

from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity_matching.entity_matching_types import \
    EntityTree
from recidiviz.persistence.entity_matching.state.state_matching_utils import \
    get_all_entities_of_cls
from recidiviz.persistence.errors import EntityMatchingError


def is_supervision_violation_response_match(
        ingested_entity: EntityTree,
        db_entity: EntityTree) -> bool:
    ingested_entity = cast(
        schema.StateSupervisionViolationResponse,
        ingested_entity.entity)
    db_entity = cast(
        schema.StateSupervisionViolationResponse,
        db_entity.entity)
    return ingested_entity.response_type == db_entity.response_type \
        and ingested_entity.response_date == db_entity.response_date


# TODO(1883): Remove this once our proto converter and data extractor can handle
# the presence of multiple paths to entities with the same id
def remove_seos_from_violation_ids(
        ingested_persons: List[schema.StatePerson]):
    """Removes SEO (sentence sequence numbers) from the end of
    StateSupervisionViolation external_ids. This allows violations across
    sentences to be merged correctly by entity matching.
    """
    ssvs = get_all_entities_of_cls(
        ingested_persons, schema.StateSupervisionViolation)
    for ssv in ssvs:
        splits = ssv.external_id.rsplit('-', 1)
        if len(splits) != 2:
            raise EntityMatchingError(
                f'Unexpected id format for StateSupervisionViolation '
                f'{ssv.external_id}', ssv.get_entity_name())
        ssv.external_id = ssv.external_id.rsplit('-', 1)[0]
