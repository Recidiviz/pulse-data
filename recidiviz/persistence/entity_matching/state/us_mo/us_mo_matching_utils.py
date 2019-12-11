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
from typing import List, Union

from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity_matching.state.state_matching_utils import \
    get_all_entities_of_cls
from recidiviz.persistence.errors import EntityMatchingError


# TODO(1883): Remove this once our proto converter and data extractor can handle
# the presence of multiple paths to entities with the same id
def remove_suffix_from_violation_ids(
        ingested_persons: List[schema.StatePerson]):
    """Removes SEO (sentence sequence numbers) and FSO (field sequence numbers)
    from the end of StateSupervisionViolation external_ids. This allows
    violations across sentences to be merged correctly by entity matching.
    """
    ssvs = get_all_entities_of_cls(
        ingested_persons, schema.StateSupervisionViolation)
    ssvrs = get_all_entities_of_cls(
        ingested_persons, schema.StateSupervisionViolationResponse)
    _remove_suffix_from_violation_entity(ssvs)
    _remove_suffix_from_violation_entity(ssvrs)


def _remove_suffix_from_violation_entity(
        violation_entities:
        List[Union[schema.StateSupervisionViolation,
                   schema.StateSupervisionViolationResponse]]):
    for entity in violation_entities:
        if not entity.external_id:
            continue
        splits = entity.external_id.rsplit('-', 2)
        if len(splits) != 3:
            raise EntityMatchingError(
                f'Unexpected id format for {entity.get_entity_name()}'
                f'{entity.external_id}', entity.get_entity_name())
        entity.external_id = splits[0]
