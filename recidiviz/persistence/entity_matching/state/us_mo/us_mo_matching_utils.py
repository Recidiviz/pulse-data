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
import datetime
from typing import List, Union

from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity_matching.state.state_matching_utils import (
    get_all_entities_of_cls,
)


# TODO(#1883): Remove this once our proto converter and data extractor can handle the presence of multiple paths to
#  entities with the same id
def remove_suffix_from_violation_ids(ingested_persons: List[schema.StatePerson]):
    """Removes SEO (sentence sequence numbers) and FSO (field sequence numbers) from the end of
    StateSupervisionViolation external_ids. This allows violations across sentences to be merged correctly by
    entity matching.
    """
    ssvs = get_all_entities_of_cls(ingested_persons, schema.StateSupervisionViolation)
    ssvrs = get_all_entities_of_cls(
        ingested_persons, schema.StateSupervisionViolationResponse
    )
    _remove_suffix_from_violation_entity(ssvs)
    _remove_suffix_from_violation_entity(ssvrs)


def _remove_suffix_from_violation_entity(
    violation_entities: List[
        Union[
            schema.StateSupervisionViolation, schema.StateSupervisionViolationResponse
        ]
    ]
):
    for entity in violation_entities:
        if not entity.external_id:
            continue
        splits = entity.external_id.rsplit("-", 2)
        if len(splits) != 3:
            raise ValueError(
                f"Unexpected id format [{entity.external_id}] for [{entity}]"
            )
        entity.external_id = splits[0]


def set_current_supervising_officer_from_supervision_periods(
    matched_persons: List[schema.StatePerson],
):
    """For every matched person, update the supervising_officer field to pull in the supervising_officer from the latest
    supervision period (sorted by termination date).
    """
    for person in matched_persons:

        sps = get_all_entities_of_cls(
            person.sentence_groups, schema.StateSupervisionPeriod
        )

        non_placeholder_sps = [sp for sp in sps if not is_placeholder(sp)]

        if not non_placeholder_sps:
            continue

        non_placeholder_sps.sort(
            key=lambda sp: sp.termination_date
            if sp.termination_date
            else datetime.date.max
        )

        latest_supervision_period = non_placeholder_sps[-1]
        person.supervising_officer = latest_supervision_period.supervising_officer
