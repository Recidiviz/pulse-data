# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
""" Normalization logic for StatePersonExternalId entities."""
from collections import defaultdict

from more_itertools import one

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StatePersonExternalId
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePersonExternalId,
)
from recidiviz.pipelines.ingest.state.normalization.state_specific_normalization_delegate import (
    StateSpecificNormalizationDelegate,
)
from recidiviz.utils.types import assert_type


def get_normalized_person_external_ids(
    *,
    state_code: StateCode,
    person_id: int,
    external_ids: list[StatePersonExternalId],
    delegate: StateSpecificNormalizationDelegate,
) -> list[NormalizedStatePersonExternalId]:
    """Creates NormalizedStatePersonExternalId entities.

    Hydrates is_current_display_id_for_type values to each external id if they are not
    already defined. If there are multiple external ids of a given id_type,
    state-specific logic defined in the |delegate| is used to select the correct
    display id.
    """

    ids_by_type: dict[str, list[StatePersonExternalId]] = defaultdict(list)
    for pei in external_ids:
        ids_by_type[pei.id_type].append(pei)

    normalized_person_external_ids = []

    for id_type, external_ids_of_type in ids_by_type.items():
        normalized_person_external_ids.extend(
            _convert_person_external_ids_of_type(
                state_code=state_code,
                person_id=person_id,
                id_type=id_type,
                external_ids_of_type=external_ids_of_type,
                delegate=delegate,
            )
        )

    return normalized_person_external_ids


def _convert_person_external_ids_of_type(
    *,
    state_code: StateCode,
    person_id: int,
    id_type: str,
    external_ids_of_type: list[StatePersonExternalId],
    delegate: StateSpecificNormalizationDelegate,
) -> list[NormalizedStatePersonExternalId]:
    """Converts the provided StatePersonExternalId into NormalizedStatePersonExternalId.
    Assumes all |external_ids_of_type| have the same id_type.

    Hydrates is_current_display_id_for_type values to each external id if they are not
    already defined. If there are multiple external ids, state-specific logic defined in
    the |delegate| is used to select the correct display id.
    """
    if len(external_ids_of_type) == 1:
        pei = one(external_ids_of_type)
        pei.is_current_display_id_for_type = True
        pei.is_stable_id_for_type = True
        return [_convert_person_external_id_strict(pei)]

    display_id_of_type = delegate.select_display_id_for_person_external_ids_of_type(
        state_code=state_code,
        person_id=person_id,
        id_type=id_type,
        person_external_ids_of_type=external_ids_of_type,
    )
    stable_id_of_type = delegate.select_stable_id_for_person_external_ids_of_type(
        state_code=state_code,
        person_id=person_id,
        id_type=id_type,
        person_external_ids_of_type=external_ids_of_type,
    )
    for pei in external_ids_of_type:
        pei.is_current_display_id_for_type = (
            pei.external_id == display_id_of_type.external_id
        )
        pei.is_stable_id_for_type = pei.external_id == stable_id_of_type.external_id
    return [
        _convert_person_external_id_strict(pei)
        for pei in sorted(external_ids_of_type, key=lambda p: p.external_id)
    ]


def _convert_person_external_id_strict(
    pei: StatePersonExternalId,
) -> NormalizedStatePersonExternalId:
    """Converts a StatePersonExternalId to a NormalizedStatePersonExternalId, assuming
    all required fields have already been hydrated with nonnull values.
    """
    return NormalizedStatePersonExternalId(
        person_external_id_id=assert_type(pei.person_external_id_id, int),
        state_code=pei.state_code,
        external_id=pei.external_id,
        id_type=pei.id_type,
        is_current_display_id_for_type=assert_type(
            pei.is_current_display_id_for_type, bool
        ),
        is_stable_id_for_type=assert_type(pei.is_stable_id_for_type, bool),
        id_active_from_datetime=pei.id_active_from_datetime,
        id_active_to_datetime=pei.id_active_to_datetime,
    )
