# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
""" Normalization logic for StateStaffExternalId entities."""
from collections import defaultdict

from more_itertools import one

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateStaffExternalId
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateStaffExternalId,
)
from recidiviz.pipelines.ingest.state.normalization.state_specific_normalization_delegate import (
    StateSpecificNormalizationDelegate,
)
from recidiviz.utils.types import assert_type


def get_normalized_staff_external_ids(
    *,
    state_code: StateCode,
    staff_id: int,
    external_ids: list[StateStaffExternalId],
    delegate: StateSpecificNormalizationDelegate,
) -> list[NormalizedStateStaffExternalId]:
    """Creates NormalizedStateStaffExternalId entities.

    Hydrates is_current_display_id_for_type and is_stable_id_for_type values to each
    external id if they are not already defined. If there are multiple external ids of a
    given id_type, state-specific logic defined in the |delegate| is used to select the
    correct display and stable ids.
    """
    ids_by_type: dict[str, list[StateStaffExternalId]] = defaultdict(list)
    for sei in external_ids:
        ids_by_type[sei.id_type].append(sei)

    normalized_staff_external_ids = []

    for id_type, external_ids_of_type in ids_by_type.items():
        normalized_staff_external_ids.extend(
            _convert_staff_external_ids_of_type(
                state_code=state_code,
                staff_id=staff_id,
                id_type=id_type,
                external_ids_of_type=external_ids_of_type,
                delegate=delegate,
            )
        )

    return normalized_staff_external_ids


def _convert_staff_external_ids_of_type(
    *,
    # TODO(#60442): Remove pylint exclusions when no longer needed
    state_code: StateCode,  # pylint: disable=unused-argument
    staff_id: int,  # pylint: disable=unused-argument
    id_type: str,  # pylint: disable=unused-argument
    external_ids_of_type: list[StateStaffExternalId],
    delegate: StateSpecificNormalizationDelegate,  # pylint: disable=unused-argument
) -> list[NormalizedStateStaffExternalId]:
    """Converts the provided StateStaffExternalId into NormalizedStateStaffExternalId.
    Assumes all |external_ids_of_type| have the same id_type.

    Hydrates is_current_display_id_for_type and is_stable_id_for_type values to each
    external id if they are not already defined. If there are multiple external ids,
    state-specific logic defined in the |delegate| is used to select the correct
    display and stable ids.
    """
    if len(external_ids_of_type) == 1:
        sei = one(external_ids_of_type)
        sei.is_current_display_id_for_type = True
        sei.is_stable_id_for_type = True
        return [_convert_staff_external_id(sei)]

    # TODO(#60442): Use delegate.select_display_id_for_staff_external_ids_of_type to get display_id_of_type when implemented

    # TODO(#60442): Use delegate.select_stable_id_for_staff_external_ids_of_type to get stable_id_of_type when implemented

    for sei in external_ids_of_type:
        # TODO(#60442): Adjust when display_id_of_type and stable_id_of_type are available
        sei.is_current_display_id_for_type = None
        sei.is_stable_id_for_type = None

    return [
        _convert_staff_external_id(sei)
        for sei in sorted(external_ids_of_type, key=lambda s: s.external_id)
    ]


def _convert_staff_external_id(
    sei: StateStaffExternalId,
) -> NormalizedStateStaffExternalId:
    """Converts a StateStaffExternalId to a NormalizedStateStaffExternalId."""
    return NormalizedStateStaffExternalId(
        staff_external_id_id=assert_type(sei.staff_external_id_id, int),
        state_code=sei.state_code,
        external_id=sei.external_id,
        id_type=sei.id_type,
        is_current_display_id_for_type=sei.is_current_display_id_for_type,
        is_stable_id_for_type=sei.is_stable_id_for_type,
        id_active_from_datetime=sei.id_active_from_datetime,
        id_active_to_datetime=sei.id_active_to_datetime,
    )
