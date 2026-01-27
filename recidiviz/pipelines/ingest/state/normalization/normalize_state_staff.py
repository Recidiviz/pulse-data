# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Functionality for normalizing the given StateStaff root entity into a
NormalizedStateStaff.
"""
from typing import Any, Mapping, Sequence

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import (
    StateStaff,
    StateStaffSupervisorPeriod,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateStaff,
    NormalizedStateStaffSupervisorPeriod,
)
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.pipelines.ingest.state.create_root_entity_id_to_staff_id_mapping import (
    StaffExternalIdToIdMap,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.staff_role_period_normalization_manager import (
    StaffRolePeriodNormalizationManager,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_root_entity_helpers import (
    build_normalized_root_entity,
)
from recidiviz.pipelines.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_staff_role_period_normalization_delegate,
)
from recidiviz.utils.types import assert_type


def get_normalized_staff_supervisor_periods(
    staff_supervisor_periods: list[StateStaffSupervisorPeriod],
    staff_external_id_to_staff_id: StaffExternalIdToIdMap,
) -> list[NormalizedStateStaffSupervisorPeriod]:
    """Normalizes all StateStaffSupervisorPeriods by:
    * Adding a supervisor_staff_id field corresponding to the
      supervisor_staff_external_id/type
    """
    normalized_supervisor_periods = []
    for supervisor_period in staff_supervisor_periods:
        normalized_supervisor_periods.append(
            NormalizedStateStaffSupervisorPeriod(
                staff_supervisor_period_id=assert_type(
                    supervisor_period.staff_supervisor_period_id, int
                ),
                state_code=supervisor_period.state_code,
                external_id=supervisor_period.external_id,
                start_date=supervisor_period.start_date,
                end_date=supervisor_period.end_date,
                supervisor_staff_external_id=supervisor_period.supervisor_staff_external_id,
                supervisor_staff_external_id_type=supervisor_period.supervisor_staff_external_id_type,
                supervisor_staff_id=staff_external_id_to_staff_id[
                    (
                        supervisor_period.supervisor_staff_external_id,
                        supervisor_period.supervisor_staff_external_id_type,
                    )
                ],
            )
        )

    return normalized_supervisor_periods


def build_normalized_state_staff(
    staff: StateStaff,
    staff_external_id_to_staff_id: StaffExternalIdToIdMap,
    # pylint: disable=unused-argument
    expected_output_entities: set[type[Entity]],
) -> NormalizedStateStaff:
    """Normalizes the given StateStaff root entity into a NormalizedStateStaff."""
    staff_role_period_normalization_manager = StaffRolePeriodNormalizationManager(
        staff.role_periods,
        get_state_specific_staff_role_period_normalization_delegate(
            staff.state_code, staff.supervisor_periods
        ),
    )
    role_periods = staff_role_period_normalization_manager.get_normalized_role_periods()

    supervisor_periods = get_normalized_staff_supervisor_periods(
        staff_supervisor_periods=staff.supervisor_periods,
        staff_external_id_to_staff_id=staff_external_id_to_staff_id,
    )

    staff_subtree_kwargs: Mapping[str, Sequence[NormalizedStateEntity]] = {
        "role_periods": role_periods,
        "supervisor_periods": supervisor_periods,
    }
    staff_flat_field_kwargs: dict[str, Any] = {}
    return assert_type(
        build_normalized_root_entity(
            pre_normalization_root_entity=staff,
            normalized_root_entity_cls=NormalizedStateStaff,
            root_entity_subtree_kwargs=staff_subtree_kwargs,
            root_entity_flat_field_kwargs=staff_flat_field_kwargs,
        ),
        NormalizedStateStaff,
    )
