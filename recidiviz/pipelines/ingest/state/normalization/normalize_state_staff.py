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
from typing import Mapping, Sequence

from recidiviz.persistence.entity.state.entities import StateStaff
from recidiviz.persistence.entity.state.normalized_entities import NormalizedStateStaff
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_root_entity_helpers import (
    build_normalized_root_entity,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.staff_role_period_normalization_manager import (
    StaffRolePeriodNormalizationManager,
)
from recidiviz.pipelines.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_staff_role_period_normalization_delegate,
)
from recidiviz.utils.types import assert_type


def build_normalized_state_staff(staff: StateStaff) -> NormalizedStateStaff:
    """Normalizes the given StateStaff root entity into a NormalizedStateStaff."""
    staff_role_period_normalization_manager = StaffRolePeriodNormalizationManager(
        staff.role_periods,
        get_state_specific_staff_role_period_normalization_delegate(
            staff.state_code, staff.supervisor_periods
        ),
    )
    role_periods = staff_role_period_normalization_manager.get_normalized_role_periods()

    staff_kwargs: Mapping[str, Sequence[NormalizedStateEntity]] = {
        "role_periods": role_periods,
    }

    return assert_type(
        build_normalized_root_entity(
            pre_normalization_root_entity=staff,
            normalized_root_entity_cls=NormalizedStateStaff,
            root_entity_subtree_kwargs=staff_kwargs,
        ),
        NormalizedStateStaff,
    )
