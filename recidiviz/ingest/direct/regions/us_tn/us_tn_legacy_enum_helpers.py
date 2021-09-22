# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""US_TN specific enum helper methods.

TODO(#8903): This file should become empty and be deleted when we have fully migrated
 this state to new ingest mappings version.
"""
from enum import Enum
from typing import Dict, List, Type

from recidiviz.common.constants.enum_overrides import (
    EnumIgnorePredicate,
    EnumMapperFn,
    EnumOverrides,
)
from recidiviz.common.constants.standard_enum_overrides import (
    get_standard_enum_overrides,
)
from recidiviz.ingest.direct.direct_ingest_controller_utils import (
    update_overrides_from_maps,
)


def generate_enum_overrides() -> EnumOverrides:
    """Provides Tennessee-specific overrides for enum mappings.

    The keys herein are raw strings directly from the source data, and the values are the enums that they are
    mapped to within our schema. The values are a list because a particular string may be used in multiple
    distinct columns in the source data.
    """
    overrides: Dict[Enum, List[str]] = {}

    ignores: Dict[Type[Enum], List[str]] = {}

    override_mappers: Dict[Type[Enum], EnumMapperFn] = {}

    ignore_predicates: Dict[Type[Enum], EnumIgnorePredicate] = {}

    base_overrides = get_standard_enum_overrides()

    return update_overrides_from_maps(
        base_overrides, overrides, ignores, override_mappers, ignore_predicates
    )
