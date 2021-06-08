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
"""US_TN specific enum helper methods."""
from typing import Dict, List

from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.enum_overrides import (
    EnumIgnorePredicate,
    EnumMapper,
    EnumOverrides,
)
from recidiviz.common.constants.person_characteristics import Ethnicity, Race
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
    overrides: Dict[EntityEnum, List[str]] = {}
    overrides.update(generate_race_enum_overrides())
    overrides.update(generate_ethnicity_enum_overrides())

    ignores: Dict[EntityEnumMeta, List[str]] = {}

    override_mappers: Dict[EntityEnumMeta, EnumMapper] = {}

    ignore_predicates: Dict[EntityEnumMeta, EnumIgnorePredicate] = {}

    base_overrides = get_standard_enum_overrides()

    return update_overrides_from_maps(
        base_overrides, overrides, ignores, override_mappers, ignore_predicates
    )


def generate_race_enum_overrides() -> Dict[EntityEnum, List[str]]:
    """Provides Race overrides for enum mappings."""
    overrides: Dict[EntityEnum, List[str]] = {
        Race.ASIAN: [
            "A",  # Asian or Pacific Islander
        ],
        Race.BLACK: [
            "B",  # Black
        ],
        Race.AMERICAN_INDIAN_ALASKAN_NATIVE: [
            "I",  # American Indian or Alaska Native
        ],
        Race.WHITE: [
            "W",  # White
        ],
    }
    return overrides


def generate_ethnicity_enum_overrides() -> Dict[EntityEnum, List[str]]:
    """Provides Ethnicity overrides for enum mappings."""
    overrides: Dict[EntityEnum, List[str]] = {
        Ethnicity.HISPANIC: [
            "H",  # Hispanic
        ],
        Ethnicity.NOT_HISPANIC: [
            "A",  # Asian or Pacific Islander
            "B",  # Black
            "I",  # American Indian or Alaska Native
            "W",  # White
        ],
    }
    return overrides
