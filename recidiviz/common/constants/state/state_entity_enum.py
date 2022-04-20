# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Contains logic related to StateEntityEnums for the state schema."""
from typing import Dict

from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


# TODO(#8905): Change superclass to Enum, remove EntityEnumMeta metaclass,
#  remove pylint ignore, and delete comment about _get_default_map
#  once all state ingest views have been migrated to v2 mappings.
# pylint: disable=abstract-method
class StateEntityEnum(EntityEnum, metaclass=EntityEnumMeta):
    """Enum class that enforces documentation and is used in the state schema.

    When extending this class, you must override: _get_default_map
    """

    @classmethod
    def get_enum_description(cls) -> str:
        return "TODO(#12127): Add enum description"
        # TODO(#12127): Uncomment below once this is enforced on all state enum classes
        # raise NotImplementedError

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return {}
        # TODO(#12127): Uncomment below once this is enforced on all state enum classes
        # raise NotImplementedError
