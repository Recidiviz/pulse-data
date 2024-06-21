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
"""NormalizedStateEntity is the common class to all normalized entities in the state dataset."""
from typing import Type

from recidiviz.common.attr_mixins import BuildableAttr


class NormalizedStateEntity(BuildableAttr):
    """Models an entity in state/entities.py that has been normalized and is prepared
    to be used in calculations.
    """

    @classmethod
    def base_class_name(cls) -> str:
        """The name of the base state entity that this normalized entity extends."""
        return cls.get_base_entity_class().__name__

    # TODO(#30075): The normalized_entities_v2 entities aren't subclasses of their
    #  corresponding entity - we need a different way to find the comparable class in
    #  entities.py (or remove usage of this function altogether).
    @classmethod
    def get_base_entity_class(cls) -> Type[BuildableAttr]:
        """For the given NormalizeStateEntity, returns the base Entity class for this class.
        For example: for NormalizedStatePerson, returns `StatePerson`.
        """
        base_class = cls.__base__
        if not base_class or not issubclass(base_class, BuildableAttr):
            raise ValueError(f"Cannot find base class for entity {cls}")
        return base_class
