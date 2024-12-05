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

from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.persistence.entity.state.state_entity_mixins import StateEntityMixin

NORMALIZED_PREFIX = "Normalized"


class NormalizedStateEntity(StateEntityMixin, BuildableAttr):
    """Models an entity in state/entities.py that has been normalized and is prepared
    to be used in calculations.
    """

    @classmethod
    def base_class_name(cls) -> str:
        """The name of the class in entities.py that is the pre-normalization version of
        this class.
        """
        if not cls.__name__.startswith(NORMALIZED_PREFIX):
            raise ValueError(
                f"Name of NormalizedStateEntity class [{cls.__name__}] must start with "
                f"'{NORMALIZED_PREFIX}'"
            )
        return cls.__name__[len(NORMALIZED_PREFIX) :]
