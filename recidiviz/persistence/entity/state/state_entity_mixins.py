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
"""Mixin classes for entities in the state dataset."""
from typing import Optional, TypeVar

import attr

from recidiviz.common import attr_validators
from recidiviz.persistence.entity.state.entity_field_validators import pre_norm_opt


@attr.s
class SequencedEntityMixin:
    """Set of attributes for a normalized entity that can be ordered in a sequence."""

    sequence_num: Optional[int] = attr.ib(
        validator=pre_norm_opt(attr_validators.is_int)
    )


SequencedEntityMixinT = TypeVar("SequencedEntityMixinT", bound=SequencedEntityMixin)
