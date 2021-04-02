# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Defines types used for direct ingest."""
import abc
from typing import Optional, Type, Dict, Any

import attr
import cattr

from recidiviz.utils.types import ClsT


@attr.s(frozen=True)
class CloudTaskArgs:
    @abc.abstractmethod
    def task_id_tag(self) -> Optional[str]:
        """Tag to add to the name of an associated cloud task."""

    def to_serializable(self) -> Dict[str, Any]:
        return cattr.unstructure(self)

    @classmethod
    def from_serializable(cls: Type[ClsT], serializable: Dict[str, Any]) -> ClsT:
        return cattr.structure(serializable, cls)
