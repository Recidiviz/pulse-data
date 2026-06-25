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
"""Base class for a single entry in a reference-data file."""
import abc
from typing import Self, TypeVar

import attr

from recidiviz.utils.yaml_dict import YAMLDict


@attr.define(frozen=True, kw_only=True)
class ReferenceDataEntry(abc.ABC):
    """One entry of a reference-data file (a known organization, an acronym,
    etc.).
    """

    @property
    @abc.abstractmethod
    def dedup_key(self) -> str:
        """The value that uniquely identifies this entry within its category.

        Used to reject duplicates within a single file and to merge the
        `shared/` and `{state_code}/` files for a category (the state-specific
        entry wins on a key collision).
        """

    @classmethod
    @abc.abstractmethod
    def from_yaml_dict(cls, yaml_dict: YAMLDict) -> Self:
        """Returns one entry parsed from its element of a file's `entries`
        block.
        """


ReferenceDataEntryT = TypeVar("ReferenceDataEntryT", bound=ReferenceDataEntry)
"""Type variable for the concrete reference-data entry type a generic registry or
render config holds.
"""
