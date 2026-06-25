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
"""One entry of an `acronyms` reference-data file: an abbreviation and its
expansion.
"""
from typing import Self

import attr

from recidiviz.common import attr_validators
from recidiviz.documents.extraction.models.reference_data.reference_data_entry import (
    ReferenceDataEntry,
)
from recidiviz.utils.yaml_dict import YAMLDict


@attr.define(frozen=True, kw_only=True)
class AcronymReferenceDataEntry(ReferenceDataEntry):
    """An abbreviation that may appear in a document and its expansion."""

    acronym: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The abbreviation as it appears in case notes (e.g. `PO`)."""

    expansion: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """What the acronym stands for (e.g. `Parole Officer`)."""

    @property
    def dedup_key(self) -> str:
        return self.acronym

    @classmethod
    def from_yaml_dict(cls, yaml_dict: YAMLDict) -> Self:
        """Returns the acronym parsed from one element of an `acronyms` file's
        `entries` block.
        """
        entry = cls(
            acronym=yaml_dict.pop("acronym", str),
            expansion=yaml_dict.pop("expansion", str),
        )
        if yaml_dict:
            raise ValueError(
                f"Found unexpected config values for acronym [{entry.acronym}]: "
                f"{repr(yaml_dict.get())}"
            )
        return entry
