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
"""One entry of a `known_organizations` reference-data file: an organization,
program, or facility that appears frequently in case notes and benefits from
pre-classification.
"""
from typing import Self

import attr

from recidiviz.common import attr_validators
from recidiviz.documents.extraction.models.reference_data.organization_type import (
    OrganizationType,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_entry import (
    ReferenceDataEntry,
)
from recidiviz.utils.yaml_dict import YAMLDict


@attr.define(frozen=True, kw_only=True)
class KnownOrganizationReferenceDataEntry(ReferenceDataEntry):
    """A named organization, program, or facility and the alternative names the
    LLM should recognize it by.
    """

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Canonical organization name."""

    organization_type: OrganizationType = attr.ib(
        validator=attr.validators.in_(OrganizationType)
    )
    """The category this organization is classified as."""

    aliases: list[str] = attr.ib(validator=attr_validators.is_list_of_non_empty_str)
    """Alternative names or abbreviations that refer to this organization. Empty
    when none are declared.
    """

    @property
    def dedup_key(self) -> str:
        return self.name

    @classmethod
    def from_yaml_dict(cls, yaml_dict: YAMLDict) -> Self:
        """Returns the organization parsed from one element of a
        `known_organizations` file's `entries` block. The `organization_type` is
        parsed into the enum here, so an unknown value fails at parse time.
        """
        name = yaml_dict.pop("name", str)
        entry = cls(
            name=name,
            organization_type=OrganizationType(yaml_dict.pop("organization_type", str)),
            aliases=yaml_dict.pop_list_optional("aliases", str) or [],
        )
        if yaml_dict:
            raise ValueError(
                f"Found unexpected config values for known organization "
                f"[{name}]: {repr(yaml_dict.get())}"
            )
        return entry
