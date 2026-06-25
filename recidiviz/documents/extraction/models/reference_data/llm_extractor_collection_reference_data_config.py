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
"""An extractor collection's declaration of how it renders reference data into
its prompt, parsed from the `reference_data` block of its `collection.yaml`.

These are state-agnostic, plain config models — they hold what the collection
declares (which reference-data types it uses, the prompt variables and headers to
render them into, and, for known organizations, how to group them), not any
state's actual entries. Binding a state's `StateSpecificReferenceDataRegistry`
entries to these configs happens later, when an extractor is resolved for a
`(collection, state)`.

Grouping is specific to known organizations (partitioning by organization type),
so it lives only on `LLMExtractorCollectionKnownOrganizationConfig`; all other
types render as a flat list via the base `…ConfigForType`.
"""
from typing import Generic

import attr

from recidiviz.common import attr_validators
from recidiviz.documents.extraction.models.reference_data.known_organization_reference_data_entry import (
    KnownOrganizationReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.organization_type import (
    OrganizationType,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_entry import (
    ReferenceDataEntry,
    ReferenceDataEntryT,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_registry import (
    ReferenceDataType,
)
from recidiviz.utils.yaml_dict import YAMLDict


@attr.define(frozen=True, kw_only=True)
class LLMExtractorCollectionKnownOrganizationGroup:
    """One labeled group of known organizations, naming the organization types
    whose entries it covers. The label is collection-specific prompt framing.
    """

    label: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Prose that frames the group for the model (collection-specific framing)."""

    organization_types: list[OrganizationType] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(OrganizationType),
        ]
    )
    """The organization types whose known organizations fall into this group."""

    def __attrs_post_init__(self) -> None:
        if duplicate_types := {
            organization_type
            for organization_type in self.organization_types
            if self.organization_types.count(organization_type) > 1
        }:
            raise ValueError(
                f"Known organization group [{self.label}] lists duplicate "
                f"organization types: {sorted(t.value for t in duplicate_types)}."
            )

    @classmethod
    def from_yaml_dict(
        cls, yaml_dict: YAMLDict
    ) -> "LLMExtractorCollectionKnownOrganizationGroup":
        """Returns the group parsed from one element of a known_organizations
        config's `groups` block.
        """
        group = cls(
            label=yaml_dict.pop("label", str).strip(),
            organization_types=[
                OrganizationType(organization_type)
                for organization_type in yaml_dict.pop_list("types", str)
            ],
        )
        if yaml_dict:
            raise ValueError(
                f"Found unexpected config values for known organization group "
                f"[{group.label}]: {repr(yaml_dict.get())}"
            )
        return group


@attr.define(frozen=True, kw_only=True)
class LLMExtractorCollectionReferenceDataConfigForType(Generic[ReferenceDataEntryT]):
    """How a collection renders one reference-data type into its prompt: the
    target prompt variable and section header. State-agnostic — holds no actual
    entries.
    """

    entry_type: type[ReferenceDataEntryT] = attr.ib(
        validator=attr_validators.is_subclass_of(ReferenceDataEntry)
    )
    """The concrete entry type this config renders."""

    prompt_var: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The prompt template variable the rendered text is injected into (e.g.
    `acronym_glossary`).
    """

    header: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Section header prepended to the rendered entries."""


@attr.define(frozen=True, kw_only=True)
class LLMExtractorCollectionKnownOrganizationConfig(
    LLMExtractorCollectionReferenceDataConfigForType[ReferenceDataEntry]
):
    """Render config for the `known_organizations` type: adds the groups that
    partition known organizations by organization type. Every organization type
    must be assigned to exactly one group, so no known organization is silently
    dropped from the prompt.
    """

    groups: list[LLMExtractorCollectionKnownOrganizationGroup] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(LLMExtractorCollectionKnownOrganizationGroup),
        ]
    )
    """Groups partitioning known organizations for rendering."""

    def __attrs_post_init__(self) -> None:
        grouped_types = [
            organization_type
            for group in self.groups
            for organization_type in group.organization_types
        ]
        if duplicate_types := {
            organization_type
            for organization_type in grouped_types
            if grouped_types.count(organization_type) > 1
        }:
            raise ValueError(
                f"Known organization config for prompt_var [{self.prompt_var}] "
                f"lists organization types in more than one group: "
                f"{sorted(t.value for t in duplicate_types)}."
            )
        if uncovered_types := set(OrganizationType) - set(grouped_types):
            raise ValueError(
                f"Known organization config for prompt_var [{self.prompt_var}] "
                f"must assign every organization type to a group; missing: "
                f"{sorted(t.value for t in uncovered_types)}."
            )


@attr.define(frozen=True, kw_only=True)
class LLMExtractorCollectionReferenceDataConfig:
    """A collection's whole `reference_data` block: the per-type render configs,
    keyed by `ReferenceDataType`. Empty when the collection declares no reference
    data.
    """

    per_type_configs: dict[
        ReferenceDataType,
        LLMExtractorCollectionReferenceDataConfigForType[ReferenceDataEntry],
    ] = attr.ib(
        validator=attr_validators.is_dict_of(
            ReferenceDataType, LLMExtractorCollectionReferenceDataConfigForType
        )
    )
    """The render config for each reference-data type the collection declares."""

    def __attrs_post_init__(self) -> None:
        prompt_vars = [config.prompt_var for config in self.per_type_configs.values()]
        if duplicate_prompt_vars := {
            prompt_var
            for prompt_var in prompt_vars
            if prompt_vars.count(prompt_var) > 1
        }:
            raise ValueError(
                f"reference_data block declares the same prompt_var more than "
                f"once: {sorted(duplicate_prompt_vars)}."
            )

    @classmethod
    def from_yaml_dict(
        cls, yaml_dict: YAMLDict
    ) -> "LLMExtractorCollectionReferenceDataConfig":
        """Returns the reference-data config parsed from a collection's
        `reference_data` block. Raises on an unknown reference-data type key.
        """
        per_type_configs: dict[
            ReferenceDataType,
            LLMExtractorCollectionReferenceDataConfigForType[ReferenceDataEntry],
        ] = {}
        for type_name in list(yaml_dict.keys()):
            reference_data_type = ReferenceDataType(type_name)
            per_type_configs[reference_data_type] = cls._config_for_type(
                reference_data_type=reference_data_type,
                yaml_dict=yaml_dict.pop_dict(type_name),
            )
        return cls(per_type_configs=per_type_configs)

    @staticmethod
    def _config_for_type(
        *, reference_data_type: ReferenceDataType, yaml_dict: YAMLDict
    ) -> LLMExtractorCollectionReferenceDataConfigForType[ReferenceDataEntry]:
        """Returns the render config for one entry of the `reference_data` block:
        the known-organization subclass (with groups) for `known_organizations`,
        the flat base config otherwise. A non-known-organizations block declaring
        `groups` fails the unused-key check.
        """
        prompt_var = yaml_dict.pop("prompt_var", str)
        header = yaml_dict.pop("header", str)
        config: LLMExtractorCollectionReferenceDataConfigForType[ReferenceDataEntry]
        if reference_data_type is ReferenceDataType.KNOWN_ORGANIZATIONS:
            config = LLMExtractorCollectionKnownOrganizationConfig(
                entry_type=KnownOrganizationReferenceDataEntry,
                prompt_var=prompt_var,
                header=header,
                groups=[
                    LLMExtractorCollectionKnownOrganizationGroup.from_yaml_dict(
                        group_dict
                    )
                    for group_dict in (yaml_dict.pop_dicts_optional("groups") or [])
                ],
            )
        else:
            config = LLMExtractorCollectionReferenceDataConfigForType(
                entry_type=reference_data_type.entry_class,
                prompt_var=prompt_var,
                header=header,
            )
        if yaml_dict:
            raise ValueError(
                f"Found unexpected config values for reference data type "
                f"[{reference_data_type.value}]: {repr(yaml_dict.get())}"
            )
        return config
