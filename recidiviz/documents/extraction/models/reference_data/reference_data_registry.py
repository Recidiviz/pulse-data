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
"""Registries of reference-data entries of a single category.

`ReferenceDataType` is the category discriminator. `ReferenceDataRegistry`
represents the entries parsed from a single reference-data file (`shared/` or
`{state_code}/`), keyed by `dedup_key` and rejecting duplicate keys within the
file. `StateSpecificReferenceDataRegistry` is the merged, state-specific view —
a state's entries overlaid on the `shared/` entries (state wins on a collision).

Because the generic `EntryT` parameter is erased at runtime, each registry holds
its concrete `entry_type` as a field. That single field both drives runtime
validation (every entry, and every value of the derived lookup, must be an
instance of it) and lets the type checker infer `EntryT` from the constructor
argument.

`load_full_reference_data_registry` is the top-level entry point: it loads and
merges every category for a state, returning a `ReferenceDataType` ->
merged-registry mapping.
"""
from enum import Enum
from functools import cache
from pathlib import Path
from types import ModuleType
from typing import Any, Generic

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents import config as default_config_module
from recidiviz.documents.extraction.models.reference_data.acronym_reference_data_entry import (
    AcronymReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.known_organization_reference_data_entry import (
    KnownOrganizationReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_entry import (
    ReferenceDataEntry,
    ReferenceDataEntryT,
)
from recidiviz.utils.yaml_dict import YAMLDict

REFERENCE_DATA_DIR_NAME = "reference_data"
SHARED_DIR_NAME = "shared"


class ReferenceDataType(Enum):
    """A category of reference data, identified by the `reference_data_type`
    field of a reference-data file. The value is also the file's base name (e.g.
    `acronyms` -> `acronyms.yaml`).
    """

    KNOWN_ORGANIZATIONS = "known_organizations"
    ACRONYMS = "acronyms"

    @property
    def entry_class(self) -> type[ReferenceDataEntry]:
        """Returns the entry model that this category's `entries` parse into."""
        if self is ReferenceDataType.KNOWN_ORGANIZATIONS:
            return KnownOrganizationReferenceDataEntry
        if self is ReferenceDataType.ACRONYMS:
            return AcronymReferenceDataEntry
        raise ValueError(f"Unexpected reference_data_type: [{self}]")


def _is_entry_type(instance: Any, attribute: attr.Attribute, value: Any) -> None:
    """Validates that |value| is an instance of the instance's own `entry_type`.

    A member validator for the list/dict composers below: attrs passes the owning
    instance through, so this resolves `entry_type` per registry instance.
    """
    if not isinstance(instance, ReferenceDataRegistry):
        raise ValueError(
            f"_is_entry_type may only validate fields of a ReferenceDataRegistry, "
            f"but was applied to a [{type(instance).__name__}]."
        )

    if not isinstance(value, instance.entry_type):
        raise ValueError(
            f"[{attribute.name}] has an entry of type [{type(value).__name__}], "
            f"expected [{instance.entry_type.__name__}]."
        )


@attr.define(frozen=True, kw_only=True)
class ReferenceDataRegistry(Generic[ReferenceDataEntryT]):
    """The reference-data entries parsed from a single file, keyed by
    `dedup_key`.
    """

    entry_type: type[ReferenceDataEntryT] = attr.ib(
        validator=attr_validators.is_subclass_of(ReferenceDataEntry)
    )
    """The concrete entry type this registry holds. Validated against, and used
    by the type checker to infer `EntryT`.
    """

    entries: list[ReferenceDataEntryT] = attr.ib(
        validator=attr_validators.is_list_where_each(_is_entry_type)
    )
    """The category's entries, in file order."""

    entries_by_dedup_key: dict[str, ReferenceDataEntryT] = attr.ib(
        init=False,
        validator=attr_validators.is_dict_where_each(
            key_validator=attr_validators.is_str, value_validator=_is_entry_type
        ),
    )
    """The entries keyed by their `dedup_key`."""

    @entries_by_dedup_key.default
    def _build_entries_by_dedup_key(self) -> dict[str, ReferenceDataEntryT]:
        entries_by_dedup_key: dict[str, ReferenceDataEntryT] = {}
        for entry in self.entries:
            if entry.dedup_key in entries_by_dedup_key:
                raise ValueError(
                    f"Found duplicate reference-data entries with dedup_key "
                    f"[{entry.dedup_key}]."
                )
            entries_by_dedup_key[entry.dedup_key] = entry
        return entries_by_dedup_key

    @staticmethod
    def _reference_data_dir(config_module: ModuleType | None) -> Path:
        """Returns the reference-data directory within |config_module| (the
        production config package by default).
        """
        module = config_module or default_config_module
        if module.__file__ is None:
            raise ValueError(f"No file associated with module [{module}].")
        return Path(module.__file__).parent / REFERENCE_DATA_DIR_NAME

    @classmethod
    def from_yaml_file_for_reference_data_type(
        cls,
        *,
        reference_data_type: ReferenceDataType,
        subdir: str,
        config_module: ModuleType | None = None,
    ) -> "ReferenceDataRegistry[ReferenceDataEntry]":
        """Returns the registry parsed from the single `{reference_data_type}.yaml`
        file under |subdir| (e.g. `shared` or a state code) of the reference-data
        directory. Every subdir is expected to declare a file for every type (with
        an empty `entries` list if it has none), so a missing file is an error.
        Validates that the file's declared `reference_data_type` matches the one
        its location implies.
        """
        path = (
            cls._reference_data_dir(config_module)
            / subdir
            / f"{reference_data_type.value}.yaml"
        )
        yaml_dict = YAMLDict.from_path(path)
        declared_type = ReferenceDataType(yaml_dict.pop("reference_data_type", str))
        if declared_type is not reference_data_type:
            raise ValueError(
                f"Reference data file at [{path}] declares type "
                f"[{declared_type.value}], but its location implies "
                f"[{reference_data_type.value}]."
            )
        entry_class = reference_data_type.entry_class
        entries: list[ReferenceDataEntry] = [
            entry_class.from_yaml_dict(entry_dict)
            for entry_dict in yaml_dict.pop_dicts("entries")
        ]
        if yaml_dict:
            raise ValueError(
                f"Found unexpected config values in reference data file at "
                f"[{path}]: {repr(yaml_dict.get())}"
            )
        return ReferenceDataRegistry(entry_type=entry_class, entries=entries)


@attr.define(frozen=True, kw_only=True)
class StateSpecificReferenceDataRegistry(ReferenceDataRegistry[ReferenceDataEntryT]):
    """The merged, state-specific reference data for one category: a state's
    entries overlaid on the `shared/` entries (the state-specific entry wins on a
    `dedup_key` collision). The merge resolves collisions before construction, so
    the inherited duplicate check is satisfied.
    """

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))
    """The state this merged reference data is for."""

    @classmethod
    def load_for_reference_data_type(
        cls,
        *,
        state_code: StateCode,
        reference_data_type: ReferenceDataType,
        config_module: ModuleType | None = None,
    ) -> "StateSpecificReferenceDataRegistry[ReferenceDataEntry]":
        """Returns the merged registry for |reference_data_type| in |state_code|,
        overlaying the `shared/` file with the state's file (state wins on a
        `dedup_key` collision).
        """
        shared = ReferenceDataRegistry.from_yaml_file_for_reference_data_type(
            reference_data_type=reference_data_type,
            subdir=SHARED_DIR_NAME,
            config_module=config_module,
        )
        state_specific = ReferenceDataRegistry.from_yaml_file_for_reference_data_type(
            reference_data_type=reference_data_type,
            subdir=state_code.value.lower(),
            config_module=config_module,
        )
        merged_by_dedup_key = {
            **shared.entries_by_dedup_key,
            **state_specific.entries_by_dedup_key,
        }
        return StateSpecificReferenceDataRegistry(
            entry_type=reference_data_type.entry_class,
            entries=list(merged_by_dedup_key.values()),
            state_code=state_code,
        )


@cache
def load_full_reference_data_registry(
    state_code: StateCode, config_module: ModuleType | None = None
) -> dict[ReferenceDataType, "StateSpecificReferenceDataRegistry[ReferenceDataEntry]"]:
    """Returns every category of reference data for |state_code|, each merged from
    its `shared/` and `{state_code}/` files (state wins on a `dedup_key`
    collision), keyed by `ReferenceDataType`.
    """
    return {
        reference_data_type: StateSpecificReferenceDataRegistry.load_for_reference_data_type(
            state_code=state_code,
            reference_data_type=reference_data_type,
            config_module=config_module,
        )
        for reference_data_type in ReferenceDataType
    }
