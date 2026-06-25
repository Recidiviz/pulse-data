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
"""A resolved extractor's reference data — the state-specific binding of a
collection's reference-data render config to a state's actual entries.

Built when an extractor is resolved for a `(collection, state)`: each
reference-data type the collection declares is paired with the state's merged
`StateSpecificReferenceDataRegistry` for that type. The prompt builder reads this
to render the reference-data prompt variables. `LLMExtractorReferenceDataForType`
is parameterized on the entry type, so its config and registry share one `EntryT`
— the type checker therefore guarantees the two cannot be bound across reference
data types. Coverage of the organization-type space is enforced state-agnostically
on the collection config, so this layer is a pure binding with no further
validation.
"""
from typing import Generic

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.models.reference_data.llm_extractor_collection_reference_data_config import (
    LLMExtractorCollectionReferenceDataConfig,
    LLMExtractorCollectionReferenceDataConfigForType,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_entry import (
    ReferenceDataEntry,
    ReferenceDataEntryT,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_registry import (
    ReferenceDataType,
    StateSpecificReferenceDataRegistry,
)


@attr.define(frozen=True, kw_only=True)
class LLMExtractorReferenceDataForType(Generic[ReferenceDataEntryT]):
    """One reference-data type for a resolved extractor: the collection's render
    config for the type bound to the state's actual entries for it. The shared
    `EntryT` ties the config and registry to the same entry type.
    """

    config: LLMExtractorCollectionReferenceDataConfigForType[
        ReferenceDataEntryT
    ] = attr.ib(
        validator=attr.validators.instance_of(
            LLMExtractorCollectionReferenceDataConfigForType
        )
    )
    """How this type is rendered into the prompt (prompt var, header, groups)."""

    registry: StateSpecificReferenceDataRegistry[ReferenceDataEntryT] = attr.ib(
        validator=attr.validators.instance_of(StateSpecificReferenceDataRegistry)
    )
    """The state's merged entries for this type."""

    @property
    def state_code(self) -> StateCode:
        """Returns the state this type's entries are for."""
        return self.registry.state_code


@attr.define(frozen=True, kw_only=True)
class LLMExtractorReferenceData:
    """All reference data for a resolved extractor (one `(collection, state)`):
    each reference-data type the collection declares, bound to the state's
    entries. Empty when the collection declares no reference data.
    """

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))
    """The state this reference data is for."""

    per_type: dict[
        ReferenceDataType, LLMExtractorReferenceDataForType[ReferenceDataEntry]
    ] = attr.ib(
        validator=attr_validators.is_dict_of(
            ReferenceDataType, LLMExtractorReferenceDataForType
        )
    )
    """The resolved reference data for each declared type."""

    def __attrs_post_init__(self) -> None:
        if mismatched_state_codes := {
            for_type.state_code
            for for_type in self.per_type.values()
            if for_type.state_code != self.state_code
        }:
            raise ValueError(
                f"Reference data for state [{self.state_code.value}] binds "
                f"registries from other states: "
                f"{sorted(state_code.value for state_code in mismatched_state_codes)}."
            )

    @classmethod
    def resolve(
        cls,
        *,
        state_code: StateCode,
        reference_data_config: LLMExtractorCollectionReferenceDataConfig,
        reference_data_registries: dict[
            ReferenceDataType, StateSpecificReferenceDataRegistry[ReferenceDataEntry]
        ],
    ) -> "LLMExtractorReferenceData":
        """Returns the reference data for an extractor in |state_code|, binding
        each type the collection declares in |reference_data_config| to the
        state's entries for that type in |reference_data_registries| (the output
        of `load_full_reference_data_registry` for |state_code|).
        """
        return cls(
            state_code=state_code,
            per_type={
                reference_data_type: LLMExtractorReferenceDataForType(
                    config=config,
                    registry=reference_data_registries[reference_data_type],
                )
                for reference_data_type, config in (
                    reference_data_config.per_type_configs.items()
                )
            },
        )
