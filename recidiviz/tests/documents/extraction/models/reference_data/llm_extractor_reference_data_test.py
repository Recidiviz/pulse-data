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
"""Tests for llm_extractor_reference_data.py."""
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.models.reference_data.acronym_reference_data_entry import (
    AcronymReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.llm_extractor_collection_reference_data_config import (
    LLMExtractorCollectionReferenceDataConfig,
    LLMExtractorCollectionReferenceDataConfigForType,
)
from recidiviz.documents.extraction.models.reference_data.llm_extractor_reference_data import (
    LLMExtractorReferenceData,
    LLMExtractorReferenceDataForType,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_entry import (
    ReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_registry import (
    ReferenceDataType,
    StateSpecificReferenceDataRegistry,
    load_full_reference_data_registry,
)
from recidiviz.tests.documents import fake_config

# Annotated with the base entry type so it slots into the [ReferenceDataEntry]-keyed
# config/registry containers (the generics are invariant).
_ACRONYMS_CONFIG: LLMExtractorCollectionReferenceDataConfigForType[
    ReferenceDataEntry
] = LLMExtractorCollectionReferenceDataConfigForType(
    entry_type=AcronymReferenceDataEntry,
    prompt_var="acronym_glossary",
    header="COMMON ABBREVIATIONS:",
)


def _acronyms_registry(
    state_code: StateCode,
) -> StateSpecificReferenceDataRegistry[ReferenceDataEntry]:
    return StateSpecificReferenceDataRegistry(
        entry_type=AcronymReferenceDataEntry, entries=[], state_code=state_code
    )


class LLMExtractorReferenceDataForTypeTest(TestCase):
    """Tests for LLMExtractorReferenceDataForType."""

    def test_state_code_delegates_to_registry(self) -> None:
        for_type = LLMExtractorReferenceDataForType(
            config=_ACRONYMS_CONFIG, registry=_acronyms_registry(StateCode.US_XX)
        )
        self.assertEqual(StateCode.US_XX, for_type.state_code)


class LLMExtractorReferenceDataTest(TestCase):
    """Tests for LLMExtractorReferenceData and its resolve()."""

    def test_resolve(self) -> None:
        registries = load_full_reference_data_registry(
            StateCode.US_XX, config_module=fake_config
        )
        resolved = LLMExtractorReferenceData.resolve(
            state_code=StateCode.US_XX,
            reference_data_config=LLMExtractorCollectionReferenceDataConfig(
                per_type_configs={ReferenceDataType.ACRONYMS: _ACRONYMS_CONFIG}
            ),
            reference_data_registries=registries,
        )
        self.assertEqual(StateCode.US_XX, resolved.state_code)
        self.assertEqual([ReferenceDataType.ACRONYMS], list(resolved.per_type))
        for_type = resolved.per_type[ReferenceDataType.ACRONYMS]
        self.assertEqual(_ACRONYMS_CONFIG, for_type.config)
        self.assertEqual(registries[ReferenceDataType.ACRONYMS], for_type.registry)

    def test_resolve_empty(self) -> None:
        resolved = LLMExtractorReferenceData.resolve(
            state_code=StateCode.US_XX,
            reference_data_config=LLMExtractorCollectionReferenceDataConfig(
                per_type_configs={}
            ),
            reference_data_registries=load_full_reference_data_registry(
                StateCode.US_XX, config_module=fake_config
            ),
        )
        self.assertEqual({}, resolved.per_type)
        self.assertEqual(StateCode.US_XX, resolved.state_code)

    def test_mixed_state_registries_raise(self) -> None:
        with self.assertRaisesRegex(ValueError, "binds registries from other states"):
            LLMExtractorReferenceData(
                state_code=StateCode.US_OZ,
                per_type={
                    ReferenceDataType.ACRONYMS: LLMExtractorReferenceDataForType(
                        config=_ACRONYMS_CONFIG,
                        registry=_acronyms_registry(StateCode.US_XX),
                    )
                },
            )
