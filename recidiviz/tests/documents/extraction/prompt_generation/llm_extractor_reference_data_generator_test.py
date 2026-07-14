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
"""Tests for llm_extractor_reference_data_generator.py.

`test_generate_renders_declared_types_in_order` pins the fully assembled
`{reference_data}` block against an inline expected string; the remaining tests
exercise each per-type renderer directly.
"""
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.models.reference_data.acronym_reference_data_entry import (
    AcronymReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.known_organization_reference_data_entry import (
    KnownOrganizationReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.llm_extractor_collection_reference_data_config import (
    LLMExtractorCollectionKnownOrganizationConfig,
    LLMExtractorCollectionKnownOrganizationGroup,
    LLMExtractorCollectionReferenceDataConfigForType,
)
from recidiviz.documents.extraction.models.reference_data.llm_extractor_reference_data import (
    LLMExtractorReferenceData,
    LLMExtractorReferenceDataForType,
)
from recidiviz.documents.extraction.models.reference_data.organization_type import (
    OrganizationType,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_entry import (
    ReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_registry import (
    ReferenceDataType,
    StateSpecificReferenceDataRegistry,
)
from recidiviz.documents.extraction.prompt_generation.llm_extractor_reference_data_generator import (
    LLMExtractorReferenceDataGenerator,
)

_STATE_CODE = StateCode.US_XX

# Annotated with the base entry type so they slot into the [ReferenceDataEntry]-keyed
# containers (the generics are invariant).
_ACRONYMS_CONFIG: LLMExtractorCollectionReferenceDataConfigForType[
    ReferenceDataEntry
] = LLMExtractorCollectionReferenceDataConfigForType(
    entry_type=AcronymReferenceDataEntry,
    prompt_var="acronym_glossary",
    header="COMMON ABBREVIATIONS:",
)

# The seven non-employer organization types; together with the two employer types
# below every OrganizationType is covered exactly once (required by the config).
_NON_EMPLOYER_TYPES = [
    OrganizationType.COMMUNITY_CORRECTIONS,
    OrganizationType.NONRESIDENTIAL_PROGRAM,
    OrganizationType.RESIDENTIAL_TREATMENT,
    OrganizationType.SOBER_LIVING,
    OrganizationType.HALFWAY_HOUSE,
    OrganizationType.SHELTER,
    OrganizationType.HOTEL_MOTEL,
]

_ORG_CONFIG: LLMExtractorCollectionReferenceDataConfigForType[
    ReferenceDataEntry
] = LLMExtractorCollectionKnownOrganizationConfig(
    entry_type=KnownOrganizationReferenceDataEntry,
    prompt_var="known_entities_context",
    header="KNOWN ENTITIES:",
    groups=[
        LLMExtractorCollectionKnownOrganizationGroup(
            label="The following are NOT employers:",
            organization_types=_NON_EMPLOYER_TYPES,
        ),
        LLMExtractorCollectionKnownOrganizationGroup(
            label="The following are known employers:",
            organization_types=[
                OrganizationType.STAFFING_AGENCY,
                OrganizationType.EMPLOYER,
            ],
            type_notes={
                OrganizationType.STAFFING_AGENCY: "treat as employment_type: temp_agency"
            },
        ),
    ],
)


def _acronyms_for_type(
    *entries: AcronymReferenceDataEntry,
) -> LLMExtractorReferenceDataForType[ReferenceDataEntry]:
    return LLMExtractorReferenceDataForType(
        config=_ACRONYMS_CONFIG,
        registry=StateSpecificReferenceDataRegistry(
            entry_type=AcronymReferenceDataEntry,
            entries=list(entries),
            state_code=_STATE_CODE,
        ),
    )


def _orgs_for_type(
    *entries: KnownOrganizationReferenceDataEntry,
) -> LLMExtractorReferenceDataForType[ReferenceDataEntry]:
    return LLMExtractorReferenceDataForType(
        config=_ORG_CONFIG,
        registry=StateSpecificReferenceDataRegistry(
            entry_type=KnownOrganizationReferenceDataEntry,
            entries=list(entries),
            state_code=_STATE_CODE,
        ),
    )


_ACRONYMS = _acronyms_for_type(
    AcronymReferenceDataEntry(acronym="PO", expansion="Parole Officer"),
    AcronymReferenceDataEntry(acronym="CC", expansion="Community Corrections"),
)
_ORGS = _orgs_for_type(
    KnownOrganizationReferenceDataEntry(
        name="Acme Staffing",
        organization_type=OrganizationType.STAFFING_AGENCY,
        aliases=["Acme"],
    ),
    KnownOrganizationReferenceDataEntry(
        name="Globex", organization_type=OrganizationType.EMPLOYER, aliases=[]
    ),
    KnownOrganizationReferenceDataEntry(
        name="Sunrise Shelter",
        organization_type=OrganizationType.SHELTER,
        aliases=["Sunrise", "SS"],
    ),
)


class GenerateTest(TestCase):
    """The fully assembled `{reference_data}` block."""

    def test_generate_renders_declared_types_in_order(self) -> None:
        reference_data = LLMExtractorReferenceData(
            state_code=_STATE_CODE,
            per_type={
                ReferenceDataType.ACRONYMS: _ACRONYMS,
                ReferenceDataType.KNOWN_ORGANIZATIONS: _ORGS,
            },
        )
        expected = """\
COMMON ABBREVIATIONS:
- "PO" = Parole Officer
- "CC" = Community Corrections

KNOWN ENTITIES:
The following are NOT employers:
  Emergency or homeless shelters:
    - Sunrise Shelter (aka Sunrise, SS)
The following are known employers:
  Temp / staffing agencies that place people in jobs (treat as employment_type: temp_agency):
    - Acme Staffing (aka Acme)
  Employers:
    - Globex"""
        self.assertEqual(
            expected, LLMExtractorReferenceDataGenerator.generate(reference_data)
        )

    def test_generate_empty_when_no_reference_data(self) -> None:
        reference_data = LLMExtractorReferenceData(state_code=_STATE_CODE, per_type={})
        self.assertEqual(
            "", LLMExtractorReferenceDataGenerator.generate(reference_data)
        )

    def test_generate_omits_types_with_no_entries(self) -> None:
        # Both types are declared but neither has entries for the state, so the
        # whole block is empty (no stray headers or blank lines).
        reference_data = LLMExtractorReferenceData(
            state_code=_STATE_CODE,
            per_type={
                ReferenceDataType.ACRONYMS: _acronyms_for_type(),
                ReferenceDataType.KNOWN_ORGANIZATIONS: _orgs_for_type(),
            },
        )
        self.assertEqual(
            "", LLMExtractorReferenceDataGenerator.generate(reference_data)
        )


class AcronymGlossaryTest(TestCase):
    """`render_acronym_glossary`."""

    def test_render_acronym_glossary(self) -> None:
        expected = """\
COMMON ABBREVIATIONS:
- "PO" = Parole Officer
- "CC" = Community Corrections"""
        self.assertEqual(
            expected,
            LLMExtractorReferenceDataGenerator.render_acronym_glossary(_ACRONYMS),
        )

    def test_render_acronym_glossary_empty(self) -> None:
        self.assertEqual(
            "",
            LLMExtractorReferenceDataGenerator.render_acronym_glossary(
                _acronyms_for_type()
            ),
        )


class KnownOrganizationsTest(TestCase):
    """`render_known_organizations`."""

    def test_render_known_organizations_groups_notes_and_aliases(self) -> None:
        # Exercises: grouping under labels; an org type with a type_note appended to
        # its header; entries with aliases (`aka`) and without; and org types listed
        # in a group but with no entries for the state being omitted.
        expected = """\
KNOWN ENTITIES:
The following are NOT employers:
  Emergency or homeless shelters:
    - Sunrise Shelter (aka Sunrise, SS)
The following are known employers:
  Temp / staffing agencies that place people in jobs (treat as employment_type: temp_agency):
    - Acme Staffing (aka Acme)
  Employers:
    - Globex"""
        self.assertEqual(
            expected,
            LLMExtractorReferenceDataGenerator.render_known_organizations(_ORGS),
        )

    def test_render_known_organizations_empty(self) -> None:
        self.assertEqual(
            "",
            LLMExtractorReferenceDataGenerator.render_known_organizations(
                _orgs_for_type()
            ),
        )
