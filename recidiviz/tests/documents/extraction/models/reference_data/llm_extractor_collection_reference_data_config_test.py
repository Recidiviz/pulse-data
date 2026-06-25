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
"""Tests for llm_extractor_collection_reference_data_config.py."""
from unittest import TestCase

from recidiviz.documents.extraction.models.reference_data.acronym_reference_data_entry import (
    AcronymReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.known_organization_reference_data_entry import (
    KnownOrganizationReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.llm_extractor_collection_reference_data_config import (
    LLMExtractorCollectionKnownOrganizationConfig,
    LLMExtractorCollectionKnownOrganizationGroup,
    LLMExtractorCollectionReferenceDataConfig,
    LLMExtractorCollectionReferenceDataConfigForType,
)
from recidiviz.documents.extraction.models.reference_data.organization_type import (
    OrganizationType,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_registry import (
    ReferenceDataType,
)
from recidiviz.utils.yaml_dict import YAMLDict, YAMLDictValueType

# A single group covering every organization type — the minimal valid grouping.
_ALL_TYPES_GROUP = LLMExtractorCollectionKnownOrganizationGroup(
    label="All organizations.", organization_types=list(OrganizationType)
)
_ALL_TYPE_VALUES: list[YAMLDictValueType] = [
    organization_type.value for organization_type in OrganizationType
]


class LLMExtractorCollectionKnownOrganizationGroupTest(TestCase):
    """Tests for LLMExtractorCollectionKnownOrganizationGroup."""

    def test_from_yaml_dict(self) -> None:
        self.assertEqual(
            LLMExtractorCollectionKnownOrganizationGroup(
                label="Employers.",
                organization_types=[
                    OrganizationType.EMPLOYER,
                    OrganizationType.STAFFING_AGENCY,
                ],
            ),
            LLMExtractorCollectionKnownOrganizationGroup.from_yaml_dict(
                YAMLDict(
                    {"label": "Employers.", "types": ["employer", "staffing_agency"]}
                )
            ),
        )

    def test_duplicate_organization_types_raise(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"lists duplicate organization types: \['employer'\]"
        ):
            LLMExtractorCollectionKnownOrganizationGroup.from_yaml_dict(
                YAMLDict({"label": "L", "types": ["employer", "employer"]})
            )

    def test_unknown_organization_type_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "'not_a_type' is not a valid OrganizationType"
        ):
            LLMExtractorCollectionKnownOrganizationGroup.from_yaml_dict(
                YAMLDict({"label": "L", "types": ["not_a_type"]})
            )

    def test_empty_types_raise(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be a non-empty list"):
            LLMExtractorCollectionKnownOrganizationGroup.from_yaml_dict(
                YAMLDict({"label": "L", "types": []})
            )

    def test_unexpected_key_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Found unexpected config values for known organization group"
        ):
            LLMExtractorCollectionKnownOrganizationGroup.from_yaml_dict(
                YAMLDict({"label": "L", "types": ["employer"], "extra": "x"})
            )


class LLMExtractorCollectionReferenceDataConfigForTypeTest(TestCase):
    """Tests for the flat base render config."""

    def test_valid_construction(self) -> None:
        config = LLMExtractorCollectionReferenceDataConfigForType(
            entry_type=AcronymReferenceDataEntry,
            prompt_var="acronym_glossary",
            header="COMMON ABBREVIATIONS:",
        )
        self.assertIs(AcronymReferenceDataEntry, config.entry_type)

    def test_entry_type_not_subclass_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"\[entry_type\] .* which is not a subclass of \[ReferenceDataEntry\]",
        ):
            LLMExtractorCollectionReferenceDataConfigForType(
                entry_type=str,  # type: ignore[type-var]
                prompt_var="x",
                header="y",
            )

    def test_empty_prompt_var_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "String value should not be empty."):
            LLMExtractorCollectionReferenceDataConfigForType(
                entry_type=AcronymReferenceDataEntry, prompt_var="", header="y"
            )


class LLMExtractorCollectionKnownOrganizationConfigTest(TestCase):
    """Tests for the known-organizations render config (with groups)."""

    def test_valid_construction(self) -> None:
        config = LLMExtractorCollectionKnownOrganizationConfig(
            entry_type=KnownOrganizationReferenceDataEntry,
            prompt_var="known_entities_context",
            header="KNOWN ENTITIES:",
            groups=[_ALL_TYPES_GROUP],
        )
        self.assertEqual([_ALL_TYPES_GROUP], config.groups)

    def test_type_in_multiple_groups_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"lists organization types in more than one group: \['employer'\]",
        ):
            LLMExtractorCollectionKnownOrganizationConfig(
                entry_type=KnownOrganizationReferenceDataEntry,
                prompt_var="p",
                header="h",
                groups=[
                    _ALL_TYPES_GROUP,
                    LLMExtractorCollectionKnownOrganizationGroup(
                        label="Employers again.",
                        organization_types=[OrganizationType.EMPLOYER],
                    ),
                ],
            )

    def test_uncovered_organization_type_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "must assign every organization type to a group; missing:"
        ):
            LLMExtractorCollectionKnownOrganizationConfig(
                entry_type=KnownOrganizationReferenceDataEntry,
                prompt_var="p",
                header="h",
                groups=[
                    LLMExtractorCollectionKnownOrganizationGroup(
                        label="Only employers.",
                        organization_types=[OrganizationType.EMPLOYER],
                    )
                ],
            )

    def test_empty_groups_raise(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be a non-empty list"):
            LLMExtractorCollectionKnownOrganizationConfig(
                entry_type=KnownOrganizationReferenceDataEntry,
                prompt_var="p",
                header="h",
                groups=[],
            )


class LLMExtractorCollectionReferenceDataConfigTest(TestCase):
    """Tests for the whole reference_data block parser."""

    def test_from_yaml_dict(self) -> None:
        config = LLMExtractorCollectionReferenceDataConfig.from_yaml_dict(
            YAMLDict(
                {
                    "known_organizations": {
                        "prompt_var": "known_entities_context",
                        "header": "KNOWN ENTITIES:",
                        "groups": [
                            {"label": "All organizations.", "types": _ALL_TYPE_VALUES}
                        ],
                    },
                    "acronyms": {
                        "prompt_var": "acronym_glossary",
                        "header": "COMMON ABBREVIATIONS:",
                    },
                }
            )
        )
        # known_organizations parses to the groups subclass; acronyms to the flat base.
        self.assertEqual(
            LLMExtractorCollectionReferenceDataConfig(
                per_type_configs={
                    ReferenceDataType.KNOWN_ORGANIZATIONS: LLMExtractorCollectionKnownOrganizationConfig(
                        entry_type=KnownOrganizationReferenceDataEntry,
                        prompt_var="known_entities_context",
                        header="KNOWN ENTITIES:",
                        groups=[_ALL_TYPES_GROUP],
                    ),
                    ReferenceDataType.ACRONYMS: LLMExtractorCollectionReferenceDataConfigForType(
                        entry_type=AcronymReferenceDataEntry,
                        prompt_var="acronym_glossary",
                        header="COMMON ABBREVIATIONS:",
                    ),
                }
            ),
            config,
        )

    def test_empty_block(self) -> None:
        config = LLMExtractorCollectionReferenceDataConfig.from_yaml_dict(YAMLDict({}))
        self.assertEqual({}, config.per_type_configs)

    def test_duplicate_prompt_var_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "declares the same prompt_var more than once"
        ):
            LLMExtractorCollectionReferenceDataConfig.from_yaml_dict(
                YAMLDict(
                    {
                        "known_organizations": {
                            "prompt_var": "shared",
                            "header": "h",
                            "groups": [{"label": "All.", "types": _ALL_TYPE_VALUES}],
                        },
                        "acronyms": {"prompt_var": "shared", "header": "h"},
                    }
                )
            )

    def test_unknown_reference_data_type_key_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "'made_up_type' is not a valid ReferenceDataType"
        ):
            LLMExtractorCollectionReferenceDataConfig.from_yaml_dict(
                YAMLDict({"made_up_type": {"prompt_var": "p", "header": "h"}})
            )

    def test_acronyms_with_groups_raises(self) -> None:
        # The base (flat) config never pops `groups`, so a non-known-org block
        # declaring them fails the unused-key check.
        with self.assertRaisesRegex(
            ValueError,
            r"^Found unexpected config values for reference data type \[acronyms\]:",
        ):
            LLMExtractorCollectionReferenceDataConfig.from_yaml_dict(
                YAMLDict(
                    {
                        "acronyms": {
                            "prompt_var": "p",
                            "header": "h",
                            "groups": [{"label": "L", "types": ["employer"]}],
                        }
                    }
                )
            )
