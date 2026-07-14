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
"""Builds the `{reference_data}` block of an extractor prompt from a resolved
extractor's reference data.

The input is an `LLMExtractorReferenceData` — a collection's reference-data render
config (headers, grouping, and the order types appear in) already bound to a
state's actual entries. Each declared type is rendered under its configured header,
in declared order: acronyms as a flat glossary, known organizations grouped by
organization type. A type with no entries for the state renders nothing.
"""

from recidiviz.documents.extraction.models.reference_data.acronym_reference_data_entry import (
    AcronymReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.known_organization_reference_data_entry import (
    KnownOrganizationReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.llm_extractor_collection_reference_data_config import (
    LLMExtractorCollectionKnownOrganizationConfig,
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
)
from recidiviz.utils.string_formatting import render_list
from recidiviz.utils.types import assert_type


class LLMExtractorReferenceDataGenerator:
    """Builds the `{reference_data}` block of an extractor prompt from a resolved
    extractor's reference data (a collection's render config bound to a state's
    entries).
    """

    @classmethod
    def generate(cls, reference_data: LLMExtractorReferenceData) -> str:
        """Returns the `{reference_data}` block: every declared reference-data type
        rendered under its configured header, in declared order. Empty string when
        the collection declares no reference data (or none has entries).
        """
        blocks = [
            block
            for reference_data_type, for_type in reference_data.per_type.items()
            if (block := cls._render_type(reference_data_type, for_type))
        ]
        return "\n\n".join(blocks)

    @classmethod
    def _render_type(
        cls,
        reference_data_type: ReferenceDataType,
        for_type: LLMExtractorReferenceDataForType[ReferenceDataEntry],
    ) -> str:
        """Returns the rendered block for one reference-data type, dispatching on
        the type.
        """
        if reference_data_type is ReferenceDataType.ACRONYMS:
            return cls.render_acronym_glossary(for_type)
        if reference_data_type is ReferenceDataType.KNOWN_ORGANIZATIONS:
            return cls.render_known_organizations(for_type)
        raise ValueError(f"Unsupported reference-data type: [{reference_data_type}].")

    @staticmethod
    def render_acronym_glossary(
        for_type: LLMExtractorReferenceDataForType[ReferenceDataEntry],
    ) -> str:
        """Returns the acronyms block — a glossary under the configured header — or
        empty if the state has no acronym entries.
        """
        acronyms = [
            assert_type(entry, AcronymReferenceDataEntry)
            for entry in for_type.registry.entries
        ]
        if not acronyms:
            return ""

        glossary = render_list(
            f'"{entry.acronym}" = {entry.expansion}' for entry in acronyms
        )
        return f"{for_type.config.header}\n{glossary}"

    @staticmethod
    def render_known_organizations(
        for_type: LLMExtractorReferenceDataForType[ReferenceDataEntry],
    ) -> str:
        """Returns the known-organizations block, grouped by organization type under
        the config's group labels, or empty if the state has no entries.
        """
        config = assert_type(
            for_type.config, LLMExtractorCollectionKnownOrganizationConfig
        )
        known_organizations = [
            assert_type(entry, KnownOrganizationReferenceDataEntry)
            for entry in for_type.registry.entries
        ]
        if not known_organizations:
            return ""

        entries_by_type: dict[
            OrganizationType, list[KnownOrganizationReferenceDataEntry]
        ] = {organization_type: [] for organization_type in OrganizationType}
        for entry in known_organizations:
            entries_by_type[entry.organization_type].append(entry)

        lines = [config.header]
        for group in config.groups:
            lines.append(group.label)
            for organization_type in group.organization_types:
                if not (entries := entries_by_type[organization_type]):
                    continue

                note_text = ""
                if note := group.type_notes.get(organization_type):
                    note_text = f" ({note})"

                lines.append(f"  {organization_type.description}{note_text}:")
                entry_labels = [
                    (
                        f"{entry.name} (aka {', '.join(entry.aliases)})"
                        if entry.aliases
                        else entry.name
                    )
                    for entry in entries
                ]
                lines.append(render_list(entry_labels, indent_level=4))
        return "\n".join(lines)
