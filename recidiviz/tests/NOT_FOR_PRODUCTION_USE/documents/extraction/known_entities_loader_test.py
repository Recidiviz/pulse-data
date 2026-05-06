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
"""Tests for known_entities_loader.py."""
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from recidiviz.common.constants.states import StateCode
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.known_entities_loader import (
    KnownEntity,
    KnownEntityType,
    load_known_entities,
    render_employment_known_entities_context,
    render_housing_known_entities_context,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.llm_prompt_extractor_metadata import (
    LLMPromptExtractorMetadata,
)

_CONFIG_DIR_PATH = "recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.known_entities_loader._KNOWN_ENTITIES_CONFIG_DIR"


class TestLoadKnownEntities(unittest.TestCase):
    """Tests for load_known_entities."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self._tmpdir)
        self.enterContext(patch(_CONFIG_DIR_PATH, self._tmpdir))

    def _write_yaml(self, state_code: StateCode, content: str) -> None:
        path = os.path.join(self._tmpdir, f"{state_code.value.lower()}.yaml")
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

    def test_missing_file_returns_empty_list(self) -> None:
        result = load_known_entities(StateCode.US_OZ)
        self.assertEqual(result, [])

    def test_loads_entity_with_aliases(self) -> None:
        self._write_yaml(
            StateCode.US_OZ,
            """
known_entities:
  - name: "BBSI Staffing"
    aliases:
      - "BBSI"
      - "BB&S"
    entity_type: staffing_agency
    notes: "PEO"
""",
        )
        entities = load_known_entities(StateCode.US_OZ)
        self.assertEqual(len(entities), 1)
        self.assertEqual(entities[0].name, "BBSI Staffing")
        self.assertEqual(entities[0].aliases, ["BBSI", "BB&S"])
        self.assertEqual(entities[0].entity_type, KnownEntityType.STAFFING_AGENCY)

    def test_loads_entity_without_aliases(self) -> None:
        self._write_yaml(
            StateCode.US_OZ,
            """
known_entities:
  - name: "Wood Court"
    entity_type: nonresidential_program
""",
        )
        entities = load_known_entities(StateCode.US_OZ)
        self.assertEqual(len(entities), 1)
        self.assertEqual(entities[0].aliases, [])

    def test_invalid_entity_type_raises(self) -> None:
        self._write_yaml(
            StateCode.US_OZ,
            """
known_entities:
  - name: "Something"
    entity_type: not_a_real_type
""",
        )
        with self.assertRaises(ValueError):
            load_known_entities(StateCode.US_OZ)

    def test_loads_multiple_entities(self) -> None:
        self._write_yaml(
            StateCode.US_OZ,
            """
known_entities:
  - name: "Wood Court"
    entity_type: nonresidential_program
  - name: "Stride Sober Living"
    aliases: ["Stride"]
    entity_type: sober_living
  - name: "BBSI"
    entity_type: staffing_agency
""",
        )
        entities = load_known_entities(StateCode.US_OZ)
        self.assertEqual(len(entities), 3)
        types = {e.entity_type for e in entities}
        self.assertIn(KnownEntityType.NONRESIDENTIAL_PROGRAM, types)
        self.assertIn(KnownEntityType.SOBER_LIVING, types)
        self.assertIn(KnownEntityType.STAFFING_AGENCY, types)


class TestRenderHousingContext(unittest.TestCase):
    """Tests for render_housing_known_entities_context."""

    def test_empty_entities_returns_empty_string(self) -> None:
        self.assertEqual(render_housing_known_entities_context([]), "")

    def test_only_staffing_agency_returns_empty_string(self) -> None:
        # staffing_agency is "not housing" so it should appear
        entities = [
            KnownEntity(name="BBSI", entity_type=KnownEntityType.STAFFING_AGENCY)
        ]
        result = render_housing_known_entities_context(entities)
        self.assertIn("BBSI", result)
        self.assertIn("not housing", result.lower())

    def test_nonresidential_program_in_not_housing_section(self) -> None:
        entities = [
            KnownEntity(
                name="Wood Court", entity_type=KnownEntityType.NONRESIDENTIAL_PROGRAM
            )
        ]
        result = render_housing_known_entities_context(entities)
        self.assertIn("Wood Court", result)
        self.assertNotIn("temporary_housing_type", result)

    def test_sober_living_in_temp_housing_section(self) -> None:
        entities = [
            KnownEntity(
                name="Stride Sober Living",
                aliases=["Stride"],
                entity_type=KnownEntityType.SOBER_LIVING,
            )
        ]
        result = render_housing_known_entities_context(entities)
        self.assertIn("Stride Sober Living", result)
        self.assertIn("Stride", result)
        self.assertIn("sober_living", result)

    def test_aliases_appear_in_output(self) -> None:
        entities = [
            KnownEntity(
                name="Trivium",
                aliases=["Trivium Inc", "Trivium Recovery"],
                entity_type=KnownEntityType.NONRESIDENTIAL_PROGRAM,
            )
        ]
        result = render_housing_known_entities_context(entities)
        self.assertIn("Trivium Inc", result)
        self.assertIn("Trivium Recovery", result)

    def test_mixed_entities_both_sections_present(self) -> None:
        entities = [
            KnownEntity(
                name="Wood Court", entity_type=KnownEntityType.NONRESIDENTIAL_PROGRAM
            ),
            KnownEntity(name="Stride", entity_type=KnownEntityType.SOBER_LIVING),
        ]
        result = render_housing_known_entities_context(entities)
        self.assertIn("Wood Court", result)
        self.assertIn("Stride", result)
        self.assertIn("sober_living", result)


class TestRenderHousingContextResidentialTreatment(unittest.TestCase):
    """Tests for residential_treatment entities in render_housing_known_entities_context."""

    def test_residential_treatment_in_own_section(self) -> None:
        entities = [
            KnownEntity(
                name="Centerstone", entity_type=KnownEntityType.RESIDENTIAL_TREATMENT
            )
        ]
        result = render_housing_known_entities_context(entities)
        self.assertIn("Centerstone", result)
        self.assertIn(
            "may be residential or outpatient",
            result,
        )
        self.assertIn("temporary_housing_type=treatment_program", result)

    def test_residential_treatment_not_in_not_housing_section(self) -> None:
        entities = [
            KnownEntity(
                name="Centerstone", entity_type=KnownEntityType.RESIDENTIAL_TREATMENT
            )
        ]
        result = render_housing_known_entities_context(entities)
        self.assertNotIn("NOT housing locations", result)

    def test_residential_treatment_not_in_temp_housing_section(self) -> None:
        entities = [
            KnownEntity(
                name="Centerstone", entity_type=KnownEntityType.RESIDENTIAL_TREATMENT
            )
        ]
        result = render_housing_known_entities_context(entities)
        self.assertNotIn("ARE temporary housing locations", result)

    def test_residential_treatment_not_an_employer(self) -> None:
        entities = [
            KnownEntity(
                name="Centerstone", entity_type=KnownEntityType.RESIDENTIAL_TREATMENT
            )
        ]
        result = render_employment_known_entities_context(entities)
        self.assertIn("Centerstone", result)
        self.assertIn("NOT employers", result)
        self.assertIn("residential or outpatient treatment facility", result)


class TestRenderEmploymentContext(unittest.TestCase):
    """Tests for render_employment_known_entities_context."""

    def test_empty_entities_returns_empty_string(self) -> None:
        self.assertEqual(render_employment_known_entities_context([]), "")

    def test_nonresidential_program_in_not_employer_section(self) -> None:
        entities = [
            KnownEntity(
                name="Wood Court", entity_type=KnownEntityType.NONRESIDENTIAL_PROGRAM
            )
        ]
        result = render_employment_known_entities_context(entities)
        self.assertIn("Wood Court", result)
        self.assertIn("NOT employers", result)

    def test_staffing_agency_in_agency_section(self) -> None:
        entities = [
            KnownEntity(
                name="BBSI Staffing",
                aliases=["BBSI"],
                entity_type=KnownEntityType.STAFFING_AGENCY,
            )
        ]
        result = render_employment_known_entities_context(entities)
        self.assertIn("BBSI Staffing", result)
        self.assertIn("employment_type: temp_agency", result)
        self.assertNotIn("NOT employers", result)

    def test_sober_living_not_employer(self) -> None:
        entities = [
            KnownEntity(name="Stride", entity_type=KnownEntityType.SOBER_LIVING)
        ]
        result = render_employment_known_entities_context(entities)
        self.assertIn("Stride", result)
        self.assertIn("NOT employers", result)

    def test_aliases_appear_in_employment_output(self) -> None:
        entities = [
            KnownEntity(
                name="BBSI Staffing",
                aliases=["BBSI", "BB&S"],
                entity_type=KnownEntityType.STAFFING_AGENCY,
            )
        ]
        result = render_employment_known_entities_context(entities)
        self.assertIn("BB&S", result)


class TestKnownEntityDisplayNames(unittest.TestCase):
    """Tests for KnownEntity.display_names."""

    def test_no_aliases(self) -> None:
        entity = KnownEntity(
            name="Foo", entity_type=KnownEntityType.NONRESIDENTIAL_PROGRAM
        )
        self.assertEqual(entity.display_names(), "Foo")

    def test_with_aliases(self) -> None:
        entity = KnownEntity(
            name="Foo",
            aliases=["Bar", "Baz"],
            entity_type=KnownEntityType.NONRESIDENTIAL_PROGRAM,
        )
        self.assertEqual(entity.display_names(), "Foo, Bar, Baz")


class TestKnownEntitiesInPromptTemplate(unittest.TestCase):
    """Integration: known entities YAML → render → prompt template substitution.

    Shows how entries in a state's known_entities YAML flow through rendering
    into the final instructions_prompt that gets sent to the LLM. The
    {known_entities_context} placeholder in a prompt template is automatically
    populated from the state YAML without any explicit prompt_vars in the
    extractor YAML.
    """

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self._tmpdir)
        self._known_entities_dir = os.path.join(self._tmpdir, "known_entities")
        os.makedirs(self._known_entities_dir)
        self.enterContext(patch(_CONFIG_DIR_PATH, self._known_entities_dir))

    def _write_known_entities_yaml(self, state_code: StateCode, content: str) -> None:
        path = os.path.join(
            self._known_entities_dir, f"{state_code.value.lower()}.yaml"
        )
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

    def _write_collection_dir(self, collection_name: str, state_code: StateCode) -> str:
        """Creates a minimal collection directory. Returns path to the extractor YAML."""
        collection_dir = os.path.join(self._tmpdir, collection_name.lower())
        os.makedirs(collection_dir, exist_ok=True)

        with open(
            os.path.join(collection_dir, "collection.yaml"), "w", encoding="utf-8"
        ) as f:
            f.write(
                f"name: {collection_name}\n"
                "description: Test collection\n"
                "output_schema:\n"
                "  full_batch_description: Extracted info\n"
                "  result_level_description: Info for one note\n"
                "  inferred_fields:\n"
                "    - name: test_field\n"
                "      type: BOOLEAN\n"
                "      required: false\n"
                "      description: Test field\n"
            )

        with open(
            os.path.join(collection_dir, "prompt_template.txt"), "w", encoding="utf-8"
        ) as f:
            f.write(
                "Extract information from this note.\n"
                "\n"
                "{known_entities_context}\n"
                "\n"
                "{output_format_instructions}\n"
            )

        extractor_path = os.path.join(
            collection_dir, f"{state_code.value.lower()}_extractor.yaml"
        )
        with open(extractor_path, "w", encoding="utf-8") as f:
            f.write(
                f"collection_name: {collection_name}\n"
                "description: Test extractor\n"
                f"input_document_collection_name: {state_code.value}_CASE_NOTES\n"
                "llm_provider: vertex_ai\n"
                "model: gemini-2-5-flash\n"
            )

        return extractor_path

    def test_housing_collection_known_entities_appear_in_prompt(self) -> None:
        self._write_known_entities_yaml(
            StateCode.US_OZ,
            """
known_entities:
  - name: "Stride Sober Living"
    aliases: ["Stride"]
    entity_type: sober_living
  - name: "Wood Court"
    entity_type: nonresidential_program
  - name: "BBSI Staffing"
    aliases: ["BBSI"]
    entity_type: staffing_agency
""",
        )
        extractor_path = self._write_collection_dir(
            "CASE_NOTE_HOUSING_INFO", StateCode.US_OZ
        )
        prompt = LLMPromptExtractorMetadata.from_yaml(
            extractor_path
        ).instructions_prompt

        # Non-housing entities appear with their housing_label in the "NOT housing" section.
        self.assertIn(
            "The following are NOT housing locations "
            "(do not set primary_status or housed_type based on these):",
            prompt,
        )
        self.assertIn(
            "- Wood Court (nonresidential program; clients attend but do not live there)",
            prompt,
        )
        self.assertIn("- BBSI Staffing, BBSI (staffing agency)", prompt)

        # Sober living appears in the temporary housing section with its sub-type.
        self.assertIn(
            "The following ARE temporary housing locations "
            "(use housed_type=temporary_housing when the person is living there):",
            prompt,
        )
        self.assertIn(
            "- Stride Sober Living, Stride → temporary_housing_type: sober_living",
            prompt,
        )

    def test_employment_collection_known_entities_appear_in_prompt(self) -> None:
        self._write_known_entities_yaml(
            StateCode.US_OZ,
            """
known_entities:
  - name: "BBSI Staffing"
    aliases: ["BBSI"]
    entity_type: staffing_agency
  - name: "Wood Court"
    entity_type: nonresidential_program
  - name: "Stride Sober Living"
    entity_type: sober_living
""",
        )
        extractor_path = self._write_collection_dir(
            "CASE_NOTE_EMPLOYMENT_INFO", StateCode.US_OZ
        )
        prompt = LLMPromptExtractorMetadata.from_yaml(
            extractor_path
        ).instructions_prompt

        # Non-employer entities appear with their employer_label in the "NOT employers" section.
        self.assertIn(
            "The following are NOT employers "
            "(do not extract as employment, even if mentioned in the note):",
            prompt,
        )
        self.assertIn(
            "- Wood Court (nonresidential program (drug court, treatment, supervision))",
            prompt,
        )
        self.assertIn("- Stride Sober Living (sober living facility)", prompt)

        # Staffing agency appears in the known employers section with its org type.
        self.assertIn(
            "The following are known employers with a specific organization type:",
            prompt,
        )
        self.assertIn("- BBSI Staffing, BBSI → employment_type: temp_agency", prompt)

    def test_no_known_entities_file_leaves_placeholder_empty(self) -> None:
        # US_XX has no known entities YAML — {known_entities_context} renders to ""
        # and the placeholder is simply absent from the final prompt.
        extractor_path = self._write_collection_dir(
            "CASE_NOTE_HOUSING_INFO", StateCode.US_XX
        )
        prompt = LLMPromptExtractorMetadata.from_yaml(
            extractor_path
        ).instructions_prompt

        self.assertNotIn("KNOWN ENTITIES", prompt)
        self.assertIn("Extract information from this note.", prompt)


if __name__ == "__main__":
    unittest.main()
