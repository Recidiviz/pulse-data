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
"""Tests for entity_resolution_output_schema_builder.py.

Builds the ER clustering output schema from the fake first-order collection's
entity groups — a top-level group (`location`, whose parent field carries a
semantic-consistency constraint) and an array group (`assignment`, with an ENUM
entity field). Asserts both the JSON schema generated through the unchanged
`LLMJsonSchemaGenerator.generate` (against checked-in goldens) and the structural
properties of the synthesized `LLMRequestOutputSchema`.
"""
import json
from unittest import TestCase

from recidiviz.documents.extraction.entity_resolution.entity_resolution_output_schema_builder import (
    ENTITY_ID_DESCRIPTION,
    ENTRY_NUMS_DESCRIPTION,
    build_entity_resolution_output_schema,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    EntityGroupConfig,
    get_llm_extractor_collection_config,
)
from recidiviz.documents.extraction.models.llm_json_schema_generator import (
    LLMJsonSchemaGenerator,
)
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfIntegerLLMRequestOutputSchemaField,
    ArrayOfStructLLMRequestOutputSchemaField,
    EnumLLMRequestOutputSchemaField,
    LLMOutputFieldMode,
    LLMOutputFieldType,
    LLMRequestOutputSchemaField,
    PrimitiveScalarLLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    ENTITIES_FIELD_NAME,
    ENTITY_ID_FIELD_NAME,
    ENTRY_NUMS_FIELD_NAME,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.ingest import fixtures

_FAKE_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_FIXTURE_SUBDIR = "fixtures/entity_resolution_output_schema"


def _entity_group(name: str) -> EntityGroupConfig:
    collection = get_llm_extractor_collection_config(
        _FAKE_COLLECTION_NAME, config_module=fake_config
    )
    return {group.name: group for group in collection.entity_groups}[name]


def _entities_field(
    schema: LLMRequestOutputSchema,
) -> ArrayOfStructLLMRequestOutputSchemaField:
    entities = schema.get_field(ENTITIES_FIELD_NAME)
    assert isinstance(entities, ArrayOfStructLLMRequestOutputSchemaField)
    return entities


class GoldenSchemaTest(TestCase):
    """Pins the full JSON schema generated (via the unchanged generator) from the
    fake collection's entity groups to checked-in goldens.
    """

    def _assert_matches_golden(self, *, group_name: str, golden_filename: str) -> None:
        schema = build_entity_resolution_output_schema(
            entity_group=_entity_group(group_name)
        )
        with open(
            fixtures.as_filepath(golden_filename, subdir=_FIXTURE_SUBDIR),
            encoding="utf-8",
        ) as golden_file:
            expected = json.load(golden_file)
        self.assertEqual(expected, LLMJsonSchemaGenerator.generate(schema))

    def test_top_level_group_schema_matches_golden(self) -> None:
        self._assert_matches_golden(
            group_name="location",
            golden_filename="fake_collection_location_er_json_schema.json",
        )

    def test_array_group_schema_matches_golden(self) -> None:
        self._assert_matches_golden(
            group_name="assignment",
            golden_filename="fake_collection_assignment_er_json_schema.json",
        )


class SynthesizedSchemaStructureTest(TestCase):
    """Structural properties of the synthesized `LLMRequestOutputSchema`."""

    def test_no_is_relevant(self) -> None:
        schema = build_entity_resolution_output_schema(
            entity_group=_entity_group("location")
        )
        self.assertIsNone(schema.relevance_criteria)
        self.assertIsNone(schema.is_relevant_field)
        # The single `entities` field is the whole output — no is_relevant.
        self.assertEqual(
            [ENTITIES_FIELD_NAME], [field.name for field in schema.all_fields]
        )

    def test_entities_is_required_array_with_min_items_one(self) -> None:
        schema = build_entity_resolution_output_schema(
            entity_group=_entity_group("location")
        )
        self.assertEqual(
            [ENTITIES_FIELD_NAME],
            [field.name for field in schema.user_defined_fields],
        )
        entities = _entities_field(schema)
        self.assertEqual(LLMOutputFieldType.ARRAY_OF_STRUCT, entities.field_type)
        self.assertTrue(entities.required)
        self.assertEqual(1, entities.min_items)

    def test_entities_sub_field_order(self) -> None:
        # entity_id first, then the entity fields in declaration order, then
        # entry_nums.
        schema = build_entity_resolution_output_schema(
            entity_group=_entity_group("assignment")
        )
        self.assertEqual(
            [
                ENTITY_ID_FIELD_NAME,
                "assignment_name",
                "assignment_type",
                ENTRY_NUMS_FIELD_NAME,
            ],
            [field.name for field in _entities_field(schema).fields],
        )

    def test_entity_id_field(self) -> None:
        schema = build_entity_resolution_output_schema(
            entity_group=_entity_group("location")
        )
        entity_id = _entities_field(schema).get_field(ENTITY_ID_FIELD_NAME)
        self.assertIsInstance(entity_id, PrimitiveScalarLLMRequestOutputSchemaField)
        self.assertEqual(LLMOutputFieldType.INTEGER, entity_id.field_type)
        self.assertEqual(LLMOutputFieldMode.STRUCTURAL, entity_id.field_mode)
        self.assertTrue(entity_id.required)
        self.assertFalse(entity_id.nullable)
        self.assertEqual(ENTITY_ID_DESCRIPTION, entity_id.description)

    def test_entry_nums_field(self) -> None:
        schema = build_entity_resolution_output_schema(
            entity_group=_entity_group("location")
        )
        entry_nums = _entities_field(schema).get_field(ENTRY_NUMS_FIELD_NAME)
        self.assertIsInstance(entry_nums, ArrayOfIntegerLLMRequestOutputSchemaField)
        assert isinstance(entry_nums, ArrayOfIntegerLLMRequestOutputSchemaField)
        self.assertEqual(LLMOutputFieldMode.STRUCTURAL, entry_nums.field_mode)
        self.assertTrue(entry_nums.required)
        self.assertFalse(entry_nums.nullable)
        self.assertEqual(1, entry_nums.min_items)
        self.assertEqual(ENTRY_NUMS_DESCRIPTION, entry_nums.description)

    def test_primary_keys_are_entity_field_names(self) -> None:
        schema = build_entity_resolution_output_schema(
            entity_group=_entity_group("assignment")
        )
        self.assertEqual(
            ["assignment_name", "assignment_type"],
            _entities_field(schema).primary_keys,
        )


class EntityFieldConversionTest(TestCase):
    """The parent entity fields copied into the schema, flipped to STRUCTURAL."""

    def _built_entity_fields_by_name(
        self, group_name: str
    ) -> dict[str, LLMRequestOutputSchemaField]:
        schema = build_entity_resolution_output_schema(
            entity_group=_entity_group(group_name)
        )
        framework_names = {ENTITY_ID_FIELD_NAME, ENTRY_NUMS_FIELD_NAME}
        return {
            field.name: field
            for field in _entities_field(schema).fields
            if field.name not in framework_names
        }

    def test_entity_fields_are_structural_present_and_unconstrained(self) -> None:
        # The parent `location` field is INFERRED and carries a
        # not_applicable_when_value constraint; the copy is STRUCTURAL, always
        # present (required), and drops the constraint.
        location = self._built_entity_fields_by_name("location")["location"]
        self.assertEqual(LLMOutputFieldMode.STRUCTURAL, location.field_mode)
        self.assertIsNone(location.inferred_field_config)
        self.assertTrue(location.required)
        self.assertEqual([], location.semantic_consistency_constraints)

    def test_entity_field_nullability_inherited_from_first_order(self) -> None:
        # Every entity field key is present (required); its value is nullable iff
        # the field was optional in the first-order collection. A first-order
        # required field is non-nullable (a value is guaranteed); an optional
        # field is nullable (the model returns null when no mention filled it).
        assignment_fields = self._built_entity_fields_by_name("assignment")
        # assignment_name is `required: true` in the fake collection -> non-null.
        self.assertFalse(assignment_fields["assignment_name"].nullable)
        # assignment_type is not required in the fake collection -> nullable.
        self.assertTrue(assignment_fields["assignment_type"].nullable)
        # location carries a constraint, so it is not required either -> nullable.
        self.assertTrue(
            self._built_entity_fields_by_name("location")["location"].nullable
        )

    def test_entity_field_type_and_description_preserved(self) -> None:
        group = _entity_group("assignment")
        built_by_name = self._built_entity_fields_by_name("assignment")
        for parent_field in group.entity_fields:
            with self.subTest(field=parent_field.name):
                built = built_by_name[parent_field.name]
                self.assertEqual(parent_field.field_type, built.field_type)
                self.assertEqual(parent_field.description, built.description)

    def test_enum_entity_field_preserves_values(self) -> None:
        assignment_type = self._built_entity_fields_by_name("assignment")[
            "assignment_type"
        ]
        self.assertIsInstance(assignment_type, EnumLLMRequestOutputSchemaField)
        assert isinstance(assignment_type, EnumLLMRequestOutputSchemaField)
        self.assertEqual(["internal", "external", "other"], assignment_type.value_names)
        self.assertEqual(LLMOutputFieldMode.STRUCTURAL, assignment_type.field_mode)
