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
"""Tests for llm_extractor_collection_config.py.

Focuses on collection-level logic not covered by the output-schema/field tests:
entity-group resolution against the schema (now storing resolved field objects),
cross-config resolution (model registry, parent directory), and tolerance of the
not-yet-modeled blocks.
"""
import json
import re
import tempfile
from pathlib import Path
from typing import Any
from unittest import TestCase

import jsonschema
import yaml

from recidiviz.documents.extraction.config_defaults import (
    DEFAULT_MINIMUM_CONFIDENCE_LEVEL,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    EntityGroupConfig,
    LLMExtractorCollectionConfig,
    get_llm_extractor_collection_config,
    load_llm_extractor_collection_configs,
)
from recidiviz.documents.extraction.models.llm_model_registry import (
    LLMModelRegistry,
    load_llm_model_registry,
)
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
    ConfidenceLevel,
    LLMOutputFieldMode,
    LLMOutputFieldType,
)
from recidiviz.documents.extraction.models.reference_data.acronym_reference_data_entry import (
    AcronymReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.llm_extractor_collection_reference_data_config import (
    LLMExtractorCollectionReferenceDataConfig,
    LLMExtractorCollectionReferenceDataConfigForType,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_registry import (
    ReferenceDataType,
)
from recidiviz.tests.documents import fake_config
from recidiviz.utils.yaml_dict import YAMLDict

_DESCRIPTION = "A description that is long enough to be meaningful."
_COLLECTION_DESCRIPTION = "Extract employment information from case notes."
# A model config that exists in recidiviz/tests/documents/fake_config/model_registry.yaml.
_FAKE_MODEL_CONFIG_NAME = "GLOBEX_BASIC_DEFAULT"
# The collection defined under recidiviz/tests/documents/fake_config/.
_FAKE_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"


def _output_schema() -> LLMRequestOutputSchema:
    """Returns a schema with a top-level ENUM + STRING and an ARRAY_OF_STRUCT,
    to resolve entity groups against.
    """
    return LLMRequestOutputSchema.from_yaml_dict(
        yaml_dict=YAMLDict(
            {
                "full_batch_description": _DESCRIPTION,
                "result_level_description": _DESCRIPTION,
                "inferred_fields": [
                    {
                        "name": "primary_status",
                        "type": "ENUM",
                        "values": ["employed"],
                        "description": _DESCRIPTION,
                    },
                    {"name": "address", "type": "STRING", "description": _DESCRIPTION},
                    {
                        "name": "employers",
                        "type": "ARRAY_OF_STRUCT",
                        "primary_keys": ["employer_name"],
                        "description": _DESCRIPTION,
                        "fields": [
                            {
                                "name": "employer_name",
                                "type": "STRING",
                                "description": _DESCRIPTION,
                            },
                            {
                                "name": "job_title",
                                "type": "STRING",
                                "description": _DESCRIPTION,
                            },
                        ],
                    },
                ],
            }
        ),
        collection_description=_COLLECTION_DESCRIPTION,
        default_minimum_confidence_level=ConfidenceLevel.INFERRED,
    )


def _entity_group(**raw: Any) -> EntityGroupConfig:
    """Parses one entity group against the shared `_output_schema`."""
    return EntityGroupConfig.from_yaml_dict(
        yaml_dict=YAMLDict(raw), output_schema=_output_schema()
    )


def _simple_schema(
    *, collection_description: str = _COLLECTION_DESCRIPTION, field_name: str = "status"
) -> LLMRequestOutputSchema:
    """Returns a one-field schema, varying only the inputs a single
    collection-version-ID test wants to perturb.
    """
    return LLMRequestOutputSchema.from_yaml_dict(
        yaml_dict=YAMLDict(
            {
                "full_batch_description": _DESCRIPTION,
                "result_level_description": _DESCRIPTION,
                "inferred_fields": [
                    {"name": field_name, "type": "STRING", "description": _DESCRIPTION}
                ],
            }
        ),
        collection_description=collection_description,
        default_minimum_confidence_level=ConfidenceLevel.INFERRED,
    )


class ParseAllCollectionConfigsTest(TestCase):
    """Guards real configs, and exercises the fake config's happy path."""

    def test_load_all_real_configs(self) -> None:
        # Raises if any real collection config fails to parse or validate. Does
        # not assert on any state's specifics — the fake config covers structure.
        configs = load_llm_extractor_collection_configs()
        self.assertTrue(configs)

    def test_all_real_configs_generate_valid_json_schema(self) -> None:
        # Every real collection's output schema must lower to a valid JSON Schema.
        configs = load_llm_extractor_collection_configs()
        self.assertTrue(configs)
        for name, collection in configs.items():
            with self.subTest(collection=name):
                # check_schema is a real classmethod missing from the type stubs.
                jsonschema.Draft202012Validator.check_schema(  # type: ignore[attr-defined]
                    collection.generate_json_schema()
                )

    def test_get_unknown_collection_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape("No extractor collection named [NOT_A_COLLECTION]."),
        ):
            get_llm_extractor_collection_config(
                "NOT_A_COLLECTION", config_module=fake_config
            )

    def test_fake_collection_parses_rich_structure(self) -> None:
        collection = get_llm_extractor_collection_config(
            _FAKE_COLLECTION_NAME, config_module=fake_config
        )

        self.assertEqual(_FAKE_COLLECTION_NAME, collection.name)
        self.assertEqual("ACME_LARGE_NO_THINKING", collection.default_model_config_name)
        self.assertEqual(ConfidenceLevel.INFERRED, collection.minimum_confidence_level)

        schema = collection.output_schema
        # is_relevant is injected first; user_defined_fields excludes it.
        self.assertEqual("is_relevant", schema.all_fields[0].name)
        self.assertEqual(
            [
                (
                    "primary_status",
                    LLMOutputFieldType.ENUM,
                    LLMOutputFieldMode.INFERRED,
                ),
                (
                    "status_note",
                    LLMOutputFieldType.STRING,
                    LLMOutputFieldMode.STRUCTURAL,
                ),
                ("location", LLMOutputFieldType.STRING, LLMOutputFieldMode.INFERRED),
                (
                    "assignments",
                    LLMOutputFieldType.ARRAY_OF_STRUCT,
                    LLMOutputFieldMode.INFERRED,
                ),
            ],
            [
                (field.name, field.field_type, field.field_mode)
                for field in schema.user_defined_fields
            ],
        )

        # Per-field minimum_confidence_level override on a nested sub-field.
        assignments = schema.get_field("assignments")
        assert isinstance(assignments, ArrayOfStructLLMRequestOutputSchemaField)
        self.assertEqual(["assignment_name"], assignments.primary_keys)
        self.assertEqual(
            ConfidenceLevel.EXPLICIT,
            assignments.get_field("rate_amount").minimum_confidence_level,
        )

    def test_fake_collection_resolves_entity_groups_to_field_objects(self) -> None:
        collection = get_llm_extractor_collection_config(
            _FAKE_COLLECTION_NAME, config_module=fake_config
        )
        groups_by_name = {group.name: group for group in collection.entity_groups}

        # Top-level group: no source_array_field; entity_fields are the actual
        # top-level field objects.
        location_group = groups_by_name["location"]
        self.assertIsNone(location_group.source_array_field)
        self.assertEqual(
            [collection.output_schema.get_field("location")],
            location_group.entity_fields,
        )

        # Array group: source_array_field is the resolved array object; entity_fields
        # are its sub-field objects.
        assignment_group = groups_by_name["assignment"]
        assignments = collection.output_schema.get_field("assignments")
        assert isinstance(assignments, ArrayOfStructLLMRequestOutputSchemaField)
        self.assertIs(assignments, assignment_group.source_array_field)
        self.assertEqual(
            [assignments.get_field("assignment_name")],
            assignment_group.entity_fields,
        )


class EntityGroupConfigResolutionTest(TestCase):
    """EntityGroupConfig.from_yaml_dict resolution against the output schema."""

    def test_top_level_group_resolves_to_top_level_field_objects(self) -> None:
        schema = _output_schema()
        group = EntityGroupConfig.from_yaml_dict(
            yaml_dict=YAMLDict({"name": "residence", "entity_fields": ["address"]}),
            output_schema=schema,
        )
        self.assertIsNone(group.source_array_field)
        self.assertEqual([schema.get_field("address")], group.entity_fields)

    def test_array_group_resolves_to_sub_field_objects(self) -> None:
        schema = _output_schema()
        employers = schema.get_field("employers")
        assert isinstance(employers, ArrayOfStructLLMRequestOutputSchemaField)
        group = EntityGroupConfig.from_yaml_dict(
            yaml_dict=YAMLDict(
                {
                    "name": "employer",
                    "source_array_field": "employers",
                    "entity_fields": ["employer_name"],
                }
            ),
            output_schema=schema,
        )
        self.assertIs(employers, group.source_array_field)
        self.assertEqual([employers.get_field("employer_name")], group.entity_fields)

    def test_top_level_entity_field_unresolved_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Entity group [residence] declares entity_fields ['ghost'] that "
                "do not resolve against the top-level output schema fields: "
            ),
        ):
            _entity_group(name="residence", entity_fields=["ghost"])

    def test_array_entity_field_unresolved_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Entity group [employer] declares entity_fields ['ghost'] that do "
                "not resolve against the sub-fields of [employers]: "
            ),
        ):
            _entity_group(
                name="employer",
                source_array_field="employers",
                entity_fields=["ghost"],
            )

    def test_source_array_field_not_an_array_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Entity group [bad] declares source_array_field [primary_status], "
                "which has type [ENUM] — it must be an ARRAY_OF_STRUCT."
            ),
        ):
            _entity_group(
                name="bad",
                source_array_field="primary_status",
                entity_fields=["employer_name"],
            )

    def test_source_array_field_missing_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape("Output schema has no top-level field named [ghost]."),
        ):
            _entity_group(name="bad", source_array_field="ghost", entity_fields=["x"])

    def test_duplicate_entity_fields_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Entity group [residence] declares duplicate entity_fields: "
                "['address']."
            ),
        ):
            _entity_group(name="residence", entity_fields=["address", "address"])

    def test_unexpected_key_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape("Found unexpected config values for entity group [residence]"),
        ):
            _entity_group(name="residence", entity_fields=["address"], bogus="x")

    def test_direct_construction_membership_invariant(self) -> None:
        # source_array_field set, but an entity_field is a top-level field (not one
        # of the array's sub-fields) — the post_init guard for direct construction.
        schema = _output_schema()
        employers = schema.get_field("employers")
        assert isinstance(employers, ArrayOfStructLLMRequestOutputSchemaField)
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Entity group [employer] declares entity_fields ['address'] that "
                "are not sub-fields of source_array_field [employers]."
            ),
        ):
            EntityGroupConfig(
                name="employer",
                entity_fields=[schema.get_field("address")],
                source_array_field=employers,
            )


class LLMExtractorCollectionConfigTest(TestCase):
    """Collection-level invariants exercised via direct construction."""

    def _collection(
        self, *, name: str = "TEST_COLLECTION", entity_groups: Any = None
    ) -> LLMExtractorCollectionConfig:
        return LLMExtractorCollectionConfig(
            name=name,
            description=_DESCRIPTION,
            default_model_config_name=_FAKE_MODEL_CONFIG_NAME,
            minimum_confidence_level=ConfidenceLevel.INFERRED,
            output_schema=_output_schema(),
            reference_data_config=LLMExtractorCollectionReferenceDataConfig(
                per_type_configs={}
            ),
            entity_groups=entity_groups or [],
        )

    def test_valid_name(self) -> None:
        self.assertEqual("TEST_COLLECTION", self._collection().name)

    def test_non_upper_snake_case_name_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be UPPER_SNAKE_CASE"):
            self._collection(name="lowercase")

    def test_reserved_word_name_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "is a reserved word"):
            self._collection(name="SELECT")

    def test_duplicate_entity_group_names_raise(self) -> None:
        schema = _output_schema()
        group = EntityGroupConfig(
            name="employer",
            entity_fields=[schema.get_field("address")],
            source_array_field=None,
        )
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Collection [TEST_COLLECTION] declares duplicate entity group "
                "names: ['employer']."
            ),
        ):
            self._collection(entity_groups=[group, group])


class CollectionVersionIdTest(TestCase):
    """Tests for LLMExtractorCollectionConfig.collection_version_id."""

    def _collection(
        self,
        *,
        name: str = "TEST_COLLECTION",
        output_schema: LLMRequestOutputSchema | None = None,
    ) -> LLMExtractorCollectionConfig:
        return LLMExtractorCollectionConfig(
            name=name,
            description=_DESCRIPTION,
            default_model_config_name=_FAKE_MODEL_CONFIG_NAME,
            minimum_confidence_level=ConfidenceLevel.INFERRED,
            output_schema=output_schema
            if output_schema is not None
            else _output_schema(),
            reference_data_config=LLMExtractorCollectionReferenceDataConfig(
                per_type_configs={}
            ),
            entity_groups=[],
        )

    def test_collection_version_id_golden(self) -> None:
        # Pinned hash for the fake collection. A change here is a real version bump
        # and must be consciously updated, not silently accepted.
        collection = get_llm_extractor_collection_config(
            _FAKE_COLLECTION_NAME, config_module=fake_config
        )
        self.assertEqual(
            "a55958b21d1f25555197cf5f9db3c1066e4a3a1cbf0809f6c6899e26c9120161",
            collection.collection_version_id,
        )

    def test_collection_version_id_stable_across_equal_collections(self) -> None:
        self.assertEqual(
            self._collection().collection_version_id,
            self._collection().collection_version_id,
        )

    def test_collection_version_id_changes_with_output_schema(self) -> None:
        self.assertNotEqual(
            self._collection(
                output_schema=_simple_schema(field_name="status")
            ).collection_version_id,
            self._collection(
                output_schema=_simple_schema(field_name="other_status")
            ).collection_version_id,
        )

    def test_collection_version_id_changes_with_name(self) -> None:
        # Versions are namespaced to a collection's name.
        self.assertNotEqual(
            self._collection(name="TEST_COLLECTION").collection_version_id,
            self._collection(name="OTHER_COLLECTION").collection_version_id,
        )

    def test_collection_version_id_changes_with_collection_description(self) -> None:
        # The collection description is folded into the generated schema via the
        # auto-generated is_relevant field description, so a description change
        # yields a new version ID even though it is not hashed separately.
        self.assertNotEqual(
            self._collection(
                output_schema=_simple_schema(
                    collection_description="First meaningful collection description."
                )
            ).collection_version_id,
            self._collection(
                output_schema=_simple_schema(
                    collection_description="Second meaningful collection description."
                )
            ).collection_version_id,
        )


class CollectionConfigFromYamlTest(TestCase):
    """from_yaml behaviors that need a file path + model registry."""

    def setUp(self) -> None:
        self.registry: LLMModelRegistry = load_llm_model_registry(
            config_module=fake_config
        )

    def _parse(
        self,
        *,
        name: str,
        directory_name: str | None = None,
        include_reference_data: bool = True,
        **body: Any,
    ) -> LLMExtractorCollectionConfig:
        """Writes a collection.yaml under a temp directory (named |directory_name|,
        defaulting to the lowercased |name|) and parses it. `reference_data` is a
        required block, so an empty one is emitted by default unless overridden in
        |body| or suppressed via |include_reference_data|.
        """
        contents: dict[str, Any] = {
            "name": name,
            "description": _DESCRIPTION,
            "default_model_config_name": _FAKE_MODEL_CONFIG_NAME,
            "output_schema": {
                "full_batch_description": _DESCRIPTION,
                "result_level_description": _DESCRIPTION,
                "inferred_fields": [
                    {"name": "a", "type": "STRING", "description": _DESCRIPTION}
                ],
            },
        }
        if include_reference_data:
            contents["reference_data"] = {}
        contents.update(body)
        # pylint: disable=consider-using-with
        tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tmp_dir.cleanup)
        collection_dir = Path(tmp_dir.name) / (directory_name or name.lower())
        collection_dir.mkdir()
        yaml_path = collection_dir / "collection.yaml"
        yaml_path.write_text(yaml.dump(contents, sort_keys=False))
        return LLMExtractorCollectionConfig.from_yaml(
            yaml_path=yaml_path, model_registry=self.registry
        )

    def test_valid_parse(self) -> None:
        config = self._parse(name="TEST_COLLECTION")
        self.assertEqual("TEST_COLLECTION", config.name)
        self.assertEqual(_FAKE_MODEL_CONFIG_NAME, config.default_model_config_name)

    def test_name_directory_mismatch_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "does not match its parent directory"):
            self._parse(name="TEST_COLLECTION", directory_name="wrong_dir")

    def test_unknown_model_config_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape("No model config named [NONEXISTENT_MODEL_CONFIG]"),
        ):
            self._parse(
                name="TEST_COLLECTION",
                default_model_config_name="NONEXISTENT_MODEL_CONFIG",
            )

    def test_unexpected_top_level_key_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Found unexpected config values for collection [TEST_COLLECTION]"
            ),
        ):
            self._parse(name="TEST_COLLECTION", bogus_key="value")

    def test_golden_eval_block_is_tolerated(self) -> None:
        # golden_eval is not yet modeled — consumed and discarded, no error.
        config = self._parse(
            name="TEST_COLLECTION",
            golden_eval={"source_sheet_uri": "https://example.com", "anything": 1},
        )
        self.assertEqual("TEST_COLLECTION", config.name)

    def test_reference_data_block_is_parsed(self) -> None:
        config = self._parse(
            name="TEST_COLLECTION",
            reference_data={"acronyms": {"prompt_var": "x", "header": "y"}},
        )
        self.assertEqual(
            LLMExtractorCollectionReferenceDataConfig(
                per_type_configs={
                    ReferenceDataType.ACRONYMS: LLMExtractorCollectionReferenceDataConfigForType(
                        entry_type=AcronymReferenceDataEntry,
                        prompt_var="x",
                        header="y",
                    )
                }
            ),
            config.reference_data_config,
        )

    def test_empty_reference_data_block_is_empty(self) -> None:
        # An explicit empty `reference_data: {}` block parses to an empty config.
        config = self._parse(name="TEST_COLLECTION")
        self.assertEqual(
            LLMExtractorCollectionReferenceDataConfig(per_type_configs={}),
            config.reference_data_config,
        )

    def test_missing_reference_data_block_raises(self) -> None:
        # `reference_data` is a required block.
        with self.assertRaises(KeyError):
            self._parse(name="TEST_COLLECTION", include_reference_data=False)

    def test_invalid_reference_data_block_raises(self) -> None:
        # acronyms is not groupable, so declaring groups fails parsing.
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Found unexpected config values for reference data type [acronyms]"
            ),
        ):
            self._parse(
                name="TEST_COLLECTION",
                reference_data={
                    "acronyms": {
                        "prompt_var": "x",
                        "header": "y",
                        "groups": [{"label": "L", "types": ["employer"]}],
                    }
                },
            )

    def test_minimum_confidence_level_defaults_when_omitted(self) -> None:
        config = self._parse(name="TEST_COLLECTION")
        self.assertEqual(
            DEFAULT_MINIMUM_CONFIDENCE_LEVEL, config.minimum_confidence_level
        )
        self.assertEqual(
            DEFAULT_MINIMUM_CONFIDENCE_LEVEL,
            config.output_schema.get_field("a").minimum_confidence_level,
        )

    def test_minimum_confidence_level_override_flows_to_fields(self) -> None:
        config = self._parse(
            name="TEST_COLLECTION", minimum_confidence_level="verbatim"
        )
        self.assertEqual(ConfidenceLevel.VERBATIM, config.minimum_confidence_level)
        self.assertEqual(
            ConfidenceLevel.VERBATIM,
            config.output_schema.get_field("a").minimum_confidence_level,
        )


class GenerateJsonSchemaTest(TestCase):
    """Tests the `generate_json_schema` / `generate_json_schema_str` methods,
    using the fake collection. The generated schema's shape is covered in detail
    in llm_json_schema_generator_test.py.
    """

    def setUp(self) -> None:
        self.collection = get_llm_extractor_collection_config(
            _FAKE_COLLECTION_NAME, config_module=fake_config
        )

    def test_generate_json_schema_is_valid(self) -> None:
        # Raises if the generated schema is not valid JSON Schema. check_schema is
        # a real classmethod missing from the type stubs.
        jsonschema.Draft202012Validator.check_schema(  # type: ignore[attr-defined]
            self.collection.generate_json_schema()
        )

    def test_generate_json_schema_str_round_trips(self) -> None:
        self.assertEqual(
            self.collection.generate_json_schema(),
            json.loads(self.collection.generate_json_schema_str()),
        )

    def test_generate_json_schema_str_is_deterministic(self) -> None:
        self.assertEqual(
            self.collection.generate_json_schema_str(),
            self.collection.generate_json_schema_str(),
        )

    def test_generate_json_schema_str_preserves_meaningful_order(self) -> None:
        # Guards against an accidental sort_keys=True, which would reorder the
        # semantically-ordered companion fields. The assertions below use
        # ordering pairs that DISAGREE with alphabetical order, so they would
        # fail if the keys were sorted: the meaningful order is
        # adversarial_interpretation, value, confidence_level, citations, but
        # alphabetized it would be adversarial_interpretation, citations,
        # confidence_level, value.
        schema_str = self.collection.generate_json_schema_str()

        def key_index(field_name: str) -> int:
            return schema_str.index(f'"{field_name}"')

        # value precedes confidence_level (alphabetically value sorts last).
        self.assertLess(key_index("value"), key_index("confidence_level"))
        # confidence_level precedes citations (alphabetically citations sorts
        # first).
        self.assertLess(key_index("confidence_level"), key_index("citations"))
