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
import re
import tempfile
from pathlib import Path
from typing import Any
from unittest import TestCase

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


class ParseAllCollectionConfigsTest(TestCase):
    """Guards real configs, and exercises the fake config's happy path."""

    def test_load_all_real_configs(self) -> None:
        # Raises if any real collection config fails to parse or validate. Does
        # not assert on any state's specifics — the fake config covers structure.
        configs = load_llm_extractor_collection_configs()
        self.assertTrue(configs)

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


class CollectionConfigFromYamlTest(TestCase):
    """from_yaml behaviors that need a file path + model registry."""

    def setUp(self) -> None:
        self.registry: LLMModelRegistry = load_llm_model_registry(
            config_module=fake_config
        )

    def _parse(
        self, *, name: str, directory_name: str | None = None, **body: Any
    ) -> LLMExtractorCollectionConfig:
        """Writes a collection.yaml under a temp directory (named |directory_name|,
        defaulting to the lowercased |name|) and parses it.
        """
        contents = {
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
            **body,
        }
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

    def test_golden_eval_and_reference_data_blocks_are_tolerated(self) -> None:
        # Present but not yet modeled — consumed and discarded, no error.
        config = self._parse(
            name="TEST_COLLECTION",
            golden_eval={"source_sheet_uri": "https://example.com", "anything": 1},
            reference_data={"acronyms": {"prompt_var": "x", "header": "y"}},
        )
        self.assertEqual("TEST_COLLECTION", config.name)

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
