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
"""The shared configuration for an LLM extractor collection, parsed from the
collection's `collection.yaml`. A collection holds everything shared across the
states that run an extractor — above all the output schema (the structure we
force the model's JSON into), plus the default model config and any declared
entity groups.

The `golden_eval` block is still consumed off the YAML and discarded so the unused-key
check passes — see the deferral note in `from_yaml`.
"""
import json
from functools import cache
from pathlib import Path
from types import ModuleType

import attr

from recidiviz.big_query.big_query_attr_validators import (
    is_valid_unquoted_bq_identifier,
)
from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.documents import config as default_config_module
from recidiviz.documents.extraction.config_defaults import (
    DEFAULT_MINIMUM_CONFIDENCE_LEVEL,
)
from recidiviz.documents.extraction.models.json_schema_nodes import JSONSchemaDict
from recidiviz.documents.extraction.models.llm_json_schema_generator import (
    LLMJsonSchemaGenerator,
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
    LLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.reference_data.llm_extractor_collection_reference_data_config import (
    LLMExtractorCollectionReferenceDataConfig,
)
from recidiviz.utils.yaml_dict import YAMLDict

EXTRACTOR_COLLECTIONS_DIR_NAME = "extractor_collections"
COLLECTION_CONFIG_FILENAME = "collection.yaml"


@attr.define(frozen=True, kw_only=True)
class EntityGroupConfig:
    """One `entity_groups` entry on a first-order collection: the declaration
    that drives an auto-generated entity-resolution document collection,
    extractor, and views for the group.
    """

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The entity group's name (e.g. `employer`, `residence`)."""

    entity_fields: list[LLMRequestOutputSchemaField] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(LLMRequestOutputSchemaField),
        ]
    )
    """The resolved output-schema fields that define the entity and get
    normalized to one canonical value per entity. Must be stable across all of a
    person's mentions. When `source_array_field` is set these are sub-fields of
    that ARRAY_OF_STRUCT; otherwise they are top-level fields.
    """

    source_array_field: ArrayOfStructLLMRequestOutputSchemaField | None = attr.ib(
        validator=attr_validators.is_opt(ArrayOfStructLLMRequestOutputSchemaField)
    )
    """When set, the resolved ARRAY_OF_STRUCT output-schema field whose sub-fields
    the `entity_fields` are (one mention per array element). When null, the
    `entity_fields` are top-level fields (one mention per document).
    """

    def __attrs_post_init__(self) -> None:
        entity_field_names = [field.name for field in self.entity_fields]
        if duplicate_names := {
            n for n in entity_field_names if entity_field_names.count(n) > 1
        }:
            raise ValueError(
                f"Entity group [{self.name}] declares duplicate entity_fields: "
                f"{sorted(duplicate_names)}."
            )
        if self.source_array_field is not None:
            sub_field_names = {field.name for field in self.source_array_field.fields}
            if non_sub_field_names := [
                name for name in entity_field_names if name not in sub_field_names
            ]:
                raise ValueError(
                    f"Entity group [{self.name}] declares entity_fields "
                    f"{sorted(non_sub_field_names)} that are not sub-fields of "
                    f"source_array_field [{self.source_array_field.name}]."
                )

    @classmethod
    def from_yaml_dict(
        cls, *, yaml_dict: YAMLDict, output_schema: LLMRequestOutputSchema
    ) -> "EntityGroupConfig":
        """Returns the entity group parsed from one entry of a collection's
        `entity_groups` block, resolving its `source_array_field` and
        `entity_fields` to the actual fields in |output_schema| — top-level
        fields when no `source_array_field` is declared, otherwise sub-fields of
        that ARRAY_OF_STRUCT field.
        """
        name = yaml_dict.pop("name", str)
        entity_field_names = yaml_dict.pop_list("entity_fields", str)
        source_array_field_name = yaml_dict.pop_optional("source_array_field", str)
        if yaml_dict:
            raise ValueError(
                f"Found unexpected config values for entity group [{name}]: "
                f"{repr(yaml_dict.get())}"
            )

        source_array_field: ArrayOfStructLLMRequestOutputSchemaField | None = None
        if source_array_field_name is None:
            candidate_fields_by_name = output_schema.fields_by_name
        else:
            resolved_field = output_schema.get_field(source_array_field_name)
            if not isinstance(resolved_field, ArrayOfStructLLMRequestOutputSchemaField):
                raise ValueError(
                    f"Entity group [{name}] declares source_array_field "
                    f"[{source_array_field_name}], which has type "
                    f"[{resolved_field.field_type.value}] — it must be an "
                    f"ARRAY_OF_STRUCT."
                )
            source_array_field = resolved_field
            candidate_fields_by_name = resolved_field.fields_by_name

        if unresolved_names := [
            field_name
            for field_name in entity_field_names
            if field_name not in candidate_fields_by_name
        ]:
            level_description = (
                "top-level output schema fields"
                if source_array_field_name is None
                else f"sub-fields of [{source_array_field_name}]"
            )
            raise ValueError(
                f"Entity group [{name}] declares entity_fields "
                f"{sorted(unresolved_names)} that do not resolve against the "
                f"{level_description}: {sorted(candidate_fields_by_name)}."
            )

        return cls(
            name=name,
            entity_fields=[
                candidate_fields_by_name[field_name]
                for field_name in entity_field_names
            ],
            source_array_field=source_array_field,
        )


@attr.define(frozen=True, kw_only=True)
class LLMExtractorCollectionConfig:
    """The shared configuration for an LLM extractor collection, parsed from
    its `collection.yaml` — everything shared across the states that run the
    extractor.
    """

    name: str = attr.ib(
        validator=[is_valid_unquoted_bq_identifier, attr_validators.is_upper_snake_case]
    )
    """UPPER_SNAKE_CASE name matching the collection's parent directory. Must be
    a valid unquoted BigQuery identifier (it becomes part of the result dataset
    and view names).
    """

    description: str = attr.ib(
        validator=recidiviz_attr_validators.is_meaningful_description
    )
    """Human-readable description of what the collection extracts. Used to
    auto-generate the `is_relevant` field description.
    """

    default_model_config_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Name of the model registry config extractors in this collection use
    unless their state-specific extractor config overrides it. Validated at parse time
    to resolve in the registry.
    """

    minimum_confidence_level: ConfidenceLevel = attr.ib(
        validator=attr.validators.in_(ConfidenceLevel)
    )
    """Minimum ordinal confidence level for validated output, applied to every
    output-schema field that does not declare its own override.
    """

    output_schema: LLMRequestOutputSchema = attr.ib(
        validator=attr.validators.instance_of(LLMRequestOutputSchema)
    )
    """The structure we force the model's JSON output into."""

    reference_data_config: LLMExtractorCollectionReferenceDataConfig = attr.ib(
        validator=attr.validators.instance_of(LLMExtractorCollectionReferenceDataConfig)
    )
    """How this collection renders reference data into its prompt (state-agnostic;
    a state's actual entries are bound when an extractor is resolved).
    """

    entity_groups: list[EntityGroupConfig] = attr.ib(
        validator=attr_validators.is_list_of(EntityGroupConfig)
    )
    """The entity groups declared on this collection, each driving an
    auto-generated entity-resolution layer. Empty when the collection declares
    none.
    """

    def __attrs_post_init__(self) -> None:
        self._validate_entity_groups()

    def _validate_entity_groups(self) -> None:
        """Validates that entity group names are unique within the collection.
        Per-group resolution of `entity_fields` / `source_array_field` against
        the output schema happens in `EntityGroupConfig.from_yaml_dict`.
        """
        group_names = [group.name for group in self.entity_groups]
        if duplicate_names := {n for n in group_names if group_names.count(n) > 1}:
            raise ValueError(
                f"Collection [{self.name}] declares duplicate entity group "
                f"names: {sorted(duplicate_names)}."
            )

    def generate_json_schema(self) -> JSONSchemaDict:
        """Returns the deterministic JSON output schema that will be provided to
        the LLM.
        """
        return LLMJsonSchemaGenerator.generate(self.output_schema)

    def generate_json_schema_str(self) -> str:
        """Returns a string representation of the JSON output schema that will be
        provided to the LLM.
        """

        # NOTE: sort_keys=True is *explicitly* omitted here. The key order in the
        # generated schema is semantically meaningful and must not be scrambled.
        # Alphabetizing the keys would reorder them.
        return json.dumps(self.generate_json_schema())

    @classmethod
    def from_yaml(
        cls, *, yaml_path: str | Path, model_registry: LLMModelRegistry
    ) -> "LLMExtractorCollectionConfig":
        """Returns the collection config parsed from the `collection.yaml` at
        |yaml_path|, validating that its declared name matches its parent
        directory and that its default model config resolves in
        |model_registry|.
        """
        yaml_path = Path(yaml_path)
        config_dict = YAMLDict.from_path(yaml_path)

        name = config_dict.pop("name", str)
        expected_name = yaml_path.parent.name.upper()
        if name != expected_name:
            raise ValueError(
                f"Collection config at [{yaml_path}] declares name [{name}], "
                f"which does not match its parent directory "
                f"[{yaml_path.parent.name}] (expected [{expected_name}])."
            )

        default_model_config_name = config_dict.pop("default_model_config_name", str)
        # Raises if the name does not resolve in the registry.
        model_registry.get_model_config(default_model_config_name)

        minimum_confidence_level = (
            ConfidenceLevel(declared_minimum)
            if (
                declared_minimum := config_dict.pop_optional(
                    "minimum_confidence_level", str
                )
            )
            is not None
            else DEFAULT_MINIMUM_CONFIDENCE_LEVEL
        )

        reference_data_block = config_dict.pop_dict("reference_data")
        reference_data_config = (
            LLMExtractorCollectionReferenceDataConfig.from_yaml_dict(
                reference_data_block
            )
        )

        # TODO(OBT-33687): Model and validate the `golden_eval` block
        # (`source_sheet_uri` + `accuracy_thresholds`, required for first-order
        # collections). For now it is consumed off the YAML and discarded so
        # the unused-key check passes.
        config_dict.pop_dict_optional("golden_eval")

        description = config_dict.pop("description", str)
        output_schema = LLMRequestOutputSchema.from_yaml_dict(
            yaml_dict=config_dict.pop_dict("output_schema"),
            collection_description=description,
            default_minimum_confidence_level=minimum_confidence_level,
        )
        config = cls(
            name=name,
            description=description,
            default_model_config_name=default_model_config_name,
            minimum_confidence_level=minimum_confidence_level,
            output_schema=output_schema,
            reference_data_config=reference_data_config,
            entity_groups=[
                EntityGroupConfig.from_yaml_dict(
                    yaml_dict=group_yaml, output_schema=output_schema
                )
                for group_yaml in (
                    config_dict.pop_dicts_optional("entity_groups") or []
                )
            ],
        )
        if config_dict:
            raise ValueError(
                f"Found unexpected config values for collection [{name}] at "
                f"[{yaml_path}]: {repr(config_dict.get())}"
            )
        return config


def extractor_collections_dir(config_module: ModuleType | None = None) -> Path:
    """Returns the path to the extractor collections directory within
    |config_module| (the production config package by default).
    """
    module = config_module or default_config_module
    if module.__file__ is None:
        raise ValueError(f"No file associated with module [{module}].")
    return Path(module.__file__).parent / EXTRACTOR_COLLECTIONS_DIR_NAME


@cache
def load_llm_extractor_collection_configs(
    config_module: ModuleType | None = None,
) -> dict[str, LLMExtractorCollectionConfig]:
    """Returns the collection configs parsed from every
    `extractor_collections/{collection_name}/collection.yaml` within
    |config_module| (the production config package by default), validated
    against the model registry in the same package, keyed by collection name.
    """
    collections_dir = extractor_collections_dir(config_module)
    if not collections_dir.is_dir():
        raise ValueError(
            f"Extractor collections directory does not exist: [{collections_dir}]."
        )
    model_registry = load_llm_model_registry(config_module)
    configs_by_name: dict[str, LLMExtractorCollectionConfig] = {}
    for collection_dir in sorted(collections_dir.iterdir()):
        if not collection_dir.is_dir():
            continue
        config = LLMExtractorCollectionConfig.from_yaml(
            yaml_path=collection_dir / COLLECTION_CONFIG_FILENAME,
            model_registry=model_registry,
        )
        if config.name in configs_by_name:
            raise ValueError(
                f"Found multiple extractor collections with name [{config.name}]."
            )
        configs_by_name[config.name] = config
    return configs_by_name


def get_llm_extractor_collection_config(
    config_name: str, config_module: ModuleType | None = None
) -> LLMExtractorCollectionConfig:
    """Returns the extractor collection config named |config_name| within
    |config_module| (the production config package by default), raising if no
    collection with that name exists.
    """
    configs_by_name = load_llm_extractor_collection_configs(config_module)
    if config_name not in configs_by_name:
        raise ValueError(
            f"No extractor collection named [{config_name}]. Known collections: "
            f"{sorted(configs_by_name)}."
        )
    return configs_by_name[config_name]
