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
"""
import json
from functools import cache
from pathlib import Path
from types import ModuleType
from typing import Any

import attr

from recidiviz.big_query.big_query_attr_validators import (
    is_valid_unquoted_bq_identifier,
)
from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.common.descriptor import Descriptor
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
    ScalarValuedLLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.reference_data.llm_extractor_collection_reference_data_config import (
    LLMExtractorCollectionReferenceDataConfig,
)
from recidiviz.utils.string import sha256_hexdigest
from recidiviz.utils.yaml_dict import YAMLDict

EXTRACTOR_COLLECTIONS_DIR_NAME = "extractor_collections"
COLLECTION_CONFIG_FILENAME = "collection.yaml"
PROMPT_TEMPLATE_FILENAME = "prompt_template.txt"

# relevance_criteria is used verbatim as the full relevance statement — both the
# auto-generated is_relevant field description and the prompt's relevance criterion
# — so it must read as a complete "Whether ..." statement. We require this exact
# prefix so it reads correctly in both spots.
RELEVANCE_CRITERIA_REQUIRED_PREFIX = "Whether the document"


def _is_valid_relevance_criteria(
    instance: Any, attribute: attr.Attribute, value: str
) -> None:
    """Validates that relevance_criteria is a meaningful statement beginning with
    "Whether the document" (see RELEVANCE_CRITERIA_REQUIRED_PREFIX).
    """
    recidiviz_attr_validators.is_meaningful_description(instance, attribute, value)
    if not value.startswith(RELEVANCE_CRITERIA_REQUIRED_PREFIX):
        raise ValueError(
            f"Field [{attribute.name}] on [{type(instance).__name__}] must be a "
            f'statement beginning with "{RELEVANCE_CRITERIA_REQUIRED_PREFIX}". '
            f"Found value [{value}]."
        )


@attr.define(frozen=True, kw_only=True)
class EntityGroupConfig:
    """One `entity_groups` entry on a first-order collection: the declaration
    that drives an auto-generated entity-resolution document collection,
    extractor, and views for the group.
    """

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The entity group's name (e.g. `employer`, `residence`)."""

    entity_descriptor: Descriptor = attr.ib(
        validator=attr.validators.instance_of(Descriptor)
    )
    """Singular and plural forms of the entity type this group resolves (e.g.
    "employer"/"employers").
    """

    entity_fields: list[ScalarValuedLLMRequestOutputSchemaField] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(ScalarValuedLLMRequestOutputSchemaField),
        ]
    )
    """The resolved output-schema fields that define the entity and get
    normalized to one canonical value per entity. Must be scalar-valued (a
    primitive or an ENUM) — an entity field has exactly one value per mention —
    and stable across all of a person's mentions. When `source_array_field` is
    set these are sub-fields of that ARRAY_OF_STRUCT; otherwise they are
    top-level fields.
    """

    source_array_field: ArrayOfStructLLMRequestOutputSchemaField | None = attr.ib(
        validator=attr_validators.is_opt(ArrayOfStructLLMRequestOutputSchemaField)
    )
    """When set, the resolved ARRAY_OF_STRUCT output-schema field whose sub-fields
    the `entity_fields` are (one mention per array element). When null, the
    `entity_fields` are top-level fields (one mention per document).
    """

    grouping_instructions: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """Optional group-level clustering guidance for the entity resolution prompt — 
    heuristics that span the whole group and so cannot live on any one `entity_field`'s
    description (e.g. "cluster all self-employment into one entity", "a permanent 
    residence and a temporary stay are separate entities"). Null when the group needs 
    no such guidance.
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
        entity_descriptor = Descriptor.from_yaml_dict(
            yaml_dict.pop_dict("entity_descriptor")
        )
        entity_field_names = yaml_dict.pop_list("entity_fields", str)
        source_array_field_name = yaml_dict.pop_optional("source_array_field", str)
        grouping_instructions = yaml_dict.pop_optional("grouping_instructions", str)
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

        entity_fields = []
        for field_name in entity_field_names:
            resolved_entity_field = candidate_fields_by_name[field_name]
            if not isinstance(
                resolved_entity_field, ScalarValuedLLMRequestOutputSchemaField
            ):
                raise ValueError(
                    f"Entity group [{name}] declares entity_field [{field_name}] of "
                    f"type [{resolved_entity_field.field_type.value}] — entity fields "
                    f"must be scalar-valued (a primitive or an ENUM). To resolve an "
                    f"ARRAY_OF_STRUCT field's mentions, set source_array_field to "
                    f"[{field_name}] and list its sub-fields as the entity_fields."
                )
            entity_fields.append(resolved_entity_field)

        return cls(
            name=name,
            entity_descriptor=entity_descriptor,
            entity_fields=entity_fields,
            source_array_field=source_array_field,
            grouping_instructions=grouping_instructions,
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
    """Human-readable description of what the collection extracts."""

    relevance_criteria: str | None = attr.ib(
        validator=attr.validators.optional(_is_valid_relevance_criteria)
    )
    """The full relevance statement for a document, phrased as a "Whether the
    document ..." clause (e.g. "Whether the document mentions jobs, work, pay,
    employers, or job searching"). `None` only for a synthesized
    entity-resolution collection, which has no `is_relevant` field (every
    composite document is relevant by construction).
    """

    prompt_template: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The collection's hand-written `prompt_template.txt` — mostly
    prompt-specific prose with `{placeholder}` gaps the prompt generator fills
    (`{output_instructions}`, `{reference_data}`, and the extractor's
    prompt_vars). Read from the `prompt_template.txt` sibling of this
    collection's `collection.yaml`.
    """

    default_model_config_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Name of the model registry config extractors in this collection use
    unless their state-specific extractor config overrides it. Validated at parse time
    to resolve in the registry.
    """

    minimum_confidence_level: ConfidenceLevel | None = attr.ib(
        validator=attr.validators.optional(attr.validators.in_(ConfidenceLevel))
    )
    """Minimum ordinal confidence level for validated output, applied to every
    output-schema field that does not declare its own override. `None` only for a
    collection with no INFERRED fields (e.g. a synthesized entity-resolution
    collection), whose fields carry no confidence metadata — enforced in
    __attrs_post_init__.
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
        self._validate_relevance_criteria_matches_schema()
        self._validate_confidence_level_present_when_fields_inferred()
        self._validate_entity_groups()

    def _validate_confidence_level_present_when_fields_inferred(self) -> None:
        """Validates that a collection with no `minimum_confidence_level` has no
        INFERRED fields. INFERRED fields carry confidence metadata that validation
        thresholds against, so a null collection-level default is only coherent
        when every field is STRUCTURAL (e.g. a synthesized entity-resolution
        collection).
        """
        if self.minimum_confidence_level is not None:
            return
        if inferred_field_names := [
            field.name
            for field in self.output_schema.user_defined_fields
            if field.is_inferred_field
        ]:
            raise ValueError(
                f"Collection [{self.name}] declares no minimum_confidence_level "
                f"but has INFERRED field(s) {inferred_field_names} — a null "
                f"minimum confidence level is only allowed when every output "
                f"schema field is STRUCTURAL."
            )

    def _validate_relevance_criteria_matches_schema(self) -> None:
        """Validates that the collection's `relevance_criteria` is the same value
        the output schema carries — they are two copies of one statement (the
        schema uses it verbatim as the injected `is_relevant` field's
        description), so a mismatch (including one being set while the other is
        `None`) means an `is_relevant` field that disagrees with the collection.
        """
        if self.relevance_criteria != self.output_schema.relevance_criteria:
            raise ValueError(
                f"Collection [{self.name}] declares relevance_criteria "
                f"[{self.relevance_criteria}], which does not match its output "
                f"schema's relevance_criteria "
                f"[{self.output_schema.relevance_criteria}]."
            )

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

    def entity_groups_for_array_field(
        self, array_field: ArrayOfStructLLMRequestOutputSchemaField | None
    ) -> list[EntityGroupConfig]:
        """Returns the entity groups whose `entity_fields` are sub-fields of
        |array_field|, or — when |array_field| is None — the groups whose
        `entity_fields` are top-level fields of this collection.

        A group's mentions live at exactly one of those places, so every group this
        collection declares is returned for exactly one |array_field| value.
        """
        target_name = array_field.name if array_field is not None else None
        return [
            group
            for group in self.entity_groups
            if (
                group.source_array_field.name
                if group.source_array_field is not None
                else None
            )
            == target_name
        ]

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

    @property
    def output_schema_version(self) -> str:
        """Returns the version ID of this collection's output schema — the hash of
        the generated JSON schema string.
        """
        return sha256_hexdigest(self.generate_json_schema_str())

    @property
    def validation_config_version_id(self) -> str:
        """Returns the version ID of the confidence-level configuration validation
        runs against — a hash of every confidence-level setting on this collection.
        Tracked separately from `extractor_version_id` because these thresholds
        affect what validation keeps but are never fed to the LLM, so a threshold
        change re-validates existing raw results without re-extraction.

        Only the collection-level default exists today; per-field overrides and the
        other threshold knobs added when full validation lands extend the hashed
        components below (they do not redefine this ID).
        """
        components = [
            (
                self.minimum_confidence_level.value
                if self.minimum_confidence_level is not None
                else None
            ),
        ]
        return sha256_hexdigest(json.dumps(components))

    @property
    def collection_version_id(self) -> str:
        """Returns the version ID of this collection. This is a hash of all the info
        from this collection fed directly to the LLM. The collection description is not
        hashed separately — it is baked into the prompt, so it flows into the extractor
        version ID transitively.
        """
        components = [
            # We also hash the collection name because versions should be unique to
            # collections with a particular name.
            self.name,
            # The prompt template is fed to the LLM, so a template edit must bump
            # the version (prompt_vars and reference data are state-specific and
            # flow into the extractor version ID instead).
            self.prompt_template,
            self.output_schema_version,
            (
                self.minimum_confidence_level.value
                if self.minimum_confidence_level is not None
                else None
            ),
        ]
        return sha256_hexdigest(json.dumps(components))

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

        description = config_dict.pop("description", str)
        relevance_criteria = config_dict.pop("relevance_criteria", str)
        prompt_template_path = yaml_path.parent / PROMPT_TEMPLATE_FILENAME
        if not prompt_template_path.exists():
            raise ValueError(
                f"Collection [{name}] is missing its required prompt template at "
                f"[{prompt_template_path}]."
            )
        prompt_template = prompt_template_path.read_text(encoding="utf-8")
        output_schema = LLMRequestOutputSchema.from_yaml_dict(
            yaml_dict=config_dict.pop_dict("output_schema"),
            relevance_criteria=relevance_criteria,
            default_minimum_confidence_level=minimum_confidence_level,
        )
        config = cls(
            name=name,
            description=description,
            relevance_criteria=relevance_criteria,
            prompt_template=prompt_template,
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
