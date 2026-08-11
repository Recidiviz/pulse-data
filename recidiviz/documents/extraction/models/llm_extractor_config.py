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
"""The fully-resolved configuration for a single LLM extractor — one
`(collection, state)` pair.

A state's `extractor.yaml` supplies the per-state half of an extractor: which
document collection it reads, the document filter, the prompt vars, any
model/cap overrides, and its golden eval set. `LLMExtractorConfig.from_yaml`
parses that file and binds it with its already-parsed collection config, its
resolved input document collection, and its resolved model config into one
fully-resolved object.
"""

import json
from pathlib import Path

import attr

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.config_defaults import (
    DEFAULT_FIRST_ORDER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD,
    DEFAULT_FIRST_ORDER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP,
    DEFAULT_MAX_TRANSIENT_RETRY_COUNT,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    LLMDocumentExtractionGoldenEvalConfig,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    EntityGroupConfig,
    LLMExtractorCollectionConfig,
)
from recidiviz.documents.extraction.models.llm_model_registry import (
    LLMModelConfig,
    LLMModelRegistry,
)
from recidiviz.documents.extraction.models.reference_data.llm_extractor_reference_data import (
    LLMExtractorReferenceData,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_entry import (
    ReferenceDataEntry,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_registry import (
    ReferenceDataType,
    StateSpecificReferenceDataRegistry,
)
from recidiviz.documents.extraction.prompt_generation.llm_extractor_output_format_instructions_generator import (
    LLMExtractorOutputFormatInstructionsGenerator,
)
from recidiviz.documents.extraction.prompt_generation.llm_extractor_reference_data_generator import (
    LLMExtractorReferenceDataGenerator,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.utils.string import StrictStringFormatter, sha256_hexdigest
from recidiviz.utils.string_formatting import collapse_blank_lines
from recidiviz.utils.yaml_dict import YAMLDict

# Prompt-template variables the extractor fills in itself, rendered from the
# collection's schema and reference data rather than supplied per-state. A
# state's `prompt_vars` may not redeclare these.
OUTPUT_INSTRUCTIONS_TEMPLATE_VAR = "output_instructions"
REFERENCE_DATA_TEMPLATE_VAR = "reference_data"
RESERVED_PROMPT_TEMPLATE_VARS = frozenset(
    {OUTPUT_INSTRUCTIONS_TEMPLATE_VAR, REFERENCE_DATA_TEMPLATE_VAR}
)

# Template variable an authored document_metadata_filter_query_template uses to
# reference the input collection's metadata tables.
INPUT_DOCUMENT_COLLECTION_METADATA_ADDRESS_TEMPLATE_VAR = (
    "input_document_collection_metadata_address"
)


def _prompt_vars_do_not_shadow_reserved(
    instance: "LLMExtractorConfig", _attribute: attr.Attribute, value: dict[str, str]
) -> None:
    """Validates that no state-supplied prompt var reuses a reserved template
    variable name (which the extractor fills in itself)."""
    if shadowed := sorted(set(value) & RESERVED_PROMPT_TEMPLATE_VARS):
        raise ValueError(
            f"Extractor [{instance.extractor_collection.name}] for "
            f"[{instance.state_code.value}] declares prompt_vars that shadow "
            f"reserved template variables: {shadowed}."
        )


@attr.define(frozen=True, kw_only=True)
class LLMExtractorDocumentFilterConfig:
    """The document-selection config for a run: the authored filter template,
    plus sandbox-only narrowing (never authored — set only via
    LLMExtractorConfig.with_sandbox_narrowing; always None in production)."""

    document_metadata_filter_query_template: str = attr.ib(
        validator=attr_validators.is_non_empty_str
    )
    """SQL template for a query that returns a single document_contents_id
    column, holding the document_contents_ids this extractor should process.

    It selects from the input collection's metadata table, referenced via the
    {input_document_collection_metadata_address} template variable."""

    is_sandbox_config: bool = attr.ib(validator=attr_validators.is_bool)
    """Whether this is a sandbox config. Only a sandbox config may carry the
    narrowing knobs below; a production config never does."""

    document_limit: int | None = attr.ib(validator=attr_validators.is_opt_positive_int)
    """Sandbox-only: cap the number of selected documents."""

    root_entity_ids: list[str] | None = attr.ib(
        validator=attr.validators.optional(
            [
                attr_validators.is_non_empty_list,
                attr_validators.is_list_of_non_empty_str,
            ]
        )
    )
    """Sandbox-only: restrict selection to these root entities, matched against
    the input collection's root-entity ID column (person or staff, per its
    root_entity_id_type). For an external-id collection these are external IDs;
    the id_type is constant per collection, so it is not part of the narrowing."""

    def __attrs_post_init__(self) -> None:
        placeholder = (
            "{" + INPUT_DOCUMENT_COLLECTION_METADATA_ADDRESS_TEMPLATE_VAR + "}"
        )
        if placeholder not in self.document_metadata_filter_query_template:
            raise ValueError(
                f"document_metadata_filter_query_template must reference the input "
                f"collection's metadata table via the [{placeholder}] template "
                f"variable, but none was found in: "
                f"[{self.document_metadata_filter_query_template}]."
            )
        if not self.is_sandbox_config and (
            self.document_limit is not None or self.root_entity_ids is not None
        ):
            raise ValueError(
                "Sandbox-only narrowing (document_limit / root_entity_ids) may only "
                "be set on a sandbox config. Set narrowing via "
                "LLMExtractorConfig.with_sandbox_narrowing, which marks the config as "
                "a sandbox config."
            )

    @property
    def version_id(self) -> str:
        """Returns the version id for this filter config. Sandbox-only fields are
        explicitly excluded from this version because the id must identify the
        authored filter logic (what documents this extractor is *defined* to
        process) not how much of it a given run happens to process. Sandbox-only
        fields only narrow a run to a cheap subset but don't change what the
        filter is. Excluding them means a narrowed sandbox run computes the exact
        document filter version_id production would, so its results are keyed
        identically to (and directly comparable with) production's.
        """
        components = [self.document_metadata_filter_query_template]
        return sha256_hexdigest(json.dumps(components))

    def build_document_metadata_filter_query(
        self,
        *,
        input_document_collection_metadata_address: ProjectSpecificBigQueryAddress,
    ) -> str:
        """Returns the authored metadata filter query with its
        {input_document_collection_metadata_address} placeholder filled in from
        |input_document_collection_metadata_address|, already scoped to the run's
        sandbox prefix when applicable.
        """
        return StrictStringFormatter().format(
            self.document_metadata_filter_query_template,
            **{
                INPUT_DOCUMENT_COLLECTION_METADATA_ADDRESS_TEMPLATE_VAR: (
                    input_document_collection_metadata_address.to_str()
                )
            },
        )


@attr.define(frozen=True, kw_only=True)
class LLMExtractorConfig:
    """The fully-resolved configuration for one extractor: a state's parsed
    `extractor.yaml` bound to its collection config, its input document
    collection, and its resolved model config.
    """

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))
    """The state this extractor runs for."""

    input_document_collection: DocumentCollectionConfig = attr.ib(
        validator=attr.validators.instance_of(DocumentCollectionConfig)
    )
    """The document collection this extractor reads, resolved from the
    `input_document_collection_name` declared in the `extractor.yaml`.
    """

    document_filter: LLMExtractorDocumentFilterConfig = attr.ib(
        validator=attr.validators.instance_of(LLMExtractorDocumentFilterConfig)
    )
    """The document-selection config for this run: the authored filter template
    plus any sandbox-only narrowing (always un-narrowed in production).
    """

    prompt_vars: dict[str, str] = attr.ib(
        validator=[
            attr_validators.is_dict_of(str, str),
            _prompt_vars_do_not_shadow_reserved,
        ]
    )
    """Dictionary holding values for the extractor collection-defined prompt template 
    variables that must be supplied by each state-specific extractor. Empty when the 
    extractor's prompt template has no variables.
    """

    max_transient_retry_count: int = attr.ib(
        validator=attr_validators.is_non_negative_int
    )
    """The number of times a document that fails with a DOCUMENT_LEVEL_FAILURE_TRANSIENT
    error is retried before being marked retries-exhausted.
    """

    total_pending_document_count_hard_cap: int = attr.ib(
        validator=attr_validators.is_positive_int
    )
    """The maximum number of documents still needing processing (the whole pending
    backlog) this extractor will process without a manual override. A larger backlog
    trips a cost guardrail and the run is skipped.
    """

    single_job_document_count_batch_threshold: int = attr.ib(
        validator=attr_validators.is_positive_int
    )
    """The number of documents in a single job at or above which it routes to a batch
    (rather than synchronous) method of processing. Keyed to document count because
    sync throughput is bounded in documents per second, not characters.
    """

    extractor_collection: LLMExtractorCollectionConfig = attr.ib(
        validator=attr.validators.instance_of(LLMExtractorCollectionConfig)
    )
    """The extractor collection config shared across all states that run this extractor.
    """

    model_config: LLMModelConfig = attr.ib(
        validator=attr.validators.instance_of(LLMModelConfig)
    )
    """The model config bound to this extractor — the state-level override when
    set, otherwise the collection default.
    """

    reference_data: LLMExtractorReferenceData = attr.ib(
        validator=attr.validators.instance_of(LLMExtractorReferenceData)
    )
    """The collection's reference-data render config bound to this state's actual
    entries. Empty when the collection declares no reference data.
    """

    entity_group: EntityGroupConfig | None = attr.ib(
        validator=attr_validators.is_opt(EntityGroupConfig)
    )
    """Set for an auto-generated entity-resolution extractor, None for a
    first-order extractor. The single signal that distinguishes the two: the
    first-order thinking-model ban below is scoped to configs where this is None.
    """

    golden_eval: LLMDocumentExtractionGoldenEvalConfig | None = attr.ib(
        validator=attr_validators.is_opt(LLMDocumentExtractionGoldenEvalConfig)
    )
    """Where this (collection, state) extractor's human-labeled eval set lives and
    how accurate each test type must be. Required for a first-order extractor;
    None on a generated entity-resolution extractor, which is exempt from golden
    eval.
    """

    instructions_prompt: str = attr.ib(init=False, eq=False, repr=False)
    """The fully-compiled system prompt sent to the LLM with every request for
    this extractor, with the output-format instructions, reference data, and
    prompt vars all rendered into the collection's `prompt_template`. Built once
    at construction (not recompiled on each access).

    The `prompt_template` supports two reserved variables the extractor fills in
    itself — `{output_instructions}` (which fields to fill and how to structure
    INFERRED results) and `{reference_data}` (acronyms, known organizations,
    etc.) — plus any additional variables supplied via `prompt_vars`.
    """

    @instructions_prompt.default
    def _build_instructions_prompt(self) -> str:
        """Returns the fully-compiled instructions prompt: the collection's
        `prompt_template` with the output-format instructions, reference data, and
        prompt vars rendered in.
        """
        template_variables = {
            **{key: value.strip() for key, value in self.prompt_vars.items()},
            OUTPUT_INSTRUCTIONS_TEMPLATE_VAR: LLMExtractorOutputFormatInstructionsGenerator.generate(
                self.extractor_collection.output_schema
            ),
            REFERENCE_DATA_TEMPLATE_VAR: LLMExtractorReferenceDataGenerator.generate(
                self.reference_data,
                # Entity-resolution extractors (the ones with an entity group) render
                # known organizations as a flat name-and-alias dictionary.
                render_flat_known_organizations=self.entity_group is not None,
            ),
        }

        all_expected_prompt_vars = (
            set(self.prompt_vars.keys()) | RESERVED_PROMPT_TEMPLATE_VARS
        )

        if missing_reserved := (
            set(template_variables.keys()) - all_expected_prompt_vars
        ):
            raise ValueError(
                f"Found template variables missing from RESERVED_PROMPT_TEMPLATE_VARS: "
                f"{missing_reserved}"
            )

        formatted = StrictStringFormatter().format(
            self.extractor_collection.prompt_template, **template_variables
        )
        return collapse_blank_lines(formatted)

    def __attrs_post_init__(self) -> None:
        if self.state_code != self.input_document_collection.state_code:
            raise ValueError(
                f"Extractor [{self.extractor_collection.name}] for "
                f"[{self.state_code.value}] reads input document collection "
                f"[{self.input_document_collection.name}], which belongs to state "
                f"[{self.input_document_collection.state_code.value}]."
            )
        if (
            self.single_job_document_count_batch_threshold
            > self.total_pending_document_count_hard_cap
        ):
            raise ValueError(
                f"Extractor [{self.extractor_collection.name}] for [{self.state_code.value}] "
                f"declares single_job_document_count_batch_threshold "
                f"[{self.single_job_document_count_batch_threshold}] greater than "
                f"total_pending_document_count_hard_cap "
                f"[{self.total_pending_document_count_hard_cap}]."
            )
        if not self.model_config.supports_structured_output:
            raise ValueError(
                f"Extractor [{self.extractor_collection.name}] for [{self.state_code.value}] "
                f"binds model config [{self.model_config.name}], which does not "
                f"support structured output."
            )
        if not self.model_config.supports_implicit_caching_in_batch:
            raise ValueError(
                f"Extractor [{self.extractor_collection.name}] for [{self.state_code.value}] "
                f"binds model config [{self.model_config.name}], which does not "
                f"support implicit caching in batch prediction."
            )
        if self.entity_group is None and self.model_config.enables_thinking:
            raise ValueError(
                f"First-order extractor [{self.extractor_collection.name}] for "
                f"[{self.state_code.value}] binds thinking-enabled model config "
                f"[{self.model_config.name}]. Thinking models are only permitted "
                f"for entity-resolution extractors."
            )

    @property
    def extractor_id(self) -> str:
        """Returns the stable, human-readable ID for this extractor (e.g.
        `US_OZ_PLAYGROUND_EMPLOYMENT_INFO`), invariant across config versions.
        Distinct from `extractor_version_id`, which changes whenever a versioned
        input does.
        """
        return f"{self.state_code.value}_{self.extractor_collection.name}"

    @property
    def instructions_prompt_hash(self) -> str:
        """Returns the hash of the fully-compiled instructions prompt."""
        return sha256_hexdigest(self.instructions_prompt)

    @property
    def extractor_version_id(self) -> str:
        """Returns the version ID of this extractor. This is a hash of every input fed
        to the LLM. Any change yields a new ID.
        """

        components = [
            # We also hash the human-readable extractor_id because versions should be
            # unique to extractors configs with a particular human-readable name.
            self.extractor_id,
            self.instructions_prompt_hash,
            self.model_config.model_config_version_id,
            self.extractor_collection.collection_version_id,
        ]
        return sha256_hexdigest(json.dumps(components))

    @property
    def document_filter_id(self) -> str:
        """Returns the version ID of this extractor's document selection: a hash
        of the authored filter template. Tracked separately from
        `extractor_version_id` because the filter narrows which documents are
        processed but is not itself fed to the LLM. Sandbox narrowing is
        deliberately excluded, so a narrowed config keeps the real filter ID.
        """
        components = [
            # We also hash the human-readable extractor_id because versions should be
            # unique to extractors configs with a particular human-readable name.
            self.extractor_id,
            self.document_filter.version_id,
        ]
        return sha256_hexdigest(json.dumps(components))

    def with_sandbox_narrowing(
        self, *, document_limit: int | None, root_entity_ids: list[str] | None
    ) -> "LLMExtractorConfig":
        """Returns a copy of this config with sandbox-only narrowing applied to
        its document filter. This is the only way narrowing is ever set;
        production configs are never narrowed.
        """
        return attr.evolve(
            self,
            document_filter=attr.evolve(
                self.document_filter,
                is_sandbox_config=True,
                document_limit=document_limit,
                root_entity_ids=root_entity_ids,
            ),
        )

    @staticmethod
    def state_code_for_yaml_path(yaml_path: str | Path) -> StateCode:
        """Returns the state an `extractor.yaml` belongs to, derived from its
        parent directory (`extractor_collections/{collection}/{state}/`).
        """
        return StateCode(Path(yaml_path).parent.name.upper())

    @classmethod
    def from_yaml(
        cls,
        yaml_path: str | Path,
        *,
        extractor_collections_by_name: dict[str, LLMExtractorCollectionConfig],
        model_registry: LLMModelRegistry,
        document_collections_by_name: dict[str, DocumentCollectionConfig],
        reference_data_registries_by_data_type: dict[
            ReferenceDataType, StateSpecificReferenceDataRegistry[ReferenceDataEntry]
        ],
    ) -> "LLMExtractorConfig":
        """Returns the fully-resolved extractor config parsed from the
        `extractor.yaml` at |yaml_path|, deriving its state from the parent
        directory and binding it to:
          - its collection config, looked up by name in |collections_by_name| and
            validated to match the grandparent directory;
          - its input document collection, looked up by name in
            |document_collections_by_name| (the state's collections keyed by name);
          - its model config — the state-level override when set, otherwise the
            collection default — looked up in |model_registry|;
          - its reference data, binding the collection's render config to the
            state's entries in |reference_data_registries| (the output of
            `load_full_reference_data_registry` for this extractor's state).
        """
        yaml_path = Path(yaml_path)
        config_dict = YAMLDict.from_path(yaml_path)

        name = config_dict.pop("extractor_collection_name", str)
        expected_name = yaml_path.parent.parent.name.upper()
        if name != expected_name:
            raise ValueError(
                f"Extractor config at [{yaml_path}] declares collection name "
                f"[{name}], which does not match its collection directory "
                f"[{yaml_path.parent.parent.name}] (expected [{expected_name}])."
            )
        if name not in extractor_collections_by_name:
            raise ValueError(
                f"Extractor config at [{yaml_path}] references collection [{name}], "
                f"which has no collection.yaml."
            )
        extractor_collection = extractor_collections_by_name[name]

        input_document_collection_name = config_dict.pop(
            "input_document_collection_name", str
        )
        if input_document_collection_name not in document_collections_by_name:
            raise ValueError(
                f"Extractor [{name}] for "
                f"[{cls.state_code_for_yaml_path(yaml_path).value}] references "
                f"unknown input document collection "
                f"[{input_document_collection_name}]. Known collections: "
                f"{sorted(document_collections_by_name)}."
            )
        input_document_collection = document_collections_by_name[
            input_document_collection_name
        ]

        prompt_vars_dict = config_dict.pop_dict_optional("prompt_vars")
        prompt_vars = (
            {key: prompt_vars_dict.pop(key, str) for key in prompt_vars_dict.keys()}
            if prompt_vars_dict is not None
            else {}
        )

        model_config_name = (
            config_dict.pop_optional("model_config_name", str)
            or extractor_collection.default_model_config_name
        )
        # Raises if the name does not resolve in the registry.
        model_config = model_registry.get_model_config(model_config_name)

        # Required here even though the field is nullable: only the generated
        # entity-resolution extractor may omit golden eval, and it is built
        # directly rather than via this code path.
        golden_eval = LLMDocumentExtractionGoldenEvalConfig.from_yaml_dict(
            config_dict.pop_dict("golden_eval")
        )

        config = cls(
            state_code=cls.state_code_for_yaml_path(yaml_path),
            input_document_collection=input_document_collection,
            # Authored files carry only the flat filter template; the sandbox-only
            # narrowing knobs are never authored and default to None here.
            document_filter=LLMExtractorDocumentFilterConfig(
                document_metadata_filter_query_template=config_dict.pop(
                    "document_metadata_filter_query_template", str
                ),
                is_sandbox_config=False,
                document_limit=None,
                root_entity_ids=None,
            ),
            prompt_vars=prompt_vars,
            max_transient_retry_count=(
                retry_count
                if (
                    retry_count := config_dict.pop_optional(
                        "max_transient_retry_count", int
                    )
                )
                is not None
                else DEFAULT_MAX_TRANSIENT_RETRY_COUNT
            ),
            # Extractors authored via YAML are always first-order, so the
            # first-order document-count defaults apply (see entity_group below).
            total_pending_document_count_hard_cap=(
                config_dict.pop_optional("total_pending_document_count_hard_cap", int)
                or DEFAULT_FIRST_ORDER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP
            ),
            single_job_document_count_batch_threshold=(
                config_dict.pop_optional(
                    "single_job_document_count_batch_threshold", int
                )
                or DEFAULT_FIRST_ORDER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD
            ),
            extractor_collection=extractor_collection,
            model_config=model_config,
            reference_data=LLMExtractorReferenceData.resolve(
                state_code=input_document_collection.state_code,
                reference_data_config=extractor_collection.reference_data_config,
                reference_data_registries=reference_data_registries_by_data_type,
            ),
            # Extractors defined via YAMLs are always first-order extractors (not entity
            # resolution extractors), so this value will always be None.
            entity_group=None,
            golden_eval=golden_eval,
        )
        if config_dict:
            raise ValueError(
                f"Found unexpected config values for extractor [{name}] at "
                f"[{yaml_path}]: {repr(config_dict.get())}"
            )
        return config
