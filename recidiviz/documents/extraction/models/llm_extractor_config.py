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
document collection it reads, the document filter, the prompt vars, and any
model/cap overrides. `LLMExtractorConfig.from_yaml` parses that file and binds it
with its already-parsed collection config, its resolved input document
collection, and its resolved model config into one fully-resolved object.
"""
from functools import cache
from pathlib import Path
from types import ModuleType

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents import config as default_config_module
from recidiviz.documents.extraction.config_defaults import (
    DEFAULT_FIRST_ORDER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD,
    DEFAULT_FIRST_ORDER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP,
    DEFAULT_MAX_TRANSIENT_RETRY_COUNT,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    EntityGroupConfig,
    LLMExtractorCollectionConfig,
    extractor_collections_dir,
    load_llm_extractor_collection_configs,
)
from recidiviz.documents.extraction.models.llm_model_registry import (
    LLMModelConfig,
    LLMModelRegistry,
    load_llm_model_registry,
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
    load_full_reference_data_registry,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    collect_document_collection_configs,
)
from recidiviz.utils.yaml_dict import YAMLDict

EXTRACTOR_CONFIG_FILENAME = "extractor.yaml"


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

    document_metadata_filter_query_template: str = attr.ib(
        validator=attr_validators.is_non_empty_str
    )
    """SQL template for a query that returns a single `document_contents_id` column,
    holding the document_content_ids with this extractor should process. Used to narrow
    the scope of documents relevant to this extractor. The only template variable should
    be {project_id}.
    """

    prompt_vars: dict[str, str] = attr.ib(
        validator=attr_validators.is_dict_of(str, str)
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

        config = cls(
            state_code=cls.state_code_for_yaml_path(yaml_path),
            input_document_collection=input_document_collection,
            document_metadata_filter_query_template=config_dict.pop(
                "document_metadata_filter_query_template", str
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
        )
        if config_dict:
            raise ValueError(
                f"Found unexpected config values for extractor [{name}] at "
                f"[{yaml_path}]: {repr(config_dict.get())}"
            )
        return config


def get_states_with_extractor_configs(
    config_module: ModuleType | None = None,
) -> set[StateCode]:
    """Returns the set of states that define at least one extractor under
    |config_module| (the production config package by default), derived from the
    `extractor_collections/{collection}/{state}/extractor.yaml` files without
    parsing them.
    """
    module = config_module or default_config_module
    collections_dir = extractor_collections_dir(module)
    if not collections_dir.is_dir():
        raise ValueError(
            f"Extractor collections directory does not exist: [{collections_dir}]."
        )
    return {
        LLMExtractorConfig.state_code_for_yaml_path(extractor_yaml_path)
        for extractor_yaml_path in collections_dir.glob(
            f"*/*/{EXTRACTOR_CONFIG_FILENAME}"
        )
    }


@cache
def load_llm_extractor_configs(
    config_module: ModuleType | None = None,
) -> dict[StateCode, dict[str, LLMExtractorConfig]]:
    """Returns the resolved extractor configs parsed from every
    `extractor_collections/{collection}/{state}/extractor.yaml` within
    |config_module| (the production config package by default), each bound to its
    collection config, input document collection, and resolved model config, keyed
    by state code and then by collection name.
    """
    module = config_module or default_config_module
    collections_dir = extractor_collections_dir(module)
    model_registry = load_llm_model_registry(module)
    extractor_collections_by_name = load_llm_extractor_collection_configs(module)

    configs_by_state: dict[StateCode, dict[str, LLMExtractorConfig]] = {}
    for state_code in get_states_with_extractor_configs(module):
        document_collections_by_name = collect_document_collection_configs(
            state_code, config_module=module
        )
        reference_data_registries = load_full_reference_data_registry(
            state_code, config_module=module
        )
        state_configs: dict[str, LLMExtractorConfig] = {}
        for extractor_yaml_path in sorted(
            collections_dir.glob(
                f"*/{state_code.value.lower()}/{EXTRACTOR_CONFIG_FILENAME}"
            )
        ):
            config = LLMExtractorConfig.from_yaml(
                extractor_yaml_path,
                extractor_collections_by_name=extractor_collections_by_name,
                model_registry=model_registry,
                document_collections_by_name=document_collections_by_name,
                reference_data_registries_by_data_type=reference_data_registries,
            )
            collection_name = config.extractor_collection.name
            if collection_name in state_configs:
                raise ValueError(
                    f"Found multiple extractor configs for collection "
                    f"[{collection_name}] in state [{state_code.value}]."
                )
            state_configs[collection_name] = config
        configs_by_state[state_code] = state_configs
    return configs_by_state


def get_llm_extractor_config(
    state_code: StateCode,
    collection_name: str,
    config_module: ModuleType | None = None,
) -> LLMExtractorConfig:
    """Returns the resolved extractor config for |collection_name| in |state_code|
    within |config_module| (the production config package by default), raising if
    no such extractor exists.
    """
    state_configs = load_llm_extractor_configs(config_module).get(state_code, {})
    if collection_name not in state_configs:
        raise ValueError(
            f"No extractor config for collection [{collection_name}] in state "
            f"[{state_code.value}]. Known collections for this state: "
            f"{sorted(state_configs)}."
        )
    return state_configs[collection_name]
