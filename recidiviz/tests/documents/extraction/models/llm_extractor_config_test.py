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
"""Tests for llm_extractor_config.py.

Covers the resolution of a state's `extractor.yaml` into a fully-bound
`LLMExtractorConfig`: collection / input-document-collection / model resolution
(via `from_yaml`), the model-binding and cap invariants (via direct
construction), and the happy path against the fake config module.
"""

import re
from typing import Any
from unittest import TestCase

import attr
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.common.descriptor import Descriptor
from recidiviz.documents.extraction.config_defaults import (
    DEFAULT_FIRST_ORDER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD,
    DEFAULT_FIRST_ORDER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP,
    DEFAULT_MAX_TRANSIENT_RETRY_COUNT,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
    LLMDocumentExtractionGoldenEvalConfig,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    LLMExtractorCollectionConfig,
    get_llm_extractor_collection_config,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
    LLMExtractorDocumentFilterConfig,
)
from recidiviz.documents.extraction.models.llm_model_registry import (
    BaseLLMModel,
    LLMAPIProvider,
    LLMModelConfig,
    LLMModelFamily,
    load_llm_model_registry,
)
from recidiviz.documents.extraction.models.reference_data.llm_extractor_reference_data import (
    LLMExtractorReferenceData,
)
from recidiviz.documents.extraction.models.reference_data.reference_data_registry import (
    ReferenceDataType,
    load_full_reference_data_registry,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    DocumentRootEntityIdType,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.store.document_store_test_utils import (
    FAKE_INPUT_DOCUMENT_COLLECTION_NAME,
)
from recidiviz.tests.ingest import fixtures
from recidiviz.utils.string import sha256_hexdigest

_DESCRIPTION = "A description that is long enough to be meaningful."
_FILTER_QUERY = "SELECT document_contents_id FROM `{project_id}.x.y`"
_GOLDEN_EVAL_SHEET_URI = "https://docs.google.com/spreadsheets/d/abc123"
_GOLDEN_EVAL_CONFIG = LLMDocumentExtractionGoldenEvalConfig(
    source_sheet_uri=_GOLDEN_EVAL_SHEET_URI,
    accuracy_thresholds={
        GoldenEvalTestType.UNIT: 1.0,
        GoldenEvalTestType.SAMPLE: 0.9,
    },
)

# Defined under recidiviz/tests/documents/fake_config/. The fake collection
# defaults to _DEFAULT_MODEL_CONFIG_NAME; _OVERRIDE_MODEL_CONFIG_NAME is a second,
# distinct first-order-valid config used to exercise the state-level override.
_FAKE_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_DEFAULT_MODEL_CONFIG_NAME = "ACME_LARGE_NO_THINKING"
_OVERRIDE_MODEL_CONFIG_NAME = "ACME_LARGE_DETERMINISTIC"


def _input_document_collection(
    *,
    name: str,
    state_code: StateCode,
) -> DocumentCollectionConfig:
    """Returns a minimal valid input document collection for resolution."""
    return DocumentCollectionConfig(
        state_code=state_code,
        name=name,
        description=_DESCRIPTION,
        root_entity_id_type=DocumentRootEntityIdType.PERSON_EXTERNAL_ID,
        document_primary_key_columns=[
            bigquery.SchemaField("note_id", "STRING"),
        ],
        other_metadata_columns=[],
        document_generation_query_template="SELECT 1",
        other_document_generation_output_columns=[],
        document_descriptor=Descriptor(singular="case note", plural="case notes"),
    )


def _model_config(
    *,
    supports_structured_output: bool = True,
    supports_implicit_caching_in_batch: bool = True,
    enables_thinking: bool = False,
) -> LLMModelConfig:
    """Builds a minimal LLMModelConfig with the given capability flags, so each
    model-binding test can show the exact trait that triggers (or satisfies) the
    extractor's invariants. Thinking is driven by the base model's
    `is_thinking_model`: with no thinking-budget parameter set, a thinking model
    has thinking enabled.
    """
    return LLMModelConfig(
        name="ACME_TEST",
        base_model=BaseLLMModel(
            name="acme-test",
            family=LLMModelFamily(
                name="acme", default_api_provider=LLMAPIProvider.VERTEX_AI
            ),
            is_thinking_model=enables_thinking,
            input_token_limit=1000,
            supports_structured_output=supports_structured_output,
            supports_implicit_caching_in_batch=supports_implicit_caching_in_batch,
            known_pinned_versions=["acme-test-001"],
            parameters={},
        ),
        model="acme-test-001",
        parameter_values={},
    )


def _document_filter(
    *,
    template: str = _FILTER_QUERY,
    is_sandbox_config: bool = False,
    document_limit: int | None = None,
    root_entity_ids: list[str] | None = None,
) -> LLMExtractorDocumentFilterConfig:
    """Builds an un-narrowed (production-shaped) document filter by default."""
    return LLMExtractorDocumentFilterConfig(
        document_metadata_filter_query_template=template,
        is_sandbox_config=is_sandbox_config,
        document_limit=document_limit,
        root_entity_ids=root_entity_ids,
    )


class LLMExtractorConfigFromYamlTest(TestCase):
    """from_yaml behaviors that need a file path + the resolution dependencies."""

    def setUp(self) -> None:
        self.registry = load_llm_model_registry(config_module=fake_config)
        self.extractor_collection = get_llm_extractor_collection_config(
            _FAKE_COLLECTION_NAME, config_module=fake_config
        )
        document_collection = _input_document_collection(
            state_code=StateCode.US_XX,
            name=FAKE_INPUT_DOCUMENT_COLLECTION_NAME,
        )
        self.document_collection_configs = {
            document_collection.name: document_collection
        }

    def _resolve_fixture(self, relative_path: str) -> LLMExtractorConfig:
        """Resolves the fixture extractor config at |relative_path| (relative to
        the fixtures/ directory) against the fake collection, registry, and
        document collection.
        """
        return LLMExtractorConfig.from_yaml(
            fixtures.as_filepath(relative_path),
            extractor_collections_by_name={
                self.extractor_collection.name: self.extractor_collection
            },
            model_registry=self.registry,
            document_collections_by_name=self.document_collection_configs,
            reference_data_registries_by_data_type=load_full_reference_data_registry(
                StateCode.US_XX, config_module=fake_config
            ),
        )

    def test_minimal_config_uses_defaults(self) -> None:
        # A config declaring only the required fields: the model resolves to the
        # collection default, prompt vars carry only the one the template requires,
        # and the caps/retry fall back to the code-level defaults.
        config = self._resolve_fixture(
            "extractor_configs/fake_extractor_collection/us_xx/minimal.yaml"
        )
        self.assertEqual(_DEFAULT_MODEL_CONFIG_NAME, config.model_config.name)
        self.assertEqual(
            FAKE_INPUT_DOCUMENT_COLLECTION_NAME, config.input_document_collection.name
        )
        self.assertEqual(
            {"agency_name": "the Department of Fictional Affairs"}, config.prompt_vars
        )
        self.assertEqual(
            DEFAULT_FIRST_ORDER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD,
            config.single_job_document_count_batch_threshold,
        )
        self.assertEqual(
            DEFAULT_FIRST_ORDER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP,
            config.total_pending_document_count_hard_cap,
        )
        self.assertEqual(
            DEFAULT_MAX_TRANSIENT_RETRY_COUNT, config.max_transient_retry_count
        )

    def test_resolves_reference_data(self) -> None:
        # The fake collection declares an `acronyms` reference-data block; resolving
        # the extractor binds it to the state's merged acronyms registry.
        config = self._resolve_fixture(
            "extractor_configs/fake_extractor_collection/us_xx/minimal.yaml"
        )
        self.assertEqual(StateCode.US_XX, config.reference_data.state_code)
        self.assertEqual(
            [ReferenceDataType.ACRONYMS], list(config.reference_data.per_type)
        )
        acronyms = config.reference_data.per_type[ReferenceDataType.ACRONYMS]
        self.assertEqual("acronym_glossary", acronyms.config.prompt_var)
        # shared + us_xx acronyms merged (state wins on the CRC collision).
        self.assertEqual(
            ["PO", "CRC", "XX"], list(acronyms.registry.entries_by_dedup_key)
        )

    def test_overrides_resolve(self) -> None:
        config = self._resolve_fixture(
            "extractor_configs/fake_extractor_collection/us_xx/all_overrides.yaml"
        )
        self.assertEqual(_OVERRIDE_MODEL_CONFIG_NAME, config.model_config.name)

        self.assertEqual(11111, config.single_job_document_count_batch_threshold)
        self.assertEqual(999999, config.total_pending_document_count_hard_cap)
        self.assertEqual(7, config.max_transient_retry_count)

        self.assertEqual({"agency_name": "Dept of X"}, config.prompt_vars)

    def test_collection_name_directory_mismatch_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "does not match its collection directory"
        ):
            self._resolve_fixture(
                "extractor_configs/mismatched_directory/us_xx/extractor.yaml"
            )

    def test_unknown_collection_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape("references collection [UNKNOWN_COLLECTION], which has no"),
        ):
            self._resolve_fixture(
                "extractor_configs/unknown_collection/us_xx/extractor.yaml"
            )

    def test_unknown_input_document_collection_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape("references unknown input document collection [MISSING_NOTES]"),
        ):
            self._resolve_fixture(
                "extractor_configs/fake_extractor_collection/us_xx/"
                "unknown_input_document_collection.yaml"
            )

    def test_unknown_override_model_config_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, re.escape("No model config named [NONEXISTENT_MODEL]")
        ):
            self._resolve_fixture(
                "extractor_configs/fake_extractor_collection/us_xx/"
                "unknown_override_model_config.yaml"
            )

    def test_unexpected_key_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Found unexpected config values for extractor "
                f"[{_FAKE_COLLECTION_NAME}]"
            ),
        ):
            self._resolve_fixture(
                "extractor_configs/fake_extractor_collection/us_xx/"
                "unexpected_key.yaml"
            )

    def test_explicit_zero_retry_count_honored(self) -> None:
        # 0 is a legitimate value and must not be coerced to the default.
        config = self._resolve_fixture(
            "extractor_configs/fake_extractor_collection/us_xx/zero_retry.yaml"
        )
        self.assertEqual(0, config.max_transient_retry_count)

    def test_golden_eval_block_is_parsed(self) -> None:
        config = self._resolve_fixture(
            "extractor_configs/fake_extractor_collection/us_xx/minimal.yaml"
        )
        self.assertEqual(_GOLDEN_EVAL_CONFIG, config.golden_eval)

    def test_missing_golden_eval_raises(self) -> None:
        # Every authored (first-order) extractor must declare a golden eval set.
        # Only the generated entity-resolution extractor is exempt, and it is
        # built directly rather than parsed from an extractor.yaml.
        with self.assertRaisesRegex(
            KeyError, re.escape("Expected nonnull [golden_eval]")
        ):
            self._resolve_fixture(
                "extractor_configs/fake_extractor_collection/us_xx/"
                "missing_golden_eval.yaml"
            )


# The full compiled prompt for the fake US_XX extractor: the fake collection's
# `prompt_template.txt` with `{agency_name}` filled from prompt_vars and the
# `{output_instructions}` / `{reference_data}` blocks generated from its schema
# and reference data. Regenerate from `.instructions_prompt` if the template, a
# generator, or the fake collection/reference-data fixtures intentionally change.
_EXPECTED_FAKE_INSTRUCTIONS_PROMPT = """\
You are extracting fictional assignment information for the Department of Fictional Affairs.

CRITICAL INSTRUCTION: First determine whether this document is relevant. A document is considered relevant according to the following criteria:
Whether the document mentions a fictional assignment record
If it is not relevant, set is_relevant to false.

Output fields:
- is_relevant (boolean, bare value): Whether the document mentions a fictional assignment record
- primary_status (enum): The primary status described in the document. Allowed values:
  - active: The record is currently active.
  - inactive: The record is currently inactive.
  - other: A status that is neither active nor inactive.
- status_note (string, bare value): A one-line model-composed summary of the status.
- location (string): The location associated with the record.
- assignments (list): Assignments mentioned in the document.
  - assignment_name (string): Name of the assignment.
  - assignment_type (enum): The kind of assignment. Allowed values:
    - internal: An assignment internal to the organization.
    - external: An assignment external to the organization.
    - other: An assignment that is neither internal nor external.
  - rate_amount (number): Pay rate amount (numeric value only).
  - rate_period (enum): Time period for the rate. Allowed values:
    - hourly: The rate is per hour.
    - monthly: The rate is per month.

Fields must be logically consistent:
- `location` must be null with null_reason='not_applicable' when `primary_status` is one of ['inactive'].
- `assignments` applies only when `primary_status` is one of ['active']; otherwise leave it empty.
- `assignments[].rate_period` applies only when `rate_amount` is set; otherwise set it to null with null_reason='not_applicable'.

Each document produces exactly one extraction result object, conforming to the output schema supplied separately with this request.

Each field listed above is reported with a metadata wrapper, in one of two shapes (the schema enforces exactly one). Emit only the keys shown for the shape you use — the key that distinguishes the other shape (`value` or `null_reason`) is absent, not null. Within a shape, include every key shown. Exceptions:
- fields tagged "bare value", which you output directly
- "list" fields, which are arrays whose elements are objects (each sub-field follows these same rules)

When you extract a value:
  {
    "adversarial_interpretation": <The strongest alternative reading of the source text — how one could reasonably arrive at a different value, or why a value might be present when none was extracted. Null when no reasonable alternative exists.>,
    "value": <the extracted value>,
    "confidence_level": "speculative" | "inferred" | "explicit" | "verbatim",
    "citations": [
      {
        "text": <The exact quoted text from the document.>,
        "start": <Character offset in the document where the quoted text begins.>,
        "end": <Character offset in the document where the quoted text ends.>,
      },
      ...
    ],  // Exact quotes from the document supporting the extracted value; at least one is required.
  }

When you cannot extract a value:
  {
    "adversarial_interpretation": <The strongest alternative reading of the source text — how one could reasonably arrive at a different value, or why a value might be present when none was extracted. Null when no reasonable alternative exists.>,
    "null_reason": "not_applicable" | "no_info_found" | "explicitly_unknown",
    "confidence_level": "speculative" | "inferred" | "explicit" | "verbatim",
    "citations": [
      {
        "text": <The exact quoted text from the document.>,
        "start": <Character offset in the document where the quoted text begins.>,
        "end": <Character offset in the document where the quoted text ends.>,
      },
      ...
    ],  // Exact quotes from the document supporting why no value was extracted. Required, but may be empty — include a quote only when one shows the absence (e.g. for explicitly_unknown).
  }

- When `adversarial_interpretation` is non-null, set
  `confidence_level` to "speculative".

`confidence_level` values, ordered from least to most
confident:
- "speculative": Lowest confidence: a plausible alternative reading of the document exists (i.e. the model recorded an adversarial interpretation), so there is genuine doubt about whether the extracted value is correct.
- "inferred": The value follows from one clear inferential step over direct evidence in the document; there is no adversarial interpretation.
- "explicit": The value is directly stated in the document, with at most minor normalization (synonym, abbreviation, or paraphrase).
- "verbatim": Highest confidence: the value appears exactly as-is in the document — the citation text IS the value, with zero rewording or substitution.

`null_reason` values, used only on the null shape above:
- "not_applicable": The field does not apply given the values of the other fields it depends on — for example, a field that is only meaningful for a particular value of another field, when that value does not hold.
- "no_info_found": The document does not mention this information.
- "explicitly_unknown": The document acknowledges the information but states it is unknown.

COMMON ABBREVIATIONS:
- "PO" = Parole Officer
- "CRC" = Citadel Reentry Center
- "XX" = Example Expansion"""


class LLMExtractorConfigVersionIdTest(TestCase):
    """Tests for the computed identity and version-ID properties:
    `extractor_id`, `extractor_version_id`, and `document_filter_id`.
    """

    def setUp(self) -> None:
        self.collection = get_llm_extractor_collection_config(
            _FAKE_COLLECTION_NAME, config_module=fake_config
        )
        self.registry = load_llm_model_registry(config_module=fake_config)
        self.reference_data = LLMExtractorReferenceData.resolve(
            state_code=StateCode.US_XX,
            reference_data_config=self.collection.reference_data_config,
            reference_data_registries=load_full_reference_data_registry(
                StateCode.US_XX, config_module=fake_config
            ),
        )

    def _config(
        self,
        *,
        state_code: StateCode = StateCode.US_XX,
        model_config: LLMModelConfig | None = None,
        extractor_collection: LLMExtractorCollectionConfig | None = None,
        document_metadata_filter_query_template: str = _FILTER_QUERY,
    ) -> LLMExtractorConfig:
        # reference_data is resolved once for US_XX and reused for every state: it
        # is not validated against the state and feeds no version ID directly (it
        # flows in only via the prompt), so it does not affect these tests.
        return LLMExtractorConfig(
            state_code=state_code,
            input_document_collection=_input_document_collection(
                state_code=state_code, name=FAKE_INPUT_DOCUMENT_COLLECTION_NAME
            ),
            document_filter=_document_filter(
                template=document_metadata_filter_query_template
            ),
            prompt_vars={"agency_name": "the Department of Fictional Affairs"},
            max_transient_retry_count=DEFAULT_MAX_TRANSIENT_RETRY_COUNT,
            total_pending_document_count_hard_cap=(
                DEFAULT_FIRST_ORDER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP
            ),
            single_job_document_count_batch_threshold=(
                DEFAULT_FIRST_ORDER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD
            ),
            extractor_collection=(
                extractor_collection
                if extractor_collection is not None
                else self.collection
            ),
            model_config=(
                model_config
                if model_config is not None
                else self.registry.get_model_config(_DEFAULT_MODEL_CONFIG_NAME)
            ),
            reference_data=self.reference_data,
            entity_group=None,
            golden_eval=_GOLDEN_EVAL_CONFIG,
        )

    def test_extractor_id_format(self) -> None:
        self.assertEqual("US_XX_FAKE_EXTRACTOR_COLLECTION", self._config().extractor_id)

    def test_instructions_prompt_matches_golden(self) -> None:
        self.assertEqual(
            _EXPECTED_FAKE_INSTRUCTIONS_PROMPT, self._config().instructions_prompt
        )

    def test_instructions_prompt_hash_is_hash_of_prompt(self) -> None:
        config = self._config()
        self.assertEqual(
            sha256_hexdigest(config.instructions_prompt),
            config.instructions_prompt_hash,
        )

    def test_extractor_version_id_golden(self) -> None:
        # Pinned hash for the fake extractor. A change here is a real version bump
        # and must be consciously updated, not silently accepted.
        config = get_first_order_llm_extractor_config(
            StateCode.US_XX, _FAKE_COLLECTION_NAME, config_module=fake_config
        )
        self.assertEqual(
            "30d16f4b0668ae7553fe741b2d1ad02a010408152f89c218f5c425c12df34db4",
            config.extractor_version_id,
        )

    def test_extractor_version_id_changes_with_model_config(self) -> None:
        self.assertNotEqual(
            self._config(
                model_config=self.registry.get_model_config(_DEFAULT_MODEL_CONFIG_NAME)
            ).extractor_version_id,
            self._config(
                model_config=self.registry.get_model_config(_OVERRIDE_MODEL_CONFIG_NAME)
            ).extractor_version_id,
        )

    def test_extractor_version_id_changes_with_collection(self) -> None:
        # Same extractor_id (collection name unchanged), different output schema.
        # The collection's relevance_criteria must match its schema's, so evolve
        # both in lockstep.
        other_relevance_criteria = "Whether the document mentions a different subject."
        other_collection = attr.evolve(
            self.collection,
            relevance_criteria=other_relevance_criteria,
            output_schema=attr.evolve(
                self.collection.output_schema,
                relevance_criteria=other_relevance_criteria,
            ),
        )
        self.assertNotEqual(
            self._config().extractor_version_id,
            self._config(extractor_collection=other_collection).extractor_version_id,
        )

    def test_extractor_version_id_changes_with_extractor_id(self) -> None:
        # Only the extractor_id differs (US_XX vs US_YY); all else is held equal.
        self.assertNotEqual(
            self._config(state_code=StateCode.US_XX).extractor_version_id,
            self._config(state_code=StateCode.US_YY).extractor_version_id,
        )

    def test_document_filter_id_golden(self) -> None:
        # Pinned hash for the fake extractor's document filter.
        config = get_first_order_llm_extractor_config(
            StateCode.US_XX, _FAKE_COLLECTION_NAME, config_module=fake_config
        )
        self.assertEqual(
            "56ab28ae678cefd96cc5568debea692e908b9118c78bb7cd230b70abfd6c81be",
            config.document_filter_id,
        )

    def test_document_filter_id_changes_with_filter_template(self) -> None:
        self.assertNotEqual(
            self._config(
                document_metadata_filter_query_template=_FILTER_QUERY
            ).document_filter_id,
            self._config(
                document_metadata_filter_query_template=(
                    "SELECT document_contents_id FROM `{project_id}.other.table`"
                )
            ).document_filter_id,
        )

    def test_document_filter_id_changes_with_extractor_id(self) -> None:
        # Same filter template, different extractor — the ID is namespaced to the
        # extractor_id.
        self.assertNotEqual(
            self._config(state_code=StateCode.US_XX).document_filter_id,
            self._config(state_code=StateCode.US_YY).document_filter_id,
        )

    def test_document_filter_id_independent_of_prompt_and_model(self) -> None:
        # The filter ID folds in only the extractor_id and the filter template, so
        # changing the model config or the collection must not change it.
        # The collection's relevance_criteria must match its schema's, so evolve
        # both in lockstep.
        other_relevance_criteria = "Whether the document mentions a different subject."
        other_collection = attr.evolve(
            self.collection,
            relevance_criteria=other_relevance_criteria,
            output_schema=attr.evolve(
                self.collection.output_schema,
                relevance_criteria=other_relevance_criteria,
            ),
        )
        self.assertEqual(
            self._config().document_filter_id,
            self._config(
                model_config=self.registry.get_model_config(
                    _OVERRIDE_MODEL_CONFIG_NAME
                ),
                extractor_collection=other_collection,
            ).document_filter_id,
        )

    def test_with_sandbox_narrowing_sets_knobs_but_preserves_filter_id(self) -> None:
        config = self._config()
        narrowed = config.with_sandbox_narrowing(
            document_limit=50, root_entity_ids=["P1", "P2"]
        )

        # The narrowing knobs (and the sandbox flag) are set on the copy; the
        # original is untouched (frozen).
        self.assertTrue(narrowed.document_filter.is_sandbox_config)
        self.assertEqual(50, narrowed.document_filter.document_limit)
        self.assertEqual(["P1", "P2"], narrowed.document_filter.root_entity_ids)
        self.assertFalse(config.document_filter.is_sandbox_config)
        self.assertIsNone(config.document_filter.document_limit)
        self.assertIsNone(config.document_filter.root_entity_ids)

        # The authored template is unchanged, so the filter ID is byte-identical
        # despite the narrowing.
        self.assertEqual(
            config.document_filter.document_metadata_filter_query_template,
            narrowed.document_filter.document_metadata_filter_query_template,
        )
        self.assertEqual(config.document_filter_id, narrowed.document_filter_id)


class LLMExtractorDocumentFilterConfigTest(TestCase):
    """Tests for LLMExtractorDocumentFilterConfig's own invariants and version_id."""

    def test_narrowing_requires_sandbox_config(self) -> None:
        # A non-sandbox config may not carry either narrowing knob.
        for document_limit, root_entity_ids in [(50, None), (None, ["P1"])]:
            with self.subTest(
                document_limit=document_limit, root_entity_ids=root_entity_ids
            ):
                with self.assertRaisesRegex(
                    ValueError, "may only be set on a sandbox config"
                ):
                    _document_filter(
                        is_sandbox_config=False,
                        document_limit=document_limit,
                        root_entity_ids=root_entity_ids,
                    )

    def test_sandbox_config_without_narrowing_is_allowed(self) -> None:
        # A sandbox config need not carry any narrowing (e.g. a sandbox run over
        # the full authored filter).
        document_filter = _document_filter(is_sandbox_config=True)
        self.assertTrue(document_filter.is_sandbox_config)

    def test_version_id_excludes_narrowing(self) -> None:
        # version_id folds in only the authored template, so a narrowed sandbox
        # filter hashes identically to its un-narrowed production counterpart.
        production = _document_filter()
        narrowed = _document_filter(
            is_sandbox_config=True, document_limit=1, root_entity_ids=["P1"]
        )
        self.assertEqual(production.version_id, narrowed.version_id)

    def test_version_id_changes_with_template(self) -> None:
        self.assertNotEqual(
            _document_filter(template=_FILTER_QUERY).version_id,
            _document_filter(
                template="SELECT document_contents_id FROM `{project_id}.other.t`"
            ).version_id,
        )


class LLMExtractorConfigConstructionTest(TestCase):
    """Model-binding and cap invariants, exercised via direct construction."""

    def setUp(self) -> None:
        self.collection = get_llm_extractor_collection_config(
            _FAKE_COLLECTION_NAME, config_module=fake_config
        )
        self.input_document_collection = _input_document_collection(
            state_code=StateCode.US_XX,
            name=FAKE_INPUT_DOCUMENT_COLLECTION_NAME,
        )
        self.reference_data = LLMExtractorReferenceData.resolve(
            state_code=StateCode.US_XX,
            reference_data_config=self.collection.reference_data_config,
            reference_data_registries=load_full_reference_data_registry(
                StateCode.US_XX, config_module=fake_config
            ),
        )

    def _config(
        self,
        *,
        state_code: StateCode = StateCode.US_XX,
        model_config: LLMModelConfig | None = None,
        entity_group: Any = None,
        prompt_vars: dict[str, str] | None = None,
        single_job_document_count_batch_threshold: int = (
            DEFAULT_FIRST_ORDER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD
        ),
        total_pending_document_count_hard_cap: int = (
            DEFAULT_FIRST_ORDER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP
        ),
    ) -> LLMExtractorConfig:
        return LLMExtractorConfig(
            state_code=state_code,
            input_document_collection=self.input_document_collection,
            document_filter=_document_filter(),
            prompt_vars=(
                prompt_vars
                if prompt_vars is not None
                else {"agency_name": "the Department of Fictional Affairs"}
            ),
            max_transient_retry_count=DEFAULT_MAX_TRANSIENT_RETRY_COUNT,
            total_pending_document_count_hard_cap=total_pending_document_count_hard_cap,
            single_job_document_count_batch_threshold=single_job_document_count_batch_threshold,
            extractor_collection=self.collection,
            model_config=model_config if model_config is not None else _model_config(),
            reference_data=self.reference_data,
            entity_group=entity_group,
            golden_eval=_GOLDEN_EVAL_CONFIG,
        )

    def test_state_code_mismatch_with_input_document_collection_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "reads input document collection .* which belongs to state",
        ):
            self._config(
                # Disagrees with the state_code on self.input_document_collection
                state_code=StateCode.US_YY
            )

    def test_batch_threshold_exceeds_hard_cap_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "single_job_document_count_batch_threshold .* greater than "
            "total_pending_document_count_hard_cap",
        ):
            self._config(
                single_job_document_count_batch_threshold=1000,
                total_pending_document_count_hard_cap=100,
            )

    def test_model_without_structured_output_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "does not support structured output"):
            self._config(model_config=_model_config(supports_structured_output=False))

    def test_model_without_batch_caching_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "does not support implicit caching in batch prediction"
        ):
            self._config(
                model_config=_model_config(supports_implicit_caching_in_batch=False)
            )

    def test_first_order_thinking_model_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "binds thinking-enabled model config"):
            self._config(model_config=_model_config(enables_thinking=True))

    def test_prompt_vars_shadowing_reserved_var_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"declares prompt_vars that shadow reserved template variables: "
            r"\['output_instructions'\]",
        ):
            self._config(
                prompt_vars={
                    "agency_name": "the Department of Fictional Affairs",
                    "output_instructions": "sneaky override",
                }
            )

    def test_unused_prompt_var_raises(self) -> None:
        # A prompt var the template does not reference is a config error, surfaced
        # by StrictStringFormatter when the prompt is compiled at construction.
        with self.assertRaisesRegex(ValueError, r"Unused kwargs passed: .*not_a_var"):
            self._config(
                prompt_vars={
                    "agency_name": "the Department of Fictional Affairs",
                    "not_a_var": "value",
                }
            )

    def test_entity_resolution_allows_thinking_model(self) -> None:
        # The thinking ban is scoped to first-order (entity_group is None); an ER
        # config with an entity group may bind a thinking model.
        config = self._config(
            model_config=_model_config(enables_thinking=True),
            entity_group=self.collection.entity_groups[0],
        )
        self.assertTrue(config.model_config.enables_thinking)
        self.assertIsNotNone(config.entity_group)
