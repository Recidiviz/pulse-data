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

from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.config_defaults import (
    DEFAULT_FIRST_ORDER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD,
    DEFAULT_FIRST_ORDER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP,
    DEFAULT_MAX_TRANSIENT_RETRY_COUNT,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    get_llm_extractor_collection_config,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
    get_llm_extractor_config,
    get_states_with_extractor_configs,
    load_llm_extractor_configs,
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
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
)
from recidiviz.tests.big_query.sqlglot_helpers import check_query_selects_output_columns
from recidiviz.tests.documents import fake_config
from recidiviz.tests.ingest import fixtures
from recidiviz.utils.string import StrictStringFormatter

_DESCRIPTION = "A description that is long enough to be meaningful."
_FILTER_QUERY = "SELECT document_contents_id FROM `{project_id}.x.y`"

# Defined under recidiviz/tests/documents/fake_config/. The fake collection
# defaults to _DEFAULT_MODEL_CONFIG_NAME; _OVERRIDE_MODEL_CONFIG_NAME is a second,
# distinct first-order-valid config used to exercise the state-level override.
_FAKE_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_DEFAULT_MODEL_CONFIG_NAME = "ACME_LARGE_NO_THINKING"
_OVERRIDE_MODEL_CONFIG_NAME = "ACME_LARGE_DETERMINISTIC"
_INPUT_DOCUMENT_COLLECTION_NAME = "FAKE_INPUT_NOTES"


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
        primary_key_columns=[
            bigquery.SchemaField("person_external_id", "STRING"),
            bigquery.SchemaField("note_id", "STRING"),
        ],
        other_metadata_columns=[],
        document_generation_query_template="SELECT 1",
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


class ParseAllExtractorConfigsTest(TestCase):
    """Guards real configs, and exercises the fake config's happy path."""

    def test_get_states_with_extractor_configs_not_empty(self) -> None:
        # Guards against the set being empty, which would make any test that
        # iterates over it (e.g. the reference-data load guard) pass vacuously.
        self.assertTrue(get_states_with_extractor_configs())

    def test_load_all_real_configs(self) -> None:
        # Raises if any real extractor.yaml fails to parse or resolve. Does not
        # assert on any state's specifics — the fake config covers structure.
        configs_by_state = load_llm_extractor_configs()
        self.assertTrue(configs_by_state)

        # Each extractor's document filter template must render with a project_id
        # into a query that selects exactly the single document_contents_id column.
        for state_configs in configs_by_state.values():
            for config in state_configs.values():
                with self.subTest(
                    collection=config.extractor_collection.name,
                    state=config.state_code.value,
                ):
                    rendered_query = StrictStringFormatter().format(
                        config.document_metadata_filter_query_template,
                        project_id="test-project",
                    )
                    check_query_selects_output_columns(
                        rendered_query, {DOCUMENT_CONTENTS_ID_COLUMN_NAME}
                    )

    def test_fake_extractor_resolves(self) -> None:
        configs_by_state = load_llm_extractor_configs(config_module=fake_config)
        self.assertEqual([StateCode.US_XX], list(configs_by_state))
        self.assertEqual(
            [_FAKE_COLLECTION_NAME], list(configs_by_state[StateCode.US_XX])
        )
        config = configs_by_state[StateCode.US_XX][_FAKE_COLLECTION_NAME]

        self.assertEqual(_FAKE_COLLECTION_NAME, config.extractor_collection.name)
        self.assertEqual(StateCode.US_XX, config.state_code)

        # Input document collection resolved to the object (not just the name);
        # its BQ table id is the lowercased name.
        self.assertEqual(
            _INPUT_DOCUMENT_COLLECTION_NAME, config.input_document_collection.name
        )
        self.assertEqual(StateCode.US_XX, config.input_document_collection.state_code)
        self.assertEqual(
            "fake_input_notes", config.input_document_collection.metadata_table_id
        )

        # The state-level override resolved to the override config, not the
        # collection default.
        self.assertEqual(_OVERRIDE_MODEL_CONFIG_NAME, config.model_config.name)
        self.assertFalse(config.model_config.enables_thinking)

        self.assertEqual(
            {"agency_name": "the Department of Fictional Affairs"},
            config.prompt_vars,
        )

        # Thresholds and retry omitted in the YAML -> code-level defaults.
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
        self.assertIsNone(config.entity_group)

    def test_get_extractor_config(self) -> None:
        config = get_llm_extractor_config(
            StateCode.US_XX, _FAKE_COLLECTION_NAME, config_module=fake_config
        )
        self.assertEqual(_FAKE_COLLECTION_NAME, config.extractor_collection.name)
        self.assertEqual(StateCode.US_XX, config.state_code)

    def test_get_unknown_extractor_config_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape("No extractor config for collection [NOT_A_COLLECTION]"),
        ):
            get_llm_extractor_config(
                StateCode.US_XX, "NOT_A_COLLECTION", config_module=fake_config
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
            name=_INPUT_DOCUMENT_COLLECTION_NAME,
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
        # collection default, prompt vars are empty, and the caps/retry fall back
        # to the code-level defaults.
        config = self._resolve_fixture(
            "extractor_configs/fake_extractor_collection/us_xx/minimal.yaml"
        )
        self.assertEqual(_DEFAULT_MODEL_CONFIG_NAME, config.model_config.name)
        self.assertEqual(
            _INPUT_DOCUMENT_COLLECTION_NAME, config.input_document_collection.name
        )
        self.assertEqual({}, config.prompt_vars)
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

        self.assertEqual({"agency": "Dept of X", "tone": "formal"}, config.prompt_vars)

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


class LLMExtractorConfigConstructionTest(TestCase):
    """Model-binding and cap invariants, exercised via direct construction."""

    def setUp(self) -> None:
        self.collection = get_llm_extractor_collection_config(
            _FAKE_COLLECTION_NAME, config_module=fake_config
        )
        self.input_document_collection = _input_document_collection(
            state_code=StateCode.US_XX,
            name=_INPUT_DOCUMENT_COLLECTION_NAME,
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
            document_metadata_filter_query_template=_FILTER_QUERY,
            prompt_vars={},
            max_transient_retry_count=DEFAULT_MAX_TRANSIENT_RETRY_COUNT,
            total_pending_document_count_hard_cap=total_pending_document_count_hard_cap,
            single_job_document_count_batch_threshold=single_job_document_count_batch_threshold,
            extractor_collection=self.collection,
            model_config=model_config if model_config is not None else _model_config(),
            reference_data=self.reference_data,
            entity_group=entity_group,
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

    def test_entity_resolution_allows_thinking_model(self) -> None:
        # The thinking ban is scoped to first-order (entity_group is None); an ER
        # config with an entity group may bind a thinking model.
        config = self._config(
            model_config=_model_config(enables_thinking=True),
            entity_group=self.collection.entity_groups[0],
        )
        self.assertTrue(config.model_config.enables_thinking)
        self.assertIsNotNone(config.entity_group)
