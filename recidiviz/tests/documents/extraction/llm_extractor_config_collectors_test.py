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
"""Tests for llm_extractor_config_collectors.py.

Covers loading and enumerating the resolved first-order extractor configs from
their `extractor.yaml` files: guarding every real config parses/resolves, and
exercising the fake config module's happy path.
"""

import re
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.config_defaults import (
    DEFAULT_FIRST_ORDER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD,
    DEFAULT_FIRST_ORDER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP,
    DEFAULT_MAX_TRANSIENT_RETRY_COUNT,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    collect_all_extractor_configs_by_state,
    collect_entity_resolution_extractor_configs,
    get_first_order_llm_extractor_config,
    get_states_with_extractor_configs,
    load_first_order_llm_extractor_configs,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
)
from recidiviz.tests.big_query.sqlglot_helpers import (
    check_query_selects_distinct_rows,
    check_query_selects_output_columns,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
    FAKE_ER_COLLECTION_NAMES,
    FAKE_LOCATION_ER_COLLECTION_NAME,
    FAKE_PAY_RATE_ER_COLLECTION_NAME,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.tests.documents.store.document_store_test_utils import (
    FAKE_INPUT_DOCUMENT_COLLECTION_NAME,
)
from recidiviz.tests.ingest import fixtures

_FAKE_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_OVERRIDE_MODEL_CONFIG_NAME = "ACME_LARGE_DETERMINISTIC"


class ParseAllExtractorConfigsTest(TestCase):
    """Guards real configs, and exercises the fake config's happy path."""

    def test_get_states_with_extractor_configs_not_empty(self) -> None:
        # Guards against the set being empty, which would make any test that
        # iterates over it (e.g. the reference-data load guard) pass vacuously.
        self.assertTrue(get_states_with_extractor_configs())

    def test_load_all_real_configs(self) -> None:
        # Raises if any real extractor — first-order or generated ER — fails to
        # parse, resolve, or build. Does not assert on any state's specifics — the
        # fake config covers structure.
        configs_by_state = collect_all_extractor_configs_by_state()
        self.assertTrue(configs_by_state)

        # Each extractor's document filter template must render into a query that
        # selects exactly the single document_contents_id column, DISTINCT
        # (document_contents_id is a hash of the document text and can be shared many
        # times across people, e.g. in the case of common boilerplate notes).
        for state_configs in configs_by_state.values():
            for config in state_configs.values():
                with self.subTest(
                    collection=config.extractor_collection.name,
                    state=config.state_code.value,
                ):
                    metadata_address = (
                        config.input_document_collection.metadata_table_address(
                            sandbox_dataset_prefix=None
                        ).to_project_specific_address("test-project")
                    )
                    rendered_query = (
                        config.document_filter.build_document_metadata_filter_query(
                            input_document_collection_metadata_address=metadata_address,
                        )
                    )
                    check_query_selects_output_columns(
                        rendered_query, {DOCUMENT_CONTENTS_ID_COLUMN_NAME}
                    )
                    check_query_selects_distinct_rows(rendered_query)

    def test_all_real_first_order_extractors_declare_golden_eval(self) -> None:
        # Every first-order extractor must ship a golden eval set so extraction
        # quality is measurable for that (collection, state). Only the
        # auto-generated entity-resolution extractors are exempt, and this loader
        # returns first-order extractors only.
        configs_by_state = load_first_order_llm_extractor_configs()
        self.assertTrue(configs_by_state)
        for state_code, state_configs in configs_by_state.items():
            for collection_name, config in state_configs.items():
                with self.subTest(collection=collection_name, state=state_code.value):
                    self.assertIsNotNone(config.golden_eval)

    def test_all_real_extractors_compute_version_ids(self) -> None:
        # Every real extractor must compute all four version IDs as 64-char hex
        # digests, and extractor_id (the llm_extractors primary key) must be
        # unique across all of them.
        configs_by_state = collect_all_extractor_configs_by_state()
        self.assertTrue(configs_by_state)

        seen_extractor_ids: set[str] = set()
        for state_configs in configs_by_state.values():
            for config in state_configs.values():
                with self.subTest(
                    collection=config.extractor_collection.name,
                    state=config.state_code.value,
                ):
                    self.assertNotIn(config.extractor_id, seen_extractor_ids)
                    seen_extractor_ids.add(config.extractor_id)
                    for version_id in (
                        config.model_config.model_config_version_id,
                        config.extractor_collection.collection_version_id,
                        config.extractor_version_id,
                        config.document_filter_id,
                    ):
                        self.assertRegex(version_id, r"^[0-9a-f]{64}$")

    def test_all_real_extractors_build_instructions_prompt(self) -> None:
        # instructions_prompt is compiled at construction, so a template/prompt-var
        # or generator failure would already surface above — this makes the guard
        # explicit: every real extractor must produce a non-empty compiled prompt.
        configs_by_state = collect_all_extractor_configs_by_state()
        self.assertTrue(configs_by_state)
        for state_configs in configs_by_state.values():
            for config in state_configs.values():
                with self.subTest(
                    collection=config.extractor_collection.name,
                    state=config.state_code.value,
                ):
                    self.assertTrue(config.instructions_prompt.strip())

    def _get_golden_instructions_prompt(self, fixture_file_name: str) -> str:
        return Path(
            fixtures.as_filepath(f"instructions_prompt/{fixture_file_name}")
        ).read_text(encoding="utf-8")

    def test_playground_employment_instructions_prompt_matches_golden(self) -> None:
        # End-to-end golden for a real config: template + output instructions +
        # reference data + prompt vars assembled into the final prompt. Exercises
        # the stack's config features (required_when_*/type_notes) that the US_OZ
        # employment collection declares. Regenerate the fixture from
        # `.instructions_prompt` if the template or a generator intentionally changes.
        config = get_first_order_llm_extractor_config(
            StateCode.US_OZ, "PLAYGROUND_EMPLOYMENT_INFO"
        )
        expected = self._get_golden_instructions_prompt(
            "playground_employment_info_us_oz.txt"
        )
        self.assertEqual(expected, config.instructions_prompt)

    def test_playground_employment_employer_er_instructions_prompt_matches_golden(
        self,
    ) -> None:
        # End-to-end golden for the entity-resolution counterpart of the config
        # above: the clustering template specialized by the employer entity
        # descriptor, the ER output-format instructions, the reference-data preamble,
        # and the flat known-organizations dictionary assembled into the final
        # prompt. Regenerate from `.instructions_prompt` if the template or a
        # generator intentionally changes.
        er_config = collect_all_extractor_configs_by_state()[StateCode.US_OZ][
            "PLAYGROUND_EMPLOYMENT_INFO_EMPLOYER_ENTITY_RESOLUTION"
        ]

        expected = self._get_golden_instructions_prompt(
            "playground_employment_info_employer_entity_resolution_us_oz.txt"
        )
        self.assertEqual(expected, er_config.instructions_prompt)

    def test_fake_extractor_resolves(self) -> None:
        configs_by_state = load_first_order_llm_extractor_configs(
            config_module=fake_config
        )
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
            FAKE_INPUT_DOCUMENT_COLLECTION_NAME, config.input_document_collection.name
        )
        self.assertEqual(StateCode.US_XX, config.input_document_collection.state_code)
        self.assertEqual(
            "fake_input_notes",
            config.input_document_collection.metadata_table_address(
                sandbox_dataset_prefix=None
            ).table_id,
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
        config = get_first_order_llm_extractor_config(
            StateCode.US_XX, _FAKE_COLLECTION_NAME, config_module=fake_config
        )
        self.assertEqual(_FAKE_COLLECTION_NAME, config.extractor_collection.name)
        self.assertEqual(StateCode.US_XX, config.state_code)

    def test_get_unknown_extractor_config_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape("No extractor config for collection [NOT_A_COLLECTION]"),
        ):
            get_first_order_llm_extractor_config(
                StateCode.US_XX, "NOT_A_COLLECTION", config_module=fake_config
            )


class CollectEntityResolutionExtractorConfigsTest(TestCase):
    """Tests for collect_entity_resolution_extractor_configs."""

    def setUp(self) -> None:
        self.enterContext(patch_fake_entity_resolution_model_config_name())

    def test_pair_per_declared_entity_group(self) -> None:
        first_order_config = get_first_order_llm_extractor_config(
            StateCode.US_XX, _FAKE_COLLECTION_NAME, config_module=fake_config
        )

        pairs = collect_entity_resolution_extractor_configs(
            first_order_configs=[first_order_config], config_module=fake_config
        )

        # One pair per declared entity group, in declaration order (the fake
        # collection declares `location`, then `assignment`, then `pay_rate`); the ER
        # collection name encodes the group.
        self.assertEqual(
            [
                FAKE_LOCATION_ER_COLLECTION_NAME,
                FAKE_ASSIGNMENT_ER_COLLECTION_NAME,
                FAKE_PAY_RATE_ER_COLLECTION_NAME,
            ],
            [
                entity_resolution_config.extractor_collection.name
                for _, entity_resolution_config in pairs
            ],
        )
        for parent_config, entity_resolution_config in pairs:
            # The pair's first element is the parent it was generated from; the
            # second is an ER config (entity_group set).
            self.assertIs(first_order_config, parent_config)
            self.assertIsNotNone(entity_resolution_config.entity_group)

    def test_no_declared_entity_groups_returns_empty(self) -> None:
        first_order_config = get_first_order_llm_extractor_config(
            StateCode.US_XX, _FAKE_COLLECTION_NAME, config_module=fake_config
        )
        without_entity_groups = attr.evolve(
            first_order_config,
            extractor_collection=attr.evolve(
                first_order_config.extractor_collection, entity_groups=[]
            ),
        )
        self.assertEqual(
            [],
            collect_entity_resolution_extractor_configs(
                first_order_configs=[without_entity_groups], config_module=fake_config
            ),
        )


class CollectAllExtractorConfigsByStateTest(TestCase):
    """Tests for collect_all_extractor_configs_by_state."""

    def setUp(self) -> None:
        self.enterContext(patch_fake_entity_resolution_model_config_name())

    def test_includes_first_order_and_entity_resolution(self) -> None:
        configs_by_state = collect_all_extractor_configs_by_state(
            config_module=fake_config
        )
        self.assertEqual([StateCode.US_XX], list(configs_by_state))

        configs = configs_by_state[StateCode.US_XX]
        self.assertEqual(
            {_FAKE_COLLECTION_NAME, *FAKE_ER_COLLECTION_NAMES},
            set(configs),
        )
        # The first-order config carries no entity_group; the ER configs do.
        self.assertIsNone(configs[_FAKE_COLLECTION_NAME].entity_group)
        self.assertIsNotNone(configs[FAKE_LOCATION_ER_COLLECTION_NAME].entity_group)

    def test_does_not_mutate_cached_first_order_configs(self) -> None:
        # load_first_order_llm_extractor_configs is @cache'd, so aliasing its result
        # would let collect_all_... splice ER configs into the shared first-order
        # map. Guards the defensive copy.
        first_order_configs = load_first_order_llm_extractor_configs(
            config_module=fake_config
        )
        collection_names_before = {
            state_code: set(configs)
            for state_code, configs in first_order_configs.items()
        }

        collect_all_extractor_configs_by_state(config_module=fake_config)

        collection_names_after = {
            state_code: set(configs)
            for state_code, configs in load_first_order_llm_extractor_configs(
                config_module=fake_config
            ).items()
        }
        self.assertEqual(collection_names_before, collection_names_after)
        self.assertNotIn(
            FAKE_LOCATION_ER_COLLECTION_NAME,
            first_order_configs[StateCode.US_XX],
        )

    def test_naming_collision_raises(self) -> None:
        # A generated ER collection name that collides with an existing config's
        # name (first-order or another ER config) must raise rather than silently
        # overwrite the earlier entry.
        first_order_config = get_first_order_llm_extractor_config(
            StateCode.US_XX, _FAKE_COLLECTION_NAME, config_module=fake_config
        )
        colliding_entity_resolution_config = attr.evolve(
            first_order_config,
            extractor_collection=attr.evolve(
                first_order_config.extractor_collection, name=_FAKE_COLLECTION_NAME
            ),
        )
        with patch(
            "recidiviz.documents.extraction.llm_extractor_config_collectors."
            "collect_entity_resolution_extractor_configs",
            return_value=[(first_order_config, colliding_entity_resolution_config)],
        ):
            with self.assertRaisesRegex(
                ValueError,
                re.escape(
                    f"Generated entity-resolution extractor collection name "
                    f"[{_FAKE_COLLECTION_NAME}] collides with an existing "
                    f"extractor config for state [US_XX]."
                ),
            ):
                collect_all_extractor_configs_by_state(config_module=fake_config)
