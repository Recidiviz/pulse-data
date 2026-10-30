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
"""Tests for golden_eval_entity_resolution_helpers."""
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.eval.golden_eval_document import GoldenEvalDocument
from recidiviz.documents.extraction.eval.golden_eval_entity_resolution_helpers import (
    first_order_config_for,
    load_golden_eval_documents,
    normalize_golden_eval_documents,
    seed_golden_eval_document_contents,
)
from recidiviz.documents.extraction.eval.golden_eval_entity_resolution_tables import (
    GoldenEvalDocumentsBQTable,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
)
from recidiviz.source_tables.document_store_source_table_collection import (
    collect_document_store_source_tables_for_configs,
)
from recidiviz.source_tables.extraction_results_source_table_collection import (
    collect_golden_eval_documents_source_table_collection,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    fake_entity_resolution_extractor_config,
    fake_first_order_extractor_config,
    patch_fake_entity_resolution_model_config_name,
)

_SANDBOX_PREFIX = "my_prefix"

_DISH_DUTY_HTML_TEXT = "<p>The record is active.   Assigned to <b>dish duty</b>.</p>"
_DISH_DUTY_TEXT = "The record is active. Assigned to dish duty."
_NO_ASSIGNMENT_TEXT = "No assignment record here."

# sha256("US_XX|The record is active. Assigned to dish duty.")
_DISH_DUTY_DOCUMENT_CONTENTS_ID = (
    "8b7709c160501f6bcc857ae1d941a83b38ad666a189230f10bb0ec6db7f157dd"
)
# The composite the ER collection's generation query builds from the dish duty
# document alone.
_DISH_DUTY_COMPOSITE_TEXT = (
    "=== Source document — 2026-03-01 ===\n"
    "document_text: The record is active. Assigned to dish duty.\n"
    "\n"
    "[Entry 1]\n"
    "location: Kitchen"
)
# sha256("US_XX|" + _DISH_DUTY_COMPOSITE_TEXT)
_DISH_DUTY_COMPOSITE_ID = (
    "e733f1a308018b22048734524a01e331328ba9534f205d826cfcba093fe13139"
)
# sha256("US_XX|No assignment record here.")
_NO_ASSIGNMENT_DOCUMENT_CONTENTS_ID = (
    "db558650949f33a1d190ded38e6561003ad9739a543c92dd1223f184e2ad60d2"
)


def _document(*, golden_document_id: str, document_text: str) -> GoldenEvalDocument:
    """Returns a golden eval document holding |document_text|. Seeding reads only
    the id and text, so the other fields hold fixed placeholder values.
    """
    return GoldenEvalDocument(
        golden_document_id=golden_document_id,
        test_type=GoldenEvalTestType.UNIT,
        test_case="base_case",
        state_code=StateCode.US_XX,
        document_text=document_text,
        expected_values={},
    )


class FirstOrderConfigForTest(TestCase):
    """Tests for first_order_config_for."""

    def setUp(self) -> None:
        self.enterContext(patch_fake_entity_resolution_model_config_name())

    def test_returns_parent_of_entity_resolution_config(self) -> None:
        self.assertEqual(
            fake_first_order_extractor_config(),
            first_order_config_for(fake_entity_resolution_extractor_config("location")),
        )

    def test_raises_for_first_order_config(self) -> None:
        with self.assertRaisesRegex(ValueError, r"not an entity-resolution extractor"):
            first_order_config_for(fake_first_order_extractor_config())


class GoldenEvalEntityResolutionHelpersEmulatorTestCase(BigQueryEmulatorTestCase):
    """Shared sandbox setup for the helpers that write to BigQuery."""

    wipe_emulator_data_on_teardown = False

    def tearDown(self) -> None:
        self._clear_emulator_table_data()
        super().tearDown()

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        return [
            collect_golden_eval_documents_source_table_collection(
                extractor_collection_names=["FAKE_EXTRACTOR_COLLECTION"]
            ).as_sandbox_collection(_SANDBOX_PREFIX),
            *[
                collection.as_sandbox_collection(_SANDBOX_PREFIX)
                for collection in collect_document_store_source_tables_for_configs(
                    [fake_first_order_extractor_config().input_document_collection]
                )
            ],
        ]

    def setUp(self) -> None:
        super().setUp()
        self.config = fake_first_order_extractor_config()
        self.documents_address = GoldenEvalDocumentsBQTable.address(
            collection_name=self.config.extractor_collection.name,
            sandbox_prefix=_SANDBOX_PREFIX,
        )
        self.contents_address = (
            self.config.input_document_collection.document_contents_table_address(
                sandbox_dataset_prefix=_SANDBOX_PREFIX
            )
        )

    def _assert_documents_rows(self, expected_rows: list[dict[str, object]]) -> None:
        self.run_query_test(
            f"SELECT state_code, golden_document_id, document_text "
            f"FROM `{self.project_id}.{self.documents_address.to_str()}`",
            expected_rows,
        )

    def _assert_contents_rows(self, expected_rows: list[dict[str, object]]) -> None:
        self.run_query_test(
            f"SELECT document_contents_id, document_text, document_length_bytes "
            f"FROM `{self.project_id}.{self.contents_address.to_str()}`",
            expected_rows,
        )


class LoadGoldenEvalDocumentsTest(GoldenEvalEntityResolutionHelpersEmulatorTestCase):
    """Tests for load_golden_eval_documents."""

    def _load(self, documents: list[GoldenEvalDocument]) -> None:
        load_golden_eval_documents(
            bq_client=self.bq_client,
            sandbox_prefix=_SANDBOX_PREFIX,
            first_order_config=self.config,
            documents=documents,
        )

    def test_writes_one_row_per_document_verbatim(self) -> None:
        self._load(
            [
                _document(
                    golden_document_id="unit_1", document_text=_DISH_DUTY_HTML_TEXT
                ),
                _document(
                    golden_document_id="unit_2", document_text=_NO_ASSIGNMENT_TEXT
                ),
            ]
        )

        self._assert_documents_rows(
            [
                {
                    "state_code": "US_XX",
                    "golden_document_id": "unit_1",
                    "document_text": _DISH_DUTY_HTML_TEXT,
                },
                {
                    "state_code": "US_XX",
                    "golden_document_id": "unit_2",
                    "document_text": _NO_ASSIGNMENT_TEXT,
                },
            ]
        )

    def test_replaces_rows_from_a_prior_run(self) -> None:
        self._load(
            [_document(golden_document_id="unit_1", document_text=_DISH_DUTY_TEXT)]
        )
        self._load(
            [_document(golden_document_id="unit_2", document_text=_NO_ASSIGNMENT_TEXT)]
        )

        self._assert_documents_rows(
            [
                {
                    "state_code": "US_XX",
                    "golden_document_id": "unit_2",
                    "document_text": _NO_ASSIGNMENT_TEXT,
                },
            ]
        )


class NormalizeGoldenEvalDocumentsTest(
    GoldenEvalEntityResolutionHelpersEmulatorTestCase
):
    """Tests for normalize_golden_eval_documents."""

    def _load_documents_rows(self, rows: list[dict[str, str]]) -> None:
        self.load_rows_into_table(self.documents_address, rows)

    def _normalize(self) -> dict[str, str]:
        return normalize_golden_eval_documents(
            bq_client=self.bq_client,
            sandbox_prefix=_SANDBOX_PREFIX,
            first_order_config=self.config,
        )

    def test_writes_normalized_contents_and_returns_mapping(self) -> None:
        self._load_documents_rows(
            [
                {
                    "state_code": "US_XX",
                    "golden_document_id": "unit_1",
                    "document_text": _DISH_DUTY_HTML_TEXT,
                },
                {
                    "state_code": "US_XX",
                    "golden_document_id": "unit_2",
                    "document_text": _NO_ASSIGNMENT_TEXT,
                },
            ]
        )

        mapping = self._normalize()

        self.assertEqual(
            {
                "unit_1": _DISH_DUTY_DOCUMENT_CONTENTS_ID,
                "unit_2": _NO_ASSIGNMENT_DOCUMENT_CONTENTS_ID,
            },
            mapping,
        )
        self._assert_contents_rows(
            [
                {
                    "document_contents_id": _DISH_DUTY_DOCUMENT_CONTENTS_ID,
                    "document_text": _DISH_DUTY_TEXT,
                    "document_length_bytes": 44,
                },
                {
                    "document_contents_id": _NO_ASSIGNMENT_DOCUMENT_CONTENTS_ID,
                    "document_text": _NO_ASSIGNMENT_TEXT,
                    "document_length_bytes": 26,
                },
            ]
        )

    def test_texts_identical_after_normalization_share_one_row(self) -> None:
        self._load_documents_rows(
            [
                {
                    "state_code": "US_XX",
                    "golden_document_id": "unit_1",
                    "document_text": _DISH_DUTY_TEXT,
                },
                {
                    "state_code": "US_XX",
                    "golden_document_id": "unit_2",
                    "document_text": f"<p>{_DISH_DUTY_TEXT}</p>",
                },
            ]
        )

        mapping = self._normalize()

        self.assertEqual(
            {
                "unit_1": _DISH_DUTY_DOCUMENT_CONTENTS_ID,
                "unit_2": _DISH_DUTY_DOCUMENT_CONTENTS_ID,
            },
            mapping,
        )
        self._assert_contents_rows(
            [
                {
                    "document_contents_id": _DISH_DUTY_DOCUMENT_CONTENTS_ID,
                    "document_text": _DISH_DUTY_TEXT,
                    "document_length_bytes": 44,
                },
            ]
        )

    def test_raises_when_text_normalizes_to_empty(self) -> None:
        self._load_documents_rows(
            [
                {
                    "state_code": "US_XX",
                    "golden_document_id": "unit_1",
                    "document_text": "<br/>  ",
                },
            ]
        )

        with self.assertRaisesRegex(ValueError, r"\[unit_1\]"):
            self._normalize()


class SeedGoldenEvalDocumentContentsTest(
    GoldenEvalEntityResolutionHelpersEmulatorTestCase
):
    """Tests for seed_golden_eval_document_contents."""

    def test_loads_then_normalizes(self) -> None:
        mapping = seed_golden_eval_document_contents(
            bq_client=self.bq_client,
            sandbox_prefix=_SANDBOX_PREFIX,
            first_order_config=self.config,
            documents=[
                _document(
                    golden_document_id="unit_1", document_text=_DISH_DUTY_HTML_TEXT
                ),
                _document(
                    golden_document_id="unit_2", document_text=_NO_ASSIGNMENT_TEXT
                ),
            ],
        )

        self.assertEqual(
            {
                "unit_1": _DISH_DUTY_DOCUMENT_CONTENTS_ID,
                "unit_2": _NO_ASSIGNMENT_DOCUMENT_CONTENTS_ID,
            },
            mapping,
        )
        self._assert_documents_rows(
            [
                {
                    "state_code": "US_XX",
                    "golden_document_id": "unit_1",
                    "document_text": _DISH_DUTY_HTML_TEXT,
                },
                {
                    "state_code": "US_XX",
                    "golden_document_id": "unit_2",
                    "document_text": _NO_ASSIGNMENT_TEXT,
                },
            ]
        )
        self._assert_contents_rows(
            [
                {
                    "document_contents_id": _DISH_DUTY_DOCUMENT_CONTENTS_ID,
                    "document_text": _DISH_DUTY_TEXT,
                    "document_length_bytes": 44,
                },
                {
                    "document_contents_id": _NO_ASSIGNMENT_DOCUMENT_CONTENTS_ID,
                    "document_text": _NO_ASSIGNMENT_TEXT,
                    "document_length_bytes": 26,
                },
            ]
        )
