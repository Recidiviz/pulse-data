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
import datetime
from typing import Any
from unittest import TestCase

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.eval.golden_eval_document import (
    GoldenEvalDocument,
    GoldenEvalEntityResolutionDocument,
)
from recidiviz.documents.extraction.eval.golden_eval_entity_resolution_helpers import (
    build_pre_resolution_rows,
    first_order_config_for,
    golden_eval_document_store_sandbox_context,
    load_golden_eval_documents,
    normalize_golden_eval_documents,
    seed_entity_resolution_golden_eval_tables,
    seed_golden_eval_document_contents,
    seed_golden_eval_pre_resolution_tables,
)
from recidiviz.documents.extraction.eval.golden_eval_entity_resolution_tables import (
    GoldenEvalCompositeDocumentsBQTable,
    GoldenEvalDocumentsBQTable,
    GoldenEvalPreResolutionBQTable,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
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
    build_fake_composite_generation_output_row,
    build_fake_entry_source_map_entry,
    fake_entity_resolution_extractor_config,
    fake_first_order_extractor_config,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.tools.documents.run_sandbox_golden_eval import (
    entry_source_map_sandbox_source_table_collection,
    pre_resolution_sandbox_source_table_collection,
)
from recidiviz.utils.types import assert_type

_SANDBOX_PREFIX = "my_prefix"
_RUN_DATETIME_UTC = datetime.datetime(2026, 10, 20, 12, tzinfo=datetime.UTC)

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


def _er_document(
    *,
    golden_document_id: str,
    test_group_id: int,
    document_date: datetime.date,
    expected_values: dict[str, Any],
) -> GoldenEvalEntityResolutionDocument:
    """Returns an entity-resolution golden eval document. Row building reads only
    the id, test group, date, and expected values, so the other fields hold fixed
    placeholder values.
    """
    return GoldenEvalEntityResolutionDocument(
        golden_document_id=golden_document_id,
        test_type=GoldenEvalTestType.UNIT,
        test_case="base_case",
        state_code=StateCode.US_XX,
        document_text="Placeholder text.",
        expected_values=expected_values,
        test_group_id=test_group_id,
        document_date=document_date,
        expected_resolved_values={},
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


class BuildPreResolutionRowsTest(TestCase):
    """Tests for build_pre_resolution_rows."""

    def setUp(self) -> None:
        self.enterContext(patch_fake_entity_resolution_model_config_name())
        self.config = fake_first_order_extractor_config()
        self.assignments = assert_type(
            self.config.extractor_collection.output_schema.get_field("assignments"),
            ArrayOfStructLLMRequestOutputSchemaField,
        )
        self.unit_1 = _er_document(
            golden_document_id="unit_1",
            test_group_id=1,
            document_date=datetime.date(2026, 3, 1),
            expected_values={
                "is_relevant": True,
                "primary_status": "active",
                "status_note": "Currently active.",
                "location": "Kitchen",
                "assignments": [
                    {
                        "assignment_name": "Dish duty",
                        "assignment_type": "internal",
                        "rate_amount": 12.5,
                        "rate_period": "hourly",
                    },
                    {
                        "assignment_name": "Laundry",
                        "assignment_type": "internal",
                        "rate_amount": None,
                        "rate_period": None,
                    },
                ],
            },
        )
        self.unit_2_irrelevant = _er_document(
            golden_document_id="unit_2",
            test_group_id=1,
            document_date=datetime.date(2026, 3, 15),
            expected_values={
                "is_relevant": False,
                "primary_status": None,
                "status_note": None,
                "location": None,
                "assignments": None,
            },
        )

    def _rows(
        self,
        *,
        array_field: ArrayOfStructLLMRequestOutputSchemaField | None,
        documents: list[GoldenEvalEntityResolutionDocument],
        first_order_values_by_golden_document_id: dict[str, dict[str, Any]],
        document_contents_id_by_golden_document_id: dict[str, str],
    ) -> list[dict[str, Any]]:
        """Builds rows for |documents| against the fake first-order config at the
        fixed run datetime.
        """
        return build_pre_resolution_rows(
            first_order_config=self.config,
            array_field=array_field,
            documents=documents,
            first_order_values_by_golden_document_id=first_order_values_by_golden_document_id,
            document_contents_id_by_golden_document_id=document_contents_id_by_golden_document_id,
            validation_datetime_utc=_RUN_DATETIME_UTC,
        )

    def test_doc_level_rows_one_per_relevant_document(self) -> None:
        rows = self._rows(
            array_field=None,
            documents=[self.unit_1, self.unit_2_irrelevant],
            first_order_values_by_golden_document_id={
                "unit_1": self.unit_1.expected_values,
                "unit_2": self.unit_2_irrelevant.expected_values,
            },
            document_contents_id_by_golden_document_id={
                "unit_1": "abc",
                "unit_2": "def",
            },
        )

        self.assertEqual(
            [
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "document_contents_id": "abc",
                    "document_update_datetime": datetime.datetime(
                        2026, 3, 1, tzinfo=datetime.UTC
                    ),
                    "validation_datetime_utc": _RUN_DATETIME_UTC,
                    "extractor_version_id": self.config.extractor_version_id,
                    "primary_status": "active",
                    "status_note": "Currently active.",
                    "location": "Kitchen",
                    "confidence_levels": None,
                    "null_reasons": None,
                    "adversarial_interpretations": None,
                    "citations": None,
                }
            ],
            rows,
        )

    def test_doc_level_row_written_even_when_every_entity_field_is_null(
        self,
    ) -> None:
        document = _er_document(
            golden_document_id="unit_1",
            test_group_id=1,
            document_date=datetime.date(2026, 3, 1),
            expected_values={
                "is_relevant": True,
                "primary_status": "active",
                "status_note": None,
                "location": None,
                "assignments": None,
            },
        )

        rows = self._rows(
            array_field=None,
            documents=[document],
            first_order_values_by_golden_document_id={
                "unit_1": document.expected_values
            },
            document_contents_id_by_golden_document_id={"unit_1": "abc"},
        )

        self.assertEqual(
            [
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "document_contents_id": "abc",
                    "document_update_datetime": datetime.datetime(
                        2026, 3, 1, tzinfo=datetime.UTC
                    ),
                    "validation_datetime_utc": _RUN_DATETIME_UTC,
                    "extractor_version_id": self.config.extractor_version_id,
                    "primary_status": "active",
                    "status_note": None,
                    "location": None,
                    "confidence_levels": None,
                    "null_reasons": None,
                    "adversarial_interpretations": None,
                    "citations": None,
                }
            ],
            rows,
        )

    def test_array_level_rows_one_per_element(self) -> None:
        unit_2 = _er_document(
            golden_document_id="unit_2",
            test_group_id=1,
            document_date=datetime.date(2026, 3, 15),
            expected_values={
                "is_relevant": True,
                "primary_status": "inactive",
                "status_note": None,
                "location": None,
                "assignments": None,
            },
        )

        rows = self._rows(
            array_field=self.assignments,
            documents=[self.unit_1, unit_2],
            first_order_values_by_golden_document_id={
                "unit_1": self.unit_1.expected_values,
                "unit_2": unit_2.expected_values,
            },
            document_contents_id_by_golden_document_id={
                "unit_1": "abc",
                "unit_2": "def",
            },
        )

        self.assertEqual(
            [
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "document_contents_id": "abc",
                    "document_update_datetime": datetime.datetime(
                        2026, 3, 1, tzinfo=datetime.UTC
                    ),
                    "validation_datetime_utc": _RUN_DATETIME_UTC,
                    "extractor_version_id": self.config.extractor_version_id,
                    "source_array_index": 0,
                    "assignment_name": "Dish duty",
                    "assignment_type": "internal",
                    "rate_amount": 12.5,
                    "rate_period": "hourly",
                    "confidence_levels": None,
                    "null_reasons": None,
                    "adversarial_interpretations": None,
                    "citations": None,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "document_contents_id": "abc",
                    "document_update_datetime": datetime.datetime(
                        2026, 3, 1, tzinfo=datetime.UTC
                    ),
                    "validation_datetime_utc": _RUN_DATETIME_UTC,
                    "extractor_version_id": self.config.extractor_version_id,
                    "source_array_index": 1,
                    "assignment_name": "Laundry",
                    "assignment_type": "internal",
                    "rate_amount": None,
                    "rate_period": None,
                    "confidence_levels": None,
                    "null_reasons": None,
                    "adversarial_interpretations": None,
                    "citations": None,
                },
            ],
            rows,
        )

    def test_array_element_missing_sub_field_is_null(self) -> None:
        document = _er_document(
            golden_document_id="unit_1",
            test_group_id=1,
            document_date=datetime.date(2026, 3, 1),
            expected_values={
                "is_relevant": True,
                "primary_status": "active",
                "status_note": None,
                "location": None,
                "assignments": [{"assignment_name": "Dish duty"}],
            },
        )

        rows = self._rows(
            array_field=self.assignments,
            documents=[document],
            first_order_values_by_golden_document_id={
                "unit_1": document.expected_values
            },
            document_contents_id_by_golden_document_id={"unit_1": "abc"},
        )

        self.assertEqual(
            [
                {
                    "state_code": "US_XX",
                    "person_id": 1,
                    "document_contents_id": "abc",
                    "document_update_datetime": datetime.datetime(
                        2026, 3, 1, tzinfo=datetime.UTC
                    ),
                    "validation_datetime_utc": _RUN_DATETIME_UTC,
                    "extractor_version_id": self.config.extractor_version_id,
                    "source_array_index": 0,
                    "assignment_name": "Dish duty",
                    "assignment_type": None,
                    "rate_amount": None,
                    "rate_period": None,
                    "confidence_levels": None,
                    "null_reasons": None,
                    "adversarial_interpretations": None,
                    "citations": None,
                }
            ],
            rows,
        )

    def test_irrelevant_document_has_no_rows_at_any_level(self) -> None:
        document = _er_document(
            golden_document_id="unit_2",
            test_group_id=1,
            document_date=datetime.date(2026, 3, 15),
            expected_values={
                "is_relevant": False,
                "primary_status": "active",
                "status_note": None,
                "location": "Kitchen",
                "assignments": [{"assignment_name": "Dish duty"}],
            },
        )

        values = {"unit_2": document.expected_values}
        contents_ids = {"unit_2": "def"}

        self.assertEqual(
            [],
            self._rows(
                array_field=None,
                documents=[document],
                first_order_values_by_golden_document_id=values,
                document_contents_id_by_golden_document_id=contents_ids,
            ),
        )
        self.assertEqual(
            [],
            self._rows(
                array_field=self.assignments,
                documents=[document],
                first_order_values_by_golden_document_id=values,
                document_contents_id_by_golden_document_id=contents_ids,
            ),
        )

    def test_values_without_is_relevant_field_are_relevant(self) -> None:
        # A synthesized entity-resolution schema declares no is_relevant field, so
        # every document is relevant.
        er_config = fake_entity_resolution_extractor_config("location")
        document = _er_document(
            golden_document_id="unit_1",
            test_group_id=1,
            document_date=datetime.date(2026, 3, 1),
            expected_values={},
        )

        rows = build_pre_resolution_rows(
            first_order_config=er_config,
            array_field=None,
            documents=[document],
            first_order_values_by_golden_document_id={"unit_1": {}},
            document_contents_id_by_golden_document_id={"unit_1": "abc"},
            validation_datetime_utc=_RUN_DATETIME_UTC,
        )

        self.assertEqual(1, len(rows))

    def test_extra_keys_in_values_are_ignored(self) -> None:
        rows = self._rows(
            array_field=None,
            documents=[self.unit_1],
            first_order_values_by_golden_document_id={
                "unit_1": {**self.unit_1.expected_values, "not_a_field": "x"}
            },
            document_contents_id_by_golden_document_id={"unit_1": "abc"},
        )

        self.assertNotIn("not_a_field", rows[0])

    def test_document_without_contents_id_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"\[unit_1\] has no document_contents_id"
        ):
            self._rows(
                array_field=None,
                documents=[self.unit_1],
                first_order_values_by_golden_document_id={
                    "unit_1": self.unit_1.expected_values
                },
                document_contents_id_by_golden_document_id={},
            )

    def test_document_without_values_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"\[unit_1\] has no first-order values"
        ):
            self._rows(
                array_field=None,
                documents=[self.unit_1],
                first_order_values_by_golden_document_id={},
                document_contents_id_by_golden_document_id={"unit_1": "abc"},
            )


class GoldenEvalEntityResolutionHelpersEmulatorTestCase(BigQueryEmulatorTestCase):
    """Shared sandbox setup for the helpers that write to BigQuery."""

    wipe_emulator_data_on_teardown = False

    def tearDown(self) -> None:
        self.bq_client.delete_table(self.composites_address, not_found_ok=True)
        self._clear_emulator_table_data()
        super().tearDown()

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        with patch_fake_entity_resolution_model_config_name():
            er_config = fake_entity_resolution_extractor_config("location")
            er_collections = [
                pre_resolution_sandbox_source_table_collection(
                    config=er_config,
                    sandbox_prefix=_SANDBOX_PREFIX,
                    table_expiration_ms=60 * 60 * 1000,
                ),
                entry_source_map_sandbox_source_table_collection(
                    config=er_config,
                    sandbox_prefix=_SANDBOX_PREFIX,
                    table_expiration_ms=60 * 60 * 1000,
                ),
            ]
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
            *er_collections,
        ]

    def setUp(self) -> None:
        super().setUp()
        self.enterContext(patch_fake_entity_resolution_model_config_name())
        self.config = fake_first_order_extractor_config()
        self.er_config = fake_entity_resolution_extractor_config("location")
        er_collection = assert_type(
            self.er_config.input_document_collection,
            EntityResolutionDocumentCollectionConfig,
        )
        self.composites_address = GoldenEvalCompositeDocumentsBQTable.address(
            er_collection=er_collection, sandbox_prefix=_SANDBOX_PREFIX
        )
        # Created here rather than seeded at emulator boot: the emulator cannot
        # load the nested entry_source_map column from its schema JSON.
        self.create_mock_table(
            self.composites_address,
            schema=GoldenEvalCompositeDocumentsBQTable.schema(
                er_collection=er_collection
            ),
        )
        self.assignments = assert_type(
            self.config.extractor_collection.output_schema.get_field("assignments"),
            ArrayOfStructLLMRequestOutputSchemaField,
        )
        self.doc_level_address = GoldenEvalPreResolutionBQTable.address(
            first_order_config=self.config,
            array_field=None,
            sandbox_prefix=_SANDBOX_PREFIX,
        )
        self.assignments_address = GoldenEvalPreResolutionBQTable.address(
            first_order_config=self.config,
            array_field=self.assignments,
            sandbox_prefix=_SANDBOX_PREFIX,
        )
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

    def test_replaces_contents_rows_from_a_prior_run(self) -> None:
        seed_golden_eval_document_contents(
            bq_client=self.bq_client,
            sandbox_prefix=_SANDBOX_PREFIX,
            first_order_config=self.config,
            documents=[
                _document(
                    golden_document_id="unit_1", document_text=_DISH_DUTY_HTML_TEXT
                )
            ],
        )
        seed_golden_eval_document_contents(
            bq_client=self.bq_client,
            sandbox_prefix=_SANDBOX_PREFIX,
            first_order_config=self.config,
            documents=[
                _document(
                    golden_document_id="unit_2", document_text=_NO_ASSIGNMENT_TEXT
                )
            ],
        )

        self._assert_contents_rows(
            [
                {
                    "document_contents_id": _NO_ASSIGNMENT_DOCUMENT_CONTENTS_ID,
                    "document_text": _NO_ASSIGNMENT_TEXT,
                    "document_length_bytes": 26,
                }
            ]
        )


class SeedGoldenEvalPreResolutionTablesTest(
    GoldenEvalEntityResolutionHelpersEmulatorTestCase
):
    """Tests for seed_golden_eval_pre_resolution_tables."""

    def _seed(self, documents: list[GoldenEvalEntityResolutionDocument]) -> None:
        seed_golden_eval_pre_resolution_tables(
            bq_client=self.bq_client,
            sandbox_prefix=_SANDBOX_PREFIX,
            first_order_config=self.config,
            documents=documents,
            document_contents_id_by_golden_document_id={
                "unit_1": "abc",
                "unit_2": "def",
            },
            validation_datetime_utc=_RUN_DATETIME_UTC,
        )

    def test_seeds_a_table_per_level(self) -> None:
        self._seed(
            [
                _er_document(
                    golden_document_id="unit_1",
                    test_group_id=1,
                    document_date=datetime.date(2026, 3, 1),
                    expected_values={
                        "is_relevant": True,
                        "primary_status": "active",
                        "status_note": "Currently active.",
                        "location": "Kitchen",
                        "assignments": [
                            {
                                "assignment_name": "Dish duty",
                                "assignment_type": "internal",
                                "rate_amount": 12.5,
                                "rate_period": "hourly",
                            },
                            {
                                "assignment_name": "Laundry",
                                "assignment_type": "internal",
                                "rate_amount": None,
                                "rate_period": None,
                            },
                        ],
                    },
                ),
                _er_document(
                    golden_document_id="unit_2",
                    test_group_id=1,
                    document_date=datetime.date(2026, 3, 15),
                    expected_values={
                        "is_relevant": False,
                        "primary_status": None,
                        "status_note": None,
                        "location": None,
                        "assignments": None,
                    },
                ),
            ]
        )

        self.run_query_test(
            f"SELECT person_id, document_contents_id, document_update_datetime, "
            f"location, citations "
            f"FROM `{self.project_id}.{self.doc_level_address.to_str()}`",
            [
                {
                    "person_id": 1,
                    "document_contents_id": "abc",
                    "document_update_datetime": datetime.datetime(
                        2026, 3, 1, tzinfo=datetime.UTC
                    ),
                    "location": "Kitchen",
                    "citations": None,
                }
            ],
        )
        self.run_query_test(
            f"SELECT person_id, source_array_index, assignment_name, rate_amount "
            f"FROM `{self.project_id}.{self.assignments_address.to_str()}`",
            [
                {
                    "person_id": 1,
                    "source_array_index": 0,
                    "assignment_name": "Dish duty",
                    "rate_amount": 12.5,
                },
                {
                    "person_id": 1,
                    "source_array_index": 1,
                    "assignment_name": "Laundry",
                    "rate_amount": None,
                },
            ],
        )

    def test_replaces_rows_from_a_prior_run(self) -> None:
        self._seed(
            [
                _er_document(
                    golden_document_id="unit_1",
                    test_group_id=1,
                    document_date=datetime.date(2026, 3, 1),
                    expected_values={
                        "is_relevant": True,
                        "primary_status": "active",
                        "status_note": None,
                        "location": "Kitchen",
                        "assignments": None,
                    },
                )
            ]
        )
        self._seed(
            [
                _er_document(
                    golden_document_id="unit_2",
                    test_group_id=1,
                    document_date=datetime.date(2026, 3, 15),
                    expected_values={
                        "is_relevant": True,
                        "primary_status": "active",
                        "status_note": None,
                        "location": "Yard",
                        "assignments": None,
                    },
                )
            ]
        )

        self.run_query_test(
            f"SELECT person_id, document_contents_id, location "
            f"FROM `{self.project_id}.{self.doc_level_address.to_str()}`",
            [{"person_id": 1, "document_contents_id": "def", "location": "Yard"}],
        )


class GoldenEvalDocumentStoreSandboxContextTest(TestCase):
    """Tests for golden_eval_document_store_sandbox_context."""

    def setUp(self) -> None:
        self.enterContext(patch_fake_entity_resolution_model_config_name())

    def test_scopes_every_read_and_write_to_the_sandbox(self) -> None:
        er_config = fake_entity_resolution_extractor_config("location")
        first_order_document_collection = (
            fake_first_order_extractor_config().input_document_collection
        )

        context = golden_eval_document_store_sandbox_context(
            config=er_config, sandbox_prefix=_SANDBOX_PREFIX
        )

        self.assertEqual(
            "my_prefix",
            context.source_read_prefix_for_document_collection(
                first_order_document_collection.name
            ),
        )
        self.assertEqual(
            "my_prefix",
            context.read_prefix_for_extractor_collection("FAKE_EXTRACTOR_COLLECTION"),
        )
        self.assertEqual(
            "my_prefix",
            context.output_prefix_for_writing(er_config.input_document_collection.name),
        )


class SeedEntityResolutionGoldenEvalTablesTest(
    GoldenEvalEntityResolutionHelpersEmulatorTestCase
):
    """Tests for seed_entity_resolution_golden_eval_tables."""

    def test_seeds_contents_pre_resolution_composites_and_map(self) -> None:
        seed_entity_resolution_golden_eval_tables(
            bq_client=self.bq_client,
            sandbox_prefix=_SANDBOX_PREFIX,
            config=self.er_config,
            documents=[
                attr.evolve(
                    _er_document(
                        golden_document_id="unit_1",
                        test_group_id=1,
                        document_date=datetime.date(2026, 3, 1),
                        expected_values={
                            "is_relevant": True,
                            "primary_status": "active",
                            "status_note": "Currently active.",
                            "location": "Kitchen",
                            "assignments": None,
                        },
                    ),
                    document_text=_DISH_DUTY_HTML_TEXT,
                ),
                attr.evolve(
                    _er_document(
                        golden_document_id="unit_2",
                        test_group_id=1,
                        document_date=datetime.date(2026, 3, 15),
                        expected_values={
                            "is_relevant": False,
                            "primary_status": None,
                            "status_note": None,
                            "location": None,
                            "assignments": None,
                        },
                    ),
                    document_text=_NO_ASSIGNMENT_TEXT,
                ),
            ],
            validation_datetime_utc=_RUN_DATETIME_UTC,
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
        # unit_2 is not relevant, so only unit_1 has a __pre_resolution row.
        self.run_query_test(
            f"SELECT person_id, document_contents_id, location "
            f"FROM `{self.project_id}.{self.doc_level_address.to_str()}`",
            [
                {
                    "person_id": 1,
                    "document_contents_id": _DISH_DUTY_DOCUMENT_CONTENTS_ID,
                    "location": "Kitchen",
                }
            ],
        )
        self.run_query_test(
            f"SELECT person_id, document_contents_id, document_text "
            f"FROM `{self.project_id}.{self.composites_address.to_str()}`",
            [
                {
                    "person_id": 1,
                    "document_contents_id": _DISH_DUTY_COMPOSITE_ID,
                    "document_text": _DISH_DUTY_COMPOSITE_TEXT,
                }
            ],
        )
        map_address = assert_type(
            self.er_config.input_document_collection,
            EntityResolutionDocumentCollectionConfig,
        ).entry_source_map_table_address(sandbox_dataset_prefix=_SANDBOX_PREFIX)
        self.run_query_test(
            f"SELECT document_contents_id, entry_num "
            f"FROM `{self.project_id}.{map_address.to_str()}`",
            [{"document_contents_id": _DISH_DUTY_COMPOSITE_ID, "entry_num": 1}],
        )


def _entry(
    *,
    entry_num: int,
    source_document_contents_id: str,
    source_document_update_datetime: datetime.datetime,
) -> dict[str, Any]:
    """Returns one top-level (non-array) entry of a composite's entry_source_map."""
    return build_fake_entry_source_map_entry(
        entry_num=entry_num,
        source_document_contents_id=source_document_contents_id,
        source_document_update_datetime=source_document_update_datetime,
        source_array_index=None,
    )


def _composite_row(
    *, test_group_id: int, document_contents_id: str, entries: list[dict[str, Any]]
) -> dict[str, Any]:
    """Returns one composites table row for |test_group_id|, dated by its last
    entry.
    """
    return build_fake_composite_generation_output_row(
        root_entity_id_column="person_id",
        root_entity_id=test_group_id,
        document_contents_id=document_contents_id,
        document_update_datetime=datetime.datetime.fromisoformat(
            entries[-1]["source_document_update_datetime"]
        ),
        entries=entries,
    )
