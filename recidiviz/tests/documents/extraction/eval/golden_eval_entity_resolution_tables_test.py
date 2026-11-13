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
"""Tests for golden_eval_entity_resolution_tables."""
import datetime
from unittest import TestCase

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.eval.golden_eval_entity_resolution_tables import (
    GoldenEvalCompositeDocumentsBQTable,
    GoldenEvalDocumentsBQTable,
    GoldenEvalPreResolutionBQTable,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
)
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    fake_entity_resolution_document_collection_config,
    fake_first_order_extractor_config,
    patch_fake_entity_resolution_model_config_name,
)
from recidiviz.utils.types import assert_type


class GoldenEvalDocumentsBQTableTest(TestCase):
    """Tests for GoldenEvalDocumentsBQTable."""

    def test_address_is_sandbox_prefixed(self) -> None:
        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_prefix_document_extraction_golden_eval_results",
                table_id="fake_extractor_collection__documents",
            ),
            GoldenEvalDocumentsBQTable.address(
                collection_name="FAKE_EXTRACTOR_COLLECTION",
                sandbox_prefix="my_prefix",
            ),
        )

    def test_unprefixed_address(self) -> None:
        self.assertEqual(
            BigQueryAddress(
                dataset_id="document_extraction_golden_eval_results",
                table_id="fake_extractor_collection__documents",
            ),
            GoldenEvalDocumentsBQTable.address(
                collection_name="FAKE_EXTRACTOR_COLLECTION", sandbox_prefix=None
            ),
        )

    def test_to_row_matches_schema(self) -> None:
        row = GoldenEvalDocumentsBQTable.to_row(
            state_code=StateCode.US_XX,
            golden_document_id="unit_1",
            document_text="<p>Assigned to dish duty.</p>",
        )

        self.assertEqual(
            {
                "state_code": "US_XX",
                "golden_document_id": "unit_1",
                "document_text": "<p>Assigned to dish duty.</p>",
            },
            row,
        )
        self.assertEqual(
            [field.name for field in GoldenEvalDocumentsBQTable.schema()],
            list(row),
        )


class GoldenEvalPreResolutionBQTableTest(TestCase):
    """Tests for GoldenEvalPreResolutionBQTable."""

    def setUp(self) -> None:
        self.enterContext(patch_fake_entity_resolution_model_config_name())
        self.config = fake_first_order_extractor_config()
        self.assignments = assert_type(
            self.config.extractor_collection.output_schema.get_field("assignments"),
            ArrayOfStructLLMRequestOutputSchemaField,
        )

    def test_doc_level_address_is_the_sandbox_materialized_view_address(
        self,
    ) -> None:
        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_prefix_us_xx_document_extraction_results__pre_resolution",
                table_id="fake_extractor_collection_materialized",
            ),
            GoldenEvalPreResolutionBQTable.address(
                first_order_config=self.config,
                array_field=None,
                sandbox_prefix="my_prefix",
            ),
        )

    def test_array_level_address_is_the_sandbox_materialized_view_address(
        self,
    ) -> None:
        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_prefix_us_xx_document_extraction_results__pre_resolution",
                table_id="fake_extractor_collection_assignments_materialized",
            ),
            GoldenEvalPreResolutionBQTable.address(
                first_order_config=self.config,
                array_field=self.assignments,
                sandbox_prefix="my_prefix",
            ),
        )

    def test_doc_level_schema_column_names(self) -> None:
        self.assertEqual(
            [
                "state_code",
                "person_id",
                "document_contents_id",
                "document_update_datetime",
                "validation_datetime_utc",
                "extractor_version_id",
                "primary_status",
                "status_note",
                "location",
                "confidence_levels",
                "null_reasons",
                "adversarial_interpretations",
                "citations",
            ],
            [
                field.name
                for field in GoldenEvalPreResolutionBQTable.schema(
                    first_order_config=self.config, array_field=None
                )
            ],
        )

    def test_array_level_schema_column_names(self) -> None:
        self.assertEqual(
            [
                "state_code",
                "person_id",
                "document_contents_id",
                "document_update_datetime",
                "validation_datetime_utc",
                "extractor_version_id",
                "source_array_index",
                "assignment_name",
                "assignment_type",
                "rate_amount",
                "rate_period",
                "confidence_levels",
                "null_reasons",
                "adversarial_interpretations",
                "citations",
            ],
            [
                field.name
                for field in GoldenEvalPreResolutionBQTable.schema(
                    first_order_config=self.config, array_field=self.assignments
                )
            ],
        )

    def test_to_row_doc_level(self) -> None:
        row = GoldenEvalPreResolutionBQTable.to_row(
            first_order_config=self.config,
            array_field=None,
            root_entity_id=1,
            document_contents_id="abc",
            document_update_datetime=datetime.datetime(2026, 3, 1, tzinfo=datetime.UTC),
            validation_datetime_utc=datetime.datetime(
                2026, 10, 20, 12, tzinfo=datetime.UTC
            ),
            source_array_index=None,
            field_values={
                "is_relevant": True,
                "primary_status": "active",
                "status_note": "Currently active.",
                "location": "Kitchen",
                "assignments": [{"assignment_name": "Dish duty"}],
            },
        )

        self.assertEqual(
            {
                "state_code": "US_XX",
                "person_id": 1,
                "document_contents_id": "abc",
                "document_update_datetime": datetime.datetime(
                    2026, 3, 1, tzinfo=datetime.UTC
                ),
                "validation_datetime_utc": datetime.datetime(
                    2026, 10, 20, 12, tzinfo=datetime.UTC
                ),
                "extractor_version_id": self.config.extractor_version_id,
                "primary_status": "active",
                "status_note": "Currently active.",
                "location": "Kitchen",
                "confidence_levels": None,
                "null_reasons": None,
                "adversarial_interpretations": None,
                "citations": None,
            },
            row,
        )
        self.assertEqual(
            [
                field.name
                for field in GoldenEvalPreResolutionBQTable.schema(
                    first_order_config=self.config, array_field=None
                )
            ],
            list(row),
        )

    def test_to_row_array_level_carries_source_array_index_and_nulls_missing_sub_fields(
        self,
    ) -> None:
        row = GoldenEvalPreResolutionBQTable.to_row(
            first_order_config=self.config,
            array_field=self.assignments,
            root_entity_id=1,
            document_contents_id="abc",
            document_update_datetime=datetime.datetime(2026, 3, 1, tzinfo=datetime.UTC),
            validation_datetime_utc=datetime.datetime(
                2026, 10, 20, 12, tzinfo=datetime.UTC
            ),
            source_array_index=1,
            field_values={
                "assignment_name": "Dish duty",
                "assignment_type": "internal",
            },
        )

        self.assertEqual(
            {
                "state_code": "US_XX",
                "person_id": 1,
                "document_contents_id": "abc",
                "document_update_datetime": datetime.datetime(
                    2026, 3, 1, tzinfo=datetime.UTC
                ),
                "validation_datetime_utc": datetime.datetime(
                    2026, 10, 20, 12, tzinfo=datetime.UTC
                ),
                "extractor_version_id": self.config.extractor_version_id,
                "source_array_index": 1,
                "assignment_name": "Dish duty",
                "assignment_type": "internal",
                "rate_amount": None,
                "rate_period": None,
                "confidence_levels": None,
                "null_reasons": None,
                "adversarial_interpretations": None,
                "citations": None,
            },
            row,
        )
        self.assertEqual(
            [
                field.name
                for field in GoldenEvalPreResolutionBQTable.schema(
                    first_order_config=self.config, array_field=self.assignments
                )
            ],
            list(row),
        )

    def test_to_row_rejects_index_without_array_field(self) -> None:
        with self.assertRaisesRegex(ValueError, r"source_array_index"):
            GoldenEvalPreResolutionBQTable.to_row(
                first_order_config=self.config,
                array_field=None,
                root_entity_id=1,
                document_contents_id="abc",
                document_update_datetime=datetime.datetime(
                    2026, 3, 1, tzinfo=datetime.UTC
                ),
                validation_datetime_utc=datetime.datetime(
                    2026, 10, 20, 12, tzinfo=datetime.UTC
                ),
                source_array_index=0,
                field_values={},
            )


class GoldenEvalCompositeDocumentsBQTableTest(TestCase):
    """Tests for GoldenEvalCompositeDocumentsBQTable"""

    def setUp(self) -> None:
        self.enterContext(patch_fake_entity_resolution_model_config_name())
        self.er_collection = fake_entity_resolution_document_collection_config(
            "location"
        )

    def test_address_carries_state_and_collection(self) -> None:
        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_prefix_document_extraction_golden_eval_results",
                table_id=(
                    "us_xx_fake_extractor_collection_location_entity_resolution"
                    "__composite_documents"
                ),
            ),
            GoldenEvalCompositeDocumentsBQTable.address(
                er_collection=self.er_collection, sandbox_prefix="my_prefix"
            ),
        )

    def test_schema_is_the_generation_output_schema(self) -> None:
        self.assertEqual(
            [
                "person_id",
                "document_contents_id",
                "document_text",
                "document_update_datetime",
                "entry_source_map",
            ],
            [
                field.name
                for field in GoldenEvalCompositeDocumentsBQTable.schema(
                    er_collection=self.er_collection
                )
            ],
        )
