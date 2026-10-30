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
"""Tests for GoldenEvalFirstOrderDocumentValidator and
GoldenEvalEntityResolutionDocumentValidator.

Covers the contract GoldenEvalDocumentReader depends on and its own tests cannot
see: which value each issue names, through the `field_path` a ValidationIssue
would carry once these checks are delegated to
LLMExtractionResultValidator. The rendered text of each issue is asserted in
golden_eval_document_reader_test.py.
"""
import datetime
from typing import Any
from unittest import TestCase

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.eval.golden_eval_document import (
    GoldenEvalDocument,
    GoldenEvalEntityResolutionDocument,
)
from recidiviz.documents.extraction.eval.golden_eval_document_validator import (
    GoldenEvalEntityResolutionDocumentValidator,
    GoldenEvalFirstOrderDocumentValidator,
)
from recidiviz.documents.extraction.llm_extraction_view_graph_stage import (
    LLMExtractionViewGraphStage,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
)
from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
    EnumLLMRequestOutputSchemaField,
    LLMOutputFieldType,
    PrimitiveScalarLLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
)
from recidiviz.tests.documents import fake_config
from recidiviz.utils.types import assert_type

_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"

_DISH_DUTY_ELEMENT = {
    "assignment_name": "Dish duty",
    "assignment_type": "internal",
    "rate_amount": 12.5,
    "rate_period": "hourly",
}


def _document(**overrides: Any) -> GoldenEvalDocument:
    """Returns one document, defaulting to expected values that are legal against
    FAKE_EXTRACTOR_COLLECTION's schema and allowing passed-in overrides.
    """
    expected_values: dict[str, Any] = {
        IS_RELEVANT_FIELD_NAME: True,
        "primary_status": "active",
        "status_note": "Currently active.",
        "location": "Kitchen",
        "assignments": [dict(_DISH_DUTY_ELEMENT)],
    }
    expected_values.update(overrides)
    return GoldenEvalDocument(
        golden_document_id="unit_1",
        test_type=GoldenEvalTestType.UNIT,
        test_case="base_case",
        state_code=StateCode.US_XX,
        document_text="The record is active.",
        expected_values=expected_values,
    )


_RESOLVED_DISH_DUTY_ELEMENT = {
    "assignment_name": "Dish duty",
    "assignment_type": "internal",
    "rate_amount": 12.5,
}


def _er_document(
    first_order_overrides: dict[str, Any] | None = None, **resolved_overrides: Any
) -> GoldenEvalEntityResolutionDocument:
    """Returns one entity-resolution document, defaulting to resolved values that
    line up with the default first-order document and allowing overrides of
    either.
    """
    expected_resolved_values: dict[str, Any] = {
        "location": "Main Kitchen",
        "assignments": [dict(_RESOLVED_DISH_DUTY_ELEMENT)],
    }
    expected_resolved_values.update(resolved_overrides)
    return GoldenEvalEntityResolutionDocument(
        **attr.asdict(_document(**(first_order_overrides or {})), recurse=False),
        test_group_id=1,
        document_date=datetime.date(2026, 3, 1),
        expected_resolved_values=expected_resolved_values,
    )


class GoldenEvalFirstOrderDocumentValidatorTest(TestCase):
    """Tests for GoldenEvalFirstOrderDocumentValidator."""

    def setUp(self) -> None:
        self.validator = GoldenEvalFirstOrderDocumentValidator(
            output_schema=get_first_order_llm_extractor_config(
                StateCode.US_XX, _COLLECTION_NAME, config_module=fake_config
            ).extractor_collection.output_schema
        )

    def _field_paths(self, document: GoldenEvalDocument) -> list[str]:
        """Returns the path of each value the validator flags in |document|."""
        return [issue.field_path for issue in self.validator.issues(document=document)]

    def test_legal_values_yield_no_issues(self) -> None:
        self.assertEqual([], self._field_paths(_document()))

    def test_undeclared_enum_value_names_the_field(self) -> None:
        self.assertEqual(
            ["primary_status"], self._field_paths(_document(primary_status="pending"))
        )

    def test_enum_value_spelled_differently_than_declared_is_flagged(self) -> None:
        # The sheet must spell the value exactly as the field declares it, even
        # though the scorer would compare the two case-insensitively.
        self.assertEqual(
            ["primary_status"], self._field_paths(_document(primary_status="ACTIVE"))
        )

    def test_float_sub_field_accepts_an_integer(self) -> None:
        # Mirrors the pipeline: a FLOAT field validates actual output as JSON
        # Schema `number`, which accepts 12, and the scorer compares 12 == 12.0.
        self.assertEqual(
            [],
            self._field_paths(
                _document(assignments=[{**_DISH_DUTY_ELEMENT, "rate_amount": 12}])
            ),
        )

    def test_float_sub_field_rejects_a_boolean(self) -> None:
        self.assertEqual(
            ["assignments[0].rate_amount"],
            self._field_paths(
                _document(assignments=[{**_DISH_DUTY_ELEMENT, "rate_amount": True}])
            ),
        )

    def test_wrongly_typed_sub_field_names_the_element_and_sub_field(self) -> None:
        self.assertEqual(
            ["assignments[0].assignment_name"],
            self._field_paths(_document(assignments=[{"assignment_name": 12}])),
        )

    def test_unknown_element_key_names_the_element(self) -> None:
        self.assertEqual(
            ["assignments[0]"],
            self._field_paths(
                _document(
                    assignments=[
                        {"assignment_name": "Dish duty", "assignment_nmae": "x"}
                    ]
                )
            ),
        )

    def test_flags_every_illegal_field_at_once(self) -> None:
        self.assertEqual(
            ["primary_status", "assignments[0].assignment_name"],
            self._field_paths(
                _document(
                    primary_status="pending", assignments=[{"assignment_name": 12}]
                )
            ),
        )

    def test_missing_relevance_answer_is_reported_alone(self) -> None:
        # Every other check reads that answer, so nothing else is worth reporting
        # while it is missing — `pending` goes unmentioned.
        self.assertEqual(
            [IS_RELEVANT_FIELD_NAME],
            self._field_paths(
                _document(**{IS_RELEVANT_FIELD_NAME: None, "primary_status": "pending"})
            ),
        )

    def test_required_field_is_not_required_of_an_irrelevant_document(self) -> None:
        self.assertEqual(
            [],
            self._field_paths(
                _document(
                    **{
                        IS_RELEVANT_FIELD_NAME: False,
                        "primary_status": None,
                        "status_note": None,
                        "location": None,
                        "assignments": None,
                    }
                )
            ),
        )

    def test_required_field_is_required_of_a_relevant_document(self) -> None:
        self.assertEqual(
            ["primary_status"], self._field_paths(_document(primary_status=None))
        )

    def test_element_with_its_only_primary_key_null_is_flagged(self) -> None:
        self.assertEqual(
            ["assignments[0]"],
            self._field_paths(
                _document(assignments=[{**_DISH_DUTY_ELEMENT, "assignment_name": None}])
            ),
        )


def _events_output_schema() -> LLMRequestOutputSchema:
    """Returns a minimal output schema declaring an ARRAY_OF_STRUCT field that
    pairs on two primary keys.
    """
    return LLMRequestOutputSchema(
        full_batch_description="Fictional event records for a batch of documents.",
        result_level_description="Fictional event record for a single document.",
        relevance_criteria="Whether the document mentions a fictional event",
        user_defined_fields=[
            ArrayOfStructLLMRequestOutputSchemaField(
                name="events",
                description="The events the document mentions.",
                required=False,
                inferred_field_config=None,
                primary_keys=["event_type", "event_date"],
                min_items=None,
                fields=[
                    PrimitiveScalarLLMRequestOutputSchemaField(
                        name="event_type",
                        description="The kind of event.",
                        required=False,
                        inferred_field_config=None,
                        scalar_type=LLMOutputFieldType.STRING,
                    ),
                    PrimitiveScalarLLMRequestOutputSchemaField(
                        name="event_date",
                        description="The date of the event.",
                        required=False,
                        inferred_field_config=None,
                        scalar_type=LLMOutputFieldType.STRING,
                    ),
                ],
            ),
        ],
    )


def _events_document(events: list[dict[str, Any]]) -> GoldenEvalDocument:
    """Returns one relevant document against the two-key events schema, expecting
    |events|.
    """
    return GoldenEvalDocument(
        golden_document_id="unit_1",
        test_type=GoldenEvalTestType.UNIT,
        test_case="base_case",
        state_code=StateCode.US_XX,
        document_text="Fired on 2025-01-15.",
        expected_values={IS_RELEVANT_FIELD_NAME: True, "events": events},
    )


class GoldenEvalFirstOrderDocumentValidatorMultiPrimaryKeyTest(TestCase):
    """Tests for GoldenEvalFirstOrderDocumentValidator's pairing-key check against
    a field that pairs on more than one primary key.
    """

    def setUp(self) -> None:
        self.validator = GoldenEvalFirstOrderDocumentValidator(
            output_schema=_events_output_schema()
        )

    def _field_paths(self, document: GoldenEvalDocument) -> list[str]:
        """Returns the path of each value the validator flags in |document|."""
        return [issue.field_path for issue in self.validator.issues(document=document)]

    def test_element_with_one_of_two_primary_keys_null_is_legal(self) -> None:
        self.assertEqual(
            [],
            self._field_paths(
                _events_document([{"event_type": "fired", "event_date": None}])
            ),
        )

    def test_element_omitting_one_primary_key_is_legal(self) -> None:
        self.assertEqual(
            [], self._field_paths(_events_document([{"event_type": "fired"}]))
        )

    def test_element_with_every_primary_key_null_is_flagged(self) -> None:
        self.assertEqual(
            ["events[0]"],
            self._field_paths(
                _events_document([{"event_type": None, "event_date": None}])
            ),
        )

    def test_element_omitting_every_primary_key_is_flagged(self) -> None:
        self.assertEqual(["events[0]"], self._field_paths(_events_document([{}])))

    def test_elements_sharing_a_pairing_key_with_a_null_value_are_flagged(self) -> None:
        # A null key value still identifies the element, so two elements whose
        # keys are identical-including-nulls cannot be told apart.
        self.assertEqual(
            ["events[1]"],
            self._field_paths(
                _events_document(
                    [
                        {"event_type": "fired", "event_date": None},
                        {"event_type": "fired", "event_date": None},
                    ]
                )
            ),
        )


class GoldenEvalEntityResolutionDocumentValidatorTest(TestCase):
    """Tests for GoldenEvalEntityResolutionDocumentValidator."""

    def setUp(self) -> None:
        self.collection = get_first_order_llm_extractor_config(
            StateCode.US_XX, _COLLECTION_NAME, config_module=fake_config
        ).extractor_collection
        # The fake collection's groups: `location` (top-level), and `assignment`
        # and `pay_rate`, which together resolve assignment_name, assignment_type,
        # and rate_amount of `assignments`.
        self.first_order_validator = GoldenEvalFirstOrderDocumentValidator(
            output_schema=self.collection.output_schema
        )
        self.validator = GoldenEvalEntityResolutionDocumentValidator(
            first_order_validator=self.first_order_validator,
            resolver_fields=self.collection.resolver_fields,
            resolver_sub_field_names_by_array_field_name=(
                self.collection.resolver_sub_field_names_by_array_field_name
            ),
        )

    def _field_paths(
        self,
        document: GoldenEvalEntityResolutionDocument,
        validator: GoldenEvalEntityResolutionDocumentValidator | None = None,
    ) -> list[str]:
        """Returns the path of each resolved value |validator| flags in
        |document|, defaulting to the validator over every fake entity group.
        """
        return [
            issue.field_path
            for issue in (validator or self.validator).issues(document=document)
        ]

    def _status_group_validator(self) -> GoldenEvalEntityResolutionDocumentValidator:
        """Returns a validator whose only resolver field is the top-level ENUM
        field primary_status.
        """
        primary_status = self.collection.output_schema.get_field("primary_status")
        return GoldenEvalEntityResolutionDocumentValidator(
            first_order_validator=self.first_order_validator,
            resolver_fields=[
                assert_type(primary_status, EnumLLMRequestOutputSchemaField)
            ],
            resolver_sub_field_names_by_array_field_name={},
        )

    def test_matching_shapes_yield_no_issues(self) -> None:
        self.assertEqual([], self._field_paths(_er_document()))

    def test_first_order_issues_are_reported_alone(self) -> None:
        # The resolved array is also one element short, but each resolved check
        # reads the first-order value it resolves, so only the first-order
        # problem is reported.
        self.assertEqual(
            ["primary_status"],
            self._field_paths(
                _er_document({"primary_status": "pending"}, assignments=[])
            ),
        )

    def test_issues_name_the_stage_of_the_value_they_flag(self) -> None:
        self.assertEqual(
            [LLMExtractionViewGraphStage.FIRST_ORDER],
            [
                issue.stage
                for issue in self.validator.issues(
                    document=_er_document({"primary_status": "pending"})
                )
            ],
        )
        self.assertEqual(
            [LLMExtractionViewGraphStage.ENTITY_RESOLUTION],
            [
                issue.stage
                for issue in self.validator.issues(
                    document=_er_document(assignments=[])
                )
            ],
        )
        self.assertEqual(
            [LLMExtractionViewGraphStage.ENTITY_RESOLUTION],
            [
                issue.stage
                for issue in self._status_group_validator().issues(
                    document=_er_document(primary_status="pending")
                )
            ],
        )

    def test_resolved_scalar_blank_with_non_null_first_order_value_is_flagged(
        self,
    ) -> None:
        issues = self.validator.issues(document=_er_document(location=None))

        self.assertEqual(["location"], [issue.field_path for issue in issues])
        self.assertIs(LLMExtractionViewGraphStage.ENTITY_RESOLUTION, issues[0].stage)
        self.assertIn(
            "every extracted mention must have a resolved form", issues[0].detail
        )

    def test_blank_resolved_values_next_to_blank_first_order_values_yield_no_issues(
        self,
    ) -> None:
        self.assertEqual(
            [],
            self._field_paths(
                _er_document(
                    {
                        IS_RELEVANT_FIELD_NAME: False,
                        "location": None,
                        "assignments": None,
                    },
                    location=None,
                    assignments=None,
                )
            ),
        )

    def test_resolved_scalar_may_differ_from_or_fill_the_first_order_value(
        self,
    ) -> None:
        # The entity's value stands in for every mention's, so the resolved
        # location may be another string or fill a null first-order location.
        self.assertEqual(
            [], self._field_paths(_er_document({"location": None}, location="Annex"))
        )

    def test_resolved_enum_value_not_declared_is_flagged(self) -> None:
        validator = self._status_group_validator()

        self.assertEqual(
            ["primary_status"],
            self._field_paths(
                _er_document(primary_status="pending"), validator=validator
            ),
        )
        self.assertEqual(
            [],
            self._field_paths(
                _er_document(primary_status="inactive"), validator=validator
            ),
        )

    def test_resolved_array_blank_with_non_empty_first_order_array_is_flagged(
        self,
    ) -> None:
        issues = self.validator.issues(document=_er_document(assignments=None))

        self.assertEqual(["assignments"], [issue.field_path for issue in issues])
        self.assertEqual(
            "is blank, but [assignments__expected] holds [1] element(s); every "
            "extracted mention must have a resolved form",
            issues[0].detail,
        )

    def test_resolved_array_non_empty_with_blank_first_order_array_is_flagged(
        self,
    ) -> None:
        first_order_values: list[Any] = [None, []]
        for first_order_value in first_order_values:
            with self.subTest(first_order_value=first_order_value):
                issues = self.validator.issues(
                    document=_er_document({"assignments": first_order_value})
                )

                self.assertEqual(
                    ["assignments"], [issue.field_path for issue in issues]
                )
                self.assertEqual(
                    "holds resolved element(s), but [assignments__expected] holds "
                    "none; only an extracted mention can be resolved",
                    issues[0].detail,
                )

    def test_resolved_empty_array_with_empty_first_order_array_yields_no_issues(
        self,
    ) -> None:
        resolved_values: list[Any] = [None, []]
        for resolved_value in resolved_values:
            with self.subTest(resolved_value=resolved_value):
                self.assertEqual(
                    [],
                    self._field_paths(
                        _er_document({"assignments": []}, assignments=resolved_value)
                    ),
                )

    def test_resolved_array_length_mismatch_names_both_counts(self) -> None:
        issues = self.validator.issues(
            document=_er_document(
                assignments=[
                    dict(_RESOLVED_DISH_DUTY_ELEMENT),
                    {"assignment_name": "Laundry"},
                ]
            )
        )

        self.assertEqual(["assignments"], [issue.field_path for issue in issues])
        self.assertEqual(
            "holds [2] element(s), but [assignments__expected] holds [1].",
            issues[0].detail,
        )

    def test_resolved_array_not_a_list_is_flagged(self) -> None:
        self.assertEqual(
            ["assignments"],
            self._field_paths(
                _er_document(assignments=dict(_RESOLVED_DISH_DUTY_ELEMENT))
            ),
        )

    def test_resolved_element_not_an_object_names_the_element(self) -> None:
        self.assertEqual(
            ["assignments[0]"],
            self._field_paths(_er_document(assignments=["Dish duty"])),
        )

    def test_resolved_element_with_non_resolver_sub_field_is_flagged(self) -> None:
        # rate_period is a declared sub-field that no entity group resolves.
        issues = self.validator.issues(
            document=_er_document(
                assignments=[{**_RESOLVED_DISH_DUTY_ELEMENT, "rate_period": "hourly"}]
            )
        )

        self.assertEqual(["assignments[0]"], [issue.field_path for issue in issues])
        self.assertEqual(
            "holds key(s) ['rate_period'] that no entity group resolves from field "
            "[assignments]; resolver sub-fields: ['assignment_name', "
            "'assignment_type', 'rate_amount']",
            issues[0].detail,
        )

    def test_resolved_element_sub_field_wrong_json_type_names_the_sub_field(
        self,
    ) -> None:
        self.assertEqual(
            ["assignments[0].rate_amount"],
            self._field_paths(
                _er_document(
                    assignments=[{**_RESOLVED_DISH_DUTY_ELEMENT, "rate_amount": "12.5"}]
                )
            ),
        )

    def test_resolved_element_undeclared_enum_sub_field_value_is_flagged(
        self,
    ) -> None:
        self.assertEqual(
            ["assignments[0].assignment_type"],
            self._field_paths(
                _er_document(
                    assignments=[
                        {**_RESOLVED_DISH_DUTY_ELEMENT, "assignment_type": "outsourced"}
                    ]
                )
            ),
        )

    def test_resolved_element_may_fill_a_null_first_order_sub_field(self) -> None:
        # The first-order mention names only the assignment; the entity it
        # resolves to carries a type and rate.
        self.assertEqual(
            [],
            self._field_paths(
                _er_document(
                    {"assignments": [{"assignment_name": "Laundry"}]},
                    assignments=[
                        {
                            "assignment_name": "Laundry",
                            "assignment_type": "internal",
                            "rate_amount": 10,
                        }
                    ],
                )
            ),
        )

    def test_resolved_element_may_omit_a_resolver_sub_field(self) -> None:
        self.assertEqual(
            [],
            self._field_paths(
                _er_document(assignments=[{"assignment_name": "Dish duty"}])
            ),
        )

    def test_resolver_sub_field_names_limit_resolved_element_keys(self) -> None:
        # A validator whose resolver sub-fields for `assignments` omit rate_amount
        # flags an element that holds it.
        assignment_only_validator = GoldenEvalEntityResolutionDocumentValidator(
            first_order_validator=self.first_order_validator,
            resolver_fields=self.collection.resolver_fields,
            resolver_sub_field_names_by_array_field_name={
                "assignments": {"assignment_name", "assignment_type"}
            },
        )

        self.assertEqual(
            ["assignments[0]"],
            self._field_paths(_er_document(), validator=assignment_only_validator),
        )
        self.assertEqual([], self._field_paths(_er_document()))

    def test_flags_at_most_one_issue_per_resolver_field(self) -> None:
        self.assertEqual(
            ["assignments[0]"],
            self._field_paths(
                _er_document(
                    {
                        "assignments": [
                            dict(_DISH_DUTY_ELEMENT),
                            {**_DISH_DUTY_ELEMENT, "assignment_name": "Laundry"},
                        ]
                    },
                    assignments=["Dish duty", "Laundry"],
                )
            ),
        )
