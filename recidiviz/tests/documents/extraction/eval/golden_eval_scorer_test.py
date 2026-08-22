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
"""Tests for LLMDocumentExtractionGoldenEvalScorer.

Scores against FAKE_EXTRACTOR_COLLECTION's output schema, which covers every
shape the scorer has to handle: a STRUCTURAL BOOLEAN (`is_relevant`), an INFERRED
ENUM (`primary_status`), a STRUCTURAL STRING (`status_note`), an INFERRED STRING
(`location`), and an ARRAY_OF_STRUCT (`assignments`, keyed on `assignment_name`)
whose sub-fields span STRING, ENUM, and FLOAT.
"""
from typing import Any
from unittest import TestCase

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.eval.golden_eval_document import GoldenEvalDocument
from recidiviz.documents.extraction.eval.golden_eval_result import GoldenEvalFieldScore
from recidiviz.documents.extraction.eval.golden_eval_scorer import (
    LLMDocumentExtractionGoldenEvalScorer,
)
from recidiviz.documents.extraction.llm_extractor_config_collectors import (
    get_first_order_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_document_extraction_golden_eval_config import (
    GoldenEvalTestType,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
    RESULT_KEY,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.tests.documents import fake_config
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_fake_extractor_assignment_result_json,
    build_fake_extractor_irrelevant_result_content,
    build_fake_extractor_result_content,
    build_inferred_field_result_json,
    build_null_inferred_field_result_json,
    wrap_in_result_key,
)

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
_DOCUMENT_ID = "unit_1"
_TEST_CASE = "base_case"
_DOCUMENT_TEXT = "The record is active. Assigned to dish duty at $12.50/hour."


def _expected_values(**overrides: Any) -> dict[str, Any]:
    """Returns the expected values for a document, defaulting to values that
    match `_actual_output_json()` exactly and overriding them key by key.
    """
    values: dict[str, Any] = {
        IS_RELEVANT_FIELD_NAME: True,
        "primary_status": "active",
        "status_note": "Currently active.",
        "location": "Kitchen",
        "assignments": [
            {
                "assignment_name": "Dish duty",
                "assignment_type": "internal",
                "rate_amount": 12.5,
                "rate_period": "hourly",
            }
        ],
    }
    values.update(overrides)
    return values


def _document(**overrides: Any) -> GoldenEvalDocument:
    """Returns a golden eval document, defaulting to the base-case unit test
    document and overriding its attributes key by key.
    """
    kwargs: dict[str, Any] = {
        "golden_document_id": _DOCUMENT_ID,
        "test_type": GoldenEvalTestType.UNIT,
        "test_case": _TEST_CASE,
        "state_code": _STATE_CODE,
        "document_text": _DOCUMENT_TEXT,
        "expected_values": _expected_values(),
    }
    kwargs.update(overrides)
    return GoldenEvalDocument(**kwargs)


def _actual_output_json(**overrides: Any) -> dict[str, Any]:
    """Returns the output JSON the extractor produced, defaulting to output that
    matches `_expected_values()` exactly and overriding it key by key.
    """
    kwargs: dict[str, Any] = {
        "primary_status": "active",
        "status_note": "Currently active.",
        "location": "Kitchen",
        "assignments": [
            build_fake_extractor_assignment_result_json(
                "Dish duty", "internal", 12.5, "hourly"
            )
        ],
    }
    kwargs.update(overrides)
    return wrap_in_result_key(build_fake_extractor_result_content(**kwargs))


def _score(
    field_name: str,
    element_index: int | None,
    expected_value: str | None,
    actual_value: str | None,
    is_correct: bool,
) -> GoldenEvalFieldScore:
    """Returns an expected scored comparison of one field of the base-case
    document.
    """
    return GoldenEvalFieldScore(
        golden_document_id=_DOCUMENT_ID,
        test_type=GoldenEvalTestType.UNIT,
        test_case=_TEST_CASE,
        field_name=field_name,
        element_index=element_index,
        expected_value=expected_value,
        actual_value=actual_value,
        is_correct=is_correct,
    )


def _scores(
    *rows: tuple[str, int | None, str | None, str | None, bool]
) -> list[GoldenEvalFieldScore]:
    """Returns one expected scored comparison for the base-case document per
    (field_name, element_index, expected_value, actual_value, is_correct) row.
    """
    return [_score(*row) for row in rows]


def _all_correct_scores() -> list[GoldenEvalFieldScore]:
    """Returns the scores for a document whose actual output matches its expected
    values on every field.
    """
    return _scores(
        (IS_RELEVANT_FIELD_NAME, None, "True", "True", True),
        ("primary_status", None, "active", "active", True),
        ("status_note", None, "Currently active.", "Currently active.", True),
        ("location", None, "Kitchen", "Kitchen", True),
        ("assignments", None, "count:1", "count:1", True),
        ("assignments.assignment_name", 0, "Dish duty", "Dish duty", True),
        ("assignments.assignment_type", 0, "internal", "internal", True),
        ("assignments.rate_amount", 0, "12.5", "12.5", True),
        ("assignments.rate_period", 0, "hourly", "hourly", True),
    )


class LLMDocumentExtractionGoldenEvalScorerTest(TestCase):
    """Tests for LLMDocumentExtractionGoldenEvalScorer."""

    def setUp(self) -> None:
        self.output_schema = get_first_order_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        ).extractor_collection.output_schema
        self.scorer = LLMDocumentExtractionGoldenEvalScorer()

    def _score_one(
        self,
        *,
        document: GoldenEvalDocument | None = None,
        actual_output_json: dict[str, Any] | None,
    ) -> list[GoldenEvalFieldScore]:
        """Returns the scores for a single document. An |actual_output_json| of
        None means the document produced no usable output at all, which the
        scorer takes as a None output value rather than an empty one."""
        document = document if document is not None else _document()
        return self.scorer.score(
            output_schema=self.output_schema,
            documents=[document],
            actual_output_values_by_document_id={
                document.golden_document_id: (
                    None
                    if actual_output_json is None
                    else LLMRequestOutputValues(
                        output_schema=self.output_schema,
                        output_json=actual_output_json,
                    )
                )
            },
        )

    def _scores_by_field(
        self, scores: list[GoldenEvalFieldScore]
    ) -> dict[tuple[str, int | None], GoldenEvalFieldScore]:
        """Returns |scores| keyed by (field_name, element_index)."""
        return {(score.field_name, score.element_index): score for score in scores}

    # ------------------------------------------------------------------
    # Flat fields
    # ------------------------------------------------------------------

    def test_all_fields_correct(self) -> None:
        self.assertEqual(
            _all_correct_scores(),
            self._score_one(actual_output_json=_actual_output_json()),
        )

    def test_string_fields_match_fuzzily(self) -> None:
        document = _document(
            expected_values=_expected_values(
                status_note="  currently   ACTIVE. ", location="kitchen"
            )
        )

        scores = self._scores_by_field(
            self._score_one(document=document, actual_output_json=_actual_output_json())
        )

        self.assertTrue(scores[("status_note", None)].is_correct)
        self.assertTrue(scores[("location", None)].is_correct)
        # The rendered values keep their original casing/whitespace — only the
        # comparison is normalized.
        self.assertEqual(
            "  currently   ACTIVE. ", scores[("status_note", None)].expected_value
        )
        self.assertEqual(
            "Currently active.", scores[("status_note", None)].actual_value
        )

    def test_enum_field_mismatch(self) -> None:
        scores = self._scores_by_field(
            self._score_one(
                actual_output_json=_actual_output_json(primary_status="inactive")
            )
        )

        self.assertEqual(
            _score("primary_status", None, "active", "inactive", False),
            scores[("primary_status", None)],
        )

    def test_boolean_field_mismatch(self) -> None:
        document = _document(
            expected_values=_expected_values(**{IS_RELEVANT_FIELD_NAME: False})
        )

        scores = self._scores_by_field(
            self._score_one(document=document, actual_output_json=_actual_output_json())
        )

        self.assertEqual(
            _score(IS_RELEVANT_FIELD_NAME, None, "False", "True", False),
            scores[(IS_RELEVANT_FIELD_NAME, None)],
        )

    def test_float_sub_field_compares_exactly(self) -> None:
        scores = self._scores_by_field(
            self._score_one(
                actual_output_json=_actual_output_json(
                    assignments=[
                        build_fake_extractor_assignment_result_json(
                            "Dish duty", "internal", 12.51, "hourly"
                        )
                    ]
                )
            )
        )

        self.assertEqual(
            _score("assignments.rate_amount", 0, "12.5", "12.51", False),
            scores[("assignments.rate_amount", 0)],
        )

    def test_expected_null_and_actual_present_is_a_false_positive(self) -> None:
        document = _document(expected_values=_expected_values(location=None))

        scores = self._scores_by_field(
            self._score_one(document=document, actual_output_json=_actual_output_json())
        )

        self.assertEqual(
            _score("location", None, None, "Kitchen", False),
            scores[("location", None)],
        )

    def test_expected_present_and_actual_null_is_a_miss(self) -> None:
        scores = self._scores_by_field(
            self._score_one(actual_output_json=_actual_output_json(location=None))
        )

        self.assertEqual(
            _score("location", None, "Kitchen", None, False),
            scores[("location", None)],
        )

    def test_expected_null_and_actual_null_is_correct(self) -> None:
        document = _document(expected_values=_expected_values(location=None))

        scores = self._scores_by_field(
            self._score_one(
                document=document,
                actual_output_json=_actual_output_json(location=None),
            )
        )

        self.assertEqual(
            _score("location", None, None, None, True),
            scores[("location", None)],
        )

    def test_field_absent_from_actual_json_reads_as_null(self) -> None:
        # An older extractor version's output can omit a field entirely; that is
        # indistinguishable from the field being null.
        actual_output_json = _actual_output_json()
        del actual_output_json["result"]["location"]

        scores = self._scores_by_field(
            self._score_one(actual_output_json=actual_output_json)
        )

        self.assertEqual(
            _score("location", None, "Kitchen", None, False),
            scores[("location", None)],
        )

    def test_irrelevant_document_scores_correct(self) -> None:
        document = _document(
            expected_values=_expected_values(
                **{
                    IS_RELEVANT_FIELD_NAME: False,
                    "primary_status": None,
                    "status_note": None,
                    "location": None,
                    "assignments": None,
                }
            )
        )

        self.assertEqual(
            _scores(
                (IS_RELEVANT_FIELD_NAME, None, "False", "False", True),
                ("primary_status", None, None, None, True),
                ("status_note", None, None, None, True),
                ("location", None, None, None, True),
                ("assignments", None, None, None, True),
            ),
            self._score_one(
                document=document,
                actual_output_json=wrap_in_result_key(
                    build_fake_extractor_irrelevant_result_content()
                ),
            ),
        )

    def test_irrelevant_actual_output_misses_every_expected_value(self) -> None:
        # The document had values to find, but the extractor called it irrelevant,
        # so its output carries the relevance field alone. Relevance itself scores
        # against what the extractor said; every other field scores as a miss,
        # because there is nothing in the output to read for it.
        scores = self._score_one(
            actual_output_json=wrap_in_result_key(
                build_fake_extractor_irrelevant_result_content()
            )
        )

        self.assertEqual(
            _scores(
                (IS_RELEVANT_FIELD_NAME, None, "True", "False", False),
                ("primary_status", None, "active", None, False),
                ("status_note", None, "Currently active.", None, False),
                ("location", None, "Kitchen", None, False),
                ("assignments", None, "count:1", None, False),
                ("assignments.assignment_name", 0, "Dish duty", None, False),
                ("assignments.assignment_type", 0, "internal", None, False),
                ("assignments.rate_amount", 0, "12.5", None, False),
                ("assignments.rate_period", 0, "hourly", None, False),
            ),
            scores,
        )

    def test_no_actual_output_scores_every_expected_field_as_a_miss(self) -> None:
        # A request error or a validation downgrade leaves nothing usable; every
        # expected field is a miss.
        scores = self._score_one(actual_output_json=None)

        self.assertEqual(
            _scores(
                (IS_RELEVANT_FIELD_NAME, None, "True", None, False),
                ("primary_status", None, "active", None, False),
                ("status_note", None, "Currently active.", None, False),
                ("location", None, "Kitchen", None, False),
                ("assignments", None, "count:1", None, False),
                ("assignments.assignment_name", 0, "Dish duty", None, False),
                ("assignments.assignment_type", 0, "internal", None, False),
                ("assignments.rate_amount", 0, "12.5", None, False),
                ("assignments.rate_period", 0, "hourly", None, False),
            ),
            scores,
        )

    def test_malformed_array_field_scores_only_that_field_as_a_miss(self) -> None:
        # A field whose shape contradicts the schema misses, but the fields that
        # read cleanly keep their scores.
        actual_output_json = _actual_output_json()
        actual_output_json[RESULT_KEY]["assignments"] = {"not": "a list"}

        scores = self._score_one(actual_output_json=actual_output_json)

        self.assertEqual(
            _scores(
                (IS_RELEVANT_FIELD_NAME, None, "True", "True", True),
                ("primary_status", None, "active", "active", True),
                ("status_note", None, "Currently active.", "Currently active.", True),
                ("location", None, "Kitchen", "Kitchen", True),
                ("assignments", None, "count:1", None, False),
                ("assignments.assignment_name", 0, "Dish duty", None, False),
                ("assignments.assignment_type", 0, "internal", None, False),
                ("assignments.rate_amount", 0, "12.5", None, False),
                ("assignments.rate_period", 0, "hourly", None, False),
            ),
            scores,
        )

    def test_malformed_inferred_field_scores_only_that_field_as_a_miss(self) -> None:
        # `location` is INFERRED, so a bare scalar contradicts the schema — a
        # miss even though the value it carries is the expected one.
        actual_output_json = _actual_output_json()
        actual_output_json[RESULT_KEY]["location"] = "Kitchen"

        scores = self._score_one(actual_output_json=actual_output_json)

        self.assertEqual(
            _scores(
                (IS_RELEVANT_FIELD_NAME, None, "True", "True", True),
                ("primary_status", None, "active", "active", True),
                ("status_note", None, "Currently active.", "Currently active.", True),
                ("location", None, "Kitchen", None, False),
                ("assignments", None, "count:1", "count:1", True),
                ("assignments.assignment_name", 0, "Dish duty", "Dish duty", True),
                ("assignments.assignment_type", 0, "internal", "internal", True),
                ("assignments.rate_amount", 0, "12.5", "12.5", True),
                ("assignments.rate_period", 0, "hourly", "hourly", True),
            ),
            scores,
        )

    def test_missing_result_envelope_scores_every_field_as_a_miss(self) -> None:
        # A malformed envelope is read for every field, so field-level scoring
        # degrades to the whole document missing.
        scores = self._score_one(
            actual_output_json=_actual_output_json()[RESULT_KEY],
        )

        self.assertEqual(
            _scores(
                (IS_RELEVANT_FIELD_NAME, None, "True", None, False),
                ("primary_status", None, "active", None, False),
                ("status_note", None, "Currently active.", None, False),
                ("location", None, "Kitchen", None, False),
                ("assignments", None, "count:1", None, False),
                ("assignments.assignment_name", 0, "Dish duty", None, False),
                ("assignments.assignment_type", 0, "internal", None, False),
                ("assignments.rate_amount", 0, "12.5", None, False),
                ("assignments.rate_period", 0, "hourly", None, False),
            ),
            scores,
        )

    # ------------------------------------------------------------------
    # ARRAY_OF_STRUCT fields
    # ------------------------------------------------------------------

    def test_array_miss_and_false_positive(self) -> None:
        # Test differences within an array of structs: `Dish duty`
        # pairs, `Laundry` was expected but not returned (a miss), and `Mopping`
        # was returned but not expected (a false positive).
        document = _document(
            expected_values=_expected_values(
                assignments=[
                    {"assignment_name": "Dish duty", "rate_amount": 12.5},
                    {"assignment_name": "Laundry", "assignment_type": "external"},
                ]
            )
        )

        scores = self._score_one(
            document=document,
            actual_output_json=_actual_output_json(
                assignments=[
                    build_fake_extractor_assignment_result_json(
                        "Dish duty", "internal", 12.5, "hourly"
                    ),
                    build_fake_extractor_assignment_result_json(
                        "Mopping", "internal", 9.0, "hourly"
                    ),
                ]
            ),
        )

        self.assertEqual(
            _scores(
                # The counts match (2 vs. 2) but the elements did not all pair.
                ("assignments", None, "count:2", "count:2", False),
                # Element 0: paired on `assignment_name`.
                ("assignments.assignment_name", 0, "Dish duty", "Dish duty", True),
                ("assignments.assignment_type", 0, None, "internal", False),
                ("assignments.rate_amount", 0, "12.5", "12.5", True),
                ("assignments.rate_period", 0, None, "hourly", False),
                # Element 1: expected but not returned — wrong on every
                # sub-field, including the two the eval sheet left blank.
                ("assignments.assignment_name", 1, "Laundry", None, False),
                ("assignments.assignment_type", 1, "external", None, False),
                ("assignments.rate_amount", 1, None, None, False),
                ("assignments.rate_period", 1, None, None, False),
                # Element 2: returned but not expected.
                ("assignments.assignment_name", 2, None, "Mopping", False),
                ("assignments.assignment_type", 2, None, "internal", False),
                ("assignments.rate_amount", 2, None, "9.0", False),
                ("assignments.rate_period", 2, None, "hourly", False),
            ),
            [score for score in scores if score.field_name.startswith("assignments")],
        )

    def test_array_unpaired_element_scores_the_same_on_either_side(self) -> None:
        # An element present on only one side is the same magnitude of error
        # whichever side it sits on, so a sub-field that is null on both sides
        # must not read as vacuously correct in either direction.
        expected_only_scores = self._score_one(
            document=_document(
                expected_values=_expected_values(
                    assignments=[{"assignment_name": "Laundry"}]
                )
            ),
            actual_output_json=_actual_output_json(assignments=[]),
        )
        actual_only_scores = self._score_one(
            document=_document(expected_values=_expected_values(assignments=[])),
            actual_output_json=_actual_output_json(
                assignments=[
                    {
                        "assignment_name": build_inferred_field_result_json("Laundry"),
                        "assignment_type": build_null_inferred_field_result_json(),
                        "rate_amount": build_null_inferred_field_result_json(),
                        "rate_period": build_null_inferred_field_result_json(),
                    }
                ]
            ),
        )

        self.assertEqual(
            _scores(
                ("assignments", None, "count:1", "count:0", False),
                ("assignments.assignment_name", 0, "Laundry", None, False),
                ("assignments.assignment_type", 0, None, None, False),
                ("assignments.rate_amount", 0, None, None, False),
                ("assignments.rate_period", 0, None, None, False),
            ),
            [s for s in expected_only_scores if s.field_name.startswith("assignments")],
        )
        self.assertEqual(
            _scores(
                ("assignments", None, "count:0", "count:1", False),
                ("assignments.assignment_name", 0, None, "Laundry", False),
                ("assignments.assignment_type", 0, None, None, False),
                ("assignments.rate_amount", 0, None, None, False),
                ("assignments.rate_period", 0, None, None, False),
            ),
            [s for s in actual_only_scores if s.field_name.startswith("assignments")],
        )

    def test_array_pairs_on_primary_key_regardless_of_element_order(self) -> None:
        document = _document(
            expected_values=_expected_values(
                assignments=[
                    {
                        "assignment_name": "Laundry",
                        "assignment_type": "external",
                        "rate_amount": 9.0,
                        "rate_period": "hourly",
                    },
                    {
                        "assignment_name": "Dish duty",
                        "assignment_type": "internal",
                        "rate_amount": 12.5,
                        "rate_period": "hourly",
                    },
                ]
            )
        )

        scores = self._scores_by_field(
            self._score_one(
                document=document,
                actual_output_json=_actual_output_json(
                    assignments=[
                        build_fake_extractor_assignment_result_json(
                            "Dish duty", "internal", 12.5, "hourly"
                        ),
                        build_fake_extractor_assignment_result_json(
                            "Laundry", "external", 9.0, "hourly"
                        ),
                    ]
                ),
            )
        )

        self.assertTrue(scores[("assignments", None)].is_correct)
        # Element indices follow the expected elements' order, not the actual's.
        self.assertEqual(
            "Laundry", scores[("assignments.assignment_name", 0)].actual_value
        )
        self.assertEqual(
            "Dish duty", scores[("assignments.assignment_name", 1)].actual_value
        )
        self.assertTrue(all(score.is_correct for score in scores.values()))

    def test_array_pairs_on_fuzzily_matched_string_primary_key(self) -> None:
        document = _document(
            expected_values=_expected_values(
                assignments=[
                    {
                        "assignment_name": "  dish   DUTY ",
                        "assignment_type": "internal",
                        "rate_amount": 12.5,
                        "rate_period": "hourly",
                    }
                ]
            )
        )

        scores = self._scores_by_field(
            self._score_one(document=document, actual_output_json=_actual_output_json())
        )

        self.assertTrue(scores[("assignments", None)].is_correct)
        self.assertTrue(scores[("assignments.assignment_name", 0)].is_correct)

    def test_array_pairs_duplicate_primary_keys_one_to_one(self) -> None:
        document = _document(
            expected_values=_expected_values(
                assignments=[
                    {"assignment_name": "Dish duty", "rate_amount": 12.5},
                    {"assignment_name": "Dish duty", "rate_amount": 9.0},
                ]
            )
        )

        scores = self._scores_by_field(
            self._score_one(
                document=document,
                actual_output_json=_actual_output_json(
                    assignments=[
                        build_fake_extractor_assignment_result_json(
                            "Dish duty", "internal", 12.5, "hourly"
                        ),
                        build_fake_extractor_assignment_result_json(
                            "Dish duty", "internal", 9.0, "hourly"
                        ),
                    ]
                ),
            )
        )

        self.assertTrue(scores[("assignments", None)].is_correct)
        self.assertEqual("12.5", scores[("assignments.rate_amount", 0)].actual_value)
        self.assertEqual("9.0", scores[("assignments.rate_amount", 1)].actual_value)

    def test_array_count_mismatch(self) -> None:
        document = _document(
            expected_values=_expected_values(
                assignments=[
                    {
                        "assignment_name": "Dish duty",
                        "assignment_type": "internal",
                        "rate_amount": 12.5,
                        "rate_period": "hourly",
                    },
                    {"assignment_name": "Laundry"},
                ]
            )
        )

        scores = self._scores_by_field(
            self._score_one(document=document, actual_output_json=_actual_output_json())
        )

        self.assertEqual(
            _score("assignments", None, "count:2", "count:1", False),
            scores[("assignments", None)],
        )

    def test_array_expected_empty_and_actual_empty(self) -> None:
        document = _document(expected_values=_expected_values(assignments=[]))

        self.assertEqual(
            _score("assignments", None, "count:0", "count:0", True),
            self._scores_by_field(
                self._score_one(
                    document=document,
                    actual_output_json=_actual_output_json(assignments=[]),
                )
            )[("assignments", None)],
        )

    def test_array_not_expected_but_returned(self) -> None:
        document = _document(expected_values=_expected_values(assignments=None))

        scores = self._score_one(
            document=document, actual_output_json=_actual_output_json()
        )

        self.assertEqual(
            _scores(
                ("assignments", None, None, "count:1", False),
                ("assignments.assignment_name", 0, None, "Dish duty", False),
                ("assignments.assignment_type", 0, None, "internal", False),
                ("assignments.rate_amount", 0, None, "12.5", False),
                ("assignments.rate_period", 0, None, "hourly", False),
            ),
            [score for score in scores if score.field_name.startswith("assignments")],
        )

    def test_array_sub_field_on_null_branch_reads_as_null(self) -> None:
        document = _document(
            expected_values=_expected_values(
                assignments=[
                    {"assignment_name": "Dish duty", "assignment_type": "internal"}
                ]
            )
        )

        scores = self._scores_by_field(
            self._score_one(
                document=document,
                actual_output_json=_actual_output_json(
                    assignments=[
                        {
                            "assignment_name": build_inferred_field_result_json(
                                "Dish duty"
                            ),
                            "assignment_type": build_null_inferred_field_result_json(),
                            "rate_amount": build_null_inferred_field_result_json(),
                            "rate_period": build_null_inferred_field_result_json(),
                        }
                    ]
                ),
            )
        )

        self.assertTrue(scores[("assignments", None)].is_correct)
        self.assertEqual(
            _score("assignments.assignment_type", 0, "internal", None, False),
            scores[("assignments.assignment_type", 0)],
        )
        self.assertTrue(scores[("assignments.rate_amount", 0)].is_correct)

    def test_array_element_is_null(self) -> None:
        # A null element the extractor returned can never pair: an expected value's
        # array holds only elements, never a null. It is a false positive on every
        # sub-field, not a row of vacuously correct null-to-null comparisons.
        document = _document(
            expected_values=_expected_values(
                assignments=[
                    {
                        "assignment_name": "Dish duty",
                        "assignment_type": "internal",
                        "rate_amount": 12.5,
                        "rate_period": "hourly",
                    }
                ]
            )
        )

        scores = self._score_one(
            document=document,
            actual_output_json=_actual_output_json(
                assignments=[
                    build_fake_extractor_assignment_result_json(
                        "Dish duty", "internal", 12.5, "hourly"
                    ),
                    None,
                ]
            ),
        )

        self.assertEqual(
            _scores(
                ("assignments", None, "count:1", "count:2", False),
                # Element 0: paired on `assignment_name`.
                ("assignments.assignment_name", 0, "Dish duty", "Dish duty", True),
                ("assignments.assignment_type", 0, "internal", "internal", True),
                ("assignments.rate_amount", 0, "12.5", "12.5", True),
                ("assignments.rate_period", 0, "hourly", "hourly", True),
                # Element 1: the null element, unpaired and every sub-field wrong.
                ("assignments.assignment_name", 1, None, None, False),
                ("assignments.assignment_type", 1, None, None, False),
                ("assignments.rate_amount", 1, None, None, False),
                ("assignments.rate_period", 1, None, None, False),
            ),
            [score for score in scores if score.field_name.startswith("assignments")],
        )

    # ------------------------------------------------------------------
    # Multiple documents
    # ------------------------------------------------------------------

    def test_scores_every_document(self) -> None:
        first = _document(golden_document_id="unit_1")
        second = _document(
            golden_document_id="sample_1",
            test_type=GoldenEvalTestType.SAMPLE,
            test_case="realistic_note",
        )

        scores = self.scorer.score(
            output_schema=self.output_schema,
            documents=[first, second],
            actual_output_values_by_document_id={
                "unit_1": LLMRequestOutputValues(
                    output_schema=self.output_schema,
                    output_json=_actual_output_json(),
                ),
                "sample_1": LLMRequestOutputValues(
                    output_schema=self.output_schema,
                    output_json=_actual_output_json(primary_status="inactive"),
                ),
            },
        )

        first_scores = [s for s in scores if s.golden_document_id == "unit_1"]
        second_scores = [s for s in scores if s.golden_document_id == "sample_1"]

        self.assertEqual(_all_correct_scores(), first_scores)
        self.assertTrue(
            all(s.test_type is GoldenEvalTestType.SAMPLE for s in second_scores)
        )
        self.assertEqual(
            [
                GoldenEvalFieldScore(
                    golden_document_id="sample_1",
                    test_type=GoldenEvalTestType.SAMPLE,
                    test_case="realistic_note",
                    field_name="primary_status",
                    element_index=None,
                    expected_value="active",
                    actual_value="inactive",
                    is_correct=False,
                )
            ],
            [s for s in second_scores if s.field_name == "primary_status"],
        )

    # ------------------------------------------------------------------
    # Malformed inputs
    # ------------------------------------------------------------------

    def test_document_missing_from_actual_outputs_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^No actual extraction output was provided for golden eval document "
            r"\[unit_1\]\.$",
        ):
            self.scorer.score(
                output_schema=self.output_schema,
                documents=[_document()],
                actual_output_values_by_document_id={},
            )

    def test_actual_output_with_a_different_schema_raises(self) -> None:
        other_schema = attr.evolve(
            self.output_schema,
            user_defined_fields=self.output_schema.user_defined_fields[:1],
        )
        with self.assertRaisesRegex(
            ValueError,
            r"^Actual output for golden eval document \[unit_1\] uses a different "
            r"output schema than the one being scored against\.$",
        ):
            self.scorer.score(
                output_schema=self.output_schema,
                documents=[_document()],
                actual_output_values_by_document_id={
                    _DOCUMENT_ID: LLMRequestOutputValues(
                        output_schema=other_schema,
                        output_json=_actual_output_json(),
                    )
                },
            )
