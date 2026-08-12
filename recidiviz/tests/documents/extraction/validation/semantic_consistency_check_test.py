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
"""Tests for SemanticConsistencyCheck, exercised directly against a small
schema built inline. Its wiring into the validator is covered in
llm_extraction_result_validator_test.py.
"""

from typing import Any
from unittest import TestCase

from recidiviz.documents.extraction.models.llm_request_output_schema import (
    LLMRequestOutputSchema,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ConfidenceLevel,
    LLMOutputSemanticConsistencyConstraint,
    LLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
    RESULT_KEY,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    ValidationCheckType,
    ValidationIssue,
)
from recidiviz.documents.extraction.validation.semantic_consistency_check import (
    _VALIDATORS_BY_CONSTRAINT_TYPE,
    SemanticConsistencyCheck,
)
from recidiviz.tests.documents.extraction.fake_extractor_result_json import (
    build_inferred_field_result_json,
    build_null_inferred_field_result_json,
)
from recidiviz.utils.yaml_dict import YAMLDict

_SC_DESCRIPTION = "A description that is long enough to be meaningful."


def _sc_field(name: str, *, field_type: str = "STRING", **extra: Any) -> dict[str, Any]:
    """Returns a raw field-definition dict for one field of the schema
    `_semantic_consistency_test_schema` builds."""
    return {"name": name, "type": field_type, "description": _SC_DESCRIPTION, **extra}


def _sc_enum_values(*names: str) -> list[dict[str, str]]:
    """Returns raw `{name, description}` enum value dicts, one per name."""
    return [
        {"name": name, "description": f"{_SC_DESCRIPTION} ({name})"} for name in names
    ]


def _semantic_consistency_test_schema() -> LLMRequestOutputSchema:
    """Returns a schema exercising every semantic-consistency constraint type:
    a `pay_rate_amount` / `pay_rate_frequency` pair testing
    `applicable_when_nonnull` + `required_when_nonnull` together (the "both set
    or both null" pattern), an `employment_status` ENUM gating three sibling
    fields via `applicable_when_value`, `not_applicable_when_value`, and
    `required_when_value`, a `bonus_amount` carrying two gates at once, and an
    `assignments` ARRAY_OF_STRUCT that is itself `applicable_when_value`-gated
    (so a list-typed field's presence is exercised) and whose
    `assignment_location` sub-field is `applicable_when_value`-gated on its
    sibling sub-field `assignment_type`.
    """
    fields = LLMRequestOutputSchemaField.build_output_schema_fields(
        field_yamls=[
            YAMLDict(field_dict)
            for field_dict in [
                _sc_field("pay_rate_amount"),
                _sc_field(
                    "pay_rate_frequency",
                    applicable_when_nonnull="pay_rate_amount",
                    required_when_nonnull="pay_rate_amount",
                ),
                _sc_field(
                    "employment_status",
                    field_type="ENUM",
                    values=_sc_enum_values("employed", "unemployed", "retired"),
                ),
                _sc_field(
                    "employer_name",
                    applicable_when_value={"employment_status": ["employed"]},
                ),
                _sc_field(
                    "unemployment_reason",
                    not_applicable_when_value={"employment_status": ["employed"]},
                ),
                _sc_field(
                    "retirement_date",
                    required_when_value={"employment_status": ["retired"]},
                ),
                _sc_field(
                    "bonus_amount",
                    applicable_when_nonnull="pay_rate_amount",
                    applicable_when_value={"employment_status": ["employed"]},
                ),
                _sc_field(
                    "assignments",
                    field_type="ARRAY_OF_STRUCT",
                    applicable_when_value={"employment_status": ["employed"]},
                    primary_keys=["assignment_type"],
                    fields=[
                        _sc_field(
                            "assignment_type",
                            field_type="ENUM",
                            values=_sc_enum_values("internal", "external"),
                        ),
                        _sc_field(
                            "assignment_location",
                            applicable_when_value={"assignment_type": ["internal"]},
                        ),
                    ],
                ),
            ]
        ],
        default_minimum_confidence_level=ConfidenceLevel.INFERRED,
    )
    return LLMRequestOutputSchema(
        full_batch_description="Batch of parsed fake documents for testing.",
        result_level_description="One parsed fake document for testing.",
        relevance_criteria="Whether the document is a test fixture.",
        user_defined_fields=fields,
    )


def _sc_content(**overrides: Any) -> dict[str, Any]:
    """Returns a baseline result content dict that satisfies every constraint
    `_semantic_consistency_test_schema` declares, with |overrides| written over
    specific fields by name to construct violating results.
    """
    base: dict[str, Any] = {
        IS_RELEVANT_FIELD_NAME: True,
        "pay_rate_amount": build_null_inferred_field_result_json(),
        "pay_rate_frequency": build_null_inferred_field_result_json(),
        "employment_status": build_inferred_field_result_json("employed"),
        "employer_name": build_inferred_field_result_json("Acme"),
        "unemployment_reason": build_null_inferred_field_result_json(),
        "retirement_date": build_null_inferred_field_result_json(),
        "bonus_amount": build_null_inferred_field_result_json(),
        "assignments": [],
    }
    base.update(overrides)
    return base


def _sc_assignment(
    *, assignment_type: str, assignment_location: str | None
) -> dict[str, Any]:
    """Returns one `assignments` array element."""
    return {
        "assignment_type": build_inferred_field_result_json(assignment_type),
        "assignment_location": (
            build_null_inferred_field_result_json()
            if assignment_location is None
            else build_inferred_field_result_json(assignment_location)
        ),
    }


class SemanticConsistencyCheckTest(TestCase):
    """Tests for SemanticConsistencyCheck."""

    def setUp(self) -> None:
        self.schema = _semantic_consistency_test_schema()

    def _issues(self, output_json: dict[str, Any]) -> list[ValidationIssue]:
        output = LLMRequestOutputValues(
            output_schema=self.schema, output_json=output_json
        )
        return SemanticConsistencyCheck.issues(output=output)

    def _assert_single_issue(
        self,
        content: dict[str, Any],
        *,
        expected_field_name: str,
        expected_detail_substring: str,
    ) -> None:
        issues = self._issues({RESULT_KEY: content})
        self.assertEqual(1, len(issues))
        issue = issues[0]
        self.assertEqual(ValidationCheckType.SEMANTIC_CONSISTENCY, issue.check_type)
        self.assertEqual(expected_field_name, issue.field_name)
        self.assertIn(expected_detail_substring, issue.detail)

    def test_clean_result_has_no_issues(self) -> None:
        self.assertEqual([], self._issues({RESULT_KEY: _sc_content()}))

    def test_irrelevant_result_has_no_issues(self) -> None:
        # An irrelevant document carries only the relevance determination, so
        # every constrained field and every condition field reads as absent.
        self.assertEqual(
            [], self._issues({RESULT_KEY: {IS_RELEVANT_FIELD_NAME: False}})
        )

    def test_nonnull_pair_satisfied_when_both_set(self) -> None:
        content = _sc_content(
            pay_rate_amount=build_inferred_field_result_json(12.5),
            pay_rate_frequency=build_inferred_field_result_json("hourly"),
        )
        self.assertEqual([], self._issues({RESULT_KEY: content}))

    def test_applicable_when_nonnull_violated(self) -> None:
        # pay_rate_frequency is set, but its condition field pay_rate_amount
        # is null.
        content = _sc_content(
            pay_rate_frequency=build_inferred_field_result_json("hourly")
        )
        self._assert_single_issue(
            content,
            expected_field_name="pay_rate_frequency",
            expected_detail_substring="condition field [pay_rate_amount] is null",
        )

    def test_required_when_nonnull_violated(self) -> None:
        # pay_rate_amount is set, but pay_rate_frequency — required whenever it
        # is — stays null.
        content = _sc_content(pay_rate_amount=build_inferred_field_result_json(12.5))
        self._assert_single_issue(
            content,
            expected_field_name="pay_rate_frequency",
            expected_detail_substring="condition field [pay_rate_amount] is non-null",
        )

    def test_applicable_when_value_violated(self) -> None:
        content = _sc_content(
            employment_status=build_inferred_field_result_json("unemployed")
        )
        self._assert_single_issue(
            content,
            expected_field_name="employer_name",
            expected_detail_substring="has value [unemployed]",
        )

    def test_not_applicable_when_value_satisfied(self) -> None:
        content = _sc_content(
            employment_status=build_inferred_field_result_json("unemployed"),
            employer_name=build_null_inferred_field_result_json(),
            unemployment_reason=build_inferred_field_result_json("layoff"),
        )
        self.assertEqual([], self._issues({RESULT_KEY: content}))

    def test_not_applicable_when_value_violated(self) -> None:
        # employment_status stays "employed" (the baseline), which forbids
        # unemployment_reason from being set.
        content = _sc_content(
            unemployment_reason=build_inferred_field_result_json("layoff")
        )
        self._assert_single_issue(
            content,
            expected_field_name="unemployment_reason",
            expected_detail_substring="not allowed when [employment_status]",
        )

    def test_required_when_value_satisfied(self) -> None:
        content = _sc_content(
            employment_status=build_inferred_field_result_json("retired"),
            employer_name=build_null_inferred_field_result_json(),
            retirement_date=build_inferred_field_result_json("2020-01-01"),
        )
        self.assertEqual([], self._issues({RESULT_KEY: content}))

    def test_required_when_value_violated(self) -> None:
        content = _sc_content(
            employment_status=build_inferred_field_result_json("retired"),
            employer_name=build_null_inferred_field_result_json(),
        )
        self._assert_single_issue(
            content,
            expected_field_name="retirement_date",
            expected_detail_substring="required when [employment_status]",
        )

    def test_array_element_constraint_satisfied(self) -> None:
        content = _sc_content(
            assignments=[
                _sc_assignment(
                    assignment_type="internal", assignment_location="Kitchen"
                ),
                _sc_assignment(assignment_type="external", assignment_location=None),
            ]
        )
        self.assertEqual([], self._issues({RESULT_KEY: content}))

    def test_array_element_constraint_violated(self) -> None:
        content = _sc_content(
            assignments=[
                _sc_assignment(
                    assignment_type="external", assignment_location="Kitchen"
                )
            ]
        )
        self._assert_single_issue(
            content,
            expected_field_name="assignments[0].assignment_location",
            expected_detail_substring=(
                "condition field [assignment_type] has value [external]"
            ),
        )

    def test_array_elements_checked_independently(self) -> None:
        # Only the second element violates its constraint; the first stays
        # clean, and both are checked against their own sibling sub-fields.
        content = _sc_content(
            assignments=[
                _sc_assignment(
                    assignment_type="internal", assignment_location="Kitchen"
                ),
                _sc_assignment(assignment_type="external", assignment_location="Yard"),
            ]
        )
        self._assert_single_issue(
            content,
            expected_field_name="assignments[1].assignment_location",
            expected_detail_substring="only allowed when [assignment_type]",
        )

    def test_empty_array_is_absent_for_its_own_gate(self) -> None:
        # `assignments` may only be set when employment_status is "employed".
        # An empty array is how the prompt tells the model to say "not
        # applicable" for a list field — arrays get no null branch — so it must
        # read as absent here rather than as a value that trips the gate.
        content = _sc_content(
            employment_status=build_inferred_field_result_json("unemployed"),
            employer_name=build_null_inferred_field_result_json(),
            assignments=[],
        )
        self.assertEqual([], self._issues({RESULT_KEY: content}))

    def test_omitted_array_is_absent_for_its_own_gate(self) -> None:
        # The other way a list field can be absent: no key at all, which reads
        # as None rather than as an empty list.
        content = _sc_content(
            employment_status=build_inferred_field_result_json("unemployed"),
            employer_name=build_null_inferred_field_result_json(),
        )
        del content["assignments"]
        self.assertEqual([], self._issues({RESULT_KEY: content}))

    def test_populated_array_is_present_for_its_own_gate(self) -> None:
        # The counterpart to the empty case: one element makes `assignments`
        # present, so the same gate that an empty array satisfied is now
        # violated.
        content = _sc_content(
            employment_status=build_inferred_field_result_json("unemployed"),
            employer_name=build_null_inferred_field_result_json(),
            assignments=[
                _sc_assignment(
                    assignment_type="internal", assignment_location="Kitchen"
                )
            ],
        )
        self._assert_single_issue(
            content,
            expected_field_name="assignments",
            expected_detail_substring="only allowed when [employment_status]",
        )

    def test_null_array_element_is_all_absent(self) -> None:
        # A literal-null element reads as every sub-field null, so the element's
        # own constraints see an absent field and an absent condition alike.
        self.assertEqual(
            [], self._issues({RESULT_KEY: _sc_content(assignments=[None])})
        )

    def test_null_condition_field_gates_applicable_but_not_the_others(self) -> None:
        # employment_status itself came back null. employer_name is set but can
        # no longer satisfy its `applicable_when_value` gate, while the
        # constraints that only fire on specific values — unemployment_reason's
        # `not_applicable_when_value` and retirement_date's
        # `required_when_value` — stay quiet, so exactly one finding lands.
        content = _sc_content(
            employment_status=build_null_inferred_field_result_json(),
            unemployment_reason=build_inferred_field_result_json("layoff"),
        )
        self._assert_single_issue(
            content,
            expected_field_name="employer_name",
            expected_detail_substring="has value [None]",
        )

    def test_field_violating_two_constraints_reports_both(self) -> None:
        # bonus_amount is gated on pay_rate_amount being set *and* on
        # employment_status being "employed"; setting it while neither holds
        # yields one finding per constraint, both naming the same field.
        content = _sc_content(
            employment_status=build_inferred_field_result_json("unemployed"),
            employer_name=build_null_inferred_field_result_json(),
            bonus_amount=build_inferred_field_result_json(500.0),
        )
        self.assertEqual(
            ["bonus_amount", "bonus_amount"],
            [issue.field_name for issue in self._issues({RESULT_KEY: content})],
        )

    def test_multiple_violations_across_fields_all_reported(self) -> None:
        content = _sc_content(
            pay_rate_frequency=build_inferred_field_result_json("hourly"),
            employment_status=build_inferred_field_result_json("unemployed"),
        )
        issues = self._issues({RESULT_KEY: content})
        self.assertEqual(
            {"pay_rate_frequency", "employer_name"},
            {issue.field_name for issue in issues},
        )

    def test_field_omitted_entirely_treated_as_null(self) -> None:
        # pay_rate_frequency isn't even a key in the JSON (as an older
        # extractor version's output would be) — treated the same as an
        # explicit null, so it satisfies applicable_when_nonnull.
        content = _sc_content()
        del content["pay_rate_frequency"]
        self.assertEqual([], self._issues({RESULT_KEY: content}))


def _concrete_constraint_types() -> set[type[LLMOutputSemanticConsistencyConstraint]]:
    """Returns every constraint type a schema field can actually hold: the leaves
    of the LLMOutputSemanticConsistencyConstraint hierarchy.
    """
    leaf_types: set[type[LLMOutputSemanticConsistencyConstraint]] = set()
    to_visit: list[type[LLMOutputSemanticConsistencyConstraint]] = [
        LLMOutputSemanticConsistencyConstraint
    ]
    while to_visit:
        constraint_type = to_visit.pop()
        if subclasses := constraint_type.__subclasses__():
            to_visit.extend(subclasses)
            continue
        leaf_types.add(constraint_type)
    return leaf_types


class ConstraintValidatorRegistryTest(TestCase):
    """The registry SemanticConsistencyCheck dispatches through covers every
    constraint type, and nothing else.
    """

    def test_every_constraint_type_has_a_validator(self) -> None:
        # Ensure every concrete constraint type has a validator registered
        # in _VALIDATORS_BY_CONSTRAINT_TYPE.
        self.assertEqual(
            _concrete_constraint_types(),
            set(_VALIDATORS_BY_CONSTRAINT_TYPE),
            "Every constraint type a field can hold needs an entry in "
            "_VALIDATORS_BY_CONSTRAINT_TYPE, and every entry must still name a "
            "live constraint type.",
        )
