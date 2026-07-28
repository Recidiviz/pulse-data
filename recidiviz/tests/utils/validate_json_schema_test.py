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
"""Tests for iter_leaf_validation_errors.

Exercises the generic union-branch resolution against handcrafted schemas: a
non-union error is its own leaf, a `const` discriminator picks the declared
branch, property overlap breaks required-vs-required ambiguity, exact ties
resolve to the earliest branch, and nested unions recurse.
"""
from typing import Any
from unittest import TestCase

from recidiviz.utils.validate_json_schema import iter_leaf_validation_errors

# A value/null-style union: the value branch requires `value` and non-empty
# `citations`; the null branch requires `null_reason` and allows empty
# `citations`. Both declare `confidence_level`.
_FIELD_UNION_SCHEMA: dict[str, Any] = {
    "anyOf": [
        {
            "type": "object",
            "properties": {
                "value": {"type": "string"},
                "confidence_level": {"type": "string", "enum": ["high", "low"]},
                "citations": {
                    "type": "array",
                    "minItems": 1,
                    "items": {"type": "object"},
                },
            },
            "required": ["value", "confidence_level", "citations"],
        },
        {
            "type": "object",
            "properties": {
                "null_reason": {"type": "string", "enum": ["not_mentioned"]},
                "confidence_level": {"type": "string", "enum": ["high", "low"]},
                "citations": {"type": "array", "items": {"type": "object"}},
            },
            "required": ["null_reason", "confidence_level", "citations"],
        },
    ]
}

# An object with a single field constrained by the value/null union above.
_FIELD_UNION_OBJECT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"status": _FIELD_UNION_SCHEMA},
    "required": ["status"],
}

# An is_relevant-style discriminated union: each branch pins a boolean
# `relevant` to a different value via `const`; the True branch nests the
# value/null union above under `status`.
_DISCRIMINATED_UNION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "result": {
            "anyOf": [
                {
                    "type": "object",
                    "properties": {"relevant": {"type": "boolean", "const": False}},
                    "required": ["relevant"],
                },
                {
                    "type": "object",
                    "properties": {
                        "relevant": {"type": "boolean", "const": True},
                        "status": _FIELD_UNION_SCHEMA,
                    },
                    "required": ["relevant", "status"],
                },
            ]
        }
    },
    "required": ["result"],
}


def _conforming_discriminated_instance() -> dict[str, Any]:
    """Returns an instance that conforms to _DISCRIMINATED_UNION_SCHEMA."""
    return {
        "result": {
            "relevant": True,
            "status": {
                "value": "active",
                "confidence_level": "high",
                "citations": [{}],
            },
        }
    }


class IterLeafValidationErrorsTest(TestCase):
    """Tests the generic flattening of union validation errors to leaf errors."""

    def _leaf_errors(
        self, json_dict: dict[str, Any], json_schema: dict[str, Any]
    ) -> list[tuple[str, str]]:
        """Validates |json_dict| against |json_schema| and returns each
        flattened leaf error as a (json_path, message) pair.
        """
        return [
            (error.json_path, error.message)
            for error in iter_leaf_validation_errors(json_dict, json_schema)
        ]

    def test_conforming_instance_yields_no_errors(self) -> None:
        self.assertEqual(
            [],
            self._leaf_errors(
                _conforming_discriminated_instance(), _DISCRIMINATED_UNION_SCHEMA
            ),
        )

    def test_non_union_error_is_its_own_leaf(self) -> None:
        schema = {"type": "object", "required": ["result"]}
        self.assertEqual(
            [("$", "'result' is a required property")], self._leaf_errors({}, schema)
        )

    def test_const_mismatch_disqualifies_other_discriminated_branch(self) -> None:
        # relevant=True with a missing required key: the False branch fails only
        # on its `const` discriminator, so the True branch's failure is reported
        # even though the False branch has fewer errors.
        instance = {"result": {"relevant": True}}
        self.assertEqual(
            [("$.result", "'status' is a required property")],
            self._leaf_errors(instance, _DISCRIMINATED_UNION_SCHEMA),
        )

    def test_nested_unions_recurse_to_the_deep_leaf(self) -> None:
        # A bad enum value two unions deep surfaces at its exact path.
        instance = _conforming_discriminated_instance()
        instance["result"]["status"]["confidence_level"] = "extreme"
        self.assertEqual(
            [
                (
                    "$.result.status.confidence_level",
                    "'extreme' is not one of ['high', 'low']",
                )
            ],
            self._leaf_errors(instance, _DISCRIMINATED_UNION_SCHEMA),
        )

    def test_property_overlap_breaks_required_vs_required_tie(self) -> None:
        # Missing `citations` fails both branches on a `required` keyword, but
        # the instance carries a `value` key that only the value branch
        # declares, so the value branch's failure is the one reported.
        instance = {"status": {"value": "active", "confidence_level": "high"}}
        self.assertEqual(
            [("$.status", "'citations' is a required property")],
            self._leaf_errors(instance, _FIELD_UNION_OBJECT_SCHEMA),
        )

    def test_property_overlap_picks_null_branch(self) -> None:
        # The same tie resolves the other way for an instance carrying
        # `null_reason`: empty `citations` are allowed on the null branch, so
        # only the bad enum value is reported.
        instance = {
            "status": {
                "null_reason": "no_such_reason",
                "confidence_level": "high",
                "citations": [],
            }
        }
        self.assertEqual(
            [
                (
                    "$.status.null_reason",
                    "'no_such_reason' is not one of ['not_mentioned']",
                )
            ],
            self._leaf_errors(instance, _FIELD_UNION_OBJECT_SCHEMA),
        )

    def test_exact_tie_resolves_to_earliest_branch(self) -> None:
        # Neither `value` nor `null_reason` present: both branches fail on one
        # `required` keyword with equal property overlap, so the first (value)
        # branch is reported.
        instance = {"status": {"confidence_level": "high", "citations": [{}]}}
        self.assertEqual(
            [("$.status", "'value' is a required property")],
            self._leaf_errors(instance, _FIELD_UNION_OBJECT_SCHEMA),
        )

    def test_non_object_instance_at_union_reports_earliest_branch(self) -> None:
        # Property overlap is meaningless for a non-object; the first branch's
        # failures are reported with the union's own path.
        self.assertEqual(
            [("$.status", "123 is not of type 'object'")],
            self._leaf_errors({"status": 123}, _FIELD_UNION_OBJECT_SCHEMA),
        )

    def test_multiple_failures_within_intended_branch_all_reported(self) -> None:
        instance = {
            "status": {
                "value": "active",
                "confidence_level": "extreme",
                "citations": [],
            }
        }
        self.assertEqual(
            [
                (
                    "$.status.confidence_level",
                    "'extreme' is not one of ['high', 'low']",
                ),
                ("$.status.citations", "[] should be non-empty"),
            ],
            self._leaf_errors(instance, _FIELD_UNION_OBJECT_SCHEMA),
        )

    def test_multiple_failures_two_unions_deep_all_reported(self) -> None:
        # Two bad values in the value/null union nested under the True branch of
        # the discriminated union both surface at their exact paths.
        instance = _conforming_discriminated_instance()
        instance["result"]["status"]["confidence_level"] = "extreme"
        instance["result"]["status"]["citations"] = []
        self.assertEqual(
            [
                (
                    "$.result.status.confidence_level",
                    "'extreme' is not one of ['high', 'low']",
                ),
                ("$.result.status.citations", "[] should be non-empty"),
            ],
            self._leaf_errors(instance, _DISCRIMINATED_UNION_SCHEMA),
        )
