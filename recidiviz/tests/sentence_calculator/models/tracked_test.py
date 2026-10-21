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
"""Tests for TrackedValue and tracked_sum."""

import unittest
from datetime import date

import attr

from recidiviz.sentence_calculator.models.tracked import TrackedValue, tracked_sum


class TestTrackedValueValidation(unittest.TestCase):
    """Tests for the attrs validators on TrackedValue."""

    def test_accepts_int_float_and_date_values(self) -> None:
        self.assertEqual(365, TrackedValue.input(365, "S1.term_days").value)
        self.assertEqual(0.65, TrackedValue.input(0.65, "S1.term_multiplier").value)
        self.assertEqual(
            date(2025, 1, 1),
            TrackedValue.input(date(2025, 1, 1), "S1.start_date").value,
        )

    def test_rejects_string_value(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Field \[value\] on \[TrackedValue\] must be an int, float, or date\. "
            r"Found value \['A'\] of type \[<class 'str'>\]\.$",
        ):
            TrackedValue.input("A", "S1.offense_category")

    def test_rejects_bool_value(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Field \[value\] on \[TrackedValue\] must be an int, float, or date\. "
            r"Found value \[True\] of type \[<class 'bool'>\]\.$",
        ):
            TrackedValue.input(True, "has_life_sentence")

    def test_rejects_empty_label(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^String value should not be empty\.$"
        ):
            TrackedValue.input(365, "")

    def test_rejects_non_tracked_value_source(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Found item in tuple type field \[sources\] on class \[.*TrackedValue'>\] "
            r"which is not the expected type \[.*TrackedValue'>\]: <class 'int'>$",
        ):
            TrackedValue(
                value=365,
                label="S1.adjusted_max_days",
                sources=(365,),  # type: ignore[arg-type]
            )

    def test_is_frozen(self) -> None:
        tracked = TrackedValue.input(365, "S1.term_days")

        with self.assertRaises(attr.exceptions.FrozenInstanceError):
            tracked.value = 400  # type: ignore[misc]


class TestTrackedValueDerivation(unittest.TestCase):
    """Tests for TrackedValue.input and TrackedValue.derive."""

    def test_input_is_leaf(self) -> None:
        self.assertEqual(
            TrackedValue(
                value=120,
                label="S1.jail_credit",
                sources=tuple(),
                rule_name=None,
                formula=None,
            ),
            TrackedValue.input(120, "S1.jail_credit"),
        )

    def test_derive_from_single_source(self) -> None:
        original = TrackedValue.input(1000, "S1.original_max_days")

        adjusted = original.derive(
            new_value=650,
            label="S1.adjusted_max_days",
            formula="int(1000 * 0.65)",
            rule_name="standard_term_reduction",
        )

        self.assertEqual(
            TrackedValue(
                value=650,
                label="S1.adjusted_max_days",
                sources=(original,),
                rule_name="standard_term_reduction",
                formula="int(1000 * 0.65)",
            ),
            adjusted,
        )

    def test_derive_lists_self_before_other_sources(self) -> None:
        original = TrackedValue.input(1000, "S1.original_max_days")
        multiplier = TrackedValue.input(0.65, "S1.term_multiplier")
        jail_credit = TrackedValue.input(50, "S1.jail_credit")

        adjusted = original.derive(
            new_value=600,
            label="S1.adjusted_max_days",
            formula="int(1000 * 0.65) - 50",
            other_sources=[multiplier, jail_credit],
        )

        self.assertEqual(
            TrackedValue(
                value=600,
                label="S1.adjusted_max_days",
                sources=(original, multiplier, jail_credit),
                rule_name=None,
                formula="int(1000 * 0.65) - 50",
            ),
            adjusted,
        )


class TestToDerivationTree(unittest.TestCase):
    """Tests for TrackedValue.to_derivation_tree."""

    def test_leaf_omits_formula_rule_and_sources(self) -> None:
        self.assertEqual(
            {"label": "S1.jail_credit", "value": 120},
            TrackedValue.input(120, "S1.jail_credit").to_derivation_tree(),
        )

    def test_date_value_serializes_as_iso_string(self) -> None:
        self.assertEqual(
            {"label": "S1.start_date", "value": "2025-01-01"},
            TrackedValue.input(date(2025, 1, 1), "S1.start_date").to_derivation_tree(),
        )

    def test_nested_derivation(self) -> None:
        original = TrackedValue.input(1000, "S1.original_max_days")
        reduced = original.derive(
            new_value=650,
            label="S1.reduced_max_days",
            formula="int(1000 * 0.65)",
            rule_name="standard_term_reduction",
        )
        jail_credit = TrackedValue.input(50, "S1.jail_credit")
        adjusted = reduced.derive(
            new_value=600,
            label="S1.adjusted_max_days",
            formula="650 - 50",
            other_sources=[jail_credit],
        )

        self.assertEqual(
            {
                "label": "S1.adjusted_max_days",
                "value": 600,
                "formula": "650 - 50",
                "derived_from": [
                    {
                        "label": "S1.reduced_max_days",
                        "value": 650,
                        "formula": "int(1000 * 0.65)",
                        "rule": "standard_term_reduction",
                        "derived_from": [
                            {"label": "S1.original_max_days", "value": 1000},
                        ],
                    },
                    {"label": "S1.jail_credit", "value": 50},
                ],
            },
            adjusted.to_derivation_tree(),
        )


class TestTrackedSum(unittest.TestCase):
    """Tests for tracked_sum."""

    def test_sum_of_several_values(self) -> None:
        s1_days = TrackedValue.input(365, "S1.total_days")
        s2_days = TrackedValue.input(730, "S2.total_days")
        s3_days = TrackedValue.input(30, "S3.total_days")

        self.assertEqual(
            TrackedValue(
                value=1125,
                label="total_days",
                sources=(s1_days, s2_days, s3_days),
                rule_name=None,
                formula="S1.total_days + S2.total_days + S3.total_days",
            ),
            tracked_sum([s1_days, s2_days, s3_days], "total_days"),
        )

    def test_sum_of_one_value(self) -> None:
        s1_days = TrackedValue.input(365, "S1.total_days")

        self.assertEqual(
            TrackedValue(
                value=365,
                label="total_days",
                sources=(s1_days,),
                rule_name=None,
                formula="S1.total_days",
            ),
            tracked_sum([s1_days], "total_days"),
        )

    def test_sum_of_no_values_is_zero_leaf(self) -> None:
        self.assertEqual(
            TrackedValue.input(0, "total_days"),
            tracked_sum([], "total_days"),
        )


if __name__ == "__main__":
    unittest.main()
